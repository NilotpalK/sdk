use super::headless::target;
use super::*;
use acyclic_harness::client::{FileClientState, HeadlessOutcome, run_headless, run_jsonl};
use std::fs::OpenOptions;

struct CheckpointStore {
    inner: MemoryClientState,
    fail: Arc<AtomicBool>,
    after_replace: bool,
}
impl ClientStateStore for CheckpointStore {
    fn load(&mut self) -> Result<Vec<SessionRecord>> {
        self.inner.load()
    }
    fn append(&mut self, record: &SessionRecord) -> Result<()> {
        self.inner.append(record)
    }
    fn replace(&mut self, record: &SessionRecord) -> Result<()> {
        if !self.fail.load(Ordering::Relaxed) {
            return self.inner.replace(record);
        }
        if self.after_replace {
            self.inner.replace(record)?;
        }
        Err(Error::Storage("checkpoint failed after append".into()))
    }
}

fn next_command() -> wire::CommandEnvelope {
    let mut next = command();
    next.operation = Some(wire::OperationIdentity {
        operation_id: "00000000-0000-0000-0000-000000000002".into(),
        idempotency_key: "submit-2".into(),
    });
    next
}

#[test]
fn checkpoint_keeps_a_paused_opener_on_the_same_writer_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    let mut writer = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    writer.track(session().resume().cursors.remove(0)).unwrap();
    let lock_path = directory.path().join("client.journal.lock");
    // An opener has opened the stable lock inode but has not yet tried locking it.
    let paused = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    for revision in 0..2 {
        writer
            .receive(frame(delivery(revision, LifecycleState::Active)))
            .unwrap();
        writer.checkpoint().unwrap();
        assert!(paused.try_lock().is_err());
        assert!(FileClientState::open(&path).is_err());
    }
    drop(writer);
    paused.try_lock().unwrap();
    assert!(FileClientState::open(&path).is_err());
    paused.unlock().unwrap();
    drop(paused);
    let mut writer = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    assert_eq!(writer.resume().cursors[0].revision, 2);
    writer.queue(command()).unwrap();
    drop(writer);
    let recovered = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    assert_eq!(recovered.resume().cursors[0].revision, 2);
    assert_eq!(
        recovered.pending().cloned().collect::<Vec<_>>(),
        [command()]
    );
}

#[tokio::test]
async fn checkpoint_failure_keeps_the_committed_delivery_before_replay() {
    for after_replace in [false, true] {
        let mut base = session();
        for index in 0..62 {
            let mut pending = command();
            pending.operation = Some(wire::OperationIdentity {
                operation_id: format!("00000000-0000-0000-0001-{index:012}"),
                idempotency_key: format!("pending-{index}"),
            });
            base.queue(pending).unwrap();
        }
        let fail = Arc::new(AtomicBool::new(true));
        let state = ClientSession::open(CheckpointStore {
            inner: base.into_store(),
            fail: fail.clone(),
            after_replace,
        })
        .unwrap();
        let host = Arc::new(Host::default());
        let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
        client.replay().await.unwrap();
        assert!(client.next_delivery().await.is_err());
        assert_eq!(client.session().resume().cursors[0].revision, 1);
        assert!(host.acknowledgements.lock().unwrap().is_empty());
        fail.store(false, Ordering::Relaxed);
        client.replay().await.unwrap();
        assert_eq!(
            client.next_delivery().await.unwrap(),
            Some(delivery(0, LifecycleState::Completed))
        );
        assert_eq!(
            client.next_delivery().await.unwrap(),
            Some(delivery(1, LifecycleState::Completed))
        );
        assert!(client.next_delivery().await.unwrap().is_none());
        let recovered = ClientSession::open(client.into_session().into_store()).unwrap();
        assert_eq!(recovered.resume().cursors[0].revision, 2);
        assert_eq!(recovered.pending().count(), 62);
        assert_eq!(host.acknowledgements.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn new_submissions_drain_earlier_pending_commands_in_order() {
    let host = Arc::new(Host::default());
    host.lost_reply.store(true, Ordering::Relaxed);
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    assert!(client.submit(command()).await.is_err());
    let next = next_command();
    assert_eq!(
        client.submit(next.clone()).await.unwrap(),
        admission(&next, wire::AdmissionState::Accepted)
    );
    assert_eq!(
        host.commands.lock().unwrap().as_slice(),
        &[command(), command(), next]
    );
    assert_eq!(client.session().pending().count(), 0);
}

#[tokio::test]
async fn indeterminate_admission_blocks_submission_and_outbox_drain() {
    let host = Arc::new(Host::default());
    host.lost_reply.store(true, Ordering::Relaxed);
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    assert!(client.submit(command()).await.is_err());
    *host.admission_state.lock().unwrap() = Some(wire::AdmissionState::Indeterminate);
    let next = next_command();
    assert!(
        matches!(client.submit(next.clone()).await, Err(Error::Indeterminate(id))
        if id.to_string() == command().operation.unwrap().operation_id)
    );
    assert_eq!(
        host.commands.lock().unwrap().as_slice(),
        &[command(), command()]
    );
    let mut admissions = Vec::new();
    client
        .flush_outbox(|admission| {
            admissions.push(admission);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        admissions,
        [admission(&command(), wire::AdmissionState::Indeterminate)]
    );
    assert_eq!(
        client.session().pending().cloned().collect::<Vec<_>>(),
        [command(), next.clone()]
    );
    *host.admission_state.lock().unwrap() = Some(wire::AdmissionState::Accepted);
    client.flush_outbox(|_| Ok(())).await.unwrap();
    assert_eq!(
        host.commands.lock().unwrap().as_slice(),
        &[command(), command(), command(), command(), next]
    );
    assert_eq!(client.session().pending().count(), 0);
}

#[tokio::test]
async fn headless_reports_an_indeterminate_predecessor_without_sending_the_target() {
    for jsonl in [false, true] {
        let host = Arc::new(Host::default());
        *host.admission_state.lock().unwrap() = Some(wire::AdmissionState::Indeterminate);
        let mut state = session();
        state.queue(command()).unwrap();
        let next = next_command();
        if jsonl {
            state.queue(next.clone()).unwrap();
        }
        let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
        let mut target = target();
        target.operation_id = next.operation.as_ref().unwrap().operation_id.clone();
        let mut output = Vec::new();
        let outcome = if jsonl {
            run_jsonl(&mut client, target, futures::stream::empty(), &mut output).await
        } else {
            run_headless(&mut client, next, target, &mut output).await
        };
        assert_eq!(outcome.unwrap(), HeadlessOutcome::Indeterminate);
        assert_eq!(host.commands.lock().unwrap().as_slice(), &[command()]);
        assert_eq!(client.session().pending().count(), 2);
        let admissions = String::from_utf8(output)
            .unwrap()
            .lines()
            .filter_map(
                |line| match jsonl::parse_server_frame(line).unwrap().frame {
                    Some(wire::server_frame::Frame::Admission(admission)) => Some(admission),
                    _ => None,
                },
            )
            .collect::<Vec<_>>();
        assert_eq!(
            admissions,
            [admission(&command(), wire::AdmissionState::Indeterminate)]
        );
    }
}

#[tokio::test]
async fn resubmitting_an_earlier_intent_leaves_later_intents_queued() {
    let host = Arc::new(Host::default());
    let mut state = session();
    state.queue(command()).unwrap();
    let next = next_command();
    state.queue(next.clone()).unwrap();
    let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
    assert_eq!(
        client.submit(command()).await.unwrap(),
        admission(&command(), wire::AdmissionState::Accepted)
    );
    assert_eq!(host.commands.lock().unwrap().as_slice(), &[command()]);
    assert_eq!(
        client.session().pending().cloned().collect::<Vec<_>>(),
        [next.clone()]
    );
    assert_eq!(
        client.submit(next.clone()).await.unwrap(),
        admission(&next, wire::AdmissionState::Accepted)
    );
    assert_eq!(host.commands.lock().unwrap().as_slice(), &[command(), next]);
    assert_eq!(client.session().pending().count(), 0);
}
