use super::*;
use acyclic_harness::client::{FileClientState, HeadlessOutcome, run_headless, run_jsonl};
use futures::FutureExt;
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
};

fn target() -> wire::ObserveRequest {
    wire::ObserveRequest {
        operation_id: command().operation.unwrap().operation_id,
        protocol: Some(current_protocol()),
        owner: Some(owner()),
        scope: Some(wire::Scope {
            id: "observe".into(),
            capabilities: vec!["operation:observe".into(), "operation:cancel".into()],
            proof: vec![0; 32],
            ..Default::default()
        }),
    }
}

fn trace(host: &Host, states: &[LifecycleState], completion: wire::CompletionState) {
    *host.state.lock().unwrap() = Some(completion);
    let frames = states
        .iter()
        .enumerate()
        .map(|(index, state)| Ok(delivery(index as u64, *state)))
        .collect::<Vec<_>>();
    *host.trace.lock().unwrap() = Some(Box::pin(futures::stream::iter(frames)));
}

fn frames(output: Vec<u8>) -> Vec<wire::ServerFrame> {
    String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| jsonl::parse_server_frame(line).unwrap())
        .collect()
}

#[tokio::test]
async fn every_settled_outcome_has_its_documented_exit_code() {
    let cases = [
        (
            LifecycleState::Completed,
            wire::CompletionState::Succeeded,
            HeadlessOutcome::Completed,
            0,
        ),
        (
            LifecycleState::Failed,
            wire::CompletionState::Failed,
            HeadlessOutcome::Failed,
            1,
        ),
        (
            LifecycleState::Cancelled,
            wire::CompletionState::Cancelled,
            HeadlessOutcome::Cancelled,
            2,
        ),
        (
            LifecycleState::Waiting,
            wire::CompletionState::Running,
            HeadlessOutcome::Blocked,
            3,
        ),
        (
            LifecycleState::Failed,
            wire::CompletionState::Indeterminate,
            HeadlessOutcome::Indeterminate,
            4,
        ),
    ];
    for (state, completion, expected, exit_code) in cases {
        let host = Arc::new(Host::default());
        trace(&host, &[state], completion);
        let mut client = HarnessClient::connect(host.clone(), session())
            .await
            .unwrap();
        let mut output = Vec::new();
        let outcome = run_headless(&mut client, command(), target(), &mut output)
            .await
            .unwrap();
        assert_eq!(outcome, expected);
        assert_eq!(outcome.exit_code(), exit_code);
        let frames = frames(output);
        assert_eq!(frames.len(), 4);
        assert!(matches!(
            frames[0].frame,
            Some(wire::server_frame::Frame::Handshake(_))
        ));
        assert!(matches!(
            frames[1].frame,
            Some(wire::server_frame::Frame::Admission(_))
        ));
        assert!(matches!(
            frames[2].frame,
            Some(wire::server_frame::Frame::Delivery(_))
        ));
        assert!(matches!(
            frames[3].frame,
            Some(wire::server_frame::Frame::Status(_))
        ));
        assert_eq!(
            host.acknowledgements.lock().unwrap()[0],
            wire::ClientFrame {
                frame: Some(wire::client_frame::Frame::Acknowledge(wire::Acknowledge {
                    authority: Some(owner()),
                    generation: "generation".into(),
                    through_revision: 1,
                })),
            }
        );
    }
}

#[tokio::test]
async fn completed_status_does_not_hide_a_racing_final_delivery() {
    let host = Arc::new(Host::default());
    trace(
        &host,
        &[LifecycleState::Active, LifecycleState::Completed],
        wire::CompletionState::Succeeded,
    );
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    let mut output = Vec::new();
    assert_eq!(
        run_headless(&mut client, command(), target(), &mut output)
            .await
            .unwrap(),
        HeadlessOutcome::Completed
    );
    let deliveries = frames(output)
        .into_iter()
        .filter_map(|frame| match frame.frame {
            Some(wire::server_frame::Frame::Delivery(delivery)) => Some(delivery),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[1].through_revision, 2);
}

#[tokio::test]
async fn earlier_admission_is_streamed_before_a_later_outbox_failure() {
    let host = Arc::new(Host::default());
    host.fail_submit.store(2, Ordering::Relaxed);
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    client.queue(command()).unwrap();
    let mut second = command();
    second.operation = Some(wire::OperationIdentity {
        operation_id: "00000000-0000-0000-0000-000000000002".into(),
        idempotency_key: "submit-2".into(),
    });
    let mut target = target();
    target.operation_id = second.operation.as_ref().unwrap().operation_id.clone();
    let mut output = Vec::new();
    assert!(
        run_headless(&mut client, second, target, &mut output)
            .await
            .is_err()
    );
    let frames = frames(output);
    assert_eq!(frames.len(), 2);
    assert!(matches!(
        frames[1].frame,
        Some(wire::server_frame::Frame::Admission(_))
    ));
    assert_eq!(client.session().pending().count(), 1);
}

#[tokio::test]
async fn commit_failure_sends_no_ack_and_ack_failure_keeps_the_committed_frame() {
    let host = Arc::new(Host::default());
    let fail = Arc::new(AtomicBool::new(false));
    let state = ClientSession::open(FailingStore {
        inner: session().into_store(),
        fail: fail.clone(),
    })
    .unwrap();
    let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
    client.replay().await.unwrap();
    fail.store(true, Ordering::Relaxed);
    assert!(client.next_delivery().await.is_err());
    assert!(host.acknowledgements.lock().unwrap().is_empty());
    assert_eq!(client.session().resume().cursors[0].revision, 0);
    fail.store(false, Ordering::Relaxed);
    client.replay().await.unwrap();
    host.lost_ack.store(true, Ordering::Relaxed);
    assert!(client.next_delivery().await.is_err());
    assert_eq!(client.session().resume().cursors[0].revision, 1);
    let saved = client.next_delivery().await.unwrap().unwrap();
    assert_eq!(saved.through_revision, 1);
    assert_eq!(host.acknowledgements.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn cancelling_an_ack_wait_does_not_drop_the_committed_delivery() {
    let host = Arc::new(Host::default());
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    client.replay().await.unwrap();
    host.hold_ack.store(true, Ordering::Relaxed);
    assert!(client.next_delivery().now_or_never().is_none());
    assert_eq!(client.session().resume().cursors[0].revision, 1);
    host.hold_ack.store(false, Ordering::Relaxed);
    assert_eq!(
        client
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
            .through_revision,
        1
    );
    assert_eq!(client.into_session().into_store().load().unwrap().len(), 2);
}

#[tokio::test]
async fn jsonl_stdin_can_cancel_while_replay_is_waiting() {
    let host = Arc::new(Host::default());
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    *host.trace.lock().unwrap() = Some(Box::pin(receiver));
    *host.live_sender.lock().unwrap() = Some(sender);
    *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let cancel = wire::ClientFrame {
        frame: Some(wire::client_frame::Frame::Cancel(wire::CancelRequest {
            operation_id: target().operation_id,
            protocol: Some(current_protocol()),
            owner: Some(owner()),
            scope: target().scope,
            recursive: true,
            idempotency_key: "cancel-1".into(),
        })),
    };
    let input = futures::stream::iter([Ok(jsonl::client_frame(&cancel).unwrap())]);
    let mut output = Vec::new();
    assert_eq!(
        run_jsonl(&mut client, target(), input, &mut output)
            .await
            .unwrap(),
        HeadlessOutcome::Cancelled
    );
    assert!(frames(output).iter().any(|frame| matches!(
        frame.frame,
        Some(wire::server_frame::Frame::Cancellation(_))
    )));
}

#[tokio::test]
async fn jsonl_stdin_submits_wire_commands_and_eof_does_not_cancel() {
    let host = Arc::new(Host::default());
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    *host.trace.lock().unwrap() = Some(Box::pin(receiver));
    *host.live_sender.lock().unwrap() = Some(sender);
    *host.submit_delivery.lock().unwrap() = Some(delivery(0, LifecycleState::Completed));
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let input = futures::stream::iter([Ok(jsonl::client_frame(&wire::ClientFrame {
        frame: Some(wire::client_frame::Frame::Command(command())),
    })
    .unwrap())]);
    let mut output = Vec::new();
    assert_eq!(
        run_jsonl(&mut client, target(), input, &mut output)
            .await
            .unwrap(),
        HeadlessOutcome::Completed
    );
    assert_eq!(host.commands.lock().unwrap().as_slice(), &[command()]);
    assert!(
        frames(output)
            .iter()
            .any(|frame| matches!(frame.frame, Some(wire::server_frame::Frame::Admission(_))))
    );
}

#[tokio::test]
async fn malformed_stdin_is_rejected_without_a_submit() {
    let host = Arc::new(Host::default());
    *host.trace.lock().unwrap() = Some(Box::pin(futures::stream::pending()));
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let input = futures::stream::iter([Ok("{\"unknown\":{}}".into())]);
    assert!(
        run_jsonl(&mut client, target(), input, &mut Vec::new())
            .await
            .is_err()
    );
    assert!(host.commands.lock().unwrap().is_empty());
}

#[test]
fn checkpoint_bounds_recovery_after_ten_thousand_deliveries() {
    let mut session = session();
    session.queue(command()).unwrap();
    for revision in 0..10_000 {
        session
            .receive(frame(delivery(revision, LifecycleState::Active)))
            .unwrap();
    }
    let views = session.views().clone();
    let mut store = session.into_store();
    let records = store.load().unwrap();
    assert!(records.len() <= 64, "{} recovery records", records.len());
    assert!(records[0].checkpoint.is_some());
    let recovered = ClientSession::open(store).unwrap();
    assert_eq!(recovered.views(), &views);
    assert_eq!(recovered.resume().cursors[0].revision, 10_000);
    assert_eq!(
        recovered.pending().cloned().collect::<Vec<_>>(),
        vec![command()]
    );
}

#[test]
fn native_journal_recovers_a_torn_tail_and_checkpoint_replaces_the_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    let mut native = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    native.track(session().resume().cursors.remove(0)).unwrap();
    native.queue(command()).unwrap();
    for revision in 0..128 {
        native
            .receive(frame(delivery(revision, LifecycleState::Active)))
            .unwrap();
    }
    assert!(FileClientState::open(&path).is_err());
    native.checkpoint().unwrap();
    let expected = native.views().clone();
    drop(native);
    let length = std::fs::metadata(&path).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[17, 0, 0])
        .unwrap();
    let mut store = FileClientState::open(&path).unwrap();
    assert_eq!(store.load().unwrap().len(), 1);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), length);
    let recovered = ClientSession::open(store).unwrap();
    assert_eq!(recovered.views(), &expected);
    assert_eq!(recovered.resume().cursors[0].revision, 128);
    assert_eq!(recovered.pending().count(), 1);
}

#[test]
fn native_journal_rejects_corruption() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    let mut native = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    native.track(session().resume().cursors.remove(0)).unwrap();
    drop(native);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(36)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(36)).unwrap();
    file.write_all(&byte).unwrap();
    drop(file);
    assert!(ClientSession::open(FileClientState::open(&path).unwrap()).is_err());
}
