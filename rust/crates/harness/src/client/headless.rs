//! Headless policy and stdin dispatch belong to the harness, not its CLI wrappers.

use super::{ClientStateStore, HarnessClient, jsonl, views::authority_key};
use crate::{Error, Result, core::LifecycleState, wire};
use futures::{FutureExt, Stream, StreamExt};
use std::io::Write;

/// Settled task outcome with stable process exit codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeadlessOutcome {
    /// The task completed, or is idle awaiting another prompt.
    Completed,
    /// Execution failed.
    Failed,
    /// Cancellation settled.
    Cancelled,
    /// Execution needs external input or approval.
    Blocked,
    /// The service cannot establish whether an operation completed.
    Indeterminate,
}

impl HeadlessOutcome {
    /// Stable CLI contract: completed=0, failed=1, cancelled=2, blocked=3, indeterminate=4.
    #[must_use]
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Completed => 0,
            Self::Failed => 1,
            Self::Cancelled => 2,
            Self::Blocked => 3,
            Self::Indeterminate => 4,
        }
    }
}

fn emit(output: &mut impl Write, frame: wire::server_frame::Frame) -> Result<()> {
    let mut line = jsonl::server_frame(&wire::ServerFrame { frame: Some(frame) })?;
    line.push('\n');
    output
        .write_all(line.as_bytes())
        .and_then(|()| output.flush())
        .map_err(|error| Error::Storage(error.to_string()))
}

fn completion(status: &wire::OperationStatus) -> Option<HeadlessOutcome> {
    match wire::CompletionState::try_from(status.state).ok()? {
        wire::CompletionState::Succeeded => Some(HeadlessOutcome::Completed),
        wire::CompletionState::Failed => Some(HeadlessOutcome::Failed),
        wire::CompletionState::Cancelled => Some(HeadlessOutcome::Cancelled),
        wire::CompletionState::Indeterminate => Some(HeadlessOutcome::Indeterminate),
        _ => None,
    }
}

/// Submits and streams every response as service JSONL until this task settles.
pub async fn run_headless<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    command: wire::CommandEnvelope,
    target: wire::ObserveRequest,
    output: &mut impl Write,
) -> Result<HeadlessOutcome> {
    crate::wire_api::validate_observe_request(&target)?;
    if command
        .operation
        .as_ref()
        .is_none_or(|identity| identity.operation_id != target.operation_id)
        || command.authority != target.owner
    {
        return Err(Error::Invalid(
            "headless command and observe target differ".into(),
        ));
    }
    emit(
        output,
        wire::server_frame::Frame::Handshake(client.handshake().clone()),
    )?;
    client.queue(command)?;
    let mut outcome = None;
    client
        .flush_outbox(|admission| {
            let target_admission = admission
                .operation
                .as_ref()
                .is_some_and(|identity| identity.operation_id == target.operation_id);
            let state = admission.state;
            emit(output, wire::server_frame::Frame::Admission(admission))?;
            if target_admission {
                if state == wire::AdmissionState::Rejected as i32 {
                    outcome = Some(HeadlessOutcome::Failed);
                }
                if state == wire::AdmissionState::Indeterminate as i32 {
                    outcome = Some(HeadlessOutcome::Indeterminate);
                }
            }
            Ok(())
        })
        .await?;
    if let Some(outcome) = outcome {
        return Ok(outcome);
    }
    client.replay().await?;
    follow(client, &target, futures::stream::empty(), output).await
}

/// Reads wire command frames from JSONL stdin while continuing to stream replay.
/// EOF closes command input; it does not cancel a running task.
pub async fn run_jsonl<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    target: wire::ObserveRequest,
    input: impl Stream<Item = Result<String>> + Unpin,
    output: &mut impl Write,
) -> Result<HeadlessOutcome> {
    crate::wire_api::validate_observe_request(&target)?;
    emit(
        output,
        wire::server_frame::Frame::Handshake(client.handshake().clone()),
    )?;
    client
        .flush_outbox(|admission| emit(output, wire::server_frame::Frame::Admission(admission)))
        .await?;
    client.replay().await?;
    follow(client, &target, input, output).await
}

enum Incoming {
    Command(Option<Result<String>>),
    Delivery(Result<Option<wire::Delivery>>),
}

async fn follow<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    target: &wire::ObserveRequest,
    mut input: impl Stream<Item = Result<String>> + Unpin,
    output: &mut impl Write,
) -> Result<HeadlessOutcome> {
    let mut input_open = true;
    loop {
        let incoming = if input_open {
            let command = input.next().fuse();
            let delivery = client.next_delivery().fuse();
            futures::pin_mut!(command, delivery);
            futures::select! {
                line = command => Incoming::Command(line),
                frame = delivery => Incoming::Delivery(frame),
            }
        } else {
            Incoming::Delivery(client.next_delivery().await)
        };
        match incoming {
            Incoming::Command(Some(line)) => {
                dispatch(client, jsonl::parse_client_frame(&line?)?, output).await?;
            }
            Incoming::Command(None) => {
                input_open = false;
            }
            Incoming::Delivery(delivery) => {
                let delivery = delivery?;
                let ended = delivery.is_none();
                if let Some(delivery) = delivery {
                    emit(output, wire::server_frame::Frame::Delivery(delivery))?;
                }
                let status = client.observe(target.clone()).await?;
                let outcome = completion(&status);
                emit(output, wire::server_frame::Frame::Status(status))?;
                let task = target
                    .owner
                    .as_ref()
                    .and_then(|owner| client.session().views().tasks.get(&authority_key(owner)));
                // A status reply can race ahead of replay. Drain through the task's
                // own settled event before leaving a live stream.
                if (ended
                    || task.is_some_and(|task| {
                        matches!(
                            task.status,
                            LifecycleState::Completed
                                | LifecycleState::Failed
                                | LifecycleState::Cancelled
                        )
                    }))
                    && let Some(outcome) = outcome
                {
                    return Ok(outcome);
                }
                if task.is_some_and(|task| task.status == LifecycleState::Waiting) {
                    return Ok(HeadlessOutcome::Blocked);
                }
                if ended {
                    return Err(Error::Indeterminate(crate::OperationId::parse(
                        &target.operation_id,
                    )?));
                }
            }
        }
    }
}

async fn dispatch<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    frame: wire::ClientFrame,
    output: &mut impl Write,
) -> Result<()> {
    use wire::client_frame::Frame;
    let response = match frame
        .frame
        .ok_or_else(|| Error::Invalid("empty input frame".into()))?
    {
        Frame::Command(command) => {
            wire::server_frame::Frame::Admission(client.submit(command).await?)
        }
        Frame::Observe(request) => {
            wire::server_frame::Frame::Status(client.observe(request).await?)
        }
        Frame::Cancel(request) => {
            wire::server_frame::Frame::Cancellation(client.cancel(request).await?)
        }
        Frame::Handshake(request) => {
            crate::wire_api::negotiate(
                &request,
                client
                    .handshake()
                    .supported
                    .as_ref()
                    .ok_or_else(|| Error::Conflict("missing handshake capabilities".into()))?,
            )?;
            wire::server_frame::Frame::Handshake(client.handshake().clone())
        }
        Frame::Resume(request) => {
            crate::wire_api::validate_resume_protocol(&request)?;
            for cursor in request.cursors {
                let existing = client
                    .session()
                    .resume()
                    .cursors
                    .into_iter()
                    .find(|saved| saved.authority == cursor.authority);
                if let Some(saved) = existing {
                    if saved != cursor {
                        return Err(Error::Conflict(
                            "stdin resume differs from durable cursor".into(),
                        ));
                    }
                } else {
                    client.track(cursor)?;
                }
            }
            client.replay().await?;
            return Ok(());
        }
        Frame::Acknowledge(ack) => {
            if !client.session().resume().cursors.iter().any(|cursor| {
                cursor.authority == ack.authority
                    && cursor.generation == ack.generation
                    && cursor.revision >= ack.through_revision
            }) {
                return Err(Error::Conflict(
                    "stdin acknowledgement exceeds durable cursor".into(),
                ));
            }
            return Ok(());
        }
    };
    emit(output, response)
}
