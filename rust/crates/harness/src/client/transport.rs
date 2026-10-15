//! Async transport facade over the shared durable client reducer.

use super::{ClientSession, ClientStateStore};
use crate::{
    Error, Result, wire,
    wire_api::{HarnessWireApi, current_protocol, negotiate, validate_operation_status},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, ProviderPlatform};
use futures::StreamExt;
use std::sync::Arc;

/// Native service discovery or an injected in-process/browser endpoint.
pub trait ServiceConnector: ProviderPlatform {
    /// Connects to the user's service; only `NotFound` requests autospawn.
    fn connect(&self) -> BoxProviderFuture<'_, Result<Arc<dyn HarnessWireApi>>>;
    /// Starts the per-user service and waits for readiness. A concurrent spawn is success.
    fn spawn(&self) -> BoxProviderFuture<'_, Result<()>>;
}

/// Connects to the per-user service, starting it only when its endpoint is absent.
pub async fn connect_or_spawn<S: ClientStateStore>(
    connector: &impl ServiceConnector,
    session: ClientSession<S>,
) -> Result<HarnessClient<S>> {
    let api = match connector.connect().await {
        Ok(api) => api,
        Err(Error::NotFound(_)) => {
            connector.spawn().await?;
            connector.connect().await?
        }
        Err(error) => return Err(error),
    };
    HarnessClient::connect(api, session).await
}

/// Thin transport runner; Rust owns retry identities, replay and projection recovery.
pub struct HarnessClient<S> {
    api: Arc<dyn HarnessWireApi>,
    session: ClientSession<S>,
    deliveries: Option<BoxProviderStream<'static, Result<wire::Delivery>>>,
}

impl<S: ClientStateStore> HarnessClient<S> {
    /// Negotiates before sending commands. Opening a client starts no task.
    pub async fn connect(api: Arc<dyn HarnessWireApi>, session: ClientSession<S>) -> Result<Self> {
        let request = wire::HandshakeRequest {
            protocol: Some(current_protocol()),
            required: None,
        };
        let response = api.handshake(request.clone()).await?;
        let expected = negotiate(
            &request,
            response
                .supported
                .as_ref()
                .ok_or_else(|| Error::Conflict("handshake capabilities missing".into()))?,
        )?;
        if response != expected {
            return Err(Error::Conflict("handshake identity mismatch".into()));
        }
        Ok(Self {
            api,
            session,
            deliveries: None,
        })
    }

    /// Queues intent durably before sending it, preserving it after a lost response.
    pub async fn submit(&mut self, command: wire::CommandEnvelope) -> Result<wire::Admission> {
        self.session.queue(command.clone())?;
        let admission = self.api.submit(command).await?;
        self.session.receive(wire::ServerFrame {
            frame: Some(wire::server_frame::Frame::Admission(admission.clone())),
        })?;
        Ok(admission)
    }

    /// Resubmits pending envelopes verbatim after a reconnect.
    pub async fn flush_outbox(&mut self) -> Result<Vec<wire::Admission>> {
        let pending = self.session.pending().cloned().collect::<Vec<_>>();
        let mut admissions = Vec::with_capacity(pending.len());
        for command in pending {
            admissions.push(self.submit(command).await?);
        }
        Ok(admissions)
    }

    /// Opens a replay/follow stream at the durable cut. No sleeps or polling.
    pub async fn replay(&mut self) -> Result<()> {
        self.deliveries = Some(self.api.replay(self.session.resume()).await?);
        Ok(())
    }

    /// Validates and durably publishes the next complete delivery.
    pub async fn next_delivery(&mut self) -> Result<Option<wire::Delivery>> {
        let stream = self
            .deliveries
            .as_mut()
            .ok_or_else(|| Error::Invalid("open replay before reading deliveries".into()))?;
        let Some(delivery) = stream.next().await else {
            return Ok(None);
        };
        let delivery = delivery?;
        self.session.receive(wire::ServerFrame {
            frame: Some(wire::server_frame::Frame::Delivery(delivery.clone())),
        })?;
        Ok(Some(delivery))
    }

    /// Observes an identity-bound operation projection.
    pub async fn observe(&self, request: wire::ObserveRequest) -> Result<wire::OperationStatus> {
        crate::wire_api::validate_observe_request(&request)?;
        let status = self.api.observe(request.clone()).await?;
        validate_operation_status(&request.operation_id, request.owner.as_ref(), &status)?;
        Ok(status)
    }

    /// Cancels with the wire protocol's durable cancellation identity.
    pub async fn cancel(&self, request: wire::CancelRequest) -> Result<wire::CancelResponse> {
        crate::wire_api::validate_cancel_request(&request)?;
        let response = self.api.cancel(request.clone()).await?;
        crate::wire_api::validate_cancel_response(&request, &response)?;
        Ok(response)
    }

    /// Shared renderer state and outbox.
    pub fn session(&self) -> &ClientSession<S> {
        &self.session
    }

    /// Consumes the transport while retaining durable state for reconnect.
    pub fn into_session(self) -> ClientSession<S> {
        self.session
    }
}
