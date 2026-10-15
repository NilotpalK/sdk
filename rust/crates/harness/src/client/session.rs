//! Durable outbox and replay reducer. A record is committed before publication.

use super::{
    ClientViews,
    views::{authority_key, payload},
};
use crate::{
    Error, Result, wire,
    wire_api::{current_protocol, validate_admission, validate_resume_protocol},
};
use std::collections::BTreeMap;

/// One atomically appended recovery record. Contains private command grants.
#[derive(Clone, PartialEq, prost::Message)]
pub struct SessionRecord {
    /// Initial replay cut or one queued command.
    #[prost(message, optional, tag = "1")]
    pub request: Option<wire::ClientFrame>,
    /// One validated admission or delivery.
    #[prost(message, optional, tag = "2")]
    pub response: Option<wire::ServerFrame>,
    /// Complete recovery cut replacing the older journal prefix.
    #[prost(message, optional, tag = "3")]
    pub checkpoint: Option<SessionCheckpoint>,
}

/// Version-bound local projection, cursor and pending command recovery cut.
#[derive(Clone, PartialEq, prost::Message)]
pub struct SessionCheckpoint {
    /// Protocol identity that produced this state.
    #[prost(message, optional, tag = "1")]
    pub protocol: Option<wire::ProtocolIdentity>,
    /// Durable per-authority cuts.
    #[prost(message, repeated, tag = "2")]
    pub cursors: Vec<wire::ReplayCursor>,
    /// Exact pending envelopes in submission order.
    #[prost(message, repeated, tag = "3")]
    pub pending: Vec<wire::CommandEnvelope>,
    /// The same renderer state paired with these cursors.
    #[prost(bytes = "vec", tag = "4")]
    pub views_json: Vec<u8>,
}

/// Private, single-writer durable journal. A failed append must leave no record.
pub trait ClientStateStore {
    /// Reads committed records in order. Reject corruption rather than skipping it.
    fn load(&mut self) -> Result<Vec<SessionRecord>>;
    /// Atomically commits one record before returning success.
    fn append(&mut self, record: &SessionRecord) -> Result<()>;
    /// Atomically replaces the journal with one complete checkpoint.
    fn replace(&mut self, checkpoint: &SessionRecord) -> Result<()>;
}

/// In-process store for embedded consumers and tests.
#[derive(Default)]
pub struct MemoryClientState {
    records: Vec<SessionRecord>,
}

impl ClientStateStore for MemoryClientState {
    fn load(&mut self) -> Result<Vec<SessionRecord>> {
        Ok(self.records.clone())
    }
    fn append(&mut self, record: &SessionRecord) -> Result<()> {
        self.records.push(record.clone());
        Ok(())
    }
    fn replace(&mut self, checkpoint: &SessionRecord) -> Result<()> {
        self.records.clear();
        self.records.push(checkpoint.clone());
        Ok(())
    }
}

/// One client identity's durable outbox, cursor and renderer state.
pub struct ClientSession<S> {
    store: S,
    cursors: BTreeMap<String, wire::ReplayCursor>,
    pending: Vec<wire::CommandEnvelope>,
    views: ClientViews,
    tail_records: usize,
}

impl<S: ClientStateStore> ClientSession<S> {
    /// Recovers the projection and pending commands from the same journal as its cursors.
    pub fn open(mut store: S) -> Result<Self> {
        let records = store.load()?;
        let mut session = Self {
            store,
            cursors: BTreeMap::new(),
            pending: Vec::new(),
            views: ClientViews::default(),
            tail_records: 0,
        };
        for record in records {
            if let Some(checkpoint) = record.checkpoint.as_ref() {
                if record.request.is_some() || record.response.is_some() {
                    return Err(Error::Storage("mixed checkpoint record".into()));
                }
                session.restore(checkpoint)?;
            } else {
                session.record(&record, false)?;
                session.tail_records += 1;
            }
        }
        Ok(session)
    }

    /// Adds the initial authority/generation cut once; reconnect uses the saved cut.
    pub fn track(&mut self, cursor: wire::ReplayCursor) -> Result<()> {
        self.record(
            &SessionRecord {
                request: Some(wire::ClientFrame {
                    frame: Some(wire::client_frame::Frame::Resume(wire::ResumeRequest {
                        protocol: Some(current_protocol()),
                        cursors: vec![cursor],
                    })),
                }),
                response: None,
                checkpoint: None,
            },
            true,
        )?;
        self.compact_if_needed()
    }

    /// Persists exact intent and retry identity before a transport may send it.
    pub fn queue(&mut self, command: wire::CommandEnvelope) -> Result<()> {
        self.record(
            &SessionRecord {
                request: Some(wire::ClientFrame {
                    frame: Some(wire::client_frame::Frame::Command(command)),
                }),
                response: None,
                checkpoint: None,
            },
            true,
        )?;
        self.compact_if_needed()
    }

    /// Commits an admission or gapless delivery before returning its acknowledgement.
    pub fn receive(&mut self, frame: wire::ServerFrame) -> Result<Option<wire::Acknowledge>> {
        let acknowledge = match &frame.frame {
            Some(wire::server_frame::Frame::Delivery(delivery)) => Some(wire::Acknowledge {
                authority: delivery.authority.clone(),
                generation: delivery.generation.clone(),
                through_revision: delivery.through_revision,
            }),
            _ => None,
        };
        self.record(
            &SessionRecord {
                request: None,
                response: Some(frame),
                checkpoint: None,
            },
            true,
        )?;
        self.compact_if_needed()?;
        Ok(acknowledge)
    }

    /// Exact resume request, including the compiled protocol identity.
    #[must_use]
    pub fn resume(&self) -> wire::ResumeRequest {
        wire::ResumeRequest {
            protocol: Some(current_protocol()),
            cursors: self.cursors.values().cloned().collect(),
        }
    }

    /// Exact envelopes awaiting definitive admission; never regenerate their identities.
    pub fn pending(&self) -> impl Iterator<Item = &wire::CommandEnvelope> {
        self.pending.iter()
    }

    /// State shared by every renderer.
    #[must_use]
    pub fn views(&self) -> &ClientViews {
        &self.views
    }

    /// Returns storage after transport shutdown, for reopening or explicit disposal.
    pub fn into_store(self) -> S {
        self.store
    }

    fn record(&mut self, record: &SessionRecord, persist: bool) -> Result<()> {
        match (&record.request, &record.response) {
            (Some(request), None) => match &request.frame {
                Some(wire::client_frame::Frame::Resume(request)) => {
                    validate_resume_protocol(request)?;
                    let [cursor] = request.cursors.as_slice() else {
                        return Err(Error::Invalid("track requires one cursor".into()));
                    };
                    let authority = valid_authority(cursor.authority.as_ref())?;
                    if cursor.generation.is_empty() || cursor.revision != 0 {
                        return Err(Error::Invalid(
                            "initial cursor requires a generation and revision zero".into(),
                        ));
                    }
                    let key = authority_key(authority);
                    if let Some(existing) = self.cursors.get(&key) {
                        return if existing.generation == cursor.generation {
                            Ok(())
                        } else {
                            Err(Error::Conflict("replay generation changed".into()))
                        };
                    }
                    if persist {
                        self.append_record(record)?;
                    }
                    self.cursors.insert(key, cursor.clone());
                }
                Some(wire::client_frame::Frame::Command(command)) => {
                    crate::wire_api::validate_command_protocol(command)?;
                    valid_authority(command.authority.as_ref())?;
                    let identity = command
                        .operation
                        .as_ref()
                        .ok_or_else(|| Error::Invalid("command identity missing".into()))?;
                    crate::OperationId::parse(&identity.operation_id)?;
                    crate::IdempotencyKey::new(identity.idempotency_key.clone())?;
                    if let Some(existing) = self.pending.iter().find(|pending| {
                        pending.operation.as_ref().is_some_and(|operation| {
                            operation.operation_id == identity.operation_id
                        })
                    }) {
                        return if existing == command {
                            Ok(())
                        } else {
                            Err(Error::Conflict("outbox operation identity reused".into()))
                        };
                    }
                    if self.pending.iter().any(|existing| {
                        existing.authority == command.authority
                            && existing.operation.as_ref().is_some_and(|operation| {
                                operation.idempotency_key == identity.idempotency_key
                            })
                    }) {
                        return Err(Error::Conflict("outbox idempotency identity reused".into()));
                    }
                    if persist {
                        self.append_record(record)?;
                    }
                    self.pending.push(command.clone());
                }
                _ => return Err(Error::Invalid("unsupported client journal request".into())),
            },
            (None, Some(response)) => match &response.frame {
                Some(wire::server_frame::Frame::Admission(admission)) => {
                    self.admit(admission, record, persist)?;
                }
                Some(wire::server_frame::Frame::Delivery(delivery)) => {
                    self.deliver(delivery, record, persist)?;
                }
                _ => return Err(Error::Invalid("unsupported client journal response".into())),
            },
            _ => {
                return Err(Error::Invalid(
                    "client journal record needs one frame".into(),
                ));
            }
        }
        Ok(())
    }

    /// Replaces the journal prefix while retaining the exact cursor/projection pair.
    pub fn checkpoint(&mut self) -> Result<()> {
        let checkpoint = SessionRecord {
            request: None,
            response: None,
            checkpoint: Some(SessionCheckpoint {
                protocol: Some(current_protocol()),
                cursors: self.cursors.values().cloned().collect(),
                pending: self.pending.clone(),
                views_json: serde_json::to_vec(&self.views)
                    .map_err(|error| Error::Storage(error.to_string()))?,
            }),
        };
        self.store.replace(&checkpoint)?;
        self.tail_records = 0;
        Ok(())
    }

    fn append_record(&mut self, record: &SessionRecord) -> Result<()> {
        self.store.append(record)?;
        self.tail_records += 1;
        Ok(())
    }

    fn compact_if_needed(&mut self) -> Result<()> {
        if self.tail_records >= 64 {
            self.checkpoint()?;
        }
        Ok(())
    }

    fn restore(&mut self, checkpoint: &SessionCheckpoint) -> Result<()> {
        if checkpoint.protocol.as_ref() != Some(&current_protocol()) {
            return Err(Error::Conflict("client checkpoint protocol changed".into()));
        }
        let mut cursors = BTreeMap::new();
        for cursor in &checkpoint.cursors {
            let authority = valid_authority(cursor.authority.as_ref())?;
            if cursor.generation.is_empty()
                || cursors
                    .insert(authority_key(authority), cursor.clone())
                    .is_some()
            {
                return Err(Error::Storage("invalid checkpoint cursor".into()));
            }
        }
        let views = serde_json::from_slice(&checkpoint.views_json)
            .map_err(|error| Error::Storage(error.to_string()))?;
        self.cursors = cursors;
        self.pending.clear();
        self.views = views;
        for command in &checkpoint.pending {
            self.record(
                &SessionRecord {
                    request: Some(wire::ClientFrame {
                        frame: Some(wire::client_frame::Frame::Command(command.clone())),
                    }),
                    response: None,
                    checkpoint: None,
                },
                false,
            )?;
        }
        self.tail_records = 0;
        Ok(())
    }

    fn admit(
        &mut self,
        admission: &wire::Admission,
        record: &SessionRecord,
        persist: bool,
    ) -> Result<()> {
        let operation = admission
            .operation
            .as_ref()
            .ok_or_else(|| Error::Conflict("admission identity missing".into()))?;
        let index = self
            .pending
            .iter()
            .position(|pending| {
                pending
                    .operation
                    .as_ref()
                    .is_some_and(|identity| identity.operation_id == operation.operation_id)
            })
            .ok_or_else(|| Error::Conflict("unsolicited admission".into()))?;
        let command = self
            .pending
            .get(index)
            .ok_or_else(|| Error::Conflict("pending command disappeared".into()))?;
        validate_admission(command, admission)?;
        match wire::AdmissionState::try_from(admission.state) {
            Ok(wire::AdmissionState::Accepted | wire::AdmissionState::Rejected) => {}
            Ok(wire::AdmissionState::Indeterminate) => return Ok(()),
            _ => return Err(Error::Invalid("invalid admission state".into())),
        }
        if persist {
            self.append_record(record)?;
        }
        self.pending.remove(index);
        Ok(())
    }

    fn deliver(
        &mut self,
        delivery: &wire::Delivery,
        record: &SessionRecord,
        persist: bool,
    ) -> Result<()> {
        let authority = valid_authority(delivery.authority.as_ref())?;
        let key = authority_key(authority);
        let cursor = self
            .cursors
            .get(&key)
            .ok_or_else(|| Error::Conflict("delivery authority was not requested".into()))?;
        if cursor.generation != delivery.generation
            || delivery.through_revision < delivery.from_revision
        {
            return Err(Error::Conflict(
                "delivery generation or range mismatch".into(),
            ));
        }
        let mut revision = delivery.from_revision;
        let protocol = current_protocol();
        let mut payloads = Vec::with_capacity(delivery.events.len());
        for event in &delivery.events {
            revision = revision
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("revision overflow".into()))?;
            if event.authority.as_ref() != Some(authority)
                || event.revision != revision
                || event.protocol.as_ref() != Some(&protocol)
            {
                return Err(Error::Conflict(
                    "delivery event identity or sequence mismatch".into(),
                ));
            }
            payloads.push(payload(event)?);
        }
        if revision != delivery.through_revision {
            return Err(Error::Conflict("delivery is not contiguous".into()));
        }
        if delivery.through_revision <= cursor.revision {
            return Ok(());
        }
        if delivery.from_revision != cursor.revision {
            return Err(Error::Conflict(
                "delivery does not follow durable cursor".into(),
            ));
        }
        if persist {
            self.append_record(record)?;
        }
        for payload in payloads {
            self.views.apply(authority, payload);
        }
        if let Some(cursor) = self.cursors.get_mut(&key) {
            cursor.revision = revision;
        }
        Ok(())
    }
}

fn valid_authority(authority: Option<&wire::Authority>) -> Result<&wire::Authority> {
    let authority = authority.ok_or_else(|| Error::Invalid("authority missing".into()))?;
    if authority.id.is_empty()
        || matches!(authority.id.as_str(), "." | "..")
        || authority.id.contains(['/', '\\'])
        || authority.id.chars().any(char::is_control)
        || wire::AggregateKind::try_from(authority.kind)
            .map_or(true, |kind| kind == wire::AggregateKind::Unspecified)
    {
        return Err(Error::Invalid("invalid authority".into()));
    }
    Ok(authority)
}
