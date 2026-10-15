//! Read-only interpretation of an agent's ordinary GOAL.md file.

use crate::{Error, OperationId, Result, TaskId, conversation::VolumeClass};
use serde::{Deserialize, Serialize};

/// Validated goal frontmatter status.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatus {
    /// Continue after a settled turn when the goal revision has changed.
    #[default]
    Active,
    /// Waiting for input or an external change.
    Blocked,
    /// The objective has been completed.
    Done,
}

/// Disposable rendering of one immutable private goal file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalView {
    /// Exact file observed through the owner's ordinary filesystem grant.
    pub source: crate::conversation::FileRef,
    /// Frontmatter status; plain Markdown defaults to active.
    pub status: GoalStatus,
    /// Markdown body, excluding frontmatter.
    pub markdown: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frontmatter {
    #[serde(default)]
    status: GoalStatus,
}

impl GoalView {
    /// Interprets verified file bytes within the caller's existing file bound.
    pub fn from_file(
        source: crate::conversation::FileRef,
        bytes: &[u8],
        maximum_bytes: u64,
    ) -> Result<Self> {
        source.validate(&mut None)?;
        if source.path() != "GOAL.md"
            || source.volume().class() != VolumeClass::AgentPrivate
            || bytes.len() as u64 > maximum_bytes
        {
            return Err(Error::Invalid(
                "invalid private goal file or byte bound".into(),
            ));
        }
        source.descriptor().verify(bytes, &mut None)?;
        let text = std::str::from_utf8(bytes)
            .map_err(|_| Error::Invalid("GOAL.md requires UTF-8".into()))?;
        let (status, markdown) = parse(text)?;
        Ok(Self {
            source,
            status,
            markdown: markdown.to_owned(),
        })
    }

    /// Content revision, unaffected by unrelated writes to the same volume.
    #[must_use]
    pub fn revision(&self) -> [u8; 32] {
        *self.source.descriptor().sha256()
    }

    /// Stable admission identity for a settled turn and newly observed active goal.
    /// The caller carries the admitted revision through continuation turns and
    /// submits this identity through the existing durable task admission path.
    #[must_use]
    pub fn continuation_operation(
        &self,
        task: TaskId,
        settled_turn: OperationId,
        admitted_revision: Option<[u8; 32]>,
    ) -> Option<OperationId> {
        let revision = self.revision();
        if self.status != GoalStatus::Active || admitted_revision == Some(revision) {
            return None;
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic.goal.continue/v1\0");
        hash.update(&task.into_bytes());
        hash.update(&settled_turn.into_bytes());
        hash.update(&revision);
        let mut id = [0; 16];
        id.copy_from_slice(hash.finalize().as_bytes().get(..16)?);
        Some(OperationId::from_bytes(id))
    }
}

fn parse(text: &str) -> Result<(GoalStatus, &str)> {
    let mut lines = text.split_inclusive('\n');
    if lines.next().map(|line| line.trim_end_matches(['\r', '\n'])) != Some("---") {
        return Ok((GoalStatus::Active, text));
    }
    let start = text
        .find('\n')
        .ok_or_else(|| Error::Invalid("unclosed goal frontmatter".into()))?
        + 1;
    let mut end = start;
    for line in lines {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            let header = text
                .get(start..end)
                .ok_or_else(|| Error::Invalid("invalid goal frontmatter range".into()))?;
            let options = serde_saphyr::options! {
                budget: serde_saphyr::budget! {
                    max_events: header.len().saturating_add(1),
                    max_nodes: header.len().saturating_add(1),
                    max_depth: 2, flow_nesting_limit: 2, max_documents: 1,
                    max_aliases: 0, max_anchors: 0, max_merge_keys: 0,
                    max_total_scalar_bytes: header.len(),
                },
                duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
                merge_keys: serde_saphyr::MergeKeyPolicy::Error,
                reject_unsupported_tags: true,
                emit_comments: false, with_snippet: false,
            };
            let frontmatter: Frontmatter = if header.trim().is_empty() {
                Frontmatter::default()
            } else {
                serde_saphyr::from_str_with_options(header, options)
                    .map_err(|_| Error::Invalid("invalid goal frontmatter".into()))?
            };
            let body = text
                .get(end + line.len()..)
                .ok_or_else(|| Error::Invalid("invalid goal body range".into()))?;
            return Ok((frontmatter.status, body));
        }
        end += line.len();
    }
    Err(Error::Invalid("unclosed goal frontmatter".into()))
}

#[cfg(all(test, feature = "filesystem"))]
mod scenarios;
#[cfg(test)]
mod tests;
