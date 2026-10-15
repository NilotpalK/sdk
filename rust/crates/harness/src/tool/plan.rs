//! Task-local plans published through the ordinary authenticated file boundary.

use crate::{NativeCustody, Result};
use serde::{Deserialize, Serialize};

/// Canonical plan path in the task's bound output volume.
pub const PLAN_PATH: &str = "plan.md";

/// Replaces the complete plan; an empty list clears the steps.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdatePlanInput {
    /// Optional reason for this revision.
    #[serde(default)]
    #[schemars(length(max = 8192))]
    pub explanation: Option<String>,
    /// Ordered steps, with at most one currently in progress.
    #[schemars(length(max = 100))]
    pub plan: Vec<PlanStep>,
}

/// One model-visible plan step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    /// Nonempty single-line description.
    #[schemars(length(min = 1, max = 1024))]
    pub step: String,
    /// Current execution status.
    pub status: PlanStatus,
}

/// Finite step lifecycle shared by the tool and client renderers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    /// Work has not started.
    Pending,
    /// Work is underway.
    InProgress,
    /// Work has finished.
    Completed,
}

/// Builds the independently registerable `update_plan` tool.
pub fn update_plan() -> Result<super::Tool> {
    super::files::FileTool::Plan.assemble(super::schema::ProjectionMode::Full)
}

impl UpdatePlanInput {
    pub(super) fn validate(&self, custody: &NativeCustody) -> Result<()> {
        let invalid = |reason| super::edit::invalid(custody, reason);
        super::edit::charge(
            custody,
            0,
            self.explanation.as_ref().map_or(0, |text| text.len()) as u64,
        )?;
        if self.plan.len() > 100
            || self.explanation.as_ref().is_some_and(|text| {
                text.len() > 8192 || text.chars().any(|c| c.is_control() && c != '\n')
            })
        {
            return Err(invalid("plan exceeds step or explanation limits"));
        }
        let mut active = 0;
        for step in &self.plan {
            super::edit::charge(custody, 0, step.step.len() as u64 * 2 + 4)?;
            if step.step.trim().is_empty()
                || step.step.len() > 1024
                || step.step.chars().any(char::is_control)
            {
                return Err(invalid(
                    "plan steps require bounded nonempty single-line text",
                ));
            }
            active += usize::from(step.status == PlanStatus::InProgress);
        }
        if active > 1 {
            return Err(invalid("plan has more than one step in progress"));
        }
        Ok(())
    }

    pub(super) fn markdown(
        &self,
        maximum_bytes: u64,
        custody: &mut NativeCustody,
    ) -> Result<String> {
        self.validate(custody)?;
        let length = 8
            + self.explanation.as_ref().map_or(0, |text| text.len() + 2)
            + self
                .plan
                .iter()
                .map(|step| step.step.len() + 7)
                .sum::<usize>();
        if length as u64 > maximum_bytes {
            return Err(super::edit::invalid(
                custody,
                "plan exceeds task file byte limit",
            ));
        }
        let mut text = String::new();
        super::edit::reserve_string(custody, &mut text, length)?;
        super::edit::charge(custody, length as u64, 0)?;
        text.push_str("# Plan\n\n");
        if let Some(explanation) = &self.explanation {
            text.push_str(explanation);
            text.push_str("\n\n");
        }
        for step in &self.plan {
            text.push_str(match step.status {
                PlanStatus::Pending => "- [ ] ",
                PlanStatus::InProgress => "- [~] ",
                PlanStatus::Completed => "- [x] ",
            });
            text.push_str(&step.step);
            text.push('\n');
        }
        Ok(text)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for UpdatePlanInput {
    fn diagnostic_label_bytes() -> u64 {
        super::TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let invalid = || super::files::native::invalid("plan DTO ownership overflow");
        let mut bytes = self
            .plan
            .capacity()
            .checked_mul(std::mem::size_of::<PlanStep>())
            .and_then(|bytes| {
                bytes.checked_add(self.explanation.as_ref().map_or(0, String::capacity))
            })
            .ok_or_else(invalid)?;
        for step in &self.plan {
            bytes = bytes
                .checked_add(step.step.capacity())
                .ok_or_else(invalid)?;
        }
        Ok(bytes as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn plan_rejects_invalid_steps_statuses_fields_and_bounds() -> Result<()> {
        let invalid = [
            json!({"plan":[{"step":"","status":"pending"}]}),
            json!({"plan":[{"step":" \t","status":"pending"}]}),
            json!({"plan":[{"step":"one\ntwo","status":"pending"}]}),
            json!({"plan":[{"step":"one\u{1b}","status":"pending"}]}),
            json!({"plan":[{"step":"x".repeat(1025),"status":"pending"}]}),
            json!({"plan":[{"step":"one","status":"started"}]}),
            json!({"plan":[{"step":"one","status":"in_progress"},{"step":"two","status":"in_progress"}]}),
            json!({"plan":[],"path":"other.md"}),
            json!({"plan":[],"explanation":"x".repeat(8193)}),
            json!({"plan":[],"explanation":"\u{1b}"}),
            json!({"plan":vec![json!({"step":"one","status":"pending"});101]}),
        ];
        for value in invalid {
            if let Ok(input) = serde_json::from_value::<UpdatePlanInput>(value.clone()) {
                assert!(input.markdown(65_536, &mut None).is_err(), "{value}");
            }
        }
        let input: UpdatePlanInput =
            serde_json::from_value(json!({"plan":[{"step":"é".repeat(512),"status":"pending"}]}))
                .map_err(|e| crate::Error::Invalid(e.to_string()))?;
        let markdown = input.markdown(1039, &mut None)?;
        assert_eq!(markdown.len(), 1039);
        assert!(input.markdown(1038, &mut None).is_err());
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn plan_rendering_uses_and_releases_the_original_native_budget() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, performance::work_budget};
        let input = UpdatePlanInput {
            explanation: None,
            plan: vec![PlanStep {
                step: "Inspect".into(),
                status: PlanStatus::InProgress,
            }],
        };
        let minimum = NativeMaterializationWork::accounting_state_bytes()
            + std::mem::size_of::<String>() as u64;
        for (items, bytes, succeeds) in [
            (10_000, 65_536, true),
            (1, 65_536, false),
            (10_000, minimum, false),
        ] {
            let work = NativeMaterializationWork::new(
                work_budget(items, bytes),
                &CancellationToken::new(),
            )
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
            let before = work.live_allocation_bytes();
            let mut custody =
                super::super::edit::allocation(Some(&work), std::mem::size_of::<String>())?;
            let result = input.markdown(65_536, &mut custody);
            assert_eq!(result.is_ok(), succeeds);
            if let Ok(text) = &result {
                assert!(work.live_allocation_bytes() >= before + text.capacity() as u64);
                assert_eq!(text, "# Plan\n\n- [~] Inspect\n");
            }
            drop(result);
            drop(custody);
            assert_eq!(work.live_allocation_bytes(), before);
        }
        Ok(())
    }
}
