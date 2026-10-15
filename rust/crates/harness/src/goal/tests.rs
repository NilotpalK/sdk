use super::*;
use crate::{
    AgentId,
    conversation::{FileDescriptor, FileRef, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};

fn file(path: &str, class: VolumeClass, bytes: &[u8], version: &str) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?,
            "private".into(),
            class,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?,
        path.into(),
        version.into(),
        FileDescriptor::from_bytes(bytes, "text/markdown", &mut None)?,
        "GOAL.md".into(),
        &mut None,
    )
}

fn goal(bytes: &[u8]) -> Result<GoalView> {
    GoalView::from_file(
        file("GOAL.md", VolumeClass::AgentPrivate, bytes, "v1")?,
        bytes,
        4096,
    )
}

#[test]
fn frontmatter_cases() -> Result<()> {
    for (text, status, body) in [
        ("", GoalStatus::Active, ""),
        ("Ship it\n", GoalStatus::Active, "Ship it\n"),
        (
            "---\nstatus: active\n---\nShip it\n",
            GoalStatus::Active,
            "Ship it\n",
        ),
        (
            "---\nstatus: blocked\n---\nNeed input",
            GoalStatus::Blocked,
            "Need input",
        ),
        (
            "---\nstatus: done\n---\nFinished",
            GoalStatus::Done,
            "Finished",
        ),
        (
            "---\r\nstatus: done\r\n---\r\nFinished\r\n",
            GoalStatus::Done,
            "Finished\r\n",
        ),
        ("---\n---\nBody", GoalStatus::Active, "Body"),
        ("---\nstatus: done\n---", GoalStatus::Done, ""),
        (
            "# done\nstatus: blocked",
            GoalStatus::Active,
            "# done\nstatus: blocked",
        ),
        ("--- \nBody", GoalStatus::Active, "--- \nBody"),
    ] {
        let view = goal(text.as_bytes())?;
        assert_eq!(view.status, status, "{text:?}");
        assert_eq!(view.markdown, body, "{text:?}");
    }
    Ok(())
}

#[test]
fn malformed_goal_does_not_silently_activate() {
    for text in [
        "---",
        "---\nstatus: active",
        "---\nstatus: other\n---\n",
        "---\nstatus: [done]\n---\n",
        "---\nstatus: null\n---\n",
        "---\nstatus: done\nstatus: active\n---\n",
        "---\nstatus: &s active\n---\n",
        "---\nstatus: *s\n---\n",
        "---\nunknown: active\n---\n",
        "---\nstatus: {nested: active}\n---\n",
    ] {
        assert!(goal(text.as_bytes()).is_err(), "{text:?}");
    }
    assert!(goal(&[0xff]).is_err());
}

#[test]
fn private_root_and_integrity_are_required() -> Result<()> {
    for (path, class) in [
        ("GOAL.md", VolumeClass::Project),
        ("GOAL.md", VolumeClass::SessionShared),
        ("nested/GOAL.md", VolumeClass::AgentPrivate),
    ] {
        assert!(GoalView::from_file(file(path, class, b"goal", "v1")?, b"goal", 4096).is_err());
    }
    let source = file("GOAL.md", VolumeClass::AgentPrivate, b"goal", "v1")?;
    assert!(GoalView::from_file(source.clone(), b"changed", 4096).is_err());
    assert!(GoalView::from_file(source, b"goal", 3).is_err());
    Ok(())
}

#[test]
fn continuation_is_stable_scoped_and_stops_on_unchanged_revision() -> Result<()> {
    let task = TaskId::from_bytes([2; 16]);
    let turn = OperationId::from_bytes([3; 16]);
    let view = goal(b"Ship it")?;
    let operation = view.continuation_operation(task, turn, None);
    assert!(operation.is_some());
    assert_eq!(operation, view.continuation_operation(task, turn, None));
    assert_ne!(
        operation,
        view.continuation_operation(TaskId::new(), turn, None)
    );
    assert_ne!(
        operation,
        view.continuation_operation(task, OperationId::new(), None)
    );
    let continued_turn = operation.ok_or_else(|| Error::Invalid("missing continuation".into()))?;
    assert_eq!(
        view.continuation_operation(task, continued_turn, Some(view.revision())),
        None
    );
    let same_bytes_new_generation = GoalView::from_file(
        file("GOAL.md", VolumeClass::AgentPrivate, b"Ship it", "v2")?,
        b"Ship it",
        4096,
    )?;
    assert_eq!(same_bytes_new_generation.revision(), view.revision());
    assert_eq!(
        same_bytes_new_generation.continuation_operation(task, turn, Some(view.revision())),
        None
    );
    let edited = goal(b"Ship it with tests")?;
    assert!(
        edited
            .continuation_operation(task, continued_turn, Some(view.revision()))
            .is_some()
    );
    for status in ["done", "blocked"] {
        let stopped = goal(format!("---\nstatus: {status}\n---\nShip it").as_bytes())?;
        assert_eq!(stopped.continuation_operation(task, turn, None), None);
    }
    Ok(())
}
