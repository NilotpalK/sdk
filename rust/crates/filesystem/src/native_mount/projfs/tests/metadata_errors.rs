use super::*;

#[test]
fn metadata_close_failure_blocks_publication_without_recapturing_contents()
-> Result<(), Box<dyn std::error::Error>> {
    let failures = Mutex::new(PostOperationFailures::default());
    let callbacks = CallbackGate::start()?;
    let path = windows_path("changed.txt");
    record_post_operation_failure(
        &failures,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION,
        &path,
        None,
        &Err(MountSourceError::Stale),
    );
    flush_callback_gate(&callbacks, &failures, |paths| {
        assert!(paths.is_empty());
        Ok(())
    })?;
    record_post_operation_failure(
        &failures,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION,
        &path,
        Some(&path),
        &Err(MountSourceError::Stale),
    );
    for _ in 0..2 {
        assert!(
            flush_callback_gate(&callbacks, &failures, |paths| {
                assert!(
                    paths.is_empty(),
                    "metadata errors must not recapture file contents"
                );
                Ok(())
            })
            .is_err()
        );
        assert!(lock_recover(&failures).unreplayable.is_some());
        assert!(lock_recover(&failures).pending_captures.is_empty());
    }
    Ok(())
}
