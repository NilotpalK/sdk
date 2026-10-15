use super::*;

fn work() -> Result<crate::NativeMaterializationWork, Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    crate::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error.into())
}

#[test]
fn name_query_matches_held_stat_and_records_one_read() -> Result<(), Box<dyn std::error::Error>> {
    let work = work()?;
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("leaf"), b"contents")?;
    std::fs::hard_link(
        temporary.path().join("leaf"),
        temporary.path().join("alias"),
    )?;
    std::fs::create_dir(temporary.path().join("nested"))?;
    std::fs::write(temporary.path().join("nested/éclair"), b"nested contents")?;
    let root = HostRoot::open(temporary.path(), &mut &work)?;
    std::fs::write(temporary.path().join("éclair"), b"unicode contents")?;
    for path in ["leaf", "LEAF", "alias", "éclair"] {
        let wide: Vec<_> = path.encode_utf16().collect();
        let before = work.receipt();
        let actual = query_metadata(&root, &wide, &mut &work)?.ok_or("name query fell back")?;
        let after = work.receipt();
        assert_eq!(
            after.backend_read_operations - before.backend_read_operations,
            1
        );
        assert_eq!(
            after.items_examined - before.items_examined,
            wide.len() as u64
        );
        assert_eq!(after.allocation_operations, before.allocation_operations);
        let expected = root.stat(&wide, &mut &work)?;
        assert_eq!(actual.volume_serial_number, expected.volume_serial_number);
        assert_eq!(actual.raw.FileId, expected.raw.FileId);
        assert_eq!(actual.raw.EndOfFile, expected.raw.EndOfFile);
        assert_eq!(actual.raw.NumberOfLinks, expected.raw.NumberOfLinks);
        assert_eq!(actual.raw.FileAttributes, expected.raw.FileAttributes);
        assert_eq!(actual.raw.CreationTime, expected.raw.CreationTime);
        assert_eq!(actual.raw.LastWriteTime, expected.raw.LastWriteTime);
    }
    Ok(())
}

#[test]
fn name_query_keeps_other_paths_on_the_original_validator() -> Result<(), Box<dyn std::error::Error>>
{
    let work = work()?;
    let temporary = tempfile::tempdir()?;
    let root = HostRoot::open(temporary.path(), &mut &work)?;
    for path in [
        "",
        ".",
        "..",
        "nested\\..\\leaf",
        "leaf:stream",
        "\\leaf",
        "nested\\\\leaf",
        "leaf\0",
        "nested\\éclair",
    ] {
        let wide: Vec<_> = path.encode_utf16().collect();
        let before = work.receipt();
        assert!(query_metadata(&root, &wide, &mut &work)?.is_none());
        assert_eq!(
            work.receipt().backend_read_operations,
            before.backend_read_operations
        );
    }
    Ok(())
}

#[test]
fn name_query_respects_cancellation_and_read_budget() -> Result<(), Box<dyn std::error::Error>> {
    let original = work()?;
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("leaf"), b"contents")?;
    let root = HostRoot::open(temporary.path(), &mut &original)?;
    let path: Vec<_> = "leaf".encode_utf16().collect();
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        backend_read_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let limited =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    assert!(query_metadata(&root, &path, &mut &limited).is_err());
    assert_eq!(limited.receipt().backend_read_operations, 0);
    let cancellable =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    cancellation.cancel();
    assert!(query_metadata(&root, &path, &mut &cancellable).is_err());
    assert_eq!(cancellable.receipt().backend_read_operations, 0);
    Ok(())
}

#[test]
fn name_query_reads_reparse_leaves_without_following_ancestors()
-> Result<(), Box<dyn std::error::Error>> {
    let work = work()?;
    let temporary = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::fs::write(outside.path().join("outside"), b"must not follow")?;
    let junction = temporary.path().join("junction");
    let mut command = std::process::Command::new("cmd.exe");
    command
        .args(["/D", "/C", "mklink", "/J"])
        .arg(&junction)
        .arg(outside.path());
    assert!(
        acyclic_native_runtime::process_status(&mut command, std::time::Duration::from_secs(30))?
            .success()
    );
    let root = HostRoot::open(temporary.path(), &mut &work)?;
    let projected = temporary.path().join("projected");
    std::fs::create_dir(&projected)?;
    let projected_name = HSTRING::from(projected.as_os_str());
    let guid = GUID::from_u128(0x7feffaae_9cb5_46d6_9dfa_20c26f344870);
    // SAFETY: the directory name and virtualization identifier outlive the call.
    unsafe {
        PrjMarkDirectoryAsPlaceholder(&projected_name, PCWSTR::null(), None, &raw const guid)
    }?;
    // SAFETY: the directory name remains live throughout the query.
    let state = unsafe {
        windows::Win32::Storage::ProjectedFileSystem::PrjGetOnDiskFileState(&projected_name)
    }?;
    assert_ne!(
        state.0 & windows::Win32::Storage::ProjectedFileSystem::PRJ_FILE_STATE_PLACEHOLDER.0,
        0
    );
    for path in ["junction", "projected"] {
        let wide: Vec<_> = path.encode_utf16().collect();
        let limited = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                items_examined: wide.len() as u64,
                backend_read_operations: 1,
                ..crate::WorkBudget::UNBOUNDED
            },
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        let actual = query_metadata(&root, &wide, &mut &limited)?.ok_or("tagged leaf fell back")?;
        if path == "junction" {
            assert_ne!(actual.raw.ReparseTag, 0);
        }
        assert_eq!(limited.receipt().backend_read_operations, 1);
        assert_eq!(limited.receipt().items_examined, wide.len() as u64);
        let expected = root.stat(&wide, &mut &work)?;
        assert_eq!(actual.raw.FileId, expected.raw.FileId);
        assert_eq!(actual.raw.ReparseTag, expected.raw.ReparseTag);
    }
    let wide: Vec<_> = "junction\\outside".encode_utf16().collect();
    let before = work.receipt();
    assert!(query_metadata(&root, &wide, &mut &work)?.is_none());
    assert_eq!(work.receipt(), before);
    let path = MountPath::root().child(
        "junction"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect(),
    );
    assert!(
        host_windows_metadata(&root, &path, &work)?.attributes & FILE_ATTRIBUTE_REPARSE_POINT.0
            != 0
    );
    let through_link = path.child(
        "outside"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect(),
    );
    assert!(host_windows_metadata(&root, &through_link, &work).is_err());
    assert!(std::fs::metadata(outside.path().join("outside"))?.is_file());
    Ok(())
}

#[test]
fn fallback_preserves_the_original_tight_budget() -> Result<(), Box<dyn std::error::Error>> {
    let original = work()?;
    let temporary = tempfile::tempdir()?;
    std::fs::create_dir(temporary.path().join("nested"))?;
    std::fs::write(temporary.path().join("nested/leaf"), b"contents")?;
    let root = HostRoot::open(temporary.path(), &mut &original)?;
    for path in [
        "",
        ".",
        "nested\\leaf",
        "missing",
        "leaf:stream",
        "nested\\..\\leaf",
    ] {
        let wide: Vec<_> = path.encode_utf16().collect();
        let baseline = work()?;
        let expected = root.stat(&wide, &mut &baseline);
        let receipt = baseline.receipt();
        let limited = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                items_examined: receipt.items_examined,
                backend_read_operations: receipt.backend_read_operations,
                ..crate::WorkBudget::UNBOUNDED
            },
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        let actual = match query_metadata(&root, &wide, &mut &limited)? {
            Some(stat) => Ok(stat),
            None => root.stat(&wide, &mut &limited),
        };
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual.raw.FileId, expected.raw.FileId),
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            other => return Err(format!("fallback changed result: {other:?}").into()),
        }
        assert_eq!(limited.receipt(), receipt);
    }
    Ok(())
}

#[test]
fn name_query_reuses_prepaid_validation_items() -> Result<(), Box<dyn std::error::Error>> {
    let original = work()?;
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("leaf"), b"contents")?;
    let root = HostRoot::open(temporary.path(), &mut &original)?;
    let path: Vec<_> = "leaf".encode_utf16().collect();
    let limited = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            items_examined: path.len() as u64,
            backend_read_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    NativeHostWork::started_io(
        &mut &limited,
        crate::WorkCounters {
            items_examined: path.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        },
    )?;
    let recheck = limited.recheck_admitted();
    assert!(query_metadata(&root, &path, &mut &recheck)?.is_some());
    assert_eq!(limited.receipt().items_examined, path.len() as u64);
    assert_eq!(limited.receipt().backend_read_operations, 1);
    Ok(())
}
