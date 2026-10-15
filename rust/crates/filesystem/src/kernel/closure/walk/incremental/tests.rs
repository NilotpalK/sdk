use super::*;
use crate::foundation::Digest;
use crate::kernel::{FileKind, InlineFileData, NameEncoding};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

fn namespace_diff(count: usize, chain: bool) -> TestResult<crate::GenerationDiff> {
    let object = |kind| ObjectId {
        kind,
        digest: Digest::from_bytes([1; 32]),
    };
    let mut diff = crate::GenerationDiff {
        files: Vec::new(),
        bindings: Vec::new(),
        truncated: false,
    };
    for index in 0..=count {
        let directory = index == 0 || chain;
        let record = FileRecord {
            file_id: FileId::from_bytes((index as u128).to_be_bytes()),
            kind: if directory {
                FileKind::Directory
            } else {
                FileKind::Regular
            },
            link_count: 1,
            metadata: object(ObjectKind::Metadata),
            payload: if directory {
                FilePayload::Directory {
                    entries: object(ObjectKind::TreePage),
                }
            } else {
                FilePayload::InlineRegular(InlineFileData::new(b"data")?)
            },
        };
        diff.files.push(crate::FileRecordChange {
            file_id: record.file_id,
            before: (index == 0).then_some(record),
            after: Some(record),
        });
        if index != 0 {
            let parent = if chain && index != count {
                index + 1
            } else {
                0
            };
            let name = LogicalName::new(NameEncoding::Utf8, index.to_string().into_bytes(), 255)?;
            diff.bindings.push(crate::DirectoryBindingChange {
                directory_id: FileId::from_bytes((parent as u128).to_be_bytes()),
                name: name.clone(),
                before: None,
                after: Some(TreeEntry {
                    name,
                    file_id: record.file_id,
                    kind: record.kind,
                }),
            });
        }
    }
    Ok(diff)
}

fn validate(diff: &crate::GenerationDiff, budget: WorkBudget) -> TestResult<(bool, WorkCounters)> {
    let store = crate::memory::MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let meter = ProofMeter::standalone(budget, &cancellation)?;
    let context = ProofContext::new(
        &store,
        ClosureLimits {
            decode: crate::kernel::DecodeLimits::default(),
            maximum_objects: 1024,
            maximum_files: 1024,
            maximum_object_bytes: 1_000_000,
            profile: FilesystemProfile::Portable,
            symbolic_links: false,
            hard_links: false,
            sparse_files: false,
        },
        meter.clone(),
    );
    let valid = context.validate_increment_namespace(diff)?;
    Ok((valid, meter.work()))
}

#[test]
fn batched_namespace_indexes_fund_and_meter_siblings_and_shared_ancestry() -> TestResult<()> {
    for chain in [false, true] {
        let mut small = 0;
        for count in [64, 256] {
            let diff = namespace_diff(count, chain)?;
            let (valid, work) = validate(
                &diff,
                crate::performance::work_budget(64 * count as u64, 1_000_000),
            )?;
            assert!(valid);
            assert!(work.allocation_operations > 0);
            assert!(work.items_examined >= count as u64 * u64::from(count.ilog2()));
            assert!(work.items_examined <= count as u64 * (2 * u64::from(count.ilog2()) + 12));
            if count == 64 {
                small = work.items_examined;
            } else {
                assert!(work.items_examined <= 6 * small, "{small} -> {work:?}");
            }
            let mut budget = crate::performance::work_budget(64 * count as u64, 1_000_000);
            budget.items_examined = 5 * count as u64;
            assert!(
                validate(&diff, budget).is_err(),
                "index probes must respect the original finite budget"
            );
            budget = crate::performance::work_budget(64 * count as u64, 1_000_000);
            budget.peak_allocation_bytes =
                crate::kernel::closure::memory::proof_standalone_control_bytes();
            assert!(
                validate(&diff, budget).is_err(),
                "the parent index needs funded storage"
            );
        }
    }
    Ok(())
}

#[test]
fn incremental_namespace_rejects_duplicate_orphan_and_cyclic_bindings() -> TestResult<()> {
    let budget = crate::performance::work_budget(10_000, 1_000_000);
    let mut diff = namespace_diff(8, false)?;
    let duplicate = diff.bindings.first().ok_or("missing binding")?.clone();
    diff.bindings.push(duplicate);
    assert!(!validate(&diff, budget)?.0);
    diff.bindings.pop();
    diff.bindings.pop();
    assert!(!validate(&diff, budget)?.0);
    let mut diff = namespace_diff(8, true)?;
    diff.bindings
        .last_mut()
        .ok_or("missing binding")?
        .directory_id = FileId::from_bytes(1_u128.to_be_bytes());
    assert!(!validate(&diff, budget)?.0);
    Ok(())
}
