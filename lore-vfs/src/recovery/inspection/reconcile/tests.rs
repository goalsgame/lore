// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_revision::instance;
use lore_revision::instance::InstanceId;
use lore_revision::instance::strict_anchors;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::repository::VfsConfig;
use lore_revision::repository::VfsType;
use lore_revision::repository::working_bootstrap::BackingBinding;
use lore_revision::repository::working_bootstrap::MetadataInitializer;
use lore_revision::repository::working_bootstrap::Mode;
use lore_revision::repository::working_bootstrap::Selection as StoreSelection;
use lore_revision::repository::working_bootstrap::StoreInitializer;
use lore_revision::repository::working_bootstrap::StoreMode;

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::CaptureMemory;
use crate::capture_memory::Footprint;
use crate::core::BackendFuture;
use crate::core::NodeKey;
use crate::namespace::Namespace;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::recovery::Limits;
use crate::recovery::StartupRecovery;
use crate::recovery::intent::Anchors;
use crate::recovery::intent::CheckpointIdentity;
use crate::recovery::intent_writer;
use crate::revision::RevisionBackend;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(std::path::PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-reconcile-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Gate;
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
fn memory() -> CaptureMemory {
    CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: 8 << 20,
        max_metadata_nodes: 100_000,
        max_name_bytes: 1 << 20,
    })
    .unwrap()
}
fn execution() -> Arc<lore_revision::interface::ExecutionContext> {
    Arc::new(lore_revision::interface::ExecutionContext::new_client(
        lore_revision::interface::LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        },
        lore_revision::relay::EventDispatcher::no_dispatch(),
    ))
}
#[derive(Clone, Copy)]
enum Choice {
    Old,
    New,
    Mixed,
}
struct Fixture {
    payload: lore_revision::repository::working_bootstrap::OwnerBuildPayload,
    storage: Arc<WorkingStorage>,
    record: Intent,
    owner: Option<Recovery>,
    source_snapshot: crate::namespace::NamespaceSnapshot,
    candidate_snapshot: crate::namespace::NamespaceSnapshot,
    source: Vec<u8>,
    candidate: Vec<u8>,
    _temp: Temp,
}
impl Fixture {
    async fn new(choice: Choice) -> Self {
        let temp = Temp::new();
        let target = temp.0.join("target");
        let metadata = temp.0.join("metadata");
        std::fs::create_dir(&target).unwrap();
        std::fs::create_dir(&metadata).unwrap();
        std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o700)).unwrap();
        let registry = ProviderRegistry::new(Default::default());
        let instance = InstanceId::generate();
        let registration = Arc::new(registry.reserve(&target, instance).unwrap());
        registration
            .reserve_metadata(&metadata, [51; 16].into())
            .unwrap();
        let token = RepositoryWriteToken::acquire(&target).await;
        let configuration = RepositoryConfig {
            vfs: Some(VfsConfig {
                vfs_type: VfsType::ExperimentalWorking,
            }),
            ..Default::default()
        };
        let prepared = MetadataInitializer::new(1)
            .unwrap()
            .prepare(
                registration.clone(),
                token.share(),
                Mode::CreateNew(configuration),
            )
            .await
            .unwrap();
        let context = StoreInitializer::new(1)
            .unwrap()
            .prepare(
                prepared,
                StoreMode::CreateNew(StoreSelection {
                    branch: [62; 16].into(),
                    branch_name: "main".into(),
                    revision: Default::default(),
                }),
            )
            .await
            .unwrap();
        let repository = Arc::new(
            context
                .repository
                .without_command_locks()
                .with_write_token(token.share()),
        );
        for (id, name) in [(63, "other"), (64, "mixed")] {
            lore_revision::branch::create(
                repository.clone(),
                &token,
                [id; 16].into(),
                name,
                lore_revision::branch::default_category(),
                "",
                0,
                vec![],
                false,
                false,
            )
            .await
            .unwrap();
        }
        let old = strict_anchors::read(&repository).await.unwrap();
        let mut new = old;
        new.branch = [63; 16].into();
        let branch = match choice {
            Choice::Old => [62; 16],
            Choice::New => [63; 16],
            Choice::Mixed => [64; 16],
        };
        instance::store_current_anchor_branch(&repository, branch.into())
            .await
            .unwrap();
        repository.flush(true).await.unwrap();
        let storage = WorkingStorage::create_new(
            &temp.0.join("backing"),
            &target,
            [51; 16].into(),
            instance,
            Default::default(),
        )
        .unwrap();
        BackingBinding::write_new(&metadata, [51; 16].into(), instance, storage.root()).unwrap();
        let backend = Arc::new(
            RevisionBackend::new(repository.clone(), Default::default())
                .await
                .unwrap(),
        );
        let namespace = Namespace::new(
            backend.clone(),
            backend.root(),
            Arc::new(Gate),
            Default::default(),
        )
        .unwrap();
        let snapshot = namespace.snapshot(1 << 20).await.unwrap();
        drop(namespace);
        drop(backend);
        let mut candidate = snapshot.clone();
        candidate.generation += 1;
        let mut budget = memory();
        budget
            .retain(Footprint::snapshot(&snapshot).unwrap())
            .unwrap();
        let mut recovery = Recovery::open(storage.recovery_directory(), Limits::default()).unwrap();
        let source_snapshot = snapshot.clone();
        let source = recovery.publish_retained(snapshot, &mut budget).unwrap();
        let mut record = Intent {
            transaction: [7; 32],
            original_root: source.snapshot().original_root.key,
            scope: source.snapshot().original_root.key,
            source: source.identity(),
            candidate: source.identity(),
            old: Anchors::from(old),
            new: Anchors::from(new),
        };
        budget
            .retain(Footprint::snapshot(&candidate).unwrap())
            .unwrap();
        let candidate_bytes = crate::recovery::encode_candidate(
            &candidate,
            Limits::default().max_checkpoint_bytes,
            &mut budget,
        )
        .unwrap();
        record.candidate = CheckpointIdentity::from_bytes(
            &candidate_bytes,
            Limits::default().max_checkpoint_bytes,
        )
        .unwrap();
        intent_writer::tests::publish_matching_fixture(
            &mut recovery,
            &source,
            &candidate,
            &candidate_bytes,
            &record,
            &mut budget,
        );
        let source_bytes = source.bytes().to_vec();
        drop(repository);
        drop(context);
        drop(token);
        drop(registration);
        let registration = Arc::new(registry.reserve(&target, instance).unwrap());
        registration
            .reserve_metadata(&metadata, [51; 16].into())
            .unwrap();
        let token = RepositoryWriteToken::acquire(&target).await;
        let pending = MetadataInitializer::new(1)
            .unwrap()
            .prepare_pending(registration, token, Mode::Reuse)
            .await
            .unwrap();
        let working = StoreInitializer::new(1)
            .unwrap()
            .prepare_pending_bound(pending, StoreMode::Reuse, storage.root().to_path_buf())
            .await
            .unwrap();
        let payload = working.into_owner_build().unwrap();
        Self {
            payload,
            storage,
            record,
            owner: Some(recovery),
            source_snapshot,
            candidate_snapshot: candidate,
            source: source_bytes,
            candidate: candidate_bytes,
            _temp: temp,
        }
    }
    async fn reconcile(&mut self) -> Result<Recovery> {
        drop(self.owner.take());
        let StartupRecovery::NeedsReconciliation(owner) =
            StartupRecovery::open_existing(self.storage.clone(), Limits::default())?
        else {
            panic!("transition unexpectedly admitted");
        };
        owner.reconcile(&mut self.payload).await
    }
    fn write_private(&self, name: &str, bytes: &[u8]) {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.storage.recovery_directory().join(name))
            .unwrap();
        std::io::Write::write_all(&mut file, bytes).unwrap();
        file.sync_all().unwrap();
        std::fs::File::open(self.storage.recovery_directory())
            .unwrap()
            .sync_all()
            .unwrap();
    }
}
#[tokio::test]
async fn actual_pending_store_reader_selects_only_exact_old_or_new_tuple() {
    LORE_CONTEXT
        .scope(execution(), async {
            for choice in [Choice::Old, Choice::New] {
                let mut fixture = Fixture::new(choice).await;
                let expected = match choice {
                    Choice::Old => fixture.record.source,
                    Choice::New => fixture.record.candidate,
                    Choice::Mixed => unreachable!(),
                };
                let recovery = fixture.reconcile().await.unwrap();
                assert_eq!(recovery.load_exact().unwrap().unwrap().identity(), expected);
                assert!(
                    !fixture
                        .storage
                        .recovery_directory()
                        .join("transition")
                        .exists()
                );
                assert!(
                    !fixture
                        .storage
                        .recovery_directory()
                        .join("transition.cleanup")
                        .exists()
                );
            }
            let mut fixture = Fixture::new(Choice::Mixed).await;
            let before =
                std::fs::read(fixture.storage.recovery_directory().join("checkpoint")).unwrap();
            assert!(matches!(
                fixture.reconcile().await,
                Err(Error::StaleSnapshot)
            ));
            assert_eq!(
                std::fs::read(fixture.storage.recovery_directory().join("checkpoint")).unwrap(),
                before
            );
            assert!(
                fixture
                    .storage
                    .recovery_directory()
                    .join("transition/intent")
                    .is_file()
            );
        })
        .await;
}
#[tokio::test]
async fn actual_partial_checkpoint_and_empty_pending_recover_but_unknown_bytes_are_retained() {
    LORE_CONTEXT
        .scope(execution(), async {
            for boundary in [
                crate::recovery::Boundary::Write,
                crate::recovery::Boundary::FileSync,
                crate::recovery::Boundary::Rename,
                crate::recovery::Boundary::DirectorySync,
            ] {
                let mut fixture = Fixture::new(Choice::New).await;
                fixture.owner.as_mut().unwrap().fault = Some(boundary);
                assert!(
                    crate::recovery::transition_completion::publish_selected(
                        fixture.owner.as_mut().unwrap(),
                        &fixture.candidate_snapshot,
                        &fixture.candidate,
                        fixture.record.candidate,
                        &fixture.record,
                        &mut memory()
                    )
                    .is_err()
                );
                let recovery = fixture.reconcile().await.unwrap();
                assert_eq!(
                    recovery.load_exact().unwrap().unwrap().identity(),
                    fixture.record.candidate
                );
                assert!(
                    !fixture
                        .storage
                        .recovery_directory()
                        .join("pending")
                        .exists()
                );
            }
            let mut fixture = Fixture::new(Choice::New).await;
            fixture.write_private("pending", b"");
            let recovery = fixture.reconcile().await.unwrap();
            assert_eq!(
                recovery.load_exact().unwrap().unwrap().identity(),
                fixture.record.candidate
            );
            drop(recovery);
            let mut fixture = Fixture::new(Choice::New).await;
            fixture.write_private("pending", b"unrelated unacknowledged bytes");
            assert!(matches!(fixture.reconcile().await, Err(Error::Pending)));
            assert_eq!(
                std::fs::read(fixture.storage.recovery_directory().join("pending")).unwrap(),
                b"unrelated unacknowledged bytes"
            );
        })
        .await;
}
#[tokio::test]
async fn valid_cleanup_marker_finishes_after_images_are_gone_and_malformed_requires_full_bundle() {
    LORE_CONTEXT
        .scope(execution(), async {
            for choice in [Choice::Old, Choice::New] {
                let mut fixture = Fixture::new(choice).await;
                let (snapshot, bytes, identity) = match choice {
                    Choice::Old => (
                        &fixture.source_snapshot,
                        &fixture.source,
                        fixture.record.source,
                    ),
                    Choice::New => (
                        &fixture.candidate_snapshot,
                        &fixture.candidate,
                        fixture.record.candidate,
                    ),
                    Choice::Mixed => unreachable!(),
                };
                crate::recovery::transition_completion::publish_selected(
                    fixture.owner.as_mut().unwrap(),
                    snapshot,
                    bytes,
                    identity,
                    &fixture.record,
                    &mut memory(),
                )
                .unwrap();
                fixture.write_private(
                    "transition.cleanup",
                    &fixture
                        .record
                        .encode(Limits::default().max_checkpoint_bytes)
                        .unwrap(),
                );
                std::fs::remove_dir_all(fixture.storage.recovery_directory().join("transition"))
                    .unwrap();
                let recovery = fixture.reconcile().await.unwrap();
                assert!(recovery.original_root().unwrap().is_some());
                assert!(
                    !fixture
                        .storage
                        .recovery_directory()
                        .join("transition.cleanup")
                        .exists()
                );
            }
            let mut fixture = Fixture::new(Choice::New).await;
            fixture.write_private("transition.cleanup", b"partial");
            let recovery = fixture.reconcile().await.unwrap();
            assert_eq!(
                recovery.load_exact().unwrap().unwrap().identity(),
                fixture.record.candidate
            );
            drop(recovery);
            let mut fixture = Fixture::new(Choice::New).await;
            fixture.write_private("transition.cleanup", b"partial");
            std::fs::remove_dir_all(fixture.storage.recovery_directory().join("transition"))
                .unwrap();
            assert!(matches!(fixture.reconcile().await, Err(Error::Corrupt)));
            assert_eq!(
                std::fs::read(
                    fixture
                        .storage
                        .recovery_directory()
                        .join("transition.cleanup")
                )
                .unwrap(),
                b"partial"
            );
        })
        .await;
}
