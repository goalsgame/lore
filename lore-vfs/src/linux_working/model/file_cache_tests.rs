// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

async fn restored_fixture() -> Fixture {
    let source = super::capture_tests::linked_fixture().await.0;
    let reader = source.model.open(source.entry.inode, READ).await.unwrap();
    source.model.fsync(reader, false).await.unwrap();
    source.model.release(reader).await.unwrap();
    let root = source
        .model
        .checkpoint
        .test_snapshot()
        .unwrap()
        .unwrap()
        .original_root;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory = Directory::new(&format!(
        "cache-restored-{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let recovery = Recovery::open(directory.0.join("recovery"), Default::default()).unwrap();
    std::fs::copy(
        source._directory.0.join("recovery/checkpoint"),
        directory.0.join("recovery/checkpoint"),
    )
    .unwrap();
    let stores = Arc::new(Stores::default());
    let model = Model::restore(
        source.model.namespace.backend().unwrap(),
        root,
        source.gate.clone(),
        Default::default(),
        stores.clone(),
        Arc::new(StorageBudget::new(4096)),
        recovery,
        Default::default(),
    )
    .await
    .unwrap();
    assert!(model.files.lock().await.is_empty());
    assert!(
        model
            .namespace
            .materialized_file(source.entry.inode)
            .await
            .unwrap()
            .is_some()
    );
    Fixture {
        model,
        stores,
        gate: source.gate.clone(),
        scope: source.scope,
        target: source.target,
        entry: source.entry.clone(),
        _directory: directory,
    }
}

#[tokio::test]
async fn restored_missing_unchanged_cell_growth_invalidates_candidate_epoch() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = restored_fixture().await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            assert!(resources.files.is_empty());
            let candidate = resources.into_owned_candidate().await.unwrap();
            assert!(!candidate.files.contains_key(&f.entry.inode));
            let before = f.model.namespace.mutation_epoch().unwrap();
            let entry = f.model.namespace.getattr(f.entry.inode).await.unwrap();
            let generation = f.model.namespace.snapshot(4096).await.unwrap().generation;
            let reader = f.model.open(f.entry.inode, READ).await.unwrap();
            assert_eq!(f.model.read(reader, 0, 4).await.unwrap(), b"EDIT");
            assert!(f.model.namespace.mutation_epoch().unwrap() > before);
            assert_eq!(
                f.model.namespace.getattr(f.entry.inode).await.unwrap(),
                entry
            );
            assert_eq!(
                f.model.namespace.snapshot(4096).await.unwrap().generation,
                generation
            );
            assert!(candidate.state.validate_source().await.is_err());
            f.model.release(reader).await.unwrap();
            drop(candidate);
            drop(plan);
        })
        .await;
}

#[tokio::test]
async fn seal_blocks_new_cells_before_store_attempt_but_shared_empty_cell_can_fill() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = restored_fixture().await;
            let plan = f.plan().await;
            let candidate = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            let stores = f.stores.count.load(Ordering::Acquire);
            let witness = candidate.state.test_witness();
            let seal = f
                .model
                .namespace
                .seal_metadata(candidate.state.test_freeze(), witness)
                .await
                .unwrap();
            assert!(matches!(
                f.model.open(f.entry.inode, READ).await,
                Err(crate::linux_working::model::Error::Core(core::Error::Busy))
            ));
            assert!(!f.model.files.lock().await.contains_key(&f.entry.inode));
            assert_eq!(f.stores.count.load(Ordering::Acquire), stores);
            drop(seal);
            drop(candidate);
            drop(plan);
            let cell = f.model.cache_cell(f.entry.inode).await.unwrap();
            assert!(cell.lock().await.is_none());
            let plan = f.plan().await;
            let candidate = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            assert!(Arc::ptr_eq(&cell, &candidate.files[&f.entry.inode]));
            let epoch = f.model.namespace.mutation_epoch().unwrap();
            let seal = f
                .model
                .namespace
                .seal_metadata(
                    candidate.state.test_freeze(),
                    candidate.state.test_witness(),
                )
                .await
                .unwrap();
            let reader = f.model.open(f.entry.inode, READ).await.unwrap();
            let actual = f.model.file_handle(reader).unwrap();
            assert!(Arc::ptr_eq(
                cell.lock().await.as_ref().unwrap(),
                &actual.file
            ));
            assert!(Arc::ptr_eq(
                candidate.files[&f.entry.inode]
                    .lock()
                    .await
                    .as_ref()
                    .unwrap(),
                &actual.file
            ));
            assert_eq!(f.model.namespace.mutation_epoch().unwrap(), epoch);
            assert_eq!(f.stores.count.load(Ordering::Acquire), stores);
            assert_eq!(f.model.read(reader, 0, 4).await.unwrap(), b"EDIT");
            f.model.release(reader).await.unwrap();
            drop(seal);
            drop(candidate);
            drop(plan);
        })
        .await;
}

#[tokio::test]
async fn concurrent_population_keeps_one_file_and_append_mutex() {
    LORE_CONTEXT.scope(execution(), async {
        let f = restored_fixture().await;
        let before = f.model.namespace.mutation_epoch().unwrap();
        let cell = f.model.cache_cell(f.entry.inode).await.unwrap();
        assert_eq!(f.model.namespace.mutation_epoch().unwrap(), before + 1);
        let hold = cell.lock().await;
        let mut both = Box::pin(async { tokio::join!(f.model.file(f.entry.inode), f.model.file(f.entry.inode)) });
        tokio::select! { _=tokio::task::yield_now()=>{}, _=&mut both=>panic!("populated a locked cell") }
        drop(hold);
        let (left, right) = both.await;
        let (left, right) = (left.unwrap(), right.unwrap());
        assert!(Arc::ptr_eq(&left, &right));
        assert!(std::ptr::eq(&left.mutations, &right.mutations));
        assert!(Arc::ptr_eq(cell.lock().await.as_ref().unwrap(), &left));
        assert_eq!(f.model.namespace.mutation_epoch().unwrap(), before + 1);
    }).await;
}

#[tokio::test]
async fn cancelling_population_keeps_announced_empty_cell_for_retry() {
    LORE_CONTEXT.scope(execution(), async {
        let f = restored_fixture().await;
        let before = f.model.namespace.mutation_epoch().unwrap();
        *f.gate.pause_after_cell.lock().unwrap() = Some(CellPause { owner: Arc::downgrade(&f.model), inode: f.entry.inode });
        let mut population = Box::pin(f.model.file(f.entry.inode));
        tokio::select! { _=f.gate.read_entered.notified()=>{}, _=&mut population=>panic!("population did not pause") }
        let cell = f.model.files.lock().await[&f.entry.inode].clone();
        assert_eq!(f.model.namespace.mutation_epoch().unwrap(), before + 1);
        drop(population);
        assert!(cell.lock().await.is_none());
        *f.gate.pause_after_cell.lock().unwrap() = None;
        let stores = f.stores.count.load(Ordering::Acquire);
        let actual = f.model.file(f.entry.inode).await.unwrap();
        assert!(Arc::ptr_eq(cell.lock().await.as_ref().unwrap(), &actual));
        assert_eq!(f.model.namespace.mutation_epoch().unwrap(), before + 1);
        assert_eq!(f.stores.count.load(Ordering::Acquire), stores);
    }).await;
}

#[tokio::test]
async fn cache_insertion_epoch_exhaustion_and_poison_are_failclosed() {
    LORE_CONTEXT
        .scope(execution(), async {
            for poisoned in [false, true] {
                let f = restored_fixture().await;
                let backend = f.model.namespace.backend().unwrap();
                let backend = backend
                    .as_any()
                    .unwrap()
                    .downcast_ref::<RevisionBackend>()
                    .unwrap();
                let gate = backend.shared_mutation_gate();
                if poisoned {
                    gate.test_poison();
                } else {
                    gate.test_set_epoch(u64::MAX);
                }
                let stores = f.stores.count.load(Ordering::Acquire);
                assert!(f.model.cache_cell(f.entry.inode).await.is_err());
                assert!(f.model.files.lock().await.is_empty());
                assert_eq!(f.stores.count.load(Ordering::Acquire), stores);
            }
        })
        .await;
}
