// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Pre-swap rejection through actual owned candidates and runtime claims.
use super::*;
use crate::publication_ownership::successor::SuccessorReservation;
use crate::transition_prepare::OwnedSourceImage;

async fn prepared(
    f: &Fixture,
) -> (
    super::super::owned::UnpublishedCandidate,
    OwnedSourceImage,
    SuccessorReservation,
) {
    let plan = f.plan().await;
    let candidate = f
        .model
        .prepare_private_resources(f.layout(&plan).await)
        .await
        .unwrap()
        .into_owned_candidate()
        .await
        .unwrap();
    let reservation = candidate.reserve_successor(&f.model).await.unwrap();
    (candidate, plan.into_owned_source().unwrap(), reservation)
}

#[tokio::test]
async fn foreign_final_proof_rejects_before_namespace_model_epoch_and_allocator_changes() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let other = Fixture::new(Default::default(), 4096, b"OTHER123", false).await;
            let (mut candidate, source, reservation) = prepared(&f).await;
            let (foreign, foreign_source, foreign_reservation) = prepared(&other).await;
            let before = f.model.namespace.snapshot(4096).await.unwrap();
            let before_binding = f.model.namespace.backend().unwrap();
            let before_files = f.model.files.lock().await.clone();
            let before_unlinked = f.model.state.lock().unwrap().unlinked.clone();
            let before_generation = f.model.directory_generation.load(Ordering::Acquire);
            let before_epoch = f.model.namespace.mutation_epoch().unwrap();
            let facts = source.facts();
            let seal = f
                .model
                .namespace
                .seal_metadata(facts.freeze().pin(), facts.witness())
                .await
                .unwrap();
            async {
                let names = f.model.names.lock().await;
                let files = f.model.files.lock().await;
                let mut namespace = f
                    .model
                    .namespace
                    .prepare_install(&mut candidate.state)
                    .await
                    .unwrap();
                let (source_backend, target_backend) = namespace.backend_pair();
                namespace.check_seal(&seal).unwrap();
                let model = f.model.state.lock().unwrap();
                let mut routing = namespace.lock_routing().unwrap();
                let foreign_proof = foreign_reservation
                    .prepare_commit(
                        &other.model.ownership,
                        foreign.state.successor_issuer().unwrap(),
                    )
                    .unwrap();
                let mut epoch = seal.prepare_install(namespace.epoch()).unwrap();
                assert_eq!(
                    namespace.commit(&mut routing, &foreign_proof, &mut epoch),
                    Err(core::Error::Stale)
                );
                drop(epoch);
                drop(foreign_proof);
                let source_concrete = source_backend
                    .as_any()
                    .unwrap()
                    .downcast_ref::<RevisionBackend>()
                    .unwrap();
                let target_concrete = target_backend
                    .as_any()
                    .unwrap()
                    .downcast_ref::<RevisionBackend>()
                    .unwrap();
                let proof = reservation
                    .prepare_commit(&f.model.ownership, namespace.issuer())
                    .unwrap();
                let allocators = source_concrete
                    .prepare_install_owners(target_concrete, &proof)
                    .unwrap();
                drop(allocators);
                drop(proof);
                // Release every final guard before any displaced/prepared resources.
                drop(routing);
                drop(model);
                drop(namespace);
                drop(files);
                drop(names);
            }
            .await;
            assert_eq!(f.model.namespace.mutation_epoch().unwrap(), before_epoch);
            assert!(Arc::ptr_eq(
                &before_binding,
                &f.model.namespace.backend().unwrap()
            ));
            let current_files = f.model.files.lock().await;
            assert_eq!(current_files.len(), before_files.len());
            for (inode, cell) in &before_files {
                assert!(Arc::ptr_eq(cell, &current_files[inode]));
            }
            drop(current_files);
            assert_eq!(f.model.state.lock().unwrap().unlinked, before_unlinked);
            assert_eq!(
                f.model.directory_generation.load(Ordering::Acquire),
                before_generation
            );
            drop(candidate);
            drop(seal);
            drop(source);
            drop(reservation);
            drop(foreign);
            drop(foreign_source);
            drop(foreign_reservation);
            let after = f.model.namespace.snapshot(4096).await.unwrap();
            assert_eq!(before.original_root, after.original_root);
            assert_eq!(before.generation, after.generation);
            assert_eq!(before.next_inode, after.next_inode);
            assert_eq!(before.active_underlays, after.active_underlays);
            assert_eq!(before.backend_state, after.backend_state);
            assert_eq!(before.baselines, after.baselines);
            assert_eq!(before.files, after.files);
            assert_eq!(before.nodes.len(), after.nodes.len());
            for (left, right) in before.nodes.iter().zip(&after.nodes) {
                assert_eq!(left.entry, right.entry);
                assert_eq!(left.attached, right.attached);
                assert_eq!(left.names, right.names);
                assert_eq!(left.receipt, right.receipt);
            }
            f.model.live().unwrap();
            other.model.live().unwrap();
            f.model.shutdown().await.unwrap();
            other.model.shutdown().await.unwrap();
        })
        .await;
}

async fn install(f: &Fixture, plan: PreparedTransition) {
    let mut candidate = f
        .model
        .prepare_private_resources(f.layout(&plan).await)
        .await
        .unwrap()
        .into_owned_candidate()
        .await
        .unwrap();
    let reservation = candidate.reserve_successor(&f.model).await.unwrap();
    let source = plan.into_owned_source().unwrap();
    let facts = source.facts();
    let seal = f
        .model
        .namespace
        .seal_metadata(facts.freeze().pin(), facts.witness())
        .await
        .unwrap();
    candidate
        .install_in_place(&seal, &reservation)
        .await
        .unwrap();
    // These drop outside every installation lock; read handles retain old routes.
    drop(candidate);
    drop(seal);
    drop(source);
    drop(reservation);
}

#[tokio::test]
async fn model_installs_successive_views_with_shared_bytes_isolated_and_old_handles_pinned() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let old_file = f.model.open(f.entry.inode, READ).await.unwrap();
            let old_directory = f.model.opendir(1).await.unwrap();
            let first_generation = f.model.directory_generation.load(Ordering::Acquire);
            install(&f, f.plan().await).await;
            f.model.live().unwrap();
            assert_eq!(
                f.model.directory_generation.load(Ordering::Acquire),
                first_generation + 1
            );
            let current = f.model.lookup(1, b"file").await.unwrap().entry;
            assert_ne!(current.inode, f.entry.inode);
            let intermediate_file = f.model.open(current.inode, READ).await.unwrap();
            let current_directory = f.model.opendir(1).await.unwrap();
            assert_eq!(f.model.read(old_file, 0, 100).await.unwrap(), b"DIRTY123");
            assert_eq!(
                f.model.read(intermediate_file, 0, 100).await.unwrap(),
                b"DIRTY123"
            );
            let old_entries = f.model.readdir(old_directory, 0, 100).await.unwrap();
            let new_entries = f.model.readdir(current_directory, 0, 100).await.unwrap();
            assert_eq!(
                old_entries.entries[0].1.entry.authority.revision,
                f.scope.revision
            );
            assert_eq!(new_entries.entries[0].1.entry.authority.revision, f.target);
            // Revoking only the current target keeps old read and directory handles
            // on their original, independently checked source route.
            f.gate.target_read.store(true, Ordering::Release);
            assert_eq!(f.model.read(old_file, 0, 100).await.unwrap(), b"DIRTY123");
            assert!(f.model.read(intermediate_file, 0, 100).await.is_err());
            assert!(f.model.readdir(old_directory, 0, 100).await.is_ok());
            assert!(f.model.readdir(current_directory, 0, 100).await.is_err());
            f.gate.target_read.store(false, Ordering::Release);
            let writer = f.model.open(current.inode, WRITE).await.unwrap();
            f.model.write(writer, 0, b"NEWVIEW!").await.unwrap();
            f.model.release(writer).await.unwrap();
            assert_eq!(f.model.read(old_file, 0, 100).await.unwrap(), b"DIRTY123");
            assert_eq!(
                f.model.read(intermediate_file, 0, 100).await.unwrap(),
                b"NEWVIEW!"
            );
            let plan = transition_prepare::prepare(
                f.model.namespace.clone(),
                f.scope,
                f.scope.revision,
                Options {
                    local: LocalPolicy::KeepLocal,
                    ..Default::default()
                },
                Default::default(),
            )
            .await
            .unwrap();
            install(&f, plan).await;
            f.model.live().unwrap();
            assert_eq!(
                f.model.directory_generation.load(Ordering::Acquire),
                first_generation + 2
            );
            let latest = f.model.lookup(1, b"file").await.unwrap().entry;
            assert_ne!(latest.inode, current.inode);
            let writer = f.model.open(latest.inode, WRITE).await.unwrap();
            f.model.write(writer, 0, b"FINAL!!!").await.unwrap();
            f.model.fsync(writer, false).await.unwrap();
            f.model.release(writer).await.unwrap();
            let reader = f.model.open(latest.inode, READ).await.unwrap();
            assert_eq!(f.model.read(reader, 0, 100).await.unwrap(), b"FINAL!!!");
            assert_eq!(
                f.model.read(intermediate_file, 0, 100).await.unwrap(),
                b"NEWVIEW!"
            );
            assert_eq!(f.model.read(old_file, 0, 100).await.unwrap(), b"DIRTY123");
            f.model.release(reader).await.unwrap();
            f.model.release(intermediate_file).await.unwrap();
            f.model.release(old_file).await.unwrap();
            f.model.release(current_directory).await.unwrap();
            f.model.release(old_directory).await.unwrap();
            f.model.shutdown().await.unwrap();
        })
        .await;
}
