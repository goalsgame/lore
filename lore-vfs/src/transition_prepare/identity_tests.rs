// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

fn key(version: u64, node: u32, exposure: u64) -> NodeKey {
    NodeKey {
        repository: RepositoryId::from([217; 16]),
        revision: Address::zero_context_hash(Hash::from_u64(version)),
        node,
        exposure,
    }
}
fn entry(path: &str, identity: Option<NodeKey>, anchor: NodeKey) -> Metadata {
    Metadata {
        path: path.to_owned(),
        summary: NodeSummary {
            kind: Kind::File,
            size: 8,
            executable: false,
            immutable: Some(Address::zero_context_hash(Hash::from_u64(123))),
            link: None,
        },
        identity,
        node: anchor.node,
        flags: 0,
        local: None,
        revision: anchor.revision,
        uncertain: false,
    }
}
fn image(entries: Vec<Metadata>) -> Image {
    entries
        .into_iter()
        .map(|entry| (entry.path.to_lowercase(), entry))
        .collect()
}

#[test]
fn reused_raw_ids_in_other_versions_repositories_or_exposures_are_not_renames() {
    let source = key(1, 7, 3);
    let origin = image(vec![entry("old", Some(source), source)]);
    let mut foreign = source;
    foreign.repository = RepositoryId::from([218; 16]);
    for other in [key(2, 7, 3), key(1, 7, 4), foreign] {
        let local = image(vec![entry("new", Some(other), other)]);
        let mut budget = Budget::new(Limits::default()).unwrap();
        assert!(
            rename_endpoints(&origin, &local, &origin, &mut budget)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn same_full_key_proves_a_rename_and_preserves_case_rename_classification() {
    let source = key(1, 7, 3);
    let origin = image(vec![entry("old", Some(source), source)]);
    let renamed = image(vec![entry("new", Some(source), source)]);
    let mut budget = Budget::new(Limits::default()).unwrap();
    assert_eq!(
        rename_endpoints(&origin, &renamed, &origin, &mut budget).unwrap(),
        std::collections::BTreeSet::from(["old".to_owned(), "new".to_owned()])
    );
    let renamed = image(vec![entry("OLD", Some(source), source)]);
    assert_eq!(
        rename_endpoints(&origin, &renamed, &origin, &mut budget).unwrap(),
        std::collections::BTreeSet::from(["old".to_owned()])
    );
}

#[test]
fn local_authority_anchor_is_not_the_removed_nodes_identity() {
    let anchor = key(1, 7, 3);
    let mut removed = entry("removed", Some(anchor), anchor);
    removed.summary.kind = Kind::Directory;
    removed.summary.size = 0;
    removed.summary.immutable = None;
    let origin = image(vec![removed]);
    let mut new = entry("local", None, anchor);
    new.local = Some((42, 1));
    new.summary.immutable = None;
    let local = image(vec![new]);
    let mut budget = Budget::new(Limits::default()).unwrap();
    assert!(
        rename_endpoints(&origin, &local, &origin, &mut budget)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn same_content_different_version_is_semantically_equal_without_rename_identity() {
    let old = key(1, 7, 3);
    let new = key(2, 7, 3);
    let before = image(vec![entry("file", Some(old), old)]);
    let after = image(vec![entry("file", Some(new), new)]);
    let mut budget = Budget::new(Limits::default()).unwrap();
    assert!(
        differences(&before, &after, false, &mut budget)
            .unwrap()
            .is_empty()
    );
    assert!(
        rename_endpoints(&before, &after, &before, &mut budget)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn exact_key_lookup_rejects_foreign_version_and_ambiguous_duplicate_identity() {
    let one = key(1, 7, 3);
    let two = key(2, 7, 3);
    let entries = image(vec![
        entry("one", Some(one), one),
        entry("two", Some(two), two),
    ]);
    let mut budget = Budget::new(Limits::default()).unwrap();
    assert_eq!(
        find_key(&entries, &one, &mut budget).unwrap().unwrap().path,
        "one"
    );
    assert_eq!(
        find_key(&entries, &two, &mut budget).unwrap().unwrap().path,
        "two"
    );
    assert!(
        find_key(&entries, &key(3, 7, 3), &mut budget)
            .unwrap()
            .is_none()
    );
    assert!(
        find_key(&entries, &key(1, 7, 4), &mut budget)
            .unwrap()
            .is_none()
    );
    let duplicate = image(vec![
        entry("one", Some(one), one),
        entry("alias", Some(one), one),
    ]);
    assert!(matches!(
        find_key(&duplicate, &one, &mut budget),
        Err(core::Error::InvalidInput)
    ));
    let mut tiny = Budget::new(Limits {
        max_comparisons: 1,
        ..Limits::default()
    })
    .unwrap();
    assert!(matches!(
        find_key(&entries, &one, &mut tiny),
        Err(core::Error::ResourceLimit)
    ));
}
