// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use super::*;

/// Check every selection before any parent or layer can change staged metadata.
pub(super) async fn check(
    repository: Arc<RepositoryContext>,
    parent_paths: &[RelativePath],
    layers: &[layer::Layer],
    layer_jobs: &[(usize, Vec<RelativePath>)],
    options: UnstageOptions,
) -> Result<(), UnstageError> {
    if repository.file_system().supports_revision_replacement() {
        return Ok(());
    }
    // Deserializers may populate caches; preflight carries no mutable-store authority.
    let repository = Arc::new(repository.without_command_locks());
    let mut remaining = 65_536usize;
    if !parent_paths.is_empty() {
        let anchors = crate::instance::strict_anchors::read(&repository)
            .await
            .internal("Reading instance anchors before unstaging")?;
        if let Some(staged) = anchors.staged {
            paths(
                repository.clone(),
                State::deserialize(repository.clone(), anchors.current)
                    .await
                    .forward::<UnstageError>("Preflighting staged link changes")?,
                State::deserialize(repository.clone(), staged)
                    .await
                    .forward::<UnstageError>("Preflighting staged link changes")?,
                parent_paths,
                options,
                &mut remaining,
            )
            .await
            .forward::<UnstageError>("Preflighting staged link changes")?;
        }
    }
    for (index, remains) in layer_jobs {
        let layer = &layers[*index];
        if layer.staged_revision().is_none() {
            continue;
        }
        let state = layer
            .deserialize_current_and_staged(repository.clone())
            .await
            .forward::<UnstageError>("Preflighting staged link changes")?;
        let source = RelativePath::new_from_initial_path(&layer.source_path)
            .forward::<UnstageError>("Reading layer source path")?;
        let selected: Vec<_> = remains
            .iter()
            .map(|path| source.join(path.as_str()))
            .collect();
        paths(
            state.repository,
            state.state_current,
            state.state_staged,
            &selected,
            options,
            &mut remaining,
        )
        .await
        .forward::<UnstageError>("Preflighting staged link changes")?;
    }
    Ok(())
}

async fn paths(
    repository: Arc<RepositoryContext>,
    current: Arc<State>,
    staged: Arc<State>,
    selected: &[RelativePath],
    options: UnstageOptions,
    remaining: &mut usize,
) -> Result<(), UnstageError> {
    for path in selected {
        let states = repository.filter.parent_exclusion_states(path);
        if path.is_empty() {
            if !options.single_node {
                children(
                    NodeMapping::root(repository.clone(), staged.clone()),
                    current.clone(),
                    options,
                    states,
                    remaining,
                    0,
                )
                .await
                .forward::<UnstageError>("Preflighting staged link changes")?;
            }
        } else if let Some(target) = resolve_unstage_target(
            repository.clone(),
            current.clone(),
            staged.clone(),
            path,
            &LinkTracker::new(),
        )
        .await
        .forward::<UnstageError>("Preflighting staged link changes")?
        {
            node(
                NodeMapping {
                    repository: target.repository,
                    state: target.state_staged,
                    path: path.clone(),
                    node: target.node_id,
                },
                target.state_current,
                options,
                states,
                remaining,
                0,
            )
            .await
            .forward::<UnstageError>("Preflighting staged link changes")?;
        }
    }
    Ok(())
}

async fn node(
    at: NodeMapping,
    current: Arc<State>,
    options: UnstageOptions,
    states: FilterStates,
    remaining: &mut usize,
    depth: usize,
) -> Result<(), UnstageError> {
    if *remaining == 0 || depth > 64 {
        return Err(UnstageError::internal(
            "Unstage preflight exceeds traversal limits",
        ));
    }
    *remaining -= 1;
    let name = at.path.name();
    if name.is_empty() || name == "." || is_reserved_node_name(name) {
        return Ok(());
    }
    let (child_states, excluded) = at.repository.filter.child_emit_excludes_unless_forced(
        execution_context().globals().force(),
        states,
        &at.path,
        true,
        FilterMode::Full,
    );
    if excluded {
        return Ok(());
    }
    let staged_node = at
        .state
        .node(at.repository.clone(), at.node)
        .await
        .forward::<UnstageError>("Preflighting staged link changes")?;
    if !staged_node.is_staged() && !execution_context().globals().force() {
        return Ok(());
    }
    if staged_node.is_link() && (staged_node.is_staged_add() || staged_node.is_staged_delete()) {
        crate::fs::hold::require_revision_replacement(&at.repository)?;
    }
    if staged_node.is_link() {
        let current_node = current
            .node(at.repository.clone(), at.node)
            .await
            .forward::<UnstageError>("Preflighting staged link changes")?;
        if link::is_staged_pin_change(&staged_node, &current_node, at.repository.clone())
            .await
            .forward::<UnstageError>("Preflighting staged link changes")?
        {
            crate::fs::hold::require_revision_replacement(&at.repository)?;
        }
        if !options.single_node && current_node.is_link() {
            let linked = at
                .repository
                .to_link_context(staged_node.linked_node().repository)
                .await;
            let staged = State::deserialize(linked.clone(), staged_node.address.hash)
                .await
                .forward::<UnstageError>("Preflighting staged link changes")?;
            let current = State::deserialize(linked.clone(), current_node.address.hash)
                .await
                .forward::<UnstageError>("Preflighting staged link changes")?;
            Box::pin(children(
                NodeMapping {
                    repository: linked,
                    state: staged,
                    path: at.path,
                    node: staged_node.child,
                },
                current,
                options,
                child_states,
                remaining,
                depth + 1,
            ))
            .await
            .forward::<UnstageError>("Preflighting staged link changes")?;
        }
    } else if staged_node.is_directory() && !options.single_node {
        Box::pin(children(
            at,
            current,
            options,
            child_states,
            remaining,
            depth + 1,
        ))
        .await
        .forward::<UnstageError>("Preflighting staged link changes")?;
    }
    Ok(())
}

async fn children(
    at: NodeMapping,
    current: Arc<State>,
    options: UnstageOptions,
    states: FilterStates,
    remaining: &mut usize,
    depth: usize,
) -> Result<(), UnstageError> {
    let mut children =
        StateNodeChildrenWithNameIterator::new(at.state.clone(), at.repository.clone(), at.node)
            .await
            .forward::<UnstageError>("Preflighting staged link changes")?;
    while let Some((id, _, name)) = children
        .next()
        .await
        .forward::<UnstageError>("Preflighting staged link changes")?
    {
        Box::pin(node(
            NodeMapping {
                repository: at.repository.clone(),
                state: at.state.clone(),
                path: at.path.join(name),
                node: id,
            },
            current.clone(),
            options,
            states,
            remaining,
            depth,
        ))
        .await
        .forward::<UnstageError>("Preflighting staged link changes")?;
    }
    Ok(())
}
