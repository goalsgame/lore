// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Opt-in test-only access to the ordinary private preparation and binding path.
//! These helpers do not install state, publish anchors or bypass authorization.

use std::sync::Arc;

use lore_base::types::Address;

use crate::core::Backend;
use crate::core::NodeKey;
use crate::namespace::Namespace;
use crate::overlay::AccessGate;
use crate::transition_prepare;

pub struct CompletePolicy {
    inner: crate::namespace::complete_binding::PreparedPolicyBinding,
}
impl CompletePolicy {
    pub fn backend(&self) -> Arc<dyn Backend> {
        self.inner.backend()
    }
    pub fn policy(&self) -> Arc<dyn AccessGate> {
        self.inner.policy()
    }
}

pub async fn prepare_complete_policy(
    namespace: Arc<Namespace>,
    scope: NodeKey,
    target: Address,
    options: transition_prepare::Options,
    limits: transition_prepare::Limits,
) -> transition_prepare::Result<CompletePolicy> {
    let plan =
        transition_prepare::prepare(namespace.clone(), scope, target, options, limits).await?;
    let prepared = plan.prepare_complete_backend().await?;
    let inner = namespace.bind_complete_backend(prepared)?;
    drop(plan);
    Ok(CompletePolicy { inner })
}
