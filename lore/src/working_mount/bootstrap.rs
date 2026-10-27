// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Private identity records and owned ordinary recovery resources.
//! The production bootstrap factory is not selected.
#![allow(dead_code, reason = "bootstrap is not exposed by command dispatch")]
#[cfg(not(feature = "test-util"))]
mod binding;
#[cfg(feature = "test-util")]
pub mod binding;

pub(super) mod admission;
#[cfg(all(target_os = "linux", not(feature = "test-util")))]
pub(crate) mod factory;
#[cfg(feature = "test-util")]
pub mod factory;
#[cfg(any(target_os = "linux", feature = "test-util"))]
mod job;
