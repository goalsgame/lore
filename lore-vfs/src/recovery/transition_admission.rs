// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Ordinary recovery admission treats the fixed transition container as a
//! quarantine marker. Contents, type and completeness never imply permission to
//! repair, publish or select a mounted view. This module creates no artifacts.
use std::path::Path;

use super::Error;
use super::Result;

pub(super) fn check(directory: &Path) -> Result<()> {
    for name in ["transition", "transition.cleanup"] {
        match std::fs::symlink_metadata(directory.join(name)) {
            Ok(_) => return Err(Error::TransitionPending),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
