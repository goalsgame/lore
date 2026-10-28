#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Test persistent FSKit callbacks without loading an extension."""
import json
from pathlib import Path
import shlex
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
PROBE = ROOT / "contrib/fskit-prototype"


def main():
    metadata = json.loads((ROOT / "target/fskit-prototype/native-libs.json").read_text())
    archive = Path(metadata["archive"]).resolve(strict=True)
    flags = [flag for flag in shlex.split(metadata["flags"]) if flag != "-lSystem"]
    with tempfile.TemporaryDirectory(prefix="lore-fskit-volume-test-") as directory:
        executable = Path(directory) / "test"
        sources = sorted((PROBE / "Sources").glob("*.swift"))
        subprocess.run([
            "xcrun", "swiftc", "-parse-as-library", "-warnings-as-errors",
            "-target", "arm64-apple-macos27.0", "-module-cache-path", str(Path(directory) / "cache"),
            "-I", str(ROOT / "lore-fskit-bridge/include"), "-D", "PROBE_VOLUME_TEST",
            *map(str, sources), str(PROBE / "Tests/PersistentVolumeTests.swift"), str(archive),
            *flags, "-o", str(executable),
        ], check=True, timeout=120)
        subprocess.run([str(executable)], check=True, timeout=45)


if __name__ == "__main__":
    main()
