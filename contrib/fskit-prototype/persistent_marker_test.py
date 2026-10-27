#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Run standalone persistence contracts; never builds, registers or mounts an extension."""
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

if __name__ == "__main__":
    output = ROOT / "target/fskit-prototype"
    output.mkdir(exist_ok=True)
    executable = output / "persistent-marker-tests"
    subprocess.run(["xcrun", "swiftc", "-parse-as-library", "-warnings-as-errors",
                    "-module-cache-path", str(output / "module-cache"),
                    str(HERE / "Sources/PersistentMarker.swift"),
                    str(HERE / "Tests/PersistentMarkerTests.swift"), "-o", str(executable)], check=True)
    subprocess.run([str(executable)], check=True, timeout=90)
