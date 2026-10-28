#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Build and run standalone Swift/Rust contracts without loading an FSKit module."""
import subprocess
import shutil
import build


def main():
    output = build.HERE.parent.parent / "target/fskit-prototype"
    output.mkdir(parents=True, exist_ok=True)
    library, libraries = build.rust_bridge(output, "dev", fixture=True)
    # Production builds replace the Cargo archive without the test fixture; keep
    # this test binary's archive and clang module cache separate.
    fixture = output / "liblore_fskit_fixture.a"
    shutil.copy2(library, fixture)
    executable = output / "bridge-tests"
    subprocess.run(["xcrun", "swiftc", "-parse-as-library", "-warnings-as-errors",
                    "-module-cache-path", str(output / "bridge-test-module-cache"),
                    "-I", str(build.HERE.parent.parent / "lore-fskit-bridge/include"),
                    "-Xcc", "-DLORE_FSKIT_TEST_FIXTURE",
                    str(build.HERE / "Sources/RepositoryScope.swift"),
                    str(build.HERE / "Sources/RustBridge.swift"),
                    str(build.HERE / "Tests/BridgeTests.swift"), str(fixture), *libraries,
                    "-o", str(executable)], check=True)
    subprocess.run([str(executable)], check=True, timeout=30)


if __name__ == "__main__":
    main()
