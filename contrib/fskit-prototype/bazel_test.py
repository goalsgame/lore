#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Bazel hash workload for a fresh mounted loreprobe fixture; retains build logs."""
from pathlib import Path
import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
import mount_test as probe


def check(mount, bazel):
    # Validate the fixture before changing it through its test-only control xattr.
    assert probe.getxattr(mount, probe.STATUS) == b"ready:0", "Use a fresh loreprobe mount"
    scratch = Path(tempfile.mkdtemp(prefix="lore-fskit-bazel-"))
    workspace = scratch / "workspace"
    workspace.mkdir()
    (workspace / "WORKSPACE").write_text('workspace(name="lore_native_hash")\n')
    (workspace / "BUILD.bazel").write_text(
        'genrule(name="result", srcs=["input.txt"], outs=["result.txt"], '
        'cmd="cp $(location input.txt) $@")\n')
    (workspace / "input.txt").symlink_to(mount / "value.txt")
    print("Retained test workspace:", scratch, flush=True)
    before = json.loads(probe.getxattr(mount, probe.STATS))
    for index, expected in enumerate([b"hello lore\n", b"HELLO lore\n"]):
        if index:
            probe.switch(mount, 1)
        command = [
            str(bazel), "--batch", f"--output_user_root={scratch}/user",
            f"--output_base={scratch}/base", "--digest_function=SHA256",
            "--unix_digest_hash_attribute_name=user.sha256", "build", "//:result",
            "--enable_bzlmod=false", "--spawn_strategy=local", "--genrule_strategy=local",
            "--color=no", "--curses=no", f"--execution_log_json_file={scratch}/{index}.json",
        ]
        log_path = scratch / f"build-{index}.log"
        with log_path.open("w") as log:
            result = subprocess.run(command, cwd=workspace, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=90)
        if result.returncode:
            raise RuntimeError(log_path.read_text()[-6000:])
        assert (workspace / "bazel-bin/result.txt").read_bytes() == expected
        assert hashlib.sha256(expected).hexdigest() in (scratch / f"{index}.json").read_text()
    after = json.loads(probe.getxattr(mount, probe.STATS))
    assert after["digest_requests"] > before["digest_requests"], (before, after)
    print("PASS native Bazel raw xattr requests and same-size rebuild", before, after, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mountpoint", type=Path)
    parser.add_argument("--bazel", required=True, type=Path, help="Existing Bazel 7.5 executable")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        check(args.mountpoint.resolve(), args.bazel.resolve(strict=True))
    else:
        subprocess.run([sys.executable, __file__, str(args.mountpoint), "--bazel", str(args.bazel),
                        "--worker"], check=True, timeout=210)


if __name__ == "__main__":
    main()
