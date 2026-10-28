#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Test acknowledged probe bytes across an isolated extension-process crash."""
import os
import json
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import uuid
from mount_test import getxattr

ROOT = Path(__file__).resolve().parents[2]
EXTENSION = (ROOT / "target/fskit-prototype/diagnostic-build/LoreFSKitProbe.app/Contents/Extensions/"
             "LoreFSKitProbe.appex/Contents/MacOS/LoreFSKitExtension").resolve(strict=True)


def mounts():
    return subprocess.check_output(["mount"], text=True, timeout=10).splitlines()


def process(pid):
    return subprocess.run(["ps", "-p", str(pid), "-o", "lstart=", "-o", "comm="],
                          text=True, capture_output=True, timeout=10, check=False).stdout.strip()


def main():
    run = str(uuid.uuid4())
    with tempfile.TemporaryDirectory(prefix="lore-fskit-crash-") as directory:
        root = Path(directory).resolve()
        resource = f"loreprobe://fixture?cache=none&persistent={run}"
        mounted = False
        try:
            subprocess.run(["mount", "-F", "-t", "loreprobe", resource + "&create=1", str(root)], check=True, timeout=30)
            mounted = True
            path = root / "edit.txt"
            expected = b"acknowledged before process crash\n"
            with path.open("wb", buffering=0) as file:
                before = json.loads(getxattr(root, "user.lore.prototype.stats"))["persistent_sequence"]
                file.write(expected)
                acknowledged = json.loads(getxattr(root, "user.lore.prototype.stats"))["persistent_sequence"]
                if acknowledged <= before:
                    raise AssertionError("write returned before durable publication")
                os.fsync(file.fileno())
            lines = [line for line in mounts() if "fskit" in line.lower()]
            if len(lines) != 1 or f" on {root} " not in lines[0] or resource not in lines[0]:
                raise RuntimeError("refusing crash: another FSKit mount is present or the fixture is not isolated")
            output = subprocess.check_output(["ps", "-axo", "pid=,comm="], text=True, timeout=10)
            candidates = [int(line.strip().split(None, 1)[0]) for line in output.splitlines()
                          if len(line.strip().split(None, 1)) == 2 and line.strip().split(None, 1)[1] == str(EXTENSION)]
            if len(candidates) != 1:
                raise RuntimeError("refusing crash: cannot identify a unique fixture process")
            pid = candidates[0]
            identity = process(pid)
            if not identity.endswith(str(EXTENSION)):
                raise RuntimeError("refusing crash: fixture identity changed")
            lines = [line for line in mounts() if "fskit" in line.lower()]
            if len(lines) != 1 or f" on {root} " not in lines[0] or resource not in lines[0] or process(pid) != identity:
                raise RuntimeError("refusing crash: fixture is no longer isolated")
            os.kill(pid, signal.SIGKILL)
            deadline = time.monotonic() + 10
            while process(pid) == identity:
                if time.monotonic() >= deadline:
                    raise RuntimeError("fixture process did not exit")
                time.sleep(0.05)
            if any(f" on {root} " in line for line in mounts()):
                subprocess.run(["umount", "-f", str(root)], check=True, timeout=30)
            mounted = False
            subprocess.run(["mount", "-F", "-t", "loreprobe", resource + "&create=0", str(root)], check=True, timeout=30)
            mounted = True
            before = json.loads(getxattr(root, "user.lore.prototype.stats"))
            if path.read_bytes() != expected:
                raise AssertionError("acknowledged bytes did not survive extension process restart")
            after = json.loads(getxattr(root, "user.lore.prototype.stats"))
            if after["persistent_sequence"] != acknowledged or after["reads"] <= before["reads"]:
                raise AssertionError("remount did not read the acknowledged generation through FSKit")
            subprocess.run(["umount", str(root)], check=True, timeout=30)
            mounted = False
            print("Acknowledged probe bytes survived extension-process termination and remount")
        finally:
            if mounted and any(f" on {root} " in line for line in mounts()):
                subprocess.run(["umount", "-f", str(root)], check=True, timeout=30)


if __name__ == "__main__":
    main()
