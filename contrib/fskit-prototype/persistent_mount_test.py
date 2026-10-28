#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Check acknowledged probe writes across unmount and remount on macOS."""
import os
import json
from pathlib import Path
import subprocess
import tempfile
import uuid
from mount_test import getxattr


def main():
    run = str(uuid.uuid4())
    with tempfile.TemporaryDirectory(prefix="lore-fskit-persistent-") as directory:
        root = Path(directory)
        mounted = False
        try:
            def mount(create):
                nonlocal mounted
                resource = f"loreprobe://fixture?cache=none&persistent={run}&create={int(create)}"
                subprocess.run(["mount", "-F", "-t", "loreprobe", resource, str(root)], check=True, timeout=30)
                mounted = True

            def unmount():
                nonlocal mounted
                subprocess.run(["umount", str(root)], check=True, timeout=30)
                mounted = False

            mount(True)
            path = root / "edit.txt"
            with path.open("wb", buffering=0) as file:
                before = json.loads(getxattr(root, "user.lore.prototype.stats"))["persistent_sequence"]
                file.write(b"persisted FSKit write\n")
                after = json.loads(getxattr(root, "user.lore.prototype.stats"))["persistent_sequence"]
                if after <= before:
                    raise AssertionError("write returned before durable publication")
                os.fsync(file.fileno())
            unmount()
            mount(False)
            assert path.read_bytes() == b"persisted FSKit write\n"
            with path.open("r+b") as file:
                file.truncate(9)
                file.flush()
                os.fsync(file.fileno())
            unmount()
            mount(False)
            assert path.read_bytes() == b"persisted"
            unmount()
            print("Persistent mounted writes and truncate survived remount")
        finally:
            if mounted:
                subprocess.run(["umount", str(root)], check=True, timeout=30)


if __name__ == "__main__":
    main()
