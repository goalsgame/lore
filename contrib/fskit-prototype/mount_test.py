#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Check an already-mounted, disposable loreprobe filesystem. Never mounts or installs."""
import argparse
import ctypes
import errno
import hashlib
import json
import mmap
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

STATUS = "user.lore.prototype.status"
SWITCH = "user.lore.prototype.switch"
STATS = "user.lore.prototype.stats"
LIBC = ctypes.CDLL(None, use_errno=True)
LIBC.getxattr.restype = ctypes.c_ssize_t
LIBC.getxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32, ctypes.c_int]
LIBC.setxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32, ctypes.c_int]


def getxattr(path, name):
    buffer = ctypes.create_string_buffer(4096)
    count = LIBC.getxattr(os.fsencode(path), name.encode(), buffer, len(buffer), 0, 0)
    if count < 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(path))
    return buffer.raw[:count]


def setxattr(path, name, data):
    if LIBC.setxattr(os.fsencode(path), name.encode(), data, len(data), 0, 0):
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(path))


def no_digest(path):
    try:
        getxattr(path, "user.sha256")
    except OSError as error:
        assert error.errno == getattr(errno, "ENOATTR", 93), error
    else:
        raise AssertionError("A dirty file exposed a digest")


def switch(root, revision):
    setxattr(root, SWITCH, str(revision).encode())
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status = getxattr(root, STATUS).decode()
        assert not status.startswith("failed:"), status
        if status == f"ready:{revision}":
            return
        time.sleep(0.02)
    raise AssertionError("Revision transition did not acknowledge completion")


def check(root, suite="all"):
    # Validate the marker before writing anything. The writable fixture is intentionally disposable.
    assert getxattr(root, STATUS) == b"ready:0", "Use a fresh loreprobe mount"
    if suite == "sync":
        check_sync(root)
        return
    if suite == "writable":
        check_writable(root)
        return
    value = root / "value.txt"
    original = b"hello lore\n"
    changed = b"HELLO lore\n"
    before = json.loads(getxattr(root, STATS))
    assert getxattr(value, "user.sha256") == hashlib.sha256(original).digest()
    after = json.loads(getxattr(root, STATS))
    assert after["reads"] == before["reads"], "Digest lookup caused file reads"
    assert after["digest_requests"] > before["digest_requests"]
    assert not (root / "appeared.txt").exists()  # Warm a negative lookup.
    assert (root / "gone.txt").read_bytes() == b"removed\n"
    assert (root / "kind").is_file()
    assert os.readlink(root / "target") == "value.txt"
    assert value.read_bytes() == original
    with value.open("rb") as opened, mmap.mmap(opened.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
        assert mapped[:] == original
        switch(root, 1)
        assert value.read_bytes() == changed
        opened.seek(0)
        assert opened.read() == changed, "Open descriptor retained old revision data"
        assert mapped[:] == changed, "Read mapping retained old revision data"
    assert getxattr(value, "user.sha256") == hashlib.sha256(changed).digest()
    if suite == "data-cache":
        return
    assert (root / "appeared.txt").read_bytes() == b"added\n"
    assert not (root / "gone.txt").exists()
    assert (root / "kind").is_dir()
    assert os.readlink(root / "target") == "appeared.txt"
    assert set(os.listdir(root)) == {"value.txt", "edit.txt", "appeared.txt", "kind", "target"}
    switch(root, 0)
    assert value.read_bytes() == original
    assert getxattr(value, "user.sha256") == hashlib.sha256(original).digest()
    assert (root / "kind").is_file()
    assert not (root / "appeared.txt").exists()
    assert os.readlink(root / "target") == "value.txt"
    check_writable(root)
    check_sync(root)


def check_sync(root):
    before = json.loads(getxattr(root, STATS))["sync_calls"]
    with (root / "edit.txt").open("r+b", buffering=0) as handle:
        handle.write(b"sync")
        os.fsync(handle.fileno())
    after = json.loads(getxattr(root, STATS))["sync_calls"]
    assert after > before, "fsync returned without a synchronize callback; durability is unqualified"


def check_writable(root):
    value = root / "value.txt"
    edit = root / "edit.txt"
    assert getxattr(edit, "user.sha256") == hashlib.sha256(b"edit me\n").digest()
    with edit.open("r+b", buffering=0) as handle:
        no_digest(edit)  # Also protects the interval before a write/mmap callback arrives.
        handle.write(b"EDIT")
        os.fsync(handle.fileno())
    assert edit.read_bytes() == b"EDIT me\n"
    no_digest(edit)
    with edit.open("r+b", buffering=0) as handle:
        handle.truncate(3)
        os.fsync(handle.fileno())
    assert edit.read_bytes() == b"EDI"
    no_digest(edit)
    with edit.open("r+b", buffering=0) as handle, mmap.mmap(handle.fileno(), 0) as mapped:
        mapped[:] = b"map"
        no_digest(edit)
        mapped.flush()
    assert edit.read_bytes() == b"map"
    no_digest(edit)
    try:
        setxattr(value, "user.sha256", bytes(32))
    except OSError as error:
        assert error.errno in (errno.EPERM, errno.EROFS, errno.EACCES), error
    else:
        raise AssertionError("The computed digest was user-writable")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mountpoint", type=Path)
    parser.add_argument("--suite", choices=("all", "data-cache", "writable", "sync"), default="all",
                        help="Use a fresh mount for each suite; partial suites do not qualify namespace switching")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if platform.system() != "Darwin" or int(platform.mac_ver()[0].split(".")[0]) < 27:
        parser.error("Mounted tests require macOS 27; no filesystem was changed")
    if args.worker:
        root = args.mountpoint.resolve()
        check(root, args.suite)
        print(json.dumps({"result": "passed", "suite": args.suite, "os": platform.mac_ver()[0],
                          "stats": json.loads(getxattr(root, STATS))}))
    else:
        # Kernel operations can hang; put the complete suite behind an external deadline.
        subprocess.run([sys.executable, __file__, str(args.mountpoint), "--worker", "--suite", args.suite], check=True, timeout=90)


if __name__ == "__main__":
    main()
