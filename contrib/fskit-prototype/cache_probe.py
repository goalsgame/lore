#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Diagnose an already-mounted disposable fixture; never installs or mounts it."""
import argparse
import contextlib
import fcntl
import json
import mmap
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import uuid

from mount_test import STATS, STATUS, getxattr, setxattr, switch

CACHE = "user.lore.prototype.cache"
ITEMS = {"root": 1, "file": 2, "directory": 7, "symlink": 8}
MODES = {"none": 0, "read": 1, "write": 2}
COHERENCY = {"none": 0, "read": 1, "through": 2, "back": 3}
ACTIONS = {"push": 0, "push-invalidate": 1, "invalidate": 2, "update": 3, "revoke": 4}


def stats(root):
    value = json.loads(getxattr(root, STATS))
    assert value.get("probe_version") == 2, "diagnostic fixture version 2 is required"
    return value


def cache_probe(root, args):
    before = stats(root)
    initial = getxattr(root, STATUS).decode()
    assert initial in ("ready:0", "ready:1"), initial
    if args.switch:
        assert initial == "ready:0", "the switch probe requires a fresh revision-0 fixture"
    target = {"root": root, "file": root / "value.txt", "directory": root / "kind", "symlink": root / "target"}[args.item]
    if args.item == "directory":
        assert initial == "ready:1", "mount with revision=1 for the directory probe"
        os.listdir(target)
    elif args.item == "symlink":
        os.readlink(target)
    elif args.item == "file":
        target.read_bytes()
    elif not args.negative_only:
        os.listdir(root)
    with contextlib.ExitStack() as stack:
        if args.open_fd:
            assert args.item != "symlink", "symlink descriptors are a separate platform-specific probe"
            fd = os.open(target, os.O_RDONLY)
            stack.callback(os.close, fd)
        missing = root / "appeared.txt"
        first = second = None
        if args.switch:
            assert not missing.exists()
            first = stats(root)
            assert not missing.exists()
            second = stats(root)
            switch(root, 1)
        token = uuid.uuid4().hex
        command = {"token": token, "item": ITEMS[args.item], "mode": MODES[args.mode],
                   "coherency": COHERENCY[args.coherency], "action": ACTIONS[args.action],
                   "delay_ms": args.delay_ms, "allow_revoke": args.allow_revoke}
        origin = root if args.origin == "root" else root / "value.txt"
        setxattr(origin, CACHE, json.dumps(command).encode())
        deadline = time.monotonic() + 10
        while True:
            after = stats(root)
            result = after["diagnostic"]
            if result.get("token") == token and result.get("status") != "pending":
                break
            assert time.monotonic() < deadline, "cache control did not complete"
            time.sleep(0.005)
        visible = missing.exists() if args.switch else None
        final = stats(root)
        return {"kind": "cache-diagnostic", "os": platform.mac_ver()[0], "initial": initial,
                "request": command, "control_origin": args.origin, "retained_fd": args.open_fd,
                "namespace_warmup": "negative-only" if args.negative_only else "target",
                "negative_lookup_samples": None if first is None else [
                    {key: sample[key] for key in ("lookups", "lookup_misses")}
                    for sample in (first, second)],
                "negative_was_cached": first is not None and first["lookup_misses"] == second["lookup_misses"],
                "added_visible": visible, "result": result, "before": before, "after": final}


def sync_probe(root):
    initial = stats(root)
    results = []
    with (root / "edit.txt").open("r+b", buffering=0) as handle:
        for label, operation in [("fsync", lambda: os.fsync(handle.fileno())),
                                 ("F_FULLFSYNC", lambda: fcntl.fcntl(handle.fileno(), 51))]:
            handle.seek(0)
            handle.write(b"sync")
            before = stats(root)
            error = None
            try:
                operation()
            except OSError as failure:
                error = {"errno": failure.errno, "message": str(failure)}
            after = stats(root)
            results.append({"operation": label, "error": error, "before": before, "after": after})
        before = stats(root)
        error = None
        try:
            with mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_WRITE) as mapping:
                mapping[:4] = b"maps"
                mapping.flush()
        except OSError as failure:
            error = {"errno": failure.errno, "message": str(failure)}
        results.append({"operation": "msync", "error": error, "before": before, "after": stats(root)})
    directory = os.open(root, os.O_RDONLY)
    try:
        before = stats(root)
        error = None
        try:
            os.fsync(directory)
        except OSError as failure:
            error = {"errno": failure.errno, "message": str(failure)}
        results.append({"operation": "directory-fsync", "error": error, "before": before, "after": stats(root)})
    finally:
        os.close(directory)
    return {"kind": "sync-diagnostic", "os": platform.mac_ver()[0], "initial": initial,
            "results": results, "durability_qualified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mountpoint", type=Path)
    parser.add_argument("--kind", choices=("cache", "sync"), default="cache")
    parser.add_argument("--item", choices=ITEMS, default="root")
    parser.add_argument("--mode", choices=MODES, default="none")
    parser.add_argument("--coherency", choices=COHERENCY, default="none")
    parser.add_argument("--action", choices=ACTIONS, default="invalidate")
    parser.add_argument("--origin", choices=("root", "file"), default="root")
    parser.add_argument("--delay-ms", type=int, choices=(0, 100, 250), default=0)
    parser.add_argument("--open-fd", action="store_true")
    parser.add_argument("--switch", action="store_true")
    parser.add_argument("--negative-only", action="store_true",
                        help="With --switch --item root, warm only the missing path, without enumeration")
    parser.add_argument("--allow-revoke", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.negative_only and (args.kind != "cache" or not args.switch or args.item != "root"):
        parser.error("--negative-only requires --kind cache --switch --item root")
    if args.action == "revoke" and not args.allow_revoke:
        parser.error("revoke requires explicit --allow-revoke")
    if not args.worker:
        subprocess.run([sys.executable, __file__, *sys.argv[1:], "--worker"], check=True, timeout=45)
        return
    root = args.mountpoint.resolve(strict=True)
    value = sync_probe(root) if args.kind == "sync" else cache_probe(root, args)
    print(json.dumps(value, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
