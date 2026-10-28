#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Preflight an experimental pinned FSKit mount; --mount explicitly invokes FSKit.

Never installs, registers, enables or provisions an extension. The extension still
needs OS-approved access to its normal Lore credential/config/content stores.
"""
import argparse
import dataclasses
import json
import os
import pathlib
import stat
import subprocess
import urllib.parse


def overlaps(first, second):
    return first == second or first in second.parents or second in first.parents


def proposed(path):
    path = pathlib.Path(path).expanduser().absolute()
    if path.is_symlink():
        raise ValueError("symbolic links are not accepted for config/cache endpoints")
    if path.exists():
        return path.resolve(strict=True)
    return path.parent.resolve(strict=True) / path.name


@dataclasses.dataclass(frozen=True)
class Plan:
    target: pathlib.Path
    config: pathlib.Path
    contents: dict
    target_identity: tuple

    @property
    def resource(self):
        return "lorerevision://mount?" + urllib.parse.urlencode({"config": str(self.config)})


def preflight(repository, revision, target, config, digest_cache=None, identity=None, offline=False):
    repository = pathlib.Path(repository).expanduser().resolve(strict=True)
    if not repository.is_dir():
        raise ValueError("repository must be an existing directory")
    raw_target = pathlib.Path(target).expanduser().absolute()
    if raw_target.is_symlink() or not raw_target.is_dir():
        raise ValueError("target must be an existing directory, not a symlink")
    target = raw_target.resolve(strict=True)
    if os.path.ismount(target):
        raise ValueError("target already contains a mounted filesystem")
    target_metadata = target.stat()
    if target_metadata.st_uid != os.getuid() or target_metadata.st_mode & 0o022:
        raise ValueError("target must belong to the caller and not be writable by others")
    if any(part.lower() in (".lore", ".urc") for part in target.parts):
        raise ValueError("target must be outside repository metadata")
    if next(target.iterdir(), None) is not None:
        raise ValueError("target must be empty, including hidden entries")
    config = proposed(config)
    if config.exists():
        raise ValueError("use a fresh bootstrap config path; existing files are never overwritten")
    parent = config.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o022:
        raise ValueError("bootstrap parent must be user-owned and not writable by others")
    if not revision.strip() or len(revision.encode("utf-8")) > 256:
        raise ValueError("an explicit revision of at most 256 UTF-8 bytes is required")
    if identity is not None and len(identity.encode("utf-8")) > 256:
        raise ValueError("identity name is too long")
    if len(str(config).encode("utf-8")) > 4096:
        raise ValueError("bootstrap config path is too long")
    protected = [repository, config]
    contents = {"repository": str(repository), "revision": revision, "offline": bool(offline)}
    if identity is not None:
        contents["identity"] = identity
    if digest_cache is not None:
        cache = proposed(digest_cache)
        if cache.exists():
            metadata = cache.stat()
            if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid() or metadata.st_mode & 0o022:
                raise ValueError("digest cache must be user-owned and not writable by others")
        protected.append(cache)
        contents["digest_cache"] = str(cache)
    if any(overlaps(target, path) for path in protected):
        raise ValueError("target must be disjoint from the repository, digest cache and bootstrap config")
    encoded = json.dumps(contents).encode("utf-8")
    if len(encoded) > 8192:
        raise ValueError("bootstrap config exceeds 8192 bytes")
    return Plan(target=target, config=config, contents=contents, target_identity=(target_metadata.st_dev, target_metadata.st_ino))


def validate_target(plan):
    metadata = plan.target.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or (metadata.st_dev, metadata.st_ino) != plan.target_identity:
        raise ValueError("target changed after preflight")
    if os.path.ismount(plan.target) or next(plan.target.iterdir(), None) is not None:
        raise ValueError("target became mounted or nonempty during preflight")


def write_config(plan):
    # Recheck immediately before publication; O_EXCL/O_NOFOLLOW preserve an
    # existing config if another process creates the name during preflight.
    validate_target(plan)
    descriptor = os.open(plan.config, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(json.dumps(plan.contents, sort_keys=True).encode("utf-8"))
        output.flush()
        os.fsync(output.fileno())


def write_bookmark(plan, helper):
    """Create a private grant for the preflighted checkout without overwriting one."""
    sidecar = pathlib.Path(str(plan.config) + ".bookmark")
    if sidecar.exists() or sidecar.is_symlink():
        raise FileExistsError("repository bookmark already exists")
    subprocess.run([str(helper), plan.contents["repository"], str(sidecar)], check=True, timeout=30)
    info = sidecar.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or
            info.st_mode & 0o077 or info.st_nlink != 1 or not 0 < info.st_size <= 16384):
        raise ValueError("repository bookmark must be a private regular file")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True, type=pathlib.Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--target", required=True, type=pathlib.Path)
    parser.add_argument("--config", required=True, type=pathlib.Path)
    parser.add_argument("--digest-cache", type=pathlib.Path)
    parser.add_argument("--identity")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--bookmark-helper", type=pathlib.Path,
                        help="create a private repository grant using this local helper before mounting")
    parser.add_argument("--mount", action="store_true", help="write private bootstrap config and invoke an already-enabled FSKit module")
    args = parser.parse_args()
    try:
        plan = preflight(args.repository, args.revision, args.target, args.config,
                         args.digest_cache, args.identity, args.offline)
        if args.mount:
            write_config(plan)
            if args.bookmark_helper is not None:
                write_bookmark(plan, args.bookmark_helper.resolve(strict=True))
            validate_target(plan)
            subprocess.run(["mount", "-r", "-F", "-t", "loreprobe", plan.resource, str(plan.target)], check=True)
        else:
            print("Preflight passed; no config written and no filesystem mounted.")
        print(plan.resource)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"revision mount failed: {error}\n")


if __name__ == "__main__":
    main()
