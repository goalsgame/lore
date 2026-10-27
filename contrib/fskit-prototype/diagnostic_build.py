#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Compile diagnostic Swift against a preserved bridge; no Cargo or provisioning."""
import argparse
import hashlib
import json
from pathlib import Path
import plistlib
import re
import shlex
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--identity", required=True, help="An existing development signing identity")
    parser.add_argument("--team", required=True, help="Apple development team matching the preserved profile and signing identity")
    parser.add_argument("--replace-fixture", action="store_true", help="Replace only the existing fixture app after verification")
    args = parser.parse_args(argv)
    if not args.identity.strip() or args.identity == "-":
        parser.error("Diagnostics require an explicit development --identity")
    if not re.fullmatch(r"[A-Z0-9]{10}", args.team):
        parser.error("--team must be a 10-character Apple development team identifier")
    return args


def main():
    args = parse_arguments()
    baseline = args.baseline.resolve(strict=True)
    output = ROOT / "target/fskit-prototype/diagnostic-build"
    output.mkdir(exist_ok=True)
    archive = baseline / "liblore_fskit_bridge.a"
    expected = (baseline / "SHA256SUMS").read_text().splitlines()[0].split()[0]
    with archive.open("rb") as source:
        assert hashlib.file_digest(source, "sha256").hexdigest() == expected
    app = output / "LoreFSKitProbe.app"
    subprocess.run(["ditto", str(baseline / "LoreFSKitProbe.app"), str(app)], check=True)
    extension = app / "Contents/Extensions/LoreFSKitProbe.appex"
    flags = [flag for flag in shlex.split(json.loads((baseline / "native-libs.json").read_text())["flags"]) if flag != "-lSystem"]
    subprocess.run(["xcrun", "swiftc", "-parse-as-library", "-target", "arm64-apple-macos27.0",
                    "-module-cache-path", str(ROOT / "target/fskit-prototype/module-cache"),
                    "-warnings-as-errors", "-I", str(ROOT / "lore-fskit-bridge/include"),
                    "-application-extension", "-module-name", "LoreFSKitExtension",
                    "-Xlinker", "-e", "-Xlinker", "_NSExtensionMain",
                    *map(str, sorted((HERE / "Sources").glob("*.swift"))), str(archive), *flags,
                    "-o", str(extension / "Contents/MacOS/LoreFSKitExtension")], check=True)
    entitlement_data = subprocess.run(["codesign", "-d", "--entitlements", "-", "--xml", str(baseline / "LoreFSKitProbe.app/Contents/Extensions/LoreFSKitProbe.appex")], check=True, capture_output=True).stdout
    entitlements = output / "preserved.entitlements"
    preserved = plistlib.loads(entitlement_data)
    if preserved.get("com.apple.developer.team-identifier") != args.team or preserved.get("com.apple.application-identifier") != args.team + ".org.example.lore.fskit-probe.extension":
        raise SystemExit("The preserved extension entitlements do not match --team and the example bundle identifier")
    entitlements.write_bytes(plistlib.dumps(preserved))
    subprocess.run(["codesign", "--force", "--sign", args.identity, "--entitlements", str(entitlements), str(extension)], check=True)
    subprocess.run(["codesign", "--force", "--sign", args.identity, str(app)], check=True)
    subprocess.run(["codesign", "--verify", "--deep", "--strict", str(app)], check=True)
    if args.replace_fixture:
        destination = ROOT / "target/fskit-prototype/LoreFSKitProbe.app"
        subprocess.run(["ditto", str(app), str(destination)], check=True)
        destination.touch()
    print(app)


if __name__ == "__main__":
    main()
