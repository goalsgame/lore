#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Build a macOS 27 FSKit app bundle; never install, register, or mount it."""
import argparse
import pathlib
import plistlib
import platform
import subprocess
import re
import shlex
import os
import json

HERE = pathlib.Path(__file__).resolve().parent


def run(*args):
    subprocess.run(args, check=True)


def plist(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(value))


def rust_bridge(output, profile, fixture=False):
    """Build an explicit unwind ABI and retain its linker requirements."""
    root = HERE.parent.parent
    features = "test-fixture" if fixture else "native-bridge"
    environment = dict(os.environ, RUSTC_WRAPPER="", CARGO_INCREMENTAL="0")
    environment.setdefault("CARGO_BUILD_JOBS", "4")
    environment["CARGO_TERM_COLOR"] = "never"
    command = ["cargo", "rustc", "--locked", "--offline", "--message-format=json", "-p", "lore-fskit-bridge",
               "--profile", profile, "--features", features, "--", "--print", "native-static-libs"]
    process = subprocess.Popen(command, cwd=root, env=environment, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True)
    requirements = []
    library = None
    for line in process.stdout:
        try:
            message = json.loads(line)
        except ValueError:
            print(line, end="", flush=True)
            requirements.extend(re.findall(r"native-static-libs: (.+)", line))
            continue
        if message.get("reason") == "compiler-message":
            diagnostic = message["message"]
            rendered = diagnostic.get("rendered") or diagnostic["message"] + "\n"
            print(rendered, end="", flush=True)
            requirements.extend(re.findall(r"native-static-libs: (.+)", rendered))
        elif message.get("reason") == "compiler-artifact" and message["target"]["name"] == "lore_fskit_bridge":
            archives = [pathlib.Path(path) for path in message["filenames"] if path.endswith(".a")]
            if len(archives) == 1:
                library = archives[0].resolve(strict=True)
    if process.wait() != 0:
        raise subprocess.CalledProcessError(process.returncode, command)
    # Only link an artifact reported by this exact Cargo invocation. This covers
    # target-dir, custom profile and build-target/triple overrides, including fresh builds.
    if library is None:
        raise RuntimeError("Cargo did not report a static bridge archive")
    saved = output / ("fixture-native-libs.json" if fixture else "native-libs.json")
    if requirements:
        saved.parent.mkdir(parents=True, exist_ok=True)
        saved.write_text(json.dumps({"archive": str(library), "flags": requirements[-1]}))
    if not saved.exists():
        raise RuntimeError("Rust did not report static library linker requirements; rebuild the bridge package")
    metadata = json.loads(saved.read_text())
    if metadata["archive"] != str(library):
        raise RuntimeError("Native linker requirements do not match Cargo's reported archive")
    libraries = shlex.split(metadata["flags"])
    # swiftc already links the macOS system runtime. Repeating it produces an
    # ld warning while the remaining Rust framework requirements are necessary.
    if platform.system() == "Darwin":
        libraries = [flag for flag in libraries if flag != "-lSystem"]
    return library, libraries


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, default=HERE.parent.parent / "target/fskit-prototype")
    parser.add_argument("--identity", required=True, help="Explicit codesign identity, or - for ad-hoc build verification")
    parser.add_argument("--team", help="Matching Apple development team; required unless --identity is -")
    parser.add_argument("--rust-profile", default="fskit", help="Rust unwind profile (fskit by default; dev for local iteration)")
    args = parser.parse_args(argv)
    if not args.identity.strip():
        parser.error("--identity must not be empty")
    if args.identity == "-":
        if args.team is not None:
            parser.error("Ad-hoc signing does not accept --team")
    elif not args.team or not re.fullmatch(r"[A-Z0-9]{10}", args.team):
        parser.error("Development signing requires an explicit 10-character --team")
    return args


def main():
    args = parse_arguments()
    if platform.system() != "Darwin":
        raise SystemExit("Build with Xcode 27 or newer on macOS")
    sdk = subprocess.check_output(["xcrun", "--sdk", "macosx", "--show-sdk-version"], text=True).strip()
    if int(sdk.split(".")[0]) < 27:
        raise SystemExit("The selected Xcode must contain the macOS 27 SDK")
    output = args.output.resolve()
    app = output / "LoreFSKitProbe.app"
    extension = app / "Contents/Extensions/LoreFSKitProbe.appex"
    common = {"CFBundleVersion": "1", "CFBundleShortVersionString": "0.1", "LSMinimumSystemVersion": "27.0"}
    plist(app / "Contents/Info.plist", dict(common, CFBundleIdentifier="org.example.lore.fskit-probe",
          CFBundleExecutable="LoreFSKitProbe", CFBundleName="Lore FSKit Probe", CFBundlePackageType="APPL"))
    attributes = {
        "EXExtensionPointIdentifier": "com.apple.fskit.fsmodule",
        "FSShortName": "loreprobe", "FSSupportsGenericURLResources": True,
        "FSSupportedSchemes": ["loreprobe", "lorerevision"], "FSSupportsBlockResources": False,
        "FSSupportsPathURLs": False, "FSSupportsServerURLs": False,
        "FSRequiresSecurityScopedPathURLResources": False,
        "FSMediaTypes": {}, "FSActivateOptionSyntax": {"shortOptions": "o:"},
        "FSPersonalities": {"LoreProbe": {"FSName": "Lore FSKit Probe", "FSfileObjectsAreCaseSensitive": True}},
    }
    plist(extension / "Contents/Info.plist", dict(common, CFBundleIdentifier="org.example.lore.fskit-probe.extension",
          CFBundleExecutable="LoreFSKitExtension", CFBundleName="Lore FSKit Probe Extension",
          CFBundlePackageType="XPC!", EXAppExtensionAttributes=attributes))
    entitlements = output / "Extension.entitlements"
    plist(entitlements, {"com.apple.developer.fskit.fsmodule": True,
                         "com.apple.security.app-sandbox": True,
                         "com.apple.security.network.client": True})
    target = f"{platform.machine()}-apple-macos27.0"
    flags = ["-parse-as-library", "-target", target, "-module-cache-path", str(output / "module-cache"), "-warnings-as-errors"]
    for path in [app / "Contents/MacOS", extension / "Contents/MacOS"]:
        path.mkdir(parents=True, exist_ok=True)
    library, native_libraries = rust_bridge(output, args.rust_profile)
    run("xcrun", "swiftc", *flags, "-I", str(HERE.parent.parent / "lore-fskit-bridge/include"), "-application-extension", "-module-name", "LoreFSKitExtension",
        "-Xlinker", "-e", "-Xlinker", "_NSExtensionMain",
        *map(str, sorted((HERE / "Sources").glob("*.swift"))), str(library), *native_libraries,
        "-o", str(extension / "Contents/MacOS/LoreFSKitExtension"))
    run("xcrun", "swiftc", *flags, str(HERE / "Host.swift"), "-o", str(app / "Contents/MacOS/LoreFSKitProbe"))
    run("xcrun", "swiftc", *flags, str(HERE / "Status.swift"), "-o", str(output / "probe-status"))
    run("xcrun", "swiftc", "-target", target, "-module-cache-path", str(output / "module-cache"),
        "-warnings-as-errors", str(HERE / "RepositoryBookmark.swift"), "-o", str(output / "repository-bookmark"))
    run("xcrun", "swiftc", *flags, str(HERE / "RevisionMount.swift"), "-o", str(output / "lore-fskit-mount"))
    if args.identity != "-":
        # Check the explicitly selected team against the actual signing certificate.
        run("codesign", "--force", "--sign", args.identity, str(output / "probe-status"))
        signature = subprocess.run(["codesign", "-dv", str(output / "probe-status")],
                                   check=True, capture_output=True, text=True).stderr
        team = next((line.split("=", 1)[1] for line in signature.splitlines()
                    if line.startswith("TeamIdentifier=")), None)
        if team != args.team:
            raise SystemExit("The signing certificate does not match --team")
        extension_entitlements = plistlib.loads(entitlements.read_bytes())
        extension_entitlements["com.apple.developer.team-identifier"] = team
        extension_entitlements["com.apple.application-identifier"] = team + ".org.example.lore.fskit-probe.extension"
        plist(entitlements, extension_entitlements)
    run("codesign", "--force", "--sign", args.identity, "--entitlements", str(entitlements), str(extension))
    run("codesign", "--force", "--sign", args.identity, str(app))
    run("codesign", "--verify", "--deep", "--strict", str(app))
    (app / "Contents/Extensions").touch()
    app.touch()
    print(app)
    print("Build verified. Runtime mounting requires macOS 27 and extension approval; signing/registration is not yet runtime-validated.")


if __name__ == "__main__":
    main()
