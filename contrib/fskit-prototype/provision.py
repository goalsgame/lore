#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Build the extension using Xcode automatic development provisioning."""
import argparse
import json
from pathlib import Path
import plistlib
import platform
import re
import shutil
import shlex
import subprocess

HERE = Path(__file__).resolve().parent


def bridge_settings(output):
    """Use the production archive and native requirements recorded by build.py."""
    metadata = json.loads((output / "native-libs.json").read_text())
    archive = Path(metadata["archive"]).resolve(strict=True)
    if archive.suffix != ".a" or not archive.is_file():
        raise RuntimeError("Run build.py first: production bridge archive is missing")
    flags = [flag for flag in shlex.split(metadata["flags"]) if flag != "-lSystem"]
    return {
        "SWIFT_INCLUDE_PATHS": [str(HERE.parent.parent / "lore-fskit-bridge/include")],
        "OTHER_LDFLAGS": [str(archive), *flags],
    }


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--team", required=True, help="Apple development team identifier")
    parser.add_argument("--identity", required=True, help="Explicit development signing identity for the extension, host and status tool")
    parser.add_argument("--generate-only", action="store_true", help="Write the Xcode project without contacting Apple or building")
    parser.add_argument("--output", type=Path, default=HERE.parent.parent / "target/fskit-prototype")
    args = parser.parse_args(argv)
    if not args.identity.strip() or args.identity == "-":
        parser.error("Provisioning requires an explicit development --identity")
    if not re.fullmatch(r"[A-Z0-9]{10}", args.team):
        parser.error("--team must be a 10-character Apple development team identifier")
    return args


def main():
    args = parse_arguments()
    output = args.output.resolve()
    app = output / "LoreFSKitProbe.app"
    extension = app / "Contents/Extensions/LoreFSKitProbe.appex"
    if not (extension / "Contents/Info.plist").exists():
        raise SystemExit("Run build.py first")
    entitlements = output / "Provision.entitlements"
    entitlements.write_bytes(plistlib.dumps({
        "com.apple.developer.fskit.fsmodule": True,
        "com.apple.security.app-sandbox": True,
        "com.apple.security.network.client": True,
    }))

    project = output / "Provision.xcodeproj"
    project.mkdir(exist_ok=True)
    objects = {}

    def add(isa, **fields):
        key = f"{len(objects) + 1:024X}"
        objects[key] = dict(isa=isa, **fields)
        return key

    files = [add("PBXFileReference", lastKnownFileType="sourcecode.swift",
                 path=str(path), sourceTree="<absolute>") for path in sorted((HERE / "Sources").glob("*.swift"))]
    builds = [add("PBXBuildFile", fileRef=ref) for ref in files]
    sources = add("PBXSourcesBuildPhase", buildActionMask=2147483647, files=builds, runOnlyForDeploymentPostprocessing=0)
    product = add("PBXFileReference", explicitFileType="wrapper.app-extension",
                  path="LoreFSKitProbe.appex", sourceTree="BUILT_PRODUCTS_DIR")
    products = add("PBXGroup", children=[product], name="Products", sourceTree="<group>")
    group = add("PBXGroup", children=files + [products], sourceTree="<group>")
    config = add("XCBuildConfiguration", name="Debug", buildSettings={
        "SDKROOT": "macosx", "MACOSX_DEPLOYMENT_TARGET": "27.0", "SWIFT_VERSION": "5.0",
        "PRODUCT_NAME": "LoreFSKitProbe", "PRODUCT_MODULE_NAME": "LoreFSKitExtension",
        "EXECUTABLE_NAME": "LoreFSKitExtension",
        "PRODUCT_BUNDLE_IDENTIFIER": "org.example.lore.fskit-probe.extension",
        "INFOPLIST_FILE": str(extension / "Contents/Info.plist"),
        "CODE_SIGN_ENTITLEMENTS": str(entitlements),
        "CODE_SIGN_STYLE": "Automatic", "CODE_SIGN_IDENTITY": args.identity,
        "DEVELOPMENT_TEAM": args.team, "ENABLE_APP_SANDBOX": "YES",
        "APPLICATION_EXTENSION_API_ONLY": "YES", "SKIP_INSTALL": "YES",
        "SWIFT_TREAT_WARNINGS_AS_ERRORS": "YES", "GENERATE_INFOPLIST_FILE": "NO",
        "ALWAYS_SEARCH_USER_PATHS": "NO",
        **bridge_settings(output),
    })
    configs = add("XCConfigurationList", buildConfigurations=[config], defaultConfigurationName="Debug", defaultConfigurationIsVisible=0)
    target = add("PBXNativeTarget", name="LoreFSKitProbe", productName="LoreFSKitProbe",
                 productReference=product, productType="com.apple.product-type.extensionkit-extension",
                 buildConfigurationList=configs, buildPhases=[sources], buildRules=[], dependencies=[])
    host_source = add("PBXFileReference", lastKnownFileType="sourcecode.swift",
                      path=str(HERE / "Host.swift"), sourceTree="<absolute>")
    host_build = add("PBXBuildFile", fileRef=host_source)
    host_sources = add("PBXSourcesBuildPhase", buildActionMask=2147483647,
                       files=[host_build], runOnlyForDeploymentPostprocessing=0)
    embedded = add("PBXBuildFile", fileRef=product,
                   settings={"ATTRIBUTES": ["CodeSignOnCopy", "RemoveHeadersOnCopy"]})
    embed = add("PBXCopyFilesBuildPhase", buildActionMask=2147483647,
                dstPath="$(CONTENTS_FOLDER_PATH)/Extensions", dstSubfolderSpec=16,
                files=[embedded], name="Embed ExtensionKit Extensions", runOnlyForDeploymentPostprocessing=0)
    host_product = add("PBXFileReference", explicitFileType="wrapper.application",
                       path="LoreFSKitProbe.app", sourceTree="BUILT_PRODUCTS_DIR")
    objects[group]["children"].append(host_source)
    objects[products]["children"].append(host_product)
    host_config = add("XCBuildConfiguration", name="Debug", buildSettings={
        "SDKROOT": "macosx", "MACOSX_DEPLOYMENT_TARGET": "27.0", "SWIFT_VERSION": "5.0",
        "PRODUCT_NAME": "LoreFSKitProbe", "PRODUCT_BUNDLE_IDENTIFIER": "org.example.lore.fskit-probe",
        "INFOPLIST_FILE": str(app / "Contents/Info.plist"), "GENERATE_INFOPLIST_FILE": "NO",
        "CODE_SIGN_STYLE": "Automatic", "CODE_SIGN_IDENTITY": args.identity,
        "DEVELOPMENT_TEAM": args.team, "SWIFT_TREAT_WARNINGS_AS_ERRORS": "YES",
        "ALWAYS_SEARCH_USER_PATHS": "NO",
    })
    host_configs = add("XCConfigurationList", buildConfigurations=[host_config],
                       defaultConfigurationName="Debug", defaultConfigurationIsVisible=0)
    dependency = add("PBXTargetDependency", target=target)
    host_target = add("PBXNativeTarget", name="LoreFSKitHost", productName="LoreFSKitProbe",
                      productReference=host_product, productType="com.apple.product-type.application",
                      buildConfigurationList=host_configs, buildPhases=[host_sources, embed],
                      buildRules=[], dependencies=[dependency])
    project_config = add("XCBuildConfiguration", name="Debug", buildSettings={})
    project_configs = add("XCConfigurationList", buildConfigurations=[project_config], defaultConfigurationName="Debug", defaultConfigurationIsVisible=0)
    root = add("PBXProject", attributes={"LastUpgradeCheck": "2710"},
               buildConfigurationList=project_configs, compatibilityVersion="Xcode 14.0",
               developmentRegion="en", knownRegions=["en", "Base"], mainGroup=group,
               productRefGroup=products, projectDirPath="", projectRoot="", targets=[target, host_target])
    (project / "project.pbxproj").write_bytes(plistlib.dumps(dict(
        archiveVersion="1", objectVersion="56", classes={}, objects=objects, rootObject=root)))
    if args.generate_only:
        print(project)
        return
    products_dir = output / "provisioned"
    subprocess.run(["xcodebuild", "-project", str(project), "-scheme", "LoreFSKitHost",
                    "-destination", f"platform=macOS,arch={platform.machine()}",
                    "-configuration", "Debug", "-allowProvisioningUpdates",
                    "-allowProvisioningDeviceRegistration", "build",
                    f"CONFIGURATION_BUILD_DIR={products_dir}",
                    f"OBJROOT={output / 'provision-objects'}"], check=True)
    built_app = products_dir / "LoreFSKitProbe.app"
    built = built_app / "Contents/Extensions/LoreFSKitProbe.appex"
    if not (built / "Contents/embedded.provisionprofile").exists():
        raise RuntimeError("Xcode did not embed a macOS provisioning profile")
    shutil.copytree(built_app, app, dirs_exist_ok=True)
    subprocess.run(["codesign", "--force", "--sign", args.identity, str(app)], check=True)
    subprocess.run(["codesign", "--force", "--sign", args.identity, str(output / "probe-status")], check=True)
    subprocess.run(["codesign", "--verify", "--deep", "--strict", str(app)], check=True)
    # LaunchServices can retain the previous extension identity when only nested
    # bundle contents change. Refresh the containing bundle for rediscovery.
    (app / "Contents/Extensions").touch()
    app.touch()
    print("Provisioned extension embedded. Re-register and re-enable before mounting.")


if __name__ == "__main__":
    main()
