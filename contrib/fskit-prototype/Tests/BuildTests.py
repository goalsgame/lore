# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
import contextlib
import importlib.util
import io
import json
import pathlib
import plistlib
import tempfile
import unittest
from unittest.mock import patch

MODULE = pathlib.Path(__file__).resolve().parents[1] / "build.py"
SPEC = importlib.util.spec_from_file_location("lore_fskit_build", MODULE)
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)
PROVISION_SPEC = importlib.util.spec_from_file_location("lore_fskit_provision", MODULE.with_name("provision.py"))
provision = importlib.util.module_from_spec(PROVISION_SPEC)
PROVISION_SPEC.loader.exec_module(provision)
DIAGNOSTIC_SPEC = importlib.util.spec_from_file_location("lore_fskit_diagnostic", MODULE.with_name("diagnostic_build.py"))
diagnostic = importlib.util.module_from_spec(DIAGNOSTIC_SPEC)
DIAGNOSTIC_SPEC.loader.exec_module(diagnostic)


class CargoOutput:
    returncode = 0

    def __init__(self, archive, flags=True):
        self.stdout = []
        if flags:
            self.stdout.append(json.dumps({"reason": "compiler-message", "message": {
                "rendered": "note: native-static-libs: -framework Security -lSystem\n"}}) + "\n")
        self.stdout.append(json.dumps({"reason": "compiler-artifact", "target": {
            "name": "lore_fskit_bridge"}, "filenames": [str(archive)], "fresh": True}) + "\n")

    def wait(self):
        return 0


class BuildTests(unittest.TestCase):
    def test_signing_requires_explicit_identity_and_matching_team_input(self):
        with contextlib.redirect_stderr(io.StringIO()):
            for arguments in [[], ["--identity", "Example Development Identity"],
                              ["--identity", "-", "--team", "TESTTEAM00"]]:
                with self.assertRaises(SystemExit):
                    build.parse_arguments(arguments)
            for module, prefix in [(provision, []), (diagnostic, ["--baseline", "example-baseline"])]:
                for arguments in [["--team", "TESTTEAM00"], ["--identity", "-", "--team", "TESTTEAM00"],
                                  ["--identity", "Example Development Identity", "--team", "invalid"]]:
                    with self.assertRaises(SystemExit):
                        module.parse_arguments(prefix + arguments)
        self.assertEqual(build.parse_arguments(["--identity", "-"]).identity, "-")
        for module, prefix in [(build, []), (provision, []), (diagnostic, ["--baseline", "example-baseline"])]:
            args = module.parse_arguments(prefix + ["--identity", "Example Development Identity", "--team", "TESTTEAM00"])
            self.assertEqual(args.identity, "Example Development Identity")
            self.assertEqual(args.team, "TESTTEAM00")

    def test_generate_only_uses_neutral_identifiers_and_explicit_signing_without_tools(self):
        with tempfile.TemporaryDirectory(prefix="lore-fskit-project-") as directory:
            output = pathlib.Path(directory)
            for relative in ["Contents/Info.plist", "Contents/Extensions/LoreFSKitProbe.appex/Contents/Info.plist"]:
                path = output / "LoreFSKitProbe.app" / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(plistlib.dumps({}))
            arguments = ["provision.py", "--generate-only", "--output", str(output),
                         "--identity", "Example Development Identity", "--team", "TESTTEAM00"]
            with patch("sys.argv", arguments), patch.object(provision, "bridge_settings", return_value={}), \
                    patch.object(provision.subprocess, "run", side_effect=AssertionError("External action forbidden")), \
                    contextlib.redirect_stdout(io.StringIO()):
                provision.main()
            project = plistlib.loads((output / "Provision.xcodeproj/project.pbxproj").read_bytes())
            configurations = [item["buildSettings"] for item in project["objects"].values()
                              if item["isa"] == "XCBuildConfiguration" and item["buildSettings"]]
            self.assertEqual({item["PRODUCT_BUNDLE_IDENTIFIER"] for item in configurations},
                             {"org.example.lore.fskit-probe", "org.example.lore.fskit-probe.extension"})
            for configuration in configurations:
                self.assertEqual(configuration["DEVELOPMENT_TEAM"], "TESTTEAM00")
                self.assertEqual(configuration["CODE_SIGN_IDENTITY"], "Example Development Identity")

    def test_provisioning_links_reported_production_archive_and_swift_module(self):
        with tempfile.TemporaryDirectory(prefix="lore-fskit-provision-") as directory:
            root = pathlib.Path(directory)
            archive = root / "custom target/liblore_fskit_bridge.a"
            archive.parent.mkdir()
            archive.write_bytes(b"production-archive")
            (root / "native-libs.json").write_text(json.dumps({
                "archive": str(archive),
                "flags": "-framework Security -liconv -lSystem",
            }))
            settings = provision.bridge_settings(root)
            self.assertEqual(settings["OTHER_LDFLAGS"], [str(archive.resolve()), "-framework", "Security", "-liconv"])
            self.assertTrue(pathlib.Path(settings["SWIFT_INCLUDE_PATHS"][0], "module.modulemap").is_file())
            archive.unlink()
            with self.assertRaises(FileNotFoundError):
                provision.bridge_settings(root)

    def test_reported_archive_handles_target_triple_and_fresh_cache(self):
        with tempfile.TemporaryDirectory(prefix="lore-fskit-build-") as directory:
            root = pathlib.Path(directory)
            archive = root / "custom-target/aarch64-apple-darwin/fskit/liblore_fskit_bridge.a"
            archive.parent.mkdir(parents=True)
            archive.write_bytes(b"reported-current-archive")
            output = root / "output"
            with patch.object(build.subprocess, "Popen", return_value=CargoOutput(archive)):
                selected, flags = build.rust_bridge(output, "fskit")
            self.assertEqual(selected, archive.resolve())
            self.assertIn("Security", flags)
            with patch.object(build.subprocess, "Popen", return_value=CargoOutput(archive, flags=False)):
                self.assertEqual(build.rust_bridge(output, "fskit")[0], selected)
            other = archive.parent / "different-current-archive.a"
            other.write_bytes(b"new-archive")
            with patch.object(build.subprocess, "Popen", return_value=CargoOutput(other, flags=False)):
                with self.assertRaises(RuntimeError):
                    build.rust_bridge(output, "fskit")

    def test_missing_flags_or_artifact_never_selects_an_existing_archive(self):
        with tempfile.TemporaryDirectory(prefix="lore-fskit-build-") as directory:
            root = pathlib.Path(directory)
            archive = root / "liblore_fskit_bridge.a"
            archive.write_bytes(b"preexisting-archive")
            with patch.object(build.subprocess, "Popen", return_value=CargoOutput(archive, flags=False)):
                with self.assertRaises(RuntimeError):
                    build.rust_bridge(root / "output", "dev")
            no_artifact = CargoOutput(archive, flags=False)
            no_artifact.stdout = []
            with patch.object(build.subprocess, "Popen", return_value=no_artifact):
                with self.assertRaises(RuntimeError):
                    build.rust_bridge(root / "output", "dev")


if __name__ == "__main__":
    unittest.main()
