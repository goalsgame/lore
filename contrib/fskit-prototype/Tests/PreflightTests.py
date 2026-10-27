# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
import importlib.util
import json
import pathlib
import stat
import sys
import tempfile
import unittest

MODULE = pathlib.Path(__file__).resolve().parents[1] / "revision_mount.py"
SPEC = importlib.util.spec_from_file_location("revision_mount", MODULE)
mount = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mount
SPEC.loader.exec_module(mount)


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="lore-fskit-preflight-")
        self.root = pathlib.Path(self.temp.name)
        self.source = self.root / "repository"
        self.source.mkdir()
        self.target = self.root / "view"
        self.target.mkdir()
        self.config = self.root / "bootstrap.json"

    def tearDown(self):
        self.temp.cleanup()

    def plan(self, **options):
        values = dict(repository=self.source, revision="main@LATEST", target=self.target, config=self.config)
        values.update(options)
        return mount.preflight(**values)

    def test_preflight_is_read_only_and_published_config_is_private(self):
        plan = self.plan(offline=True)
        self.assertFalse(self.config.exists())
        mount.write_config(plan)
        self.assertEqual(stat.S_IMODE(self.config.stat().st_mode), 0o600)
        self.assertTrue(json.loads(self.config.read_text())["offline"])
        with self.assertRaises(ValueError):
            self.plan()

    def test_hidden_entries_and_symlink_targets_are_rejected(self):
        (self.target / ".edits").write_bytes(b"preserve")
        with self.assertRaises(ValueError):
            self.plan()
        self.assertEqual((self.target / ".edits").read_bytes(), b"preserve")
        alias = self.root / "alias"
        alias.symlink_to(self.target, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.plan(target=alias)

    def test_source_metadata_and_alias_cache_ancestry_cannot_be_overmounted(self):
        nested = self.source / "empty"
        nested.mkdir()
        with self.assertRaises(ValueError):
            self.plan(target=nested)
        metadata = self.root / ".lore"
        metadata.mkdir()
        with self.assertRaises(ValueError):
            self.plan(target=metadata)
        alias = self.root / "alias"
        alias.symlink_to(self.target, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.plan(digest_cache=alias / "new-cache")
        self.assertFalse((self.target / "new-cache").exists())

    def test_publication_preserves_files_created_after_preflight(self):
        plan = self.plan()
        self.config.write_bytes(b"edits")
        with self.assertRaises(FileExistsError):
            mount.write_config(plan)
        self.assertEqual(self.config.read_bytes(), b"edits")
        self.config.unlink()
        (self.target / "new-file").write_bytes(b"edits")
        with self.assertRaises(ValueError):
            mount.write_config(plan)
        self.assertFalse(self.config.exists())

    def test_config_under_target_and_unsafe_cache_are_rejected(self):
        with self.assertRaises(ValueError):
            self.plan(config=self.target / "bootstrap.json")
        cache = self.root / "cache"
        cache.mkdir(mode=0o777)
        cache.chmod(0o777)
        with self.assertRaises(ValueError):
            self.plan(digest_cache=cache)
        self.assertFalse(self.config.exists())


if __name__ == "__main__":
    unittest.main()
