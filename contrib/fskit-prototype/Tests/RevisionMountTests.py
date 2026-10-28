# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Preflight regression checks for the native FSKit mount helper."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HELPER = Path(os.environ.get("LORE_FSKIT_MOUNT_HELPER", "target/fskit-prototype/lore-fskit-mount")).resolve()


class RevisionMountTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="lore-fskit-native-preflight-")
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name).resolve()
        self.source = root / "source"
        self.target = root / "view"
        self.config = root / "config"
        for path in (self.source, self.target, self.config):
            path.mkdir(mode=0o700)

    def rejected(self, source=None, target=None, config=None, revision="main@LATEST"):
        result = subprocess.run([str(HELPER), str(source or self.source), revision,
                                 str(target or self.target), str(config or self.config)],
                                capture_output=True, text=True, timeout=20)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(list(self.config.iterdir()), [], "rejected requests must not create config")

    def test_nonempty_target_is_preserved(self):
        sentinel = self.target / "existing"
        sentinel.write_bytes(b"keep")
        self.rejected()
        self.assertEqual(sentinel.read_bytes(), b"keep")

    def test_overlap_symlinks_and_missing_revision_are_rejected(self):
        nested = self.source / "nested"
        nested.mkdir()
        self.rejected(target=nested)
        self.rejected(config=nested)
        alias = self.target.parent / "alias"
        alias.symlink_to(self.target, target_is_directory=True)
        self.rejected(target=alias)
        self.rejected(revision=" ")

    def test_unsafe_config_parent_is_rejected(self):
        self.config.chmod(0o777)
        self.rejected()


if __name__ == "__main__":
    unittest.main()
