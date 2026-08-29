#!/usr/bin/env python3
"""Contracts for tag-published SQLite+FTS native binaries."""

from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


class NativeReleaseContractTests(unittest.TestCase):
    def test_release_builds_only_the_local_storage_profile(self) -> None:
        source = WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("Native SQLite+FTS binary", source)
        self.assertIn("--no-default-features", source)
        self.assertIn("--features sqlite-fts", source)
        self.assertIn("x86_64-unknown-linux-musl", source)
        self.assertIn("aarch64-apple-darwin", source)

    def test_release_publishes_versioned_checksummed_assets(self) -> None:
        source = WORKFLOW.read_text(encoding="utf-8")

        self.assertIn('asset="abyss-backend-${GITHUB_REF_NAME}-${{ matrix.target }}"', source)
        self.assertIn("sha256sum abyss-backend-* > SHA256SUMS", source)
        self.assertIn("gh release create", source)
        self.assertIn('"${GITHUB_REF_NAME}"', source)
        self.assertIn("release tag ${GITHUB_REF_NAME} does not match Cargo version", source)


if __name__ == "__main__":
    unittest.main()
