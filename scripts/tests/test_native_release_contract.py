#!/usr/bin/env python3
"""Contracts for tag-published SQLite+FTS native binaries."""

from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
CD_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "cd.yml"


class NativeReleaseContractTests(unittest.TestCase):
    def test_ci_is_a_reusable_validation_only_workflow(self) -> None:
        source = CI_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("workflow_call:", source)
        self.assertNotIn("docker/login-action", source)
        self.assertNotIn("gh release", source)

    def test_cd_runs_the_quality_gate_before_publishing_docker(self) -> None:
        source = CD_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("uses: ./.github/workflows/ci.yml", source)
        self.assertIn("docker/login-action", source)
        self.assertIn("docker/build-push-action", source)
        self.assertIn("needs: quality", source)

    def test_release_builds_only_the_local_storage_profile(self) -> None:
        source = CD_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("Native SQLite+FTS binary", source)
        self.assertIn("--no-default-features", source)
        self.assertIn("--features sqlite-fts", source)
        self.assertIn("x86_64-unknown-linux-musl", source)
        self.assertIn("aarch64-apple-darwin", source)

    def test_release_publishes_versioned_checksummed_assets(self) -> None:
        source = CD_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn('asset="abyss-backend-${GITHUB_REF_NAME}-${{ matrix.target }}"', source)
        self.assertIn("sha256sum abyss-backend-* > SHA256SUMS", source)
        self.assertIn("gh release create", source)
        self.assertIn('"${GITHUB_REF_NAME}"', source)
        self.assertIn("release tag ${GITHUB_REF_NAME} does not match Cargo version", source)


if __name__ == "__main__":
    unittest.main()
