# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Integration tests for AirflowReleaseValidator using real SVN checkout.

These tests require:
- SVN installed and available
- Network access to ASF SVN repository (for initial checkout)
- Java installed (for Apache RAT license checks)
- GPG installed (for signature verification)

Tests are pinned to specific SVN revisions for reproducibility.

To update pinned revisions:
    svn info https://dist.apache.org/repos/dist/dev/airflow/3.1.6rc1
    svn info https://dist.apache.org/repos/dist/dev/airflow/task-sdk/1.1.6rc1

Current pinned revision: 81709 (as of 2026-01-23)
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

# Pinned SVN revision for reproducible tests
# Update these when testing a new release candidate
PINNED_SVN_REVISION = "81709"
AIRFLOW_VERSION = "3.1.6rc1"
TASK_SDK_VERSION = "1.1.6rc1"

# SVN URLs
SVN_DEV_AIRFLOW_URL = "https://dist.apache.org/repos/dist/dev/airflow"


def svn_available() -> bool:
    """Check if SVN is installed."""
    return shutil.which("svn") is not None


def java_available() -> bool:
    """Check if Java is installed."""
    return shutil.which("java") is not None


def gpg_available() -> bool:
    """Check if GPG is installed."""
    return shutil.which("gpg") is not None


def docker_available() -> bool:
    """Check if Docker is installed and running."""
    if not shutil.which("docker"):
        return False
    result = subprocess.run(["docker", "info"], capture_output=True, check=False)
    return result.returncode == 0


def hatch_available() -> bool:
    """Check if hatch is installed."""
    return shutil.which("hatch") is not None


# Skip markers for missing prerequisites
requires_svn = pytest.mark.skipif(not svn_available(), reason="SVN not installed")
requires_java = pytest.mark.skipif(not java_available(), reason="Java not installed")
requires_gpg = pytest.mark.skipif(not gpg_available(), reason="GPG not installed")
requires_docker = pytest.mark.skipif(not docker_available(), reason="Docker not available")
requires_hatch = pytest.mark.skipif(not hatch_available(), reason="hatch not installed")


@pytest.fixture(scope="module")
def svn_checkout(tmp_path_factory) -> Path | None:
    """
    Checkout SVN at pinned revision for integration tests.

    This fixture is module-scoped to avoid repeated checkouts.
    Returns None if checkout fails (tests will be skipped).
    """
    if not svn_available():
        return None

    svn_path = tmp_path_factory.mktemp("svn_checkout")

    # Checkout Airflow release directory at pinned revision
    airflow_dir = svn_path / AIRFLOW_VERSION
    result = subprocess.run(
        [
            "svn",
            "checkout",
            "--revision",
            PINNED_SVN_REVISION,
            f"{SVN_DEV_AIRFLOW_URL}/{AIRFLOW_VERSION}",
            str(airflow_dir),
        ],
        capture_output=True,
        text=True,
        timeout=300,  # 5 minute timeout
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Failed to checkout Airflow SVN: {result.stderr}")
        return None

    # Checkout Task SDK release directory at pinned revision
    task_sdk_dir = svn_path / "task-sdk" / TASK_SDK_VERSION
    task_sdk_dir.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            "svn",
            "checkout",
            "--revision",
            PINNED_SVN_REVISION,
            f"{SVN_DEV_AIRFLOW_URL}/task-sdk/{TASK_SDK_VERSION}",
            str(task_sdk_dir),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Failed to checkout Task SDK SVN: {result.stderr}")
        return None

    return svn_path


@pytest.fixture
def airflow_repo_root() -> Path:
    """Return the Airflow repository root (current working directory in CI)."""
    # In CI, we're running from the repo root
    # For local runs, try to find it
    cwd = Path.cwd()
    if (cwd / "airflow-core").exists():
        return cwd

    # Try parent directories
    for parent in cwd.parents:
        if (parent / "airflow-core").exists():
            return parent

    pytest.skip("Could not find Airflow repository root")


@pytest.fixture
def validator(svn_checkout: Path | None, airflow_repo_root: Path):
    """Create AirflowReleaseValidator for integration tests."""
    if svn_checkout is None:
        pytest.skip("SVN checkout not available")

    from airflow_breeze.utils.airflow_release_validator import AirflowReleaseValidator

    return AirflowReleaseValidator(
        version=AIRFLOW_VERSION,
        svn_path=svn_checkout,
        airflow_repo_root=airflow_repo_root,
        task_sdk_version=TASK_SDK_VERSION,
        update_svn=False,  # We already checked out at specific revision
        verbose=True,
    )


@pytest.mark.integration
@requires_svn
class TestValidateSvnFilesIntegration:
    """Integration tests for SVN file validation."""

    def test_all_expected_files_present(self, validator):
        """Should find all expected files in real SVN checkout."""
        result = validator.validate_svn_files()

        assert result.passed is True
        assert "21 expected files present" in result.message

    def test_svn_directories_exist(self, validator):
        """Should have correct SVN directory structure."""
        assert validator.get_svn_directory().exists()
        assert validator.get_task_sdk_svn_directory().exists()


@pytest.mark.integration
@requires_svn
@requires_gpg
class TestValidateSignaturesIntegration:
    """Integration tests for GPG signature validation."""

    def test_all_signatures_valid(self, validator):
        """Should verify all GPG signatures successfully."""
        # Note: This requires GPG keys to be imported
        # In CI, we may need to download keys first
        result = validator.validate_signatures()

        # We expect this to pass if keys are available
        # If keys are missing, it will fail with helpful message
        if not result.passed:
            assert result.details is not None
            assert any("download" in d.lower() or "key" in d.lower() for d in result.details)
        else:
            assert "signatures verified" in result.message


@pytest.mark.integration
@requires_svn
class TestValidateChecksumsIntegration:
    """Integration tests for SHA512 checksum validation."""

    def test_all_checksums_valid(self, validator):
        """Should verify all SHA512 checksums successfully."""
        result = validator.validate_checksums()

        assert result.passed is True
        assert "checksums valid" in result.message


@pytest.mark.integration
@requires_svn
@requires_java
class TestValidateLicensesIntegration:
    """Integration tests for Apache RAT license validation (requires Java + RAT)."""

    def test_all_licenses_approved(self, validator):
        """Should verify all files have approved licenses."""
        result = validator.validate_licenses()

        assert result.passed is True
        assert "approved licenses" in result.message


@pytest.fixture
def ensure_clean_git_state(airflow_repo_root: Path):
    """
    Ensure the git repository is in a clean state before and after the test.

    This fixture saves the current branch/HEAD, checks for uncommitted changes,
    and restores the original branch after the test completes (even on failure).
    """
    # Save current branch name (if on a branch) or HEAD commit
    branch_result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=str(airflow_repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if branch_result.returncode != 0:
        pytest.skip("Could not get current git branch")

    current_ref = branch_result.stdout.strip()

    # If we're in detached HEAD state, get the commit hash instead
    if current_ref == "HEAD":
        head_result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(airflow_repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if head_result.returncode != 0:
            pytest.skip("Could not get current git HEAD")
        current_ref = head_result.stdout.strip()

    # Check for uncommitted changes
    status_result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=str(airflow_repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if status_result.stdout.strip():
        pytest.skip("Repository has uncommitted changes - skipping to avoid data loss")

    yield current_ref

    # Always restore original branch/HEAD after test
    subprocess.run(
        ["git", "checkout", current_ref],
        cwd=str(airflow_repo_root),
        check=False,
    )


@pytest.mark.integration
@requires_svn
@requires_docker
@requires_hatch
class TestValidateReproducibleBuildIntegration:
    """
    Integration tests for reproducible build validation.

    These tests are slow (60-90+ seconds) and require Docker.
    """

    def test_reproducible_build_matches_svn(self, validator, ensure_clean_git_state):
        """Should build packages that match SVN artifacts."""
        result = validator.validate_reproducible_build()

        assert result.passed is True
        assert "packages are identical" in result.message


@pytest.mark.integration
@requires_svn
class TestCheckSvnLocksIntegration:
    """Integration tests for SVN lock detection."""

    def test_clean_checkout_has_no_locks(self, validator):
        """Should detect no locks in clean SVN checkout."""
        svn_dir = validator.get_svn_directory()

        result = validator._check_svn_locks(svn_dir.parent.parent)

        assert result is False  # No locks in clean checkout
