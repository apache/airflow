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

Tests dynamically discover the latest available release versions from SVN HEAD.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration_tests

# SVN URLs
SVN_DEV_AIRFLOW_URL = "https://dist.apache.org/repos/dist/dev/airflow"

# Pattern to match version folders like "3.1.7rc2"
VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)(rc\d+)?$")

# Simulated versions for CI environment (when SVN is not available)
CI_SIMULATED_AIRFLOW_VERSION = "3.1.7rc2"
CI_SIMULATED_TASK_SDK_VERSION = "1.1.7rc2"


def is_ci_environment() -> bool:
    """Check if running in CI environment by checking the CI environment variable."""
    return os.environ.get("CI", "").lower() in ("true", "1", "yes")


def svn_available() -> bool:
    """Check if SVN is installed."""
    try:
        subprocess.run(["svn", "--version"], capture_output=True, check=True, timeout=5)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def java_available() -> bool:
    """Check if Java is installed."""
    try:
        subprocess.run(["java", "-version"], capture_output=True, check=True, timeout=5)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def gpg_available() -> bool:
    """Check if GPG is installed."""
    try:
        subprocess.run(["gpg", "--version"], capture_output=True, check=True, timeout=5)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def docker_available() -> bool:
    """Check if Docker is installed and running."""
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, check=False, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def hatch_available() -> bool:
    """Check if hatch is installed."""
    try:
        subprocess.run(["hatch", "--version"], capture_output=True, check=True, timeout=5)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def get_latest_releases_from_svn() -> tuple[str, str] | None:
    """
    Query SVN at HEAD to find the latest Airflow and Task SDK release versions.

    Returns:
        Tuple of (airflow_version, task_sdk_version) or None if not found.
        Example: ("3.1.7rc2", "1.1.7rc2")
    """
    try:
        # List Airflow versions at HEAD
        result = subprocess.run(
            ["svn", "list", SVN_DEV_AIRFLOW_URL],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if result.returncode != 0:
            return None

        # Parse airflow versions matching X.Y.Z pattern
        airflow_versions = []
        for line in result.stdout.strip().split("\n"):
            folder = line.rstrip("/")
            if VERSION_PATTERN.match(folder):
                airflow_versions.append(folder)

        if not airflow_versions:
            return None

        # Sort versions (lexicographic sort works for X.Y.Zrcn format)
        airflow_versions.sort(reverse=True)
        latest_airflow = airflow_versions[0]

        # List Task SDK versions at HEAD
        result = subprocess.run(
            ["svn", "list", f"{SVN_DEV_AIRFLOW_URL}/task-sdk/"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if result.returncode != 0:
            return None

        # Parse task-sdk versions matching X.Y.Z pattern
        task_sdk_versions = []
        for line in result.stdout.strip().split("\n"):
            folder = line.rstrip("/")
            if VERSION_PATTERN.match(folder):
                task_sdk_versions.append(folder)

        if not task_sdk_versions:
            return None

        # Sort versions
        task_sdk_versions.sort(reverse=True)
        latest_task_sdk = task_sdk_versions[0]

        print(f"Discovered latest releases - Airflow: {latest_airflow}, Task SDK: {latest_task_sdk}")
        return (latest_airflow, latest_task_sdk)

    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


# Skip markers for missing prerequisites
requires_svn = pytest.mark.skipif(not svn_available(), reason="SVN not installed")
requires_java = pytest.mark.skipif(not java_available(), reason="Java not installed")
requires_gpg = pytest.mark.skipif(not gpg_available(), reason="GPG not installed")
requires_docker = pytest.mark.skipif(not docker_available(), reason="Docker not available")
requires_hatch = pytest.mark.skipif(not hatch_available(), reason="hatch not installed")


def _create_test_artifacts(directory: Path, version: str, package_name: str) -> None:
    """
    Create test artifacts (tar.gz, whl, checksums, signatures) for CI testing.

    Args:
        directory: Directory to create artifacts in
        version: Version string (e.g., "3.1.7rc2")
        package_name: Package name (e.g., "apache-airflow" or "apache_airflow_task_sdk")
    """
    import hashlib

    # Remove rc suffix for filenames using regex pattern
    version_without_rc = re.sub(r"rc\d+$", "", version)

    # Create tar.gz file with minimal content
    tar_name = f"{package_name}-{version_without_rc}.tar.gz"
    tar_path = directory / tar_name
    tar_content = f"Test content for {package_name} version {version}\n".encode()
    tar_path.write_bytes(tar_content)

    # Create wheel file
    whl_name = f"{package_name}-{version_without_rc}-py3-none-any.whl"
    whl_path = directory / whl_name
    whl_content = f"Test wheel content for {package_name} version {version}\n".encode()
    whl_path.write_bytes(whl_content)

    # Create SHA512 checksums
    for artifact_path in [tar_path, whl_path]:
        sha512 = hashlib.sha512(artifact_path.read_bytes()).hexdigest()
        checksum_path = directory / f"{artifact_path.name}.sha512"
        checksum_path.write_text(f"{sha512}  {artifact_path.name}\n")

    # Create GPG signatures (simulated)
    for artifact_path in [tar_path, whl_path]:
        sig_path = directory / f"{artifact_path.name}.asc"
        # Create a minimal GPG signature format (not a real signature, but has correct structure)
        sig_content = f"""-----BEGIN PGP SIGNATURE-----

iQIzBAABCAAdFiEETest GPG Key For CI TestingAAKCRATestKeyID
TestSignatureDataForFile{artifact_path.name}
Simulated GPG signature for CI testing purposes only
This is not a real cryptographic signature
-----END PGP SIGNATURE-----
"""
        sig_path.write_text(sig_content)

    print(f"[info]Created test artifacts: {tar_name}, {whl_name} with checksums and signatures")


@pytest.fixture(scope="module")
def latest_versions():
    """
    Discover the latest Airflow and Task SDK versions from SVN HEAD.

    Returns tuple of (airflow_version, task_sdk_version) or None.
    In CI environment, returns simulated hardcoded versions.
    """
    if is_ci_environment():
        print(
            f"Running in CI environment - using simulated versions: "
            f"Airflow: {CI_SIMULATED_AIRFLOW_VERSION}, Task SDK: {CI_SIMULATED_TASK_SDK_VERSION}"
        )
        return (CI_SIMULATED_AIRFLOW_VERSION, CI_SIMULATED_TASK_SDK_VERSION)

    if not svn_available():
        return None

    versions = get_latest_releases_from_svn()
    if versions is None:
        pytest.skip("Could not discover latest releases from SVN")

    return versions


@pytest.fixture(scope="module")
def checkout_path(tmp_path_factory, latest_versions) -> Path | None:
    """
    Checkout SVN at HEAD revision for integration tests.

    This fixture is module-scoped to avoid repeated checkouts.
    Returns None if checkout fails (tests will be skipped).
    In CI environment, simulates checkout by creating empty directory structure.
    """
    if latest_versions is None:
        return None

    airflow_version, task_sdk_version = latest_versions
    svn_path = tmp_path_factory.mktemp("svn_checkout")

    if is_ci_environment():
        # In CI, simulate SVN checkout by creating directory structure with test files
        print("[info]Running in CI environment - simulating SVN checkout with test files")

        # Create Airflow directory structure
        airflow_dir = svn_path / airflow_version
        airflow_dir.mkdir(parents=True, exist_ok=True)

        # Create test artifacts for Airflow
        _create_test_artifacts(airflow_dir, airflow_version, "apache-airflow")

        print(f"[info]Simulated checkout: {airflow_dir}")

        # Create Task SDK directory structure
        task_sdk_dir = svn_path / "task-sdk" / task_sdk_version
        task_sdk_dir.mkdir(parents=True, exist_ok=True)

        # Create test artifacts for Task SDK
        _create_test_artifacts(task_sdk_dir, task_sdk_version, "apache_airflow_task_sdk")

        print(f"[info]Simulated checkout: {task_sdk_dir}")

        print("[success]Simulated SVN checkout completed in CI with test files")
        return svn_path

    if not svn_available():
        return None

    # Checkout Airflow release directory at HEAD
    airflow_dir = svn_path / airflow_version
    cmd = [
        "svn",
        "checkout",
        f"{SVN_DEV_AIRFLOW_URL}/{airflow_version}",
        str(airflow_dir),
    ]
    print(f"Running command: {shlex.join(cmd)}")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300,  # 5 minute timeout
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Failed to checkout Airflow SVN: {result.stderr}")
        return None

    # Checkout Task SDK release directory at HEAD
    task_sdk_dir = svn_path / "task-sdk" / task_sdk_version
    task_sdk_dir.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "svn",
        "checkout",
        f"{SVN_DEV_AIRFLOW_URL}/task-sdk/{task_sdk_version}",
        str(task_sdk_dir),
    ]
    print(f"Running command: {shlex.join(cmd)}")
    result = subprocess.run(
        cmd,
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
def validator(checkout_path, airflow_repo_root: Path, latest_versions):
    """Create AirflowReleaseValidator for integration tests."""
    if checkout_path is None or latest_versions is None:
        pytest.skip("SVN checkout path or versions not available")

    airflow_version, task_sdk_version = latest_versions

    from airflow_breeze.utils.airflow_release_validator import AirflowReleaseValidator

    return AirflowReleaseValidator(
        version=airflow_version,
        svn_path=checkout_path,
        airflow_repo_root=airflow_repo_root,
        task_sdk_version=task_sdk_version,
        update_svn=False,  # We already checked out at HEAD
        verbose=True,
    )


@requires_svn
class TestValidateSvnFilesIntegration:
    """Integration tests for SVN file validation."""

    def test_all_expected_files_present(self, validator):
        """Should find all expected files in real SVN checkout."""
        if is_ci_environment():
            # In CI, we only create minimal test files for checksum/signature testing
            # Skip full file validation
            pytest.skip("Full SVN file validation skipped in CI environment (minimal test files only)")

        result = validator.validate_svn_files()

        assert result.passed is True
        assert "21 expected files present" in result.message

    def test_svn_directories_exist(self, validator):
        """Should have correct SVN directory structure."""
        assert validator.get_svn_directory().exists()
        assert validator.get_task_sdk_svn_directory().exists()


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


@requires_svn
class TestValidateChecksumsIntegration:
    """Integration tests for SHA512 checksum validation."""

    def test_all_checksums_valid(self, validator):
        """Should verify all SHA512 checksums successfully."""
        result = validator.validate_checksums()

        assert result.passed is True
        assert "checksums valid" in result.message


@requires_svn
@requires_java
class TestValidateLicensesIntegration:
    """Integration tests for Apache RAT license validation (requires Java + RAT)."""

    def test_all_licenses_approved(self, validator):
        """Should verify all files have approved licenses."""
        if is_ci_environment():
            # Skip license validation in CI - we only create minimal test files
            pytest.skip("License validation skipped in CI environment (no real package files)")

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
        if is_ci_environment():
            # Skip reproducible build validation in CI - requires real source code
            pytest.skip("Reproducible build validation skipped in CI environment (no real packages)")

        result = validator.validate_reproducible_build()

        assert result.passed is True
        assert "packages are identical" in result.message


@requires_svn
class TestCheckSvnLocksIntegration:
    """Integration tests for SVN lock detection."""

    def test_clean_checkout_has_no_locks(self, validator):
        """Should detect no locks in clean SVN checkout."""
        svn_dir = validator.get_svn_directory()

        result = validator._check_svn_locks(svn_dir.parent.parent)

        assert result is False  # No locks in clean checkout
