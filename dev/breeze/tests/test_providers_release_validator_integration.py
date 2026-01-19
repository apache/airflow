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
Integration tests for ProvidersReleaseValidator class.

These tests use real SVN checkout to validate the release validation logic.
They are pinned to a specific SVN revision to ensure reproducibility.

Run integration tests with: pytest -m integration
Run slow integration tests with: pytest -m slow_integration
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from airflow_breeze.utils.providers_release_validator import ProvidersReleaseValidator

# Pinned SVN revision for reproducible tests
# This revision contains the providers 2026-01-17 release candidate (just before release)
# At r82039 the files were moved to release, so r82038 is the last revision with files in dev
PINNED_SVN_REVISION = "82038"

# Test configuration for providers 2026-01-17
TEST_RELEASE_DATE = "2026-01-17"

# Packages from the 2026-01-17 release for testing
# These are the actual packages in this release (version without rc suffix as files are final)
TEST_PACKAGES = [
    ("apache-airflow-providers-cncf-kubernetes", "10.12.2"),
    ("apache-airflow-providers-google", "19.4.0"),
]


@pytest.fixture(scope="module")
def svn_path(tmp_path_factory) -> Path:
    """
    Fixture that provides a checked out SVN dev path at a pinned revision.

    This fixture is module-scoped to avoid re-checking out for each test.
    """
    svn_base = tmp_path_factory.mktemp("svn")

    # Checkout the specific providers directory at the pinned revision
    svn_url = f"https://dist.apache.org/repos/dist/dev/airflow/providers/{TEST_RELEASE_DATE}"
    checkout_path = svn_base / "providers" / TEST_RELEASE_DATE

    result = subprocess.run(
        ["svn", "checkout", "-r", PINNED_SVN_REVISION, svn_url, str(checkout_path)],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    if result.returncode != 0:
        pytest.skip(f"Failed to checkout SVN: {result.stderr}")

    return svn_base


@pytest.fixture(scope="module")
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


@pytest.mark.integration
class TestProvidersValidateSvnFilesIntegration:
    """Integration tests for SVN file validation."""

    def test_validate_svn_files_with_real_checkout(self, svn_path, airflow_repo_root):
        """Test that SVN files validation passes with real checkout."""
        validator = ProvidersReleaseValidator(
            release_date=TEST_RELEASE_DATE,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            packages=TEST_PACKAGES,
        )

        result = validator.validate_svn_files()

        assert result.passed is True, (
            f"SVN files validation should pass with pinned revision: {result.message}"
        )

    def test_expected_files_exist(self, svn_path, airflow_repo_root):
        """Test that expected files actually exist in the checkout."""
        validator = ProvidersReleaseValidator(
            release_date=TEST_RELEASE_DATE,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            packages=TEST_PACKAGES,
        )

        svn_dir = validator.get_svn_directory()
        expected_files = validator.get_expected_files()

        for expected_file in expected_files:
            file_path = svn_dir / expected_file
            assert file_path.exists(), f"Expected file {expected_file} should exist in SVN checkout"


@pytest.mark.integration
class TestProvidersValidateSignaturesIntegration:
    """Integration tests for signature validation."""

    def test_validate_signatures_with_real_files(self, svn_path, airflow_repo_root):
        """Test that signature validation passes with real SVN files."""
        validator = ProvidersReleaseValidator(
            release_date=TEST_RELEASE_DATE,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            packages=TEST_PACKAGES,
        )

        result = validator.validate_signatures()

        assert result.passed is True, f"Signature validation should pass with real RC files: {result.message}"


@pytest.mark.integration
class TestProvidersValidateChecksumsIntegration:
    """Integration tests for checksum validation."""

    def test_validate_checksums_with_real_files(self, svn_path, airflow_repo_root):
        """Test that checksum validation passes with real SVN files."""
        validator = ProvidersReleaseValidator(
            release_date=TEST_RELEASE_DATE,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            packages=TEST_PACKAGES,
        )

        result = validator.validate_checksums()

        assert result.passed is True, f"Checksum validation should pass with real RC files: {result.message}"


@pytest.mark.integration
class TestProvidersValidateLicensesIntegration:
    """Integration tests for license validation."""

    def test_validate_licenses_with_real_files(self, svn_path, airflow_repo_root):
        """Test that license validation passes with real SVN files."""
        validator = ProvidersReleaseValidator(
            release_date=TEST_RELEASE_DATE,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            packages=TEST_PACKAGES,
        )

        result = validator.validate_licenses()

        assert result.passed is True, f"License validation should pass with real RC files: {result.message}"


@pytest.mark.slow_integration
class TestProvidersValidateReproducibleBuildIntegration:
    """
    Slow integration tests for reproducible build validation.

    These tests are marked as slow_integration because they:
    - Download and build packages from source
    - May take several minutes to complete

    Run with: pytest -m slow_integration
    """

    def test_validate_reproducible_build_with_real_files(self, svn_path, airflow_repo_root):
        """Test that reproducible build validation passes with real SVN files."""
        validator = ProvidersReleaseValidator(
            release_date=TEST_RELEASE_DATE,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            packages=TEST_PACKAGES,
        )

        result = validator.validate_reproducible_build()

        assert result.passed is True, (
            f"Reproducible build validation should pass with real RC files: {result.message}"
        )


@pytest.mark.integration
class TestProvidersFullValidationIntegration:
    """Integration test for the full validation workflow."""

    def test_full_validation_excluding_reproducible_build(self, svn_path, airflow_repo_root):
        """
        Test the full validation workflow (excluding slow reproducible build).

        This tests the typical validation path used by PMC members.
        """
        validator = ProvidersReleaseValidator(
            release_date=TEST_RELEASE_DATE,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            packages=TEST_PACKAGES,
        )

        # Run validations in order
        svn_result = validator.validate_svn_files()
        assert svn_result.passed, f"SVN files validation failed: {svn_result.message}"

        sig_result = validator.validate_signatures()
        assert sig_result.passed, f"Signature validation failed: {sig_result.message}"

        checksum_result = validator.validate_checksums()
        assert checksum_result.passed, f"Checksum validation failed: {checksum_result.message}"

        license_result = validator.validate_licenses()
        assert license_result.passed, f"License validation failed: {license_result.message}"
