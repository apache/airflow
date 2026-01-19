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
"""Unit tests for ProvidersReleaseValidator class."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from airflow_breeze.utils.providers_release_validator import (
    ProvidersReleaseValidator,
    parse_packages_file,
)


class TestProvidersReleaseValidatorInit:
    """Tests for ProvidersReleaseValidator initialization."""

    def test_init_basic(self, tmp_path):
        """Test basic initialization with required arguments."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        assert validator.release_date == "2026-01-17"
        assert validator.packages == packages
        assert validator.svn_path == svn_path
        assert validator.airflow_repo_root == repo_root

    def test_init_packages_without_rc_computed(self, tmp_path):
        """Test that packages_without_rc is computed correctly during init."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [
            ("apache-airflow-providers-amazon", "1.0.0rc1"),
            ("apache-airflow-providers-google", "2.0.0rc2"),
        ]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        assert validator.packages_without_rc == [
            ("apache-airflow-providers-amazon", "1.0.0"),
            ("apache-airflow-providers-google", "2.0.0"),
        ]

    def test_init_with_optional_flags(self, tmp_path):
        """Test initialization with optional flags."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
            download_gpg_keys=True,
            update_svn=False,
            docker_install_check=True,
            verbose=True,
        )

        assert validator.download_gpg_keys is True
        assert validator.update_svn is False
        assert validator.docker_install_check is True
        assert validator.verbose is True


class TestParsePackagesFile:
    """Tests for parse_packages_file standalone function."""

    def test_parse_packages_file_pypi_urls(self, tmp_path):
        """Test parsing a packages file with PyPI URLs."""
        packages_file = tmp_path / "packages.txt"
        packages_file.write_text(
            "https://pypi.org/project/apache-airflow-providers-amazon/1.0.0rc1/\n"
            "https://pypi.org/project/apache-airflow-providers-google/2.0.0rc1/\n"
        )

        packages = parse_packages_file(packages_file)

        assert packages == [
            ("apache-airflow-providers-amazon", "1.0.0rc1"),
            ("apache-airflow-providers-google", "2.0.0rc1"),
        ]

    def test_parse_packages_file_space_separated(self, tmp_path):
        """Test parsing a packages file with space-separated URLs."""
        packages_file = tmp_path / "packages.txt"
        packages_file.write_text(
            "https://pypi.org/project/apache-airflow-providers-amazon/1.0.0rc1/ "
            "https://pypi.org/project/apache-airflow-providers-google/2.0.0rc1/"
        )

        packages = parse_packages_file(packages_file)

        assert packages == [
            ("apache-airflow-providers-amazon", "1.0.0rc1"),
            ("apache-airflow-providers-google", "2.0.0rc1"),
        ]

    def test_parse_packages_file_not_found(self, tmp_path):
        """Test parsing a non-existent packages file raises SystemExit."""
        packages_file = tmp_path / "nonexistent.txt"

        with pytest.raises(SystemExit):
            parse_packages_file(packages_file)

    def test_parse_packages_file_empty(self, tmp_path):
        """Test parsing an empty packages file raises SystemExit."""
        packages_file = tmp_path / "packages.txt"
        packages_file.write_text("")

        with pytest.raises(SystemExit):
            parse_packages_file(packages_file)

    def test_parse_packages_file_with_empty_lines(self, tmp_path):
        """Test parsing a packages file with empty lines."""
        packages_file = tmp_path / "packages.txt"
        packages_file.write_text(
            "https://pypi.org/project/apache-airflow-providers-amazon/1.0.0rc1/\n"
            "\n"
            "https://pypi.org/project/apache-airflow-providers-google/2.0.0rc1/\n"
            "\n"
        )

        packages = parse_packages_file(packages_file)

        assert packages == [
            ("apache-airflow-providers-amazon", "1.0.0rc1"),
            ("apache-airflow-providers-google", "2.0.0rc1"),
        ]


class TestGetExpectedFiles:
    """Tests for get_expected_files method."""

    def test_get_expected_files_single_package(self, tmp_path):
        """Test get_expected_files for a single package."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        files = validator.get_expected_files()

        # Should include source tarball
        assert "apache_airflow_providers-2026-01-17-source.tar.gz" in files
        assert "apache_airflow_providers-2026-01-17-source.tar.gz.asc" in files
        assert "apache_airflow_providers-2026-01-17-source.tar.gz.sha512" in files

        # Should include provider-specific files (version without RC for release files)
        assert "apache_airflow_providers_amazon-1.0.0-py3-none-any.whl" in files
        assert "apache_airflow_providers_amazon-1.0.0-py3-none-any.whl.asc" in files
        assert "apache_airflow_providers_amazon-1.0.0.tar.gz" in files
        assert "apache_airflow_providers_amazon-1.0.0.tar.gz.asc" in files

    def test_get_expected_files_multiple_packages(self, tmp_path):
        """Test get_expected_files for multiple packages."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [
            ("apache-airflow-providers-amazon", "1.0.0rc1"),
            ("apache-airflow-providers-google", "2.0.0rc1"),
        ]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        files = validator.get_expected_files()

        # Should have files for both packages
        amazon_files = [f for f in files if "amazon" in f]
        google_files = [f for f in files if "google" in f]

        assert len(amazon_files) > 0
        assert len(google_files) > 0


class TestGetSvnDirectory:
    """Tests for get_svn_directory method."""

    def test_get_svn_directory(self, tmp_path):
        """Test get_svn_directory returns correct path."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        svn_dir = validator.get_svn_directory()

        assert svn_dir == svn_path / "providers" / "2026-01-17"


class TestValidateSvnFiles:
    """Tests for validate_svn_files method."""

    @patch("airflow_breeze.utils.providers_release_validator.console_print")
    def test_validate_svn_files_directory_not_found(self, mock_console_print, tmp_path):
        """Test validate_svn_files fails when SVN directory doesn't exist."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        result = validator.validate_svn_files()

        assert result.passed is False
        assert "not found" in result.message.lower()

    @patch("airflow_breeze.utils.providers_release_validator.console_print")
    def test_validate_svn_files_missing_files(self, mock_console_print, tmp_path):
        """Test validate_svn_files fails when files are missing."""
        svn_path = tmp_path / "svn"
        svn_dir = svn_path / "providers" / "2026-01-17"
        svn_dir.mkdir(parents=True)
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        # Create only some files (missing signature)
        (svn_dir / "apache_airflow_providers_amazon-1.0.0.tar.gz").touch()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        result = validator.validate_svn_files()

        assert result.passed is False
        assert "Missing" in result.message

    @patch("airflow_breeze.utils.providers_release_validator.console_print")
    def test_validate_svn_files_success(self, mock_console_print, tmp_path):
        """Test validate_svn_files succeeds with all expected files."""
        svn_path = tmp_path / "svn"
        svn_dir = svn_path / "providers" / "2026-01-17"
        svn_dir.mkdir(parents=True)
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        # Create all expected files
        for expected_file in validator.get_expected_files():
            (svn_dir / expected_file).touch()

        result = validator.validate_svn_files()

        assert result.passed is True


class TestVerifyInstalledProviders:
    """Tests for _verify_installed_providers method."""

    def test_verify_installed_providers_all_match(self, tmp_path):
        """Test _verify_installed_providers succeeds when all providers match."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [
            ("apache-airflow-providers-amazon", "1.0.0rc1"),
            ("apache-airflow-providers-google", "2.0.0rc1"),
        ]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        # Mock airflow info output with table format
        airflow_info_output = """
| apache-airflow-providers-amazon | 1.0.0rc1 |
| apache-airflow-providers-google | 2.0.0rc1 |
"""

        passed, details = validator._verify_installed_providers(airflow_info_output)

        assert passed is True
        assert any("verified" in d.lower() for d in details)

    def test_verify_installed_providers_missing_package(self, tmp_path):
        """Test _verify_installed_providers fails when package is missing."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [
            ("apache-airflow-providers-amazon", "1.0.0rc1"),
            ("apache-airflow-providers-google", "2.0.0rc1"),
        ]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        # Mock airflow info output missing google provider
        airflow_info_output = """
| apache-airflow-providers-amazon | 1.0.0rc1 |
"""

        passed, details = validator._verify_installed_providers(airflow_info_output)

        assert passed is False
        assert any("missing" in d.lower() for d in details)

    def test_verify_installed_providers_version_mismatch(self, tmp_path):
        """Test _verify_installed_providers fails on version mismatch."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        # Mock airflow info output with wrong version
        airflow_info_output = """
| apache-airflow-providers-amazon | 0.9.0 |
"""

        passed, details = validator._verify_installed_providers(airflow_info_output)

        assert passed is False
        assert any("mismatch" in d.lower() for d in details)

    def test_verify_installed_providers_colon_format(self, tmp_path):
        """Test _verify_installed_providers parses colon format."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        # Mock airflow info output with colon format
        airflow_info_output = "apache-airflow-providers-amazon: 1.0.0rc1"

        passed, details = validator._verify_installed_providers(airflow_info_output)

        assert passed is True


class TestVerifyDockerInstallation:
    """Tests for _verify_docker_installation method."""

    @patch("shutil.which")
    def test_verify_docker_installation_not_installed(self, mock_which, tmp_path):
        """Test _verify_docker_installation returns True with warning when Docker not installed."""
        mock_which.return_value = None

        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        passed, details = validator._verify_docker_installation()

        # Returns True with warning (doesn't fail if Docker not available)
        assert passed is True
        assert any("not available" in d.lower() for d in details)


class TestGetDistributionName:
    """Tests for get_distribution_name method."""

    def test_get_distribution_name(self, tmp_path):
        """Test get_distribution_name returns correct name."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "airflow"
        repo_root.mkdir()

        packages = [("apache-airflow-providers-amazon", "1.0.0rc1")]

        validator = ProvidersReleaseValidator(
            release_date="2026-01-17",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            packages=packages,
        )

        assert validator.get_distribution_name() == "Apache Airflow Providers"
