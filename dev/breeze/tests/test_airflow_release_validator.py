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
"""Unit tests for AirflowReleaseValidator class."""

from __future__ import annotations

import tarfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.utils.airflow_release_validator import AirflowReleaseValidator
from airflow_breeze.utils.release_validator import CheckType


@pytest.fixture
def validator(tmp_path: Path) -> AirflowReleaseValidator:
    """Create a test validator with temporary paths."""
    svn_path = tmp_path / "svn"
    svn_path.mkdir()
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    return AirflowReleaseValidator(
        version="3.1.6rc1",
        svn_path=svn_path,
        airflow_repo_root=repo_root,
        task_sdk_version="1.1.6rc1",
        update_svn=False,
        verbose=False,
    )


class TestExpectedFiles:
    """Tests for expected file properties."""

    def test_expected_airflow_file_bases(self, validator: AirflowReleaseValidator):
        """Should return correct Airflow file base names."""
        expected = validator.expected_airflow_file_bases

        assert "apache_airflow-3.1.6-source.tar.gz" in expected
        assert "apache_airflow-3.1.6.tar.gz" in expected
        assert "apache_airflow-3.1.6-py3-none-any.whl" in expected
        assert "apache_airflow_core-3.1.6.tar.gz" in expected
        assert "apache_airflow_core-3.1.6-py3-none-any.whl" in expected
        assert len(expected) == 5

    def test_expected_task_sdk_file_bases(self, validator: AirflowReleaseValidator):
        """Should return correct Task SDK file base names."""
        expected = validator.expected_task_sdk_file_bases

        assert "apache_airflow_task_sdk-1.1.6.tar.gz" in expected
        assert "apache_airflow_task_sdk-1.1.6-py3-none-any.whl" in expected
        assert len(expected) == 2

    def test_get_expected_files_includes_signatures_and_checksums(self, validator: AirflowReleaseValidator):
        """Should include .asc and .sha512 for each base file."""
        files = validator.get_expected_files()

        # 5 base files * 3 (base + .asc + .sha512) = 15 files
        assert len(files) == 15
        assert "apache_airflow-3.1.6.tar.gz" in files
        assert "apache_airflow-3.1.6.tar.gz.asc" in files
        assert "apache_airflow-3.1.6.tar.gz.sha512" in files

    def test_get_task_sdk_expected_files(self, validator: AirflowReleaseValidator):
        """Should include Task SDK files with signatures and checksums."""
        files = validator.get_task_sdk_expected_files()

        # 2 base files * 3 = 6 files
        assert len(files) == 6
        assert "apache_airflow_task_sdk-1.1.6.tar.gz" in files
        assert "apache_airflow_task_sdk-1.1.6.tar.gz.asc" in files


class TestGetSvnDirectories:
    """Tests for SVN directory path methods."""

    def test_get_svn_directory(self, validator: AirflowReleaseValidator):
        """Should return correct Airflow SVN directory path."""
        svn_dir = validator.get_svn_directory()

        assert svn_dir == validator.svn_path / "3.1.6rc1"

    def test_get_task_sdk_svn_directory(self, validator: AirflowReleaseValidator):
        """Should return correct Task SDK SVN directory path."""
        task_sdk_dir = validator.get_task_sdk_svn_directory()

        assert task_sdk_dir == validator.svn_path / "task-sdk" / "1.1.6rc1"

    def test_get_svn_directories_returns_both(self, validator: AirflowReleaseValidator):
        """Should return both Airflow and Task SDK directories."""
        dirs = validator.get_svn_directories()

        assert len(dirs) == 2
        assert validator.get_svn_directory() in dirs
        assert validator.get_task_sdk_svn_directory() in dirs


class TestValidateSvnFiles:
    """Tests for validate_svn_files method."""

    @patch("airflow_breeze.utils.airflow_release_validator.console_print")
    def test_all_files_present(self, mock_print: MagicMock, validator: AirflowReleaseValidator):
        """Should pass when all expected files are present."""
        # Create Airflow SVN directory with all files
        airflow_dir = validator.get_svn_directory()
        airflow_dir.mkdir(parents=True)
        for f in validator.get_expected_files():
            (airflow_dir / f).write_text("content")

        # Create Task SDK SVN directory with all files
        task_sdk_dir = validator.get_task_sdk_svn_directory()
        task_sdk_dir.mkdir(parents=True)
        for f in validator.get_task_sdk_expected_files():
            (task_sdk_dir / f).write_text("content")

        result = validator.validate_svn_files()

        assert result.passed is True
        assert result.check_type == CheckType.SVN
        assert "21 expected files present" in result.message

    @patch("airflow_breeze.utils.airflow_release_validator.console_print")
    def test_airflow_directory_missing(self, mock_print: MagicMock, validator: AirflowReleaseValidator):
        """Should fail when Airflow SVN directory doesn't exist."""
        # Only create Task SDK directory
        task_sdk_dir = validator.get_task_sdk_svn_directory()
        task_sdk_dir.mkdir(parents=True)

        result = validator.validate_svn_files()

        assert result.passed is False
        assert "not found" in result.message

    @patch("airflow_breeze.utils.airflow_release_validator.console_print")
    def test_task_sdk_directory_missing(self, mock_print: MagicMock, validator: AirflowReleaseValidator):
        """Should fail when Task SDK SVN directory doesn't exist."""
        # Only create Airflow directory
        airflow_dir = validator.get_svn_directory()
        airflow_dir.mkdir(parents=True)

        result = validator.validate_svn_files()

        assert result.passed is False
        assert "Task SDK SVN directory not found" in result.message

    @patch("airflow_breeze.utils.airflow_release_validator.console_print")
    def test_missing_files_reported(self, mock_print: MagicMock, validator: AirflowReleaseValidator):
        """Should report missing files in details."""
        # Create directories but only some files
        airflow_dir = validator.get_svn_directory()
        airflow_dir.mkdir(parents=True)
        (airflow_dir / "apache_airflow-3.1.6.tar.gz").write_text("content")

        task_sdk_dir = validator.get_task_sdk_svn_directory()
        task_sdk_dir.mkdir(parents=True)
        (task_sdk_dir / "apache_airflow_task_sdk-1.1.6.tar.gz").write_text("content")

        result = validator.validate_svn_files()

        assert result.passed is False
        assert "Missing" in result.message
        assert result.details is not None


class TestCompareArchives:
    """Tests for _compare_archives method."""

    def test_identical_wheel_files(self, validator: AirflowReleaseValidator, tmp_path: Path):
        """Should return True for identical wheel files."""
        # Create two identical wheel files (zip format)
        whl1 = tmp_path / "test1.whl"
        whl2 = tmp_path / "test2.whl"

        with zipfile.ZipFile(whl1, "w") as z:
            z.writestr("module/__init__.py", "# init")
            z.writestr("module/main.py", "def main(): pass")

        with zipfile.ZipFile(whl2, "w") as z:
            z.writestr("module/__init__.py", "# init")
            z.writestr("module/main.py", "def main(): pass")

        matches, details = validator._compare_archives(whl1, whl2)

        assert matches is True
        assert details == []

    def test_different_wheel_content(self, validator: AirflowReleaseValidator, tmp_path: Path):
        """Should return False when wheel contents differ."""
        whl1 = tmp_path / "test1.whl"
        whl2 = tmp_path / "test2.whl"

        with zipfile.ZipFile(whl1, "w") as z:
            z.writestr("module/main.py", "def main(): pass")

        with zipfile.ZipFile(whl2, "w") as z:
            z.writestr("module/main.py", "def main(): return True")

        matches, details = validator._compare_archives(whl1, whl2)

        assert matches is False
        assert any("Content differs" in d for d in details)

    def test_wheel_different_files(self, validator: AirflowReleaseValidator, tmp_path: Path):
        """Should detect files only in one archive."""
        whl1 = tmp_path / "test1.whl"
        whl2 = tmp_path / "test2.whl"

        with zipfile.ZipFile(whl1, "w") as z:
            z.writestr("module/main.py", "content")
            z.writestr("module/extra.py", "extra")  # Only in whl1

        with zipfile.ZipFile(whl2, "w") as z:
            z.writestr("module/main.py", "content")

        matches, details = validator._compare_archives(whl1, whl2)

        assert matches is False
        assert any("Only in built" in d for d in details)

    def test_identical_tarball_files(self, validator: AirflowReleaseValidator, tmp_path: Path):
        """Should return True for identical tar.gz files."""
        tar1 = tmp_path / "test1.tar.gz"
        tar2 = tmp_path / "test2.tar.gz"

        # Create source files
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.py").write_text("def main(): pass")

        # Create identical tarballs
        with tarfile.open(tar1, "w:gz") as t:
            t.add(src_dir / "main.py", arcname="pkg/main.py")

        with tarfile.open(tar2, "w:gz") as t:
            t.add(src_dir / "main.py", arcname="pkg/main.py")

        matches, details = validator._compare_archives(tar1, tar2)

        assert matches is True

    def test_tarball_size_mismatch(self, validator: AirflowReleaseValidator, tmp_path: Path):
        """Should detect size differences in tarball files."""
        tar1 = tmp_path / "test1.tar.gz"
        tar2 = tmp_path / "test2.tar.gz"

        src_dir = tmp_path / "src"
        src_dir.mkdir()

        (src_dir / "main.py").write_text("short")
        with tarfile.open(tar1, "w:gz") as t:
            t.add(src_dir / "main.py", arcname="pkg/main.py")

        (src_dir / "main.py").write_text("much longer content here")
        with tarfile.open(tar2, "w:gz") as t:
            t.add(src_dir / "main.py", arcname="pkg/main.py")

        matches, details = validator._compare_archives(tar1, tar2)

        assert matches is False
        assert any("Size differs" in d for d in details)


class TestVersionWithoutRc:
    """Tests for version stripping in __init__."""

    def test_version_without_rc_computed(self, tmp_path: Path):
        """Should compute version_without_rc correctly."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "repo"
        repo_root.mkdir()

        validator = AirflowReleaseValidator(
            version="3.1.6rc2",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            task_sdk_version="1.1.6rc3",
        )

        assert validator.version_without_rc == "3.1.6"
        assert validator.task_sdk_version_without_rc == "1.1.6"

    def test_task_sdk_version_defaults_to_version(self, tmp_path: Path):
        """Should use main version for task_sdk if not specified."""
        svn_path = tmp_path / "svn"
        svn_path.mkdir()
        repo_root = tmp_path / "repo"
        repo_root.mkdir()

        validator = AirflowReleaseValidator(
            version="3.1.6rc1",
            svn_path=svn_path,
            airflow_repo_root=repo_root,
            # task_sdk_version not specified
        )

        assert validator.task_sdk_version == "3.1.6rc1"
        assert validator.task_sdk_version_without_rc == "3.1.6"


class TestGetDistributionName:
    """Tests for get_distribution_name method."""

    def test_returns_apache_airflow(self, validator: AirflowReleaseValidator):
        """Should return 'Apache Airflow' as distribution name."""
        assert validator.get_distribution_name() == "Apache Airflow"
