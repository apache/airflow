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
"""Unit tests for ReleaseValidator base class."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.utils.release_validator import CheckType, ReleaseValidator, ValidationResult


class ConcreteValidator(ReleaseValidator):
    """Concrete implementation of ReleaseValidator for testing."""

    def __init__(self, version: str, svn_path: Path, airflow_repo_root: Path, **kwargs):
        super().__init__(version, svn_path, airflow_repo_root, **kwargs)

    def get_distribution_name(self) -> str:
        return "Test Distribution"

    def get_svn_directory(self) -> Path:
        return self.svn_path / self.version

    def get_expected_files(self) -> list[str]:
        return ["test-1.0.tar.gz", "test-1.0.tar.gz.asc", "test-1.0.tar.gz.sha512"]

    def build_packages(self, source_dir: Path | None = None) -> bool:
        return True

    def validate_svn_files(self) -> ValidationResult:
        return ValidationResult(check_type=CheckType.SVN, passed=True, message="OK")

    def validate_reproducible_build(self) -> ValidationResult:
        return ValidationResult(check_type=CheckType.REPRODUCIBLE_BUILD, passed=True, message="OK")

    def validate_licenses(self) -> ValidationResult:
        return ValidationResult(check_type=CheckType.LICENSES, passed=True, message="OK")


@pytest.fixture
def validator(tmp_path: Path) -> ConcreteValidator:
    """Create a test validator with temporary paths."""
    svn_path = tmp_path / "svn"
    svn_path.mkdir()
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    return ConcreteValidator(
        version="1.0.0rc1",
        svn_path=svn_path,
        airflow_repo_root=repo_root,
        update_svn=False,
        verbose=False,
    )


class TestStripRcSuffix:
    """Tests for _strip_rc_suffix method."""

    def test_strips_rc1_suffix(self, validator: ConcreteValidator):
        assert validator._strip_rc_suffix("3.1.6rc1") == "3.1.6"

    def test_strips_rc2_suffix(self, validator: ConcreteValidator):
        assert validator._strip_rc_suffix("2.8.0rc2") == "2.8.0"

    def test_strips_high_rc_number(self, validator: ConcreteValidator):
        assert validator._strip_rc_suffix("1.0.0rc99") == "1.0.0"

    def test_no_rc_suffix_unchanged(self, validator: ConcreteValidator):
        assert validator._strip_rc_suffix("3.1.6") == "3.1.6"

    def test_empty_string(self, validator: ConcreteValidator):
        assert validator._strip_rc_suffix("") == ""


class TestCheckSvnLocks:
    """Tests for _check_svn_locks method."""

    @patch("airflow_breeze.utils.release_validator.run_command")
    def test_detects_lock_in_column_3(self, mock_run_command: MagicMock, validator: ConcreteValidator):
        """Should return True when SVN status shows 'L' in column 3 (lock indicator)."""
        # SVN status format: columns [item][props][lock] - L at index 2
        # Example: "! L     path" - item=!, props=space, lock=L
        mock_run_command.return_value = MagicMock(
            returncode=0,
            stdout="  L     some/locked/path\n",
            stderr="",
        )

        result = validator._check_svn_locks(validator.svn_path)

        assert result is True
        mock_run_command.assert_called_once()

    @patch("airflow_breeze.utils.release_validator.run_command")
    def test_detects_lock_with_other_status(self, mock_run_command: MagicMock, validator: ConcreteValidator):
        """Should return True when lock indicator appears with other status flags."""
        # Format: [item][props][lock] - 'L' at index 2
        # Example: "! L     path" - missing item with lock
        mock_run_command.return_value = MagicMock(
            returncode=0,
            stdout="! L     modified/and/locked/file\n",
            stderr="",
        )

        result = validator._check_svn_locks(validator.svn_path)

        assert result is True

    @patch("airflow_breeze.utils.release_validator.run_command")
    def test_detects_E155037_error(self, mock_run_command: MagicMock, validator: ConcreteValidator):
        """Should return True when SVN returns E155037 lock error in stderr."""
        mock_run_command.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="svn: E155037: Previous operation has not finished; run 'cleanup' if it was interrupted",
        )

        result = validator._check_svn_locks(validator.svn_path)

        assert result is True

    @patch("airflow_breeze.utils.release_validator.run_command")
    def test_no_lock_returns_false(self, mock_run_command: MagicMock, validator: ConcreteValidator):
        """Should return False when no locks are detected."""
        mock_run_command.return_value = MagicMock(
            returncode=0,
            stdout="M      modified/file\nA      added/file\n",
            stderr="",
        )

        result = validator._check_svn_locks(validator.svn_path)

        assert result is False

    @patch("airflow_breeze.utils.release_validator.run_command")
    def test_empty_output_returns_false(self, mock_run_command: MagicMock, validator: ConcreteValidator):
        """Should return False when SVN status output is empty (clean working copy)."""
        mock_run_command.return_value = MagicMock(
            returncode=0,
            stdout="",
            stderr="",
        )

        result = validator._check_svn_locks(validator.svn_path)

        assert result is False

    @patch("airflow_breeze.utils.release_validator.run_command")
    def test_short_lines_handled(self, mock_run_command: MagicMock, validator: ConcreteValidator):
        """Should handle lines shorter than 3 characters without crashing."""
        mock_run_command.return_value = MagicMock(
            returncode=0,
            stdout="M\nAB\n",  # Lines shorter than 3 chars
            stderr="",
        )

        result = validator._check_svn_locks(validator.svn_path)

        assert result is False


class TestUpdateSvn:
    """Tests for _update_svn method."""

    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_successful_update(
        self, mock_print: MagicMock, mock_run_command: MagicMock, validator: ConcreteValidator, tmp_path: Path
    ):
        """Should return True on successful SVN update."""
        # First call: _check_svn_locks (no lock)
        # Second call: svn update (success)
        mock_run_command.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),  # svn status (no locks)
            MagicMock(returncode=0, stdout="", stderr=""),  # svn update (success)
        ]

        # Create the expected directory structure
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)

        result = validator._update_svn()

        assert result is True
        assert mock_run_command.call_count == 2

    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_fails_when_locked(
        self, mock_print: MagicMock, mock_run_command: MagicMock, validator: ConcreteValidator
    ):
        """Should return False and print hint when working copy is locked."""
        # SVN status with lock indicator at column 3 (index 2)
        mock_run_command.return_value = MagicMock(
            returncode=0,
            stdout="! L     locked/path\n",
            stderr="",
        )

        result = validator._update_svn()

        assert result is False
        # Verify hint was printed
        hint_printed = any("svn cleanup" in str(call) for call in mock_print.call_args_list)
        assert hint_printed

    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_fails_on_update_error(
        self, mock_print: MagicMock, mock_run_command: MagicMock, validator: ConcreteValidator
    ):
        """Should return False when svn update fails."""
        mock_run_command.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),  # svn status (no locks)
            MagicMock(returncode=1, stdout="", stderr="svn: E123456: Some error"),  # svn update fails
        ]

        result = validator._update_svn()

        assert result is False


class TestValidateSignatures:
    """Tests for validate_signatures method."""

    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_all_signatures_valid(
        self, mock_print: MagicMock, mock_run_command: MagicMock, validator: ConcreteValidator
    ):
        """Should pass when all signatures are valid."""
        # Create test .asc files
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)
        (svn_dir / "test-1.0.tar.gz").write_text("content")
        (svn_dir / "test-1.0.tar.gz.asc").write_text("signature")

        mock_run_command.return_value = MagicMock(
            returncode=0,
            stderr='Good signature from "Test User <test@example.com>"',
        )

        result = validator.validate_signatures()

        assert result.passed is True
        assert result.check_type == CheckType.SIGNATURES
        assert "1 signatures verified" in result.message

    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_signature_verification_fails(
        self, mock_print: MagicMock, mock_run_command: MagicMock, validator: ConcreteValidator
    ):
        """Should fail when signature verification fails."""
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)
        (svn_dir / "test-1.0.tar.gz.asc").write_text("bad signature")

        mock_run_command.return_value = MagicMock(
            returncode=1,
            stderr="gpg: BAD signature",
        )

        result = validator.validate_signatures()

        assert result.passed is False
        assert "1 of 1 signatures failed" in result.message

    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_no_asc_files_found(self, mock_print: MagicMock, validator: ConcreteValidator):
        """Should fail when no .asc files are found."""
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)
        # No .asc files created

        result = validator.validate_signatures()

        assert result.passed is False
        assert "No .asc files found" in result.message


class TestValidateChecksums:
    """Tests for validate_checksums method."""

    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_all_checksums_valid(
        self, mock_print: MagicMock, mock_run_command: MagicMock, validator: ConcreteValidator
    ):
        """Should pass when all checksums match."""
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)

        # Create test file and compute its real SHA512
        test_content = b"test content for checksum"
        expected_hash = hashlib.sha512(test_content).hexdigest()

        (svn_dir / "test-1.0.tar.gz").write_bytes(test_content)
        (svn_dir / "test-1.0.tar.gz.sha512").write_text(f"{expected_hash}  test-1.0.tar.gz\n")

        mock_run_command.return_value = MagicMock(
            returncode=0,
            stdout=f"{expected_hash}  test-1.0.tar.gz\n",
        )

        result = validator.validate_checksums()

        assert result.passed is True
        assert result.check_type == CheckType.CHECKSUMS
        assert "1 checksums valid" in result.message

    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_checksum_mismatch(
        self, mock_print: MagicMock, mock_run_command: MagicMock, validator: ConcreteValidator
    ):
        """Should fail when checksum doesn't match."""
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)

        (svn_dir / "test-1.0.tar.gz").write_text("content")
        (svn_dir / "test-1.0.tar.gz.sha512").write_text("expected_hash_value  test-1.0.tar.gz\n")

        mock_run_command.return_value = MagicMock(
            returncode=0,
            stdout="different_hash_value  test-1.0.tar.gz\n",
        )

        result = validator.validate_checksums()

        assert result.passed is False
        assert "1 of 1 checksums failed" in result.message

    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_target_file_missing(self, mock_print: MagicMock, validator: ConcreteValidator):
        """Should fail when target file for checksum is missing."""
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)

        # Create .sha512 file but not the target file
        (svn_dir / "test-1.0.tar.gz.sha512").write_text("somehash  test-1.0.tar.gz\n")

        result = validator.validate_checksums()

        assert result.passed is False
        assert result.details is not None
        assert any("target file missing" in d for d in result.details)

    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_no_sha512_files_found(self, mock_print: MagicMock, validator: ConcreteValidator):
        """Should fail when no .sha512 files are found."""
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)

        result = validator.validate_checksums()

        assert result.passed is False
        assert "No .sha512 files found" in result.message


class TestValidatePrerequisites:
    """Tests for validate_prerequisites method."""

    @patch("airflow_breeze.utils.release_validator.shutil.which")
    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_all_prerequisites_met(
        self,
        mock_print: MagicMock,
        mock_run_command: MagicMock,
        mock_which: MagicMock,
        validator: ConcreteValidator,
    ):
        """Should return True when all prerequisites are available."""
        # All tools available
        mock_which.return_value = "/usr/bin/tool"

        # Mock run_command for different calls
        def run_command_side_effect(cmd, **kwargs):
            result = MagicMock(returncode=0)
            # git status --porcelain should return empty for clean directory
            if "git" in cmd and "status" in cmd and "--porcelain" in cmd:
                result.stdout = ""
            else:
                result.stdout = ""
            return result

        mock_run_command.side_effect = run_command_side_effect

        # Create SVN directory with files
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)
        (svn_dir / "test-1.0.tar.gz").write_text("content")

        result = validator.validate_prerequisites()

        assert result is True

    @patch("airflow_breeze.utils.release_validator.shutil.which")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_missing_java(self, mock_print: MagicMock, mock_which: MagicMock, validator: ConcreteValidator):
        """Should fail when Java is not installed."""

        def which_side_effect(tool):
            if tool == "java":
                return None
            return "/usr/bin/tool"

        mock_which.side_effect = which_side_effect

        result = validator.validate_prerequisites()

        assert result is False

    @patch("airflow_breeze.utils.release_validator.shutil.which")
    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_docker_not_running(
        self,
        mock_print: MagicMock,
        mock_run_command: MagicMock,
        mock_which: MagicMock,
        validator: ConcreteValidator,
    ):
        """Should fail when Docker daemon is not running."""
        mock_which.return_value = "/usr/bin/tool"
        mock_run_command.return_value = MagicMock(returncode=1)  # docker info fails

        result = validator.validate_prerequisites()

        assert result is False

    @patch("airflow_breeze.utils.release_validator.shutil.which")
    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_signatures_check_only_requires_gpg_and_svn(
        self,
        mock_print: MagicMock,
        mock_run_command: MagicMock,
        mock_which: MagicMock,
        validator: ConcreteValidator,
    ):
        """Should pass when only running signatures check without Java/Docker/hatch."""

        def which_side_effect(tool):
            # Only gpg and svn are available
            if tool in ("gpg", "svn"):
                return f"/usr/bin/{tool}"
            return None  # java, docker, hatch not available

        mock_which.side_effect = which_side_effect
        mock_run_command.return_value = MagicMock(returncode=0)

        # Create SVN directory with files
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)
        (svn_dir / "test-1.0.tar.gz").write_text("content")

        # Should pass with only signatures check (requires gpg + svn)
        result = validator.validate_prerequisites(checks=[CheckType.SIGNATURES])

        assert result is True

    @patch("airflow_breeze.utils.release_validator.shutil.which")
    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_checksums_check_only_requires_svn(
        self,
        mock_print: MagicMock,
        mock_run_command: MagicMock,
        mock_which: MagicMock,
        validator: ConcreteValidator,
    ):
        """Should pass when only running checksums check without Java/Docker/hatch/GPG."""

        def which_side_effect(tool):
            # Only svn is available
            if tool == "svn":
                return "/usr/bin/svn"
            return None  # java, docker, hatch, gpg not available

        mock_which.side_effect = which_side_effect
        mock_run_command.return_value = MagicMock(returncode=0)

        # Create SVN directory with files
        svn_dir = validator.get_svn_directory()
        svn_dir.mkdir(parents=True)
        (svn_dir / "test-1.0.tar.gz").write_text("content")

        # Should pass with only checksums check (requires only svn)
        result = validator.validate_prerequisites(checks=[CheckType.CHECKSUMS])

        assert result is True

    @patch("airflow_breeze.utils.release_validator.shutil.which")
    @patch("airflow_breeze.utils.release_validator.run_command")
    @patch("airflow_breeze.utils.release_validator.console_print")
    def test_reproducible_build_requires_docker_and_hatch(
        self,
        mock_print: MagicMock,
        mock_run_command: MagicMock,
        mock_which: MagicMock,
        validator: ConcreteValidator,
    ):
        """Should fail when reproducible build check runs without Docker."""

        def which_side_effect(tool):
            # Docker not available
            if tool == "docker":
                return None
            return f"/usr/bin/{tool}"

        mock_which.side_effect = which_side_effect
        mock_run_command.return_value = MagicMock(returncode=0)

        # Should fail for reproducible-build check (requires docker)
        result = validator.validate_prerequisites(checks=[CheckType.REPRODUCIBLE_BUILD])

        assert result is False


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_create_passed_result(self):
        result = ValidationResult(
            check_type=CheckType.SVN,
            passed=True,
            message="All files present",
        )
        assert result.passed is True
        assert result.check_type == CheckType.SVN
        assert result.details is None

    def test_create_failed_result_with_details(self):
        result = ValidationResult(
            check_type=CheckType.SIGNATURES,
            passed=False,
            message="Verification failed",
            details=["file1.asc", "file2.asc"],
            duration_seconds=1.5,
        )
        assert result.passed is False
        assert len(result.details) == 2
        assert result.duration_seconds == 1.5


class TestCheckType:
    """Tests for CheckType enum."""

    def test_check_type_values(self):
        assert CheckType.SVN.value == "svn"
        assert CheckType.REPRODUCIBLE_BUILD.value == "reproducible-build"
        assert CheckType.SIGNATURES.value == "signatures"
        assert CheckType.CHECKSUMS.value == "checksums"
        assert CheckType.LICENSES.value == "licenses"

    def test_check_type_from_string(self):
        assert CheckType("svn") == CheckType.SVN
        assert CheckType("signatures") == CheckType.SIGNATURES
