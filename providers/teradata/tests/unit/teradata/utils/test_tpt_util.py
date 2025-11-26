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
from __future__ import annotations

import os
import subprocess
import tempfile
from unittest.mock import Mock, patch

import pytest

from airflow.providers.teradata.utils.tpt_util import (
    TPTConfig,
    decrypt_remote_file,
    execute_remote_command,
    get_remote_os,
    get_remote_temp_directory,
    prepare_tpt_ddl_script,
    remote_secure_delete,
    secure_delete,
    set_local_file_permissions,
    set_remote_file_permissions,
    terminate_subprocess,
    transfer_file_sftp,
    verify_tpt_utility_installed,
    verify_tpt_utility_on_remote_host,
    write_file,
)


class TestTptUtil:
    """Test cases for TPT utility functions."""

    def test_write_file(self):
        """Test write_file function."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
            test_path = tmp_file.name

        try:
            test_content = "Test content\nLine 2"
            write_file(test_path, test_content)

            # Verify file was written correctly
            with open(test_path, encoding="utf-8") as f:
                assert f.read() == test_content
        finally:
            if os.path.exists(test_path):
                os.unlink(test_path)

    @patch("os.path.exists")
    def test_secure_delete_file_not_exists(self, mock_exists):
        """Test secure_delete when file doesn't exist."""
        mock_exists.return_value = False
        mock_logger = Mock()

        # Should return early without error
        secure_delete("/nonexistent/file", mock_logger)
        mock_exists.assert_called_once_with("/nonexistent/file")

    @patch("shutil.which")
    @patch("subprocess.run")
    @patch("os.path.exists")
    def test_secure_delete_with_shred(self, mock_exists, mock_subprocess, mock_which):
        """Test secure_delete with shred available."""
        mock_exists.return_value = True
        mock_which.return_value = "/usr/bin/shred"
        mock_logger = Mock()

        secure_delete("/test/file", mock_logger)

        mock_which.assert_called_once_with("shred")
        mock_subprocess.assert_called_once_with(
            ["shred", "--remove", "/test/file"], check=True, timeout=TPTConfig.DEFAULT_TIMEOUT
        )
        mock_logger.info.assert_called_with("Securely removed file using shred: %s", "/test/file")

    @patch("shutil.which")
    @patch("os.remove")
    @patch("os.path.exists")
    def test_secure_delete_without_shred(self, mock_exists, mock_remove, mock_which):
        """Test secure_delete without shred available."""
        mock_exists.return_value = True
        mock_which.return_value = None
        mock_logger = Mock()

        secure_delete("/test/file", mock_logger)

        mock_which.assert_called_once_with("shred")
        mock_remove.assert_called_once_with("/test/file")
        mock_logger.info.assert_called_with("Removed file: %s", "/test/file")

    @patch("shutil.which")
    @patch("os.remove")
    @patch("os.path.exists")
    def test_secure_delete_os_error(self, mock_exists, mock_remove, mock_which):
        """Test secure_delete handles OSError gracefully."""
        mock_exists.return_value = True
        mock_which.return_value = None
        mock_remove.side_effect = OSError("Permission denied")
        mock_logger = Mock()

        secure_delete("/test/file", mock_logger)

        mock_logger.warning.assert_called_with(
            "Failed to remove file %s: %s", "/test/file", "Permission denied"
        )

    def test_remote_secure_delete_no_ssh_client(self):
        """Test remote_secure_delete with no SSH client."""
        mock_logger = Mock()
        remote_secure_delete(None, ["/remote/file"], mock_logger)
        # Should return early without errors

    def test_remote_secure_delete_no_files(self):
        """Test remote_secure_delete with no files."""
        mock_ssh = Mock()
        mock_logger = Mock()
        remote_secure_delete(mock_ssh, [], mock_logger)
        # Should return early without errors

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_remote_secure_delete_with_shred(self, mock_execute_cmd, mock_get_remote_os):
        """Test remote_secure_delete when shred is available."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "unix"

        # Mock the command execution for shred availability check and file deletion
        mock_execute_cmd.side_effect = [
            (0, "/usr/bin/shred", ""),  # shred availability check
            (0, "", ""),  # file1 deletion
            (0, "", ""),  # file2 deletion
        ]

        remote_secure_delete(mock_ssh, ["/remote/file1", "/remote/file2"], mock_logger)

        # Should call get_remote_os and execute_remote_command
        mock_get_remote_os.assert_called_once_with(mock_ssh, mock_logger)
        expected_calls = [
            ((mock_ssh, "command -v shred"),),
            ((mock_ssh, "shred --remove /remote/file1"),),
            ((mock_ssh, "shred --remove /remote/file2"),),
        ]
        assert mock_execute_cmd.call_args_list == expected_calls
        mock_logger.info.assert_called_with("Processed remote files: %s", "/remote/file1, /remote/file2")

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_remote_secure_delete_without_shred(self, mock_execute_cmd, mock_get_remote_os):
        """Test remote_secure_delete when shred is not available."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "unix"

        # Mock the command execution for shred not available and fallback deletion
        mock_execute_cmd.side_effect = [
            (1, "", ""),  # shred not available
            (0, "", ""),  # fallback deletion
        ]

        remote_secure_delete(mock_ssh, ["/remote/file"], mock_logger)

        # Should call get_remote_os and execute_remote_command
        mock_get_remote_os.assert_called_once_with(mock_ssh, mock_logger)
        calls = mock_execute_cmd.call_args_list
        assert len(calls) == 2
        assert calls[0][0] == (mock_ssh, "command -v shred")
        # Check that fallback command contains expected elements
        fallback_cmd = calls[1][0][1]
        assert "dd if=/dev/zero" in fallback_cmd
        assert "rm -f" in fallback_cmd

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_remote_secure_delete_windows(self, mock_execute_cmd, mock_get_remote_os):
        """Test remote_secure_delete on Windows."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "windows"

        # Mock the command execution for Windows deletion
        mock_execute_cmd.return_value = (0, "", "")

        remote_secure_delete(mock_ssh, ["/remote/file"], mock_logger)

        # Should call get_remote_os and execute_remote_command for Windows
        mock_get_remote_os.assert_called_once_with(mock_ssh, mock_logger)
        calls = mock_execute_cmd.call_args_list
        assert len(calls) == 1
        windows_cmd = calls[0][0][1]
        assert windows_cmd == 'if exist "\\remote\\file" del /f /q "\\remote\\file"'

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    def test_remote_secure_delete_exception(self, mock_get_remote_os):
        """Test remote_secure_delete handles exceptions gracefully."""
        mock_ssh = Mock()
        mock_get_remote_os.side_effect = Exception("SSH error")
        mock_logger = Mock()

        remote_secure_delete(mock_ssh, ["/remote/file"], mock_logger)

        mock_logger.warning.assert_called_with("Failed to remove remote files: %s", "SSH error")

    def test_terminate_subprocess_none(self):
        """Test terminate_subprocess with None subprocess."""
        mock_logger = Mock()
        terminate_subprocess(None, mock_logger)
        # Should return early without errors

    def test_terminate_subprocess_running(self):
        """Test terminate_subprocess with running subprocess."""
        mock_sp = Mock()
        mock_sp.poll.return_value = None  # Process is running
        mock_sp.pid = 12345
        mock_sp.wait.return_value = 0
        mock_logger = Mock()

        terminate_subprocess(mock_sp, mock_logger)

        mock_sp.terminate.assert_called_once()
        mock_sp.wait.assert_called_with(timeout=TPTConfig.DEFAULT_TIMEOUT)
        mock_logger.info.assert_any_call("Terminating subprocess (PID: %s)", 12345)
        mock_logger.info.assert_any_call("Subprocess terminated gracefully")

    def test_terminate_subprocess_timeout(self):
        """Test terminate_subprocess with timeout."""
        mock_sp = Mock()
        mock_sp.poll.return_value = None
        mock_sp.pid = 12345
        mock_sp.wait.side_effect = [
            subprocess.TimeoutExpired("cmd", 5),
            0,
        ]  # First call times out, second succeeds
        mock_logger = Mock()

        terminate_subprocess(mock_sp, mock_logger)

        # Should call terminate first, then kill
        mock_sp.terminate.assert_called_once()
        mock_sp.kill.assert_called_once()
        mock_logger.warning.assert_called_with(
            "Subprocess did not terminate gracefully within %d seconds, killing it", TPTConfig.DEFAULT_TIMEOUT
        )
        mock_logger.info.assert_any_call("Subprocess killed successfully")

    def test_terminate_subprocess_kill_error(self):
        """Test terminate_subprocess handles kill errors."""
        mock_sp = Mock()
        mock_sp.poll.return_value = None
        mock_sp.pid = 12345
        mock_sp.wait.side_effect = [subprocess.TimeoutExpired("cmd", 5), Exception("Kill failed")]
        mock_logger = Mock()

        terminate_subprocess(mock_sp, mock_logger)

        mock_sp.terminate.assert_called_once()
        mock_sp.kill.assert_called_once()
        mock_logger.error.assert_called_with("Error killing subprocess: %s", "Kill failed")

    def test_terminate_subprocess_not_running(self):
        """Test terminate_subprocess with already terminated subprocess."""
        mock_sp = Mock()
        mock_sp.poll.return_value = 0  # Process has terminated
        mock_logger = Mock()

        terminate_subprocess(mock_sp, mock_logger)

        # Should not attempt to terminate or kill
        mock_sp.terminate.assert_not_called()
        mock_sp.wait.assert_not_called()

    def test_terminate_subprocess_terminate_error(self):
        """Test terminate_subprocess handles terminate errors."""
        mock_sp = Mock()
        mock_sp.poll.return_value = None
        mock_sp.pid = 12345
        mock_sp.terminate.side_effect = Exception("Terminate failed")
        mock_logger = Mock()

        terminate_subprocess(mock_sp, mock_logger)

        mock_sp.terminate.assert_called_once()
        mock_logger.error.assert_called_with("Error terminating subprocess: %s", "Terminate failed")

    @patch("shutil.which")
    def test_verify_tpt_utility_installed_success(self, mock_which):
        """Test verify_tpt_utility_installed when utility is found."""
        mock_which.return_value = "/usr/bin/tbuild"

        # Should not raise exception
        verify_tpt_utility_installed("tbuild")
        mock_which.assert_called_once_with("tbuild")

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_verify_tpt_utility_on_remote_host_success(self, mock_execute_cmd, mock_get_remote_os):
        """Test verify_tpt_utility_on_remote_host when utility is found."""
        mock_ssh = Mock()
        mock_get_remote_os.return_value = "unix"
        mock_execute_cmd.return_value = (0, "/usr/bin/tbuild", "")

        # Should not raise exception
        verify_tpt_utility_on_remote_host(mock_ssh, "tbuild")
        mock_get_remote_os.assert_called_once()
        mock_execute_cmd.assert_called_once_with(mock_ssh, "which tbuild")

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_verify_tpt_utility_on_remote_host_windows(self, mock_execute_cmd, mock_get_remote_os):
        """Test verify_tpt_utility_on_remote_host on Windows."""
        mock_ssh = Mock()
        mock_get_remote_os.return_value = "windows"
        mock_execute_cmd.return_value = (0, "C:\\Program Files\\tbuild.exe", "")

        # Should not raise exception
        verify_tpt_utility_on_remote_host(mock_ssh, "tbuild")
        mock_get_remote_os.assert_called_once()
        mock_execute_cmd.assert_called_once_with(mock_ssh, "where tbuild")

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_verify_tpt_utility_on_remote_host_exception(self, mock_execute_cmd, mock_get_remote_os):
        """Test verify_tpt_utility_on_remote_host handles exceptions."""
        mock_ssh = Mock()
        mock_get_remote_os.side_effect = Exception("SSH connection failed")

        with pytest.raises(RuntimeError, match="Failed to verify TPT utility 'tbuild'"):
            verify_tpt_utility_on_remote_host(mock_ssh, "tbuild")

    def test_prepare_tpt_ddl_script_basic(self):
        """Test prepare_tpt_ddl_script with basic input."""
        sql = ["CREATE TABLE test (id INT)", "INSERT INTO test VALUES (1)"]
        error_list = [1001, 1002]
        source_conn = {"host": "testhost", "login": "testuser", "password": "testpass"}
        job_name = "test_job"

        result = prepare_tpt_ddl_script(sql, error_list, source_conn, job_name)

        assert "DEFINE JOB test_job" in result
        assert "TdpId = 'testhost'" in result
        assert "UserName = 'testuser'" in result
        assert "UserPassword = 'testpass'" in result
        assert "ErrorList = ['1001', '1002']" in result
        assert "('CREATE TABLE test (id INT);')" in result
        assert "('INSERT INTO test VALUES (1);')" in result

    def test_prepare_tpt_ddl_script_auto_job_name(self):
        """Test prepare_tpt_ddl_script with auto-generated job name."""
        sql = ["CREATE TABLE test (id INT)"]
        source_conn = {"host": "testhost", "login": "testuser", "password": "testpass"}

        result = prepare_tpt_ddl_script(sql, None, source_conn, None)

        assert "DEFINE JOB airflow_tptddl_" in result
        assert "ErrorList = ['']" in result

    def test_prepare_tpt_ddl_script_empty_sql(self):
        """Test prepare_tpt_ddl_script with empty SQL list."""
        source_conn = {"host": "testhost", "login": "testuser", "password": "testpass"}

        with pytest.raises(ValueError, match="SQL statement list must be a non-empty list"):
            prepare_tpt_ddl_script([], None, source_conn)

    def test_prepare_tpt_ddl_script_invalid_sql(self):
        """Test prepare_tpt_ddl_script with invalid SQL input."""
        source_conn = {"host": "testhost", "login": "testuser", "password": "testpass"}

        with pytest.raises(ValueError, match="SQL statement list must be a non-empty list"):
            prepare_tpt_ddl_script("not a list", None, source_conn)

    def test_prepare_tpt_ddl_script_empty_statements(self):
        """Test prepare_tpt_ddl_script with empty SQL statements."""
        sql = ["", "   ", None]
        source_conn = {"host": "testhost", "login": "testuser", "password": "testpass"}

        with pytest.raises(ValueError, match="No valid SQL statements found"):
            prepare_tpt_ddl_script(sql, None, source_conn)

    def test_prepare_tpt_ddl_script_sql_escaping(self):
        """Test prepare_tpt_ddl_script properly escapes single quotes."""
        sql = ["INSERT INTO test VALUES ('O''Reilly')"]
        source_conn = {"host": "testhost", "login": "testuser", "password": "testpass"}

        result = prepare_tpt_ddl_script(sql, None, source_conn)

        assert "('INSERT INTO test VALUES (''O''''Reilly'');')" in result

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_decrypt_remote_file_success(self, mock_execute_cmd, mock_get_remote_os):
        """Test decrypt_remote_file with successful decryption."""
        mock_ssh = Mock()
        mock_get_remote_os.return_value = "unix"
        mock_execute_cmd.return_value = (0, "", "")

        result = decrypt_remote_file(
            mock_ssh, "/remote/encrypted.file", "/remote/decrypted.file", "password123"
        )

        assert result == 0
        expected_cmd = "openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass pass:'password123' -in /remote/encrypted.file -out /remote/decrypted.file"
        mock_execute_cmd.assert_called_once_with(mock_ssh, expected_cmd)

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_decrypt_remote_file_with_quotes_in_password(self, mock_execute_cmd, mock_get_remote_os):
        """Test decrypt_remote_file with quotes in password."""
        mock_ssh = Mock()
        mock_get_remote_os.return_value = "unix"
        mock_execute_cmd.return_value = (0, "", "")

        decrypt_remote_file(mock_ssh, "/remote/encrypted.file", "/remote/decrypted.file", "pass'word")

        # Should escape single quotes
        expected_cmd = "openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass pass:'pass'\\''word' -in /remote/encrypted.file -out /remote/decrypted.file"
        mock_execute_cmd.assert_called_once_with(mock_ssh, expected_cmd)

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_decrypt_remote_file_windows(self, mock_execute_cmd, mock_get_remote_os):
        """Test decrypt_remote_file on Windows."""
        mock_ssh = Mock()
        mock_get_remote_os.return_value = "windows"
        mock_execute_cmd.return_value = (0, "", "")

        decrypt_remote_file(mock_ssh, "/remote/encrypted.file", "/remote/decrypted.file", 'pass"word')

        # Should escape double quotes for Windows
        expected_cmd = 'openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass pass:"pass""word" -in "/remote/encrypted.file" -out "/remote/decrypted.file"'
        mock_execute_cmd.assert_called_once_with(mock_ssh, expected_cmd)

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_decrypt_remote_file_failure(self, mock_execute_cmd, mock_get_remote_os):
        """Test decrypt_remote_file with decryption failure."""
        mock_ssh = Mock()
        mock_get_remote_os.return_value = "unix"
        mock_execute_cmd.return_value = (1, "", "Bad decrypt")

        with pytest.raises(RuntimeError, match="Decryption failed with exit status 1"):
            decrypt_remote_file(mock_ssh, "/remote/encrypted.file", "/remote/decrypted.file", "password123")

    def test_tpt_config_constants(self):
        """Test TPTConfig constants."""
        assert TPTConfig.DEFAULT_TIMEOUT == 5
        assert TPTConfig.FILE_PERMISSIONS_READ_ONLY == 0o400
        assert TPTConfig.TEMP_DIR_WINDOWS == "C:\\Windows\\Temp"
        assert TPTConfig.TEMP_DIR_UNIX == "/tmp"

    def test_execute_remote_command_success(self):
        """Test execute_remote_command with successful execution."""
        mock_ssh = Mock()
        mock_stdin = Mock()
        mock_stdout = Mock()
        mock_stderr = Mock()
        mock_channel = Mock()

        mock_stdout.channel = mock_channel
        mock_channel.recv_exit_status.return_value = 0
        mock_stdout.read.return_value.decode.return_value.strip.return_value = "output"
        mock_stderr.read.return_value.decode.return_value.strip.return_value = ""

        mock_ssh.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        exit_status, stdout_data, stderr_data = execute_remote_command(mock_ssh, "test command")

        assert exit_status == 0
        assert stdout_data == "output"
        assert stderr_data == ""
        mock_ssh.exec_command.assert_called_once_with("test command")
        mock_stdin.close.assert_called_once()
        mock_stdout.close.assert_called_once()
        mock_stderr.close.assert_called_once()

    def test_get_remote_os_windows(self):
        """Test get_remote_os detects Windows."""
        mock_ssh = Mock()
        mock_logger = Mock()

        with patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command") as mock_execute:
            mock_execute.return_value = (0, "Windows_NT", "")

            result = get_remote_os(mock_ssh, mock_logger)

            assert result == "windows"
            mock_execute.assert_called_once_with(mock_ssh, "echo %OS%")

    def test_get_remote_os_unix(self):
        """Test get_remote_os detects Unix."""
        mock_ssh = Mock()
        mock_logger = Mock()

        with patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command") as mock_execute:
            mock_execute.return_value = (0, "", "")

            result = get_remote_os(mock_ssh, mock_logger)

            assert result == "unix"

    def test_get_remote_os_exception(self):
        """Test get_remote_os handles exceptions."""
        mock_ssh = Mock()
        mock_logger = Mock()

        with patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command") as mock_execute:
            mock_execute.side_effect = Exception("SSH error")

            result = get_remote_os(mock_ssh, mock_logger)

            assert result == "unix"
            mock_logger.error.assert_called_with("Error detecting remote OS: %s", "SSH error")

    def test_set_local_file_permissions_success(self):
        """Test set_local_file_permissions with successful permission setting."""
        mock_logger = Mock()

        with tempfile.NamedTemporaryFile() as tmp_file:
            set_local_file_permissions(tmp_file.name, mock_logger)

            # Check if permissions were set correctly
            file_stat = os.stat(tmp_file.name)
            assert file_stat.st_mode & 0o777 == 0o400

    def test_set_local_file_permissions_file_not_exists(self):
        """Test set_local_file_permissions with non-existent file."""
        mock_logger = Mock()

        with pytest.raises(FileNotFoundError, match="File does not exist"):
            set_local_file_permissions("/nonexistent/file", mock_logger)

    def test_set_local_file_permissions_empty_path(self):
        """Test set_local_file_permissions with empty path."""
        mock_logger = Mock()

        set_local_file_permissions("", mock_logger)

        mock_logger.warning.assert_called_with("No file path provided for permission setting")

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_set_remote_file_permissions_unix(self, mock_execute_cmd, mock_get_remote_os):
        """Test set_remote_file_permissions on Unix."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "unix"
        mock_execute_cmd.return_value = (0, "", "")

        set_remote_file_permissions(mock_ssh, "/remote/file", mock_logger)

        mock_get_remote_os.assert_called_once_with(mock_ssh, mock_logger)
        mock_execute_cmd.assert_called_once_with(mock_ssh, "chmod 400 /remote/file")

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_set_remote_file_permissions_windows(self, mock_execute_cmd, mock_get_remote_os):
        """Test set_remote_file_permissions on Windows."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "windows"
        mock_execute_cmd.return_value = (0, "", "")

        set_remote_file_permissions(mock_ssh, "/remote/file", mock_logger)

        mock_get_remote_os.assert_called_once_with(mock_ssh, mock_logger)
        expected_cmd = 'icacls "/remote/file" /inheritance:r /grant:r "%USERNAME%":R'
        mock_execute_cmd.assert_called_once_with(mock_ssh, expected_cmd)

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_set_remote_file_permissions_failure(self, mock_execute_cmd, mock_get_remote_os):
        """Test set_remote_file_permissions with command failure."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "unix"
        mock_execute_cmd.return_value = (1, "", "Permission denied")

        with pytest.raises(RuntimeError, match="Failed to set permissions"):
            set_remote_file_permissions(mock_ssh, "/remote/file", mock_logger)

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_get_remote_temp_directory_windows(self, mock_execute_cmd, mock_get_remote_os):
        """Test get_remote_temp_directory on Windows."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "windows"
        mock_execute_cmd.return_value = (0, "C:\\Users\\User\\AppData\\Local\\Temp", "")

        result = get_remote_temp_directory(mock_ssh, mock_logger)

        assert result == "C:\\Users\\User\\AppData\\Local\\Temp"
        mock_execute_cmd.assert_called_once_with(mock_ssh, "echo %TEMP%")

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    def test_get_remote_temp_directory_unix(self, mock_get_remote_os):
        """Test get_remote_temp_directory on Unix."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "unix"

        result = get_remote_temp_directory(mock_ssh, mock_logger)

        assert result == "/tmp"

    @patch("airflow.providers.teradata.utils.tpt_util.get_remote_os")
    @patch("airflow.providers.teradata.utils.tpt_util.execute_remote_command")
    def test_get_remote_temp_directory_windows_fallback(self, mock_execute_cmd, mock_get_remote_os):
        """Test get_remote_temp_directory Windows fallback."""
        mock_ssh = Mock()
        mock_logger = Mock()
        mock_get_remote_os.return_value = "windows"
        mock_execute_cmd.return_value = (0, "%TEMP%", "")  # Command didn't expand

        result = get_remote_temp_directory(mock_ssh, mock_logger)

        assert result == TPTConfig.TEMP_DIR_WINDOWS
        mock_logger.warning.assert_called_with(
            "Could not get TEMP directory, using default: %s", TPTConfig.TEMP_DIR_WINDOWS
        )

    def test_transfer_file_sftp_success(self):
        """Test transfer_file_sftp with successful transfer."""
        mock_ssh = Mock()
        mock_sftp = Mock()
        mock_ssh.open_sftp.return_value = mock_sftp
        mock_logger = Mock()

        with tempfile.NamedTemporaryFile() as tmp_file:
            transfer_file_sftp(mock_ssh, tmp_file.name, "/remote/path/file.txt", mock_logger)

            mock_ssh.open_sftp.assert_called_once()
            mock_sftp.put.assert_called_once_with(tmp_file.name, "/remote/path/file.txt")
            mock_sftp.close.assert_called_once()

    def test_transfer_file_sftp_local_file_not_exists(self):
        """Test transfer_file_sftp with non-existent local file."""
        mock_ssh = Mock()
        mock_logger = Mock()

        with pytest.raises(FileNotFoundError, match="Local file does not exist"):
            transfer_file_sftp(mock_ssh, "/nonexistent/local/file", "/remote/path/file.txt", mock_logger)

    def test_transfer_file_sftp_transfer_error(self):
        """Test transfer_file_sftp with transfer error."""
        mock_ssh = Mock()
        mock_sftp = Mock()
        mock_sftp.put.side_effect = Exception("Transfer failed")
        mock_ssh.open_sftp.return_value = mock_sftp
        mock_logger = Mock()

        with tempfile.NamedTemporaryFile() as tmp_file:
            with pytest.raises(RuntimeError, match="Failed to transfer file"):
                transfer_file_sftp(mock_ssh, tmp_file.name, "/remote/path/file.txt", mock_logger)

            mock_sftp.close.assert_called_once()
