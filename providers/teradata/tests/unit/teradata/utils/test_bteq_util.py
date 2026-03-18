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
import stat
import unittest
from unittest.mock import MagicMock, call, patch

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.teradata.utils.bteq_util import (
    identify_os,
    is_valid_encoding,
    is_valid_file,
    is_valid_remote_bteq_script_file,
    prepare_bteq_script_for_local_execution,
    prepare_bteq_script_for_remote_execution,
    read_file,
    transfer_file_sftp,
    verify_bteq_installed,
    verify_bteq_installed_remote,
)


class TestBteqUtils:
    def test_identify_os_linux(self):
        # Arrange
        ssh_client = MagicMock()
        stdout_mock = MagicMock()
        stdout_mock.read.return_value = b"Linux\n"
        ssh_client.exec_command.return_value = (MagicMock(), stdout_mock, MagicMock())

        # Act
        os_info = identify_os(ssh_client)

        # Assert
        ssh_client.exec_command.assert_called_once_with("uname || ver")
        assert os_info == "linux\n"

    def test_identify_os_windows(self):
        # Arrange
        ssh_client = MagicMock()
        stdout_mock = MagicMock()
        stdout_mock.read.return_value = b"Microsoft Windows [Version 10.0.19045.3324]\n"
        ssh_client.exec_command.return_value = (MagicMock(), stdout_mock, MagicMock())

        # Act
        os_info = identify_os(ssh_client)

        # Assert
        ssh_client.exec_command.assert_called_once_with("uname || ver")
        assert "windows" in os_info

    def test_identify_os_macos(self):
        # Arrange
        ssh_client = MagicMock()
        stdout_mock = MagicMock()
        stdout_mock.read.return_value = b"Darwin\n"
        ssh_client.exec_command.return_value = (MagicMock(), stdout_mock, MagicMock())

        # Act
        os_info = identify_os(ssh_client)

        # Assert
        ssh_client.exec_command.assert_called_once_with("uname || ver")
        assert os_info == "darwin\n"

    def test_identify_os_empty_response(self):
        # Arrange
        ssh_client = MagicMock()
        stdout_mock = MagicMock()
        stdout_mock.read.return_value = b""
        ssh_client.exec_command.return_value = (MagicMock(), stdout_mock, MagicMock())

        # Act
        os_info = identify_os(ssh_client)

        # Assert
        ssh_client.exec_command.assert_called_once_with("uname || ver")
        assert os_info == ""

    @patch("shutil.which")
    def test_verify_bteq_installed_success(self, mock_which):
        mock_which.return_value = "/usr/bin/bteq"
        # Should not raise
        verify_bteq_installed()
        mock_which.assert_called_with("bteq")

    @patch("shutil.which")
    def test_verify_bteq_installed_fail(self, mock_which):
        mock_which.return_value = None
        with pytest.raises(AirflowException):
            verify_bteq_installed()

    def test_prepare_bteq_script_for_remote_execution(self):
        conn = {"host": "myhost", "login": "user", "password": "pass"}
        sql = "SELECT * FROM DUAL;"
        script = prepare_bteq_script_for_remote_execution(conn, sql)
        assert ".LOGON myhost/user,pass" in script
        assert "SELECT * FROM DUAL;" in script
        assert ".EXIT" in script

    def test_prepare_bteq_script_for_local_execution(self):
        sql = "SELECT 1;"
        script = prepare_bteq_script_for_local_execution(sql)
        assert "SELECT 1;" in script
        assert ".EXIT" in script

    @patch("airflow.providers.teradata.utils.bteq_util.identify_os", return_value="linux")
    def test_verify_bteq_installed_remote_linux(self, mock_os):
        ssh_client = MagicMock()
        stdout_mock = MagicMock()
        stdout_mock.read.return_value = b"/usr/bin/bteq"
        stdout_mock.channel.recv_exit_status.return_value = 0

        ssh_client.exec_command.return_value = (MagicMock(), stdout_mock, MagicMock())

        verify_bteq_installed_remote(ssh_client)
        ssh_client.exec_command.assert_called_once_with("which bteq")

    @patch("airflow.providers.teradata.utils.bteq_util.identify_os", return_value="windows")
    def test_verify_bteq_installed_remote_windows(self, mock_os):
        ssh_client = MagicMock()
        stdout_mock = MagicMock()
        stdout_mock.read.return_value = b"C:\\Program Files\\bteq.exe"
        stdout_mock.channel.recv_exit_status.return_value = 0

        ssh_client.exec_command.return_value = (MagicMock(), stdout_mock, MagicMock())

        verify_bteq_installed_remote(ssh_client)
        ssh_client.exec_command.assert_called_once_with("where bteq")

    @patch("airflow.providers.teradata.utils.bteq_util.identify_os", return_value="darwin")
    def test_verify_bteq_installed_remote_macos(self, mock_os):
        ssh_client = MagicMock()
        stdout_mock = MagicMock()
        stdout_mock.read.return_value = b"/usr/local/bin/bteq"
        stdout_mock.channel.recv_exit_status.return_value = 0

        ssh_client.exec_command.return_value = (MagicMock(), stdout_mock, MagicMock())

        verify_bteq_installed_remote(ssh_client)

        ssh_client.exec_command.assert_has_calls(
            [
                call("command -v zsh"),
                call('zsh -l -c "which bteq"'),
            ]
        )

    @patch("airflow.providers.teradata.utils.bteq_util.identify_os", return_value="darwin")
    def test_verify_bteq_installed_remote_macos_which_called_when_no_zsh(self, mock_os):
        ssh_client = MagicMock()

        # Mock for "command -v zsh" returning empty (no zsh)
        stdin_mock_1 = MagicMock()
        stdout_mock_1 = MagicMock()
        stderr_mock_1 = MagicMock()
        stdout_mock_1.read.return_value = b""  # No zsh path found
        stderr_mock_1.read.return_value = b""  # Return empty bytes here!
        ssh_client.exec_command.side_effect = [
            (stdin_mock_1, stdout_mock_1, stderr_mock_1),  # command -v zsh
            (MagicMock(), MagicMock(), MagicMock()),  # which bteq
        ]

        # Mock for "which bteq" command response
        stdin_mock_2 = MagicMock()
        stdout_mock_2 = MagicMock()
        stderr_mock_2 = MagicMock()
        stdout_mock_2.channel.recv_exit_status.return_value = 0
        stdout_mock_2.read.return_value = b"/usr/local/bin/bteq"
        stderr_mock_2.read.return_value = b""  # Also return bytes here

        # Since side_effect was already assigned, override second call manually
        ssh_client.exec_command.side_effect = [
            (stdin_mock_1, stdout_mock_1, stderr_mock_1),  # command -v zsh
            (stdin_mock_2, stdout_mock_2, stderr_mock_2),  # which bteq
        ]

        verify_bteq_installed_remote(ssh_client)

        ssh_client.exec_command.assert_has_calls(
            [
                call("command -v zsh"),
                call("which bteq"),
            ]
        )

    @patch("airflow.providers.teradata.utils.bteq_util.identify_os", return_value="darwin")
    def test_verify_bteq_installed_remote_macos_which_fails_no_zsh(self, mock_os):
        ssh_client = MagicMock()

        # Mock for "command -v zsh" returning empty (no zsh)
        stdin_mock_1 = MagicMock()
        stdout_mock_1 = MagicMock()
        stderr_mock_1 = MagicMock()
        stdout_mock_1.read.return_value = b""  # No zsh path found
        ssh_client.exec_command.side_effect = [
            (stdin_mock_1, stdout_mock_1, stderr_mock_1),  # command -v zsh
            (MagicMock(), MagicMock(), MagicMock()),  # which bteq
        ]

        # For which bteq failure
        ssh_client.exec_command.return_value[1].channel.recv_exit_status.return_value = 1
        ssh_client.exec_command.return_value[1].read.return_value = b""
        ssh_client.exec_command.return_value[2].read.return_value = b"command not found"

        with pytest.raises(AirflowException) as exc_info:
            verify_bteq_installed_remote(ssh_client)

        assert "BTEQ is not installed or not available in PATH" in str(exc_info.value)

        ssh_client.exec_command.assert_has_calls(
            [
                call("command -v zsh"),
                call("which bteq"),
            ]
        )

    @patch("airflow.providers.teradata.utils.bteq_util.identify_os", return_value="linux")
    def test_verify_bteq_installed_remote_fail(self, mock_os):
        ssh_client = MagicMock()
        stdout_mock = MagicMock()
        stderr_mock = MagicMock()
        stdout_mock.read.return_value = b""
        stderr_mock.read.return_value = b"command not found"
        stdout_mock.channel.recv_exit_status.return_value = 1

        ssh_client.exec_command.return_value = (MagicMock(), stdout_mock, stderr_mock)

        with pytest.raises(AirflowException, match="BTEQ is not installed or not available in PATH"):
            verify_bteq_installed_remote(ssh_client)
        ssh_client.exec_command.assert_called_once_with("which bteq")

    @patch("paramiko.SSHClient.exec_command")
    def test_verify_bteq_installed_remote_success(self, mock_exec):
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stdout.read.return_value = b"/usr/bin/bteq"
        mock_stderr.read.return_value = b""
        mock_exec.return_value = (mock_stdin, mock_stdout, mock_stderr)

        ssh_client = MagicMock()
        ssh_client.exec_command = mock_exec

        # Should not raise
        verify_bteq_installed_remote(ssh_client)

    @patch("paramiko.SSHClient.open_sftp")
    def test_transfer_file_sftp(self, mock_open_sftp):
        mock_sftp = MagicMock()
        mock_open_sftp.return_value = mock_sftp

        ssh_client = MagicMock()
        ssh_client.open_sftp = mock_open_sftp

        transfer_file_sftp(ssh_client, "local_file.txt", "remote_file.txt")

        mock_open_sftp.assert_called_once()
        mock_sftp.put.assert_called_once_with("local_file.txt", "remote_file.txt")
        mock_sftp.close.assert_called_once()

    def test_is_valid_file(self):
        # create temp file
        with open("temp_test_file.txt", "w") as f:
            f.write("hello")

        assert is_valid_file("temp_test_file.txt") is True
        assert is_valid_file("non_existent_file.txt") is False

        os.remove("temp_test_file.txt")

    def test_is_valid_encoding(self):
        # Write a file with UTF-8 encoding
        with open("temp_utf8_file.txt", "w", encoding="utf-8") as f:
            f.write("hello world")

        # Should return True
        assert is_valid_encoding("temp_utf8_file.txt", encoding="utf-8") is True

        # Cleanup
        os.remove("temp_utf8_file.txt")

    def test_read_file_success(self):
        content = "Sample content"
        with open("temp_read_file.txt", "w") as f:
            f.write(content)

        read_content = read_file("temp_read_file.txt")
        assert read_content == content
        os.remove("temp_read_file.txt")

    def test_read_file_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            read_file("non_existent_file.txt")

    @patch("paramiko.SSHClient.open_sftp")
    def test_is_valid_remote_bteq_script_file_exists(self, mock_open_sftp):
        mock_sftp = MagicMock()
        mock_open_sftp.return_value = mock_sftp

        # Mock stat to return a regular file mode
        mock_stat = MagicMock()
        mock_stat.st_mode = stat.S_IFREG
        mock_sftp.stat.return_value = mock_stat

        ssh_client = MagicMock()
        ssh_client.open_sftp = mock_open_sftp

        result = is_valid_remote_bteq_script_file(ssh_client, "/remote/path/to/file")
        assert result is True
        mock_sftp.close.assert_called_once()

    @patch("paramiko.SSHClient.open_sftp")
    def test_is_valid_remote_bteq_script_file_not_exists(self, mock_open_sftp):
        mock_sftp = MagicMock()
        mock_open_sftp.return_value = mock_sftp

        # Raise FileNotFoundError for stat
        mock_sftp.stat.side_effect = FileNotFoundError

        ssh_client = MagicMock()
        ssh_client.open_sftp = mock_open_sftp

        result = is_valid_remote_bteq_script_file(ssh_client, "/remote/path/to/file")
        assert result is False
        mock_sftp.close.assert_called_once()

    def test_is_valid_remote_bteq_script_file_none_path(self):
        ssh_client = MagicMock()
        result = is_valid_remote_bteq_script_file(ssh_client, None)
        assert result is False


if __name__ == "__main__":
    unittest.main()
