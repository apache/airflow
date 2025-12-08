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

import logging
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.teradata.hooks.tpt import TptHook


class TestTptHook:
    @patch("airflow.providers.teradata.hooks.tpt.SSHHook")
    @patch("airflow.models.Connection")
    def test_init_with_ssh(self, mock_conn, mock_ssh_hook):
        hook = TptHook(ssh_conn_id="ssh_default")
        assert hook.ssh_conn_id == "ssh_default"
        assert hook.ssh_hook is not None

    @patch("airflow.providers.teradata.hooks.tpt.SSHHook")
    @patch("airflow.models.Connection")
    def test_init_without_ssh(self, mock_conn, mock_ssh_hook):
        hook = TptHook()
        assert hook.ssh_conn_id is None
        assert hook.ssh_hook is None

    @patch("airflow.providers.teradata.hooks.tpt.SSHHook")
    @patch("airflow.models.Connection")
    @patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tbuild_via_ssh")
    @patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tbuild_locally")
    def test_execute_ddl_dispatch(self, mock_local, mock_ssh, mock_conn, mock_ssh_hook):
        # Local execution
        hook = TptHook()
        mock_local.return_value = 0
        assert hook.execute_ddl("SOME DDL", "/tmp") == 0
        mock_local.assert_called_once()

        # SSH execution
        hook = TptHook(ssh_conn_id="ssh_default")
        hook.ssh_hook = MagicMock()
        mock_ssh.return_value = 0
        assert hook.execute_ddl("SOME DDL", "/tmp") == 0
        mock_ssh.assert_called_once()

    def test_execute_ddl_empty_script(self):
        hook = TptHook()
        with pytest.raises(ValueError, match="TPT script must not be empty"):
            hook.execute_ddl("", "/tmp")

    def test_execute_ddl_empty_script_content(self):
        hook = TptHook()
        with pytest.raises(ValueError, match="TPT script content must not be empty after processing"):
            hook.execute_ddl("   ", "/tmp")  # Only whitespace

    @patch("airflow.providers.teradata.hooks.tpt.SSHHook")
    @patch("airflow.providers.teradata.hooks.tpt.terminate_subprocess")
    @patch("airflow.providers.teradata.hooks.tpt.secure_delete")
    @patch("airflow.providers.teradata.hooks.tpt.set_local_file_permissions")
    @patch("airflow.providers.teradata.hooks.tpt.subprocess.Popen")
    @patch("airflow.providers.teradata.hooks.tpt.shutil.which", return_value="/usr/bin/tbuild")
    def test_execute_tbuild_locally_success(
        self, mock_which, mock_popen, mock_set_permissions, mock_secure_delete, mock_terminate, mock_ssh_hook
    ):
        hook = TptHook()
        process = MagicMock()
        process.stdout.readline.side_effect = [b"All good\n", b""]
        process.wait.return_value = None
        process.returncode = 0
        mock_popen.return_value = process

        result = hook._execute_tbuild_locally("CREATE TABLE test (id INT);")
        assert result == 0
        mock_set_permissions.assert_called_once()

    @patch("airflow.providers.teradata.hooks.tpt.SSHHook")
    @patch("airflow.providers.teradata.hooks.tpt.terminate_subprocess")
    @patch("airflow.providers.teradata.hooks.tpt.secure_delete")
    @patch("airflow.providers.teradata.hooks.tpt.set_local_file_permissions")
    @patch("airflow.providers.teradata.hooks.tpt.subprocess.Popen")
    @patch("airflow.providers.teradata.hooks.tpt.shutil.which", return_value="/usr/bin/tbuild")
    def test_execute_tbuild_locally_failure(
        self, mock_which, mock_popen, mock_set_permissions, mock_secure_delete, mock_terminate, mock_ssh_hook
    ):
        hook = TptHook()
        process = MagicMock()
        process.stdout.readline.side_effect = [b"error: failed\n", b""]
        process.wait.return_value = None
        process.returncode = 1
        mock_popen.return_value = process

        with pytest.raises(RuntimeError):
            hook._execute_tbuild_locally("CREATE TABLE test (id INT);")
        mock_set_permissions.assert_called_once()

    @patch("airflow.providers.teradata.hooks.tpt.SSHHook")
    @patch("airflow.providers.teradata.hooks.tpt.execute_remote_command")
    @patch("airflow.providers.teradata.hooks.tpt.remote_secure_delete")
    @patch("airflow.providers.teradata.hooks.tpt.secure_delete")
    @patch("airflow.providers.teradata.hooks.tpt.set_remote_file_permissions")
    @patch("airflow.providers.teradata.hooks.tpt.decrypt_remote_file")
    @patch("airflow.providers.teradata.hooks.tpt.transfer_file_sftp")
    @patch("airflow.providers.teradata.hooks.tpt.generate_encrypted_file_with_openssl")
    @patch("airflow.providers.teradata.hooks.tpt.generate_random_password")
    @patch("airflow.providers.teradata.hooks.tpt.verify_tpt_utility_on_remote_host")
    @patch("airflow.providers.teradata.hooks.tpt.write_file")
    def test_execute_tbuild_via_ssh_success(
        self,
        mock_write_file,
        mock_verify_tpt,
        mock_gen_password,
        mock_encrypt_file,
        mock_transfer_file,
        mock_decrypt_file,
        mock_set_permissions,
        mock_secure_delete,
        mock_remote_secure_delete,
        mock_execute_remote_command,
        mock_ssh_hook,
    ):
        """Test successful execution of tbuild via SSH"""
        # Setup hook with SSH
        hook = TptHook(ssh_conn_id="ssh_default")
        hook.ssh_hook = MagicMock()

        # Mock SSH client
        mock_ssh_client = MagicMock()
        hook.ssh_hook.get_conn.return_value.__enter__.return_value = mock_ssh_client

        # Mock execute_remote_command
        mock_execute_remote_command.return_value = (0, "DDL executed successfully", "")

        # Mock password generation
        mock_gen_password.return_value = "test_password"

        # Execute the method
        result = hook._execute_tbuild_via_ssh("CREATE TABLE test (id INT);", "/tmp")

        # Assertions
        assert result == 0
        mock_verify_tpt.assert_called_once_with(
            mock_ssh_client, "tbuild", logging.getLogger("airflow.providers.teradata.hooks.tpt")
        )
        mock_write_file.assert_called_once()
        mock_gen_password.assert_called_once()
        mock_encrypt_file.assert_called_once()
        mock_transfer_file.assert_called_once()
        mock_decrypt_file.assert_called_once()
        mock_set_permissions.assert_called_once()
        mock_execute_remote_command.assert_called_once()
        mock_remote_secure_delete.assert_called_once()
        mock_secure_delete.assert_called()

    @patch("airflow.providers.teradata.hooks.tpt.SSHHook")
    @patch("airflow.providers.teradata.hooks.tpt.execute_remote_command")
    @patch("airflow.providers.teradata.hooks.tpt.remote_secure_delete")
    @patch("airflow.providers.teradata.hooks.tpt.secure_delete")
    @patch("airflow.providers.teradata.hooks.tpt.set_remote_file_permissions")
    @patch("airflow.providers.teradata.hooks.tpt.decrypt_remote_file")
    @patch("airflow.providers.teradata.hooks.tpt.transfer_file_sftp")
    @patch("airflow.providers.teradata.hooks.tpt.generate_encrypted_file_with_openssl")
    @patch("airflow.providers.teradata.hooks.tpt.generate_random_password")
    @patch("airflow.providers.teradata.hooks.tpt.verify_tpt_utility_on_remote_host")
    @patch("airflow.providers.teradata.hooks.tpt.write_file")
    def test_execute_tbuild_via_ssh_failure(
        self,
        mock_write_file,
        mock_verify_tpt,
        mock_gen_password,
        mock_encrypt_file,
        mock_transfer_file,
        mock_decrypt_file,
        mock_set_permissions,
        mock_secure_delete,
        mock_remote_secure_delete,
        mock_execute_remote_command,
        mock_ssh_hook,
    ):
        """Test failed execution of tbuild via SSH"""
        # Setup hook with SSH
        hook = TptHook(ssh_conn_id="ssh_default")
        hook.ssh_hook = MagicMock()

        # Mock SSH client
        mock_ssh_client = MagicMock()
        hook.ssh_hook.get_conn.return_value.__enter__.return_value = mock_ssh_client

        # Mock execute_remote_command with failure
        mock_execute_remote_command.return_value = (1, "DDL failed", "Syntax error")

        # Mock password generation
        mock_gen_password.return_value = "test_password"

        # Execute the method and expect failure
        with pytest.raises(RuntimeError, match="tbuild command failed with exit code 1"):
            hook._execute_tbuild_via_ssh("CREATE TABLE test (id INT);", "/tmp")

        # Verify cleanup was called even on failure
        mock_remote_secure_delete.assert_called_once()
        mock_secure_delete.assert_called()

    @patch("airflow.providers.teradata.hooks.tpt.SSHHook")
    def test_execute_tbuild_via_ssh_no_ssh_hook(self, mock_ssh_hook):
        """Test tbuild via SSH when SSH hook is not initialized"""
        hook = TptHook(ssh_conn_id="ssh_default")
        hook.ssh_hook = None  # Simulate uninitialized SSH hook

        with pytest.raises(ConnectionError, match="SSH connection is not established"):
            hook._execute_tbuild_via_ssh("CREATE TABLE test (id INT);", "/tmp")

    def test_on_kill(self):
        """Test on_kill method"""
        hook = TptHook()
        # Should not raise any exception
        hook.on_kill()
