#
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

from unittest import mock

import pytest

from airflow.providers.ssh.hooks.ssh import SSHHookAsync


class TestSSHHookAsync:
    def test_init_with_conn_id(self):
        """Test initialization with connection ID."""
        hook = SSHHookAsync(ssh_conn_id="test_ssh_conn")
        assert hook.ssh_conn_id == "test_ssh_conn"
        assert hook.host is None
        assert hook.port is None

    def test_init_with_overrides(self):
        """Test initialization with parameter overrides."""
        hook = SSHHookAsync(
            ssh_conn_id="test_ssh_conn",
            host="custom.host.com",
            port=2222,
            username="testuser",
            password="testpass",
        )
        assert hook.host == "custom.host.com"
        assert hook.port == 2222
        assert hook.username == "testuser"
        assert hook.password == "testpass"

    def test_init_default_known_hosts(self):
        """Test default known_hosts path."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")
        assert "known_hosts" in str(hook.known_hosts)

    def test_parse_extras_key_file(self):
        """Test parsing key_file from connection extras."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")
        mock_conn = mock.MagicMock()
        mock_conn.extra_dejson = {"key_file": "/path/to/key"}
        mock_conn.host = "test.host"

        hook._parse_extras(mock_conn)
        assert hook.key_file == "/path/to/key"

    def test_parse_extras_no_host_key_check(self):
        """Test parsing no_host_key_check from connection extras."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")
        mock_conn = mock.MagicMock()
        mock_conn.extra_dejson = {"no_host_key_check": "true"}
        mock_conn.host = "test.host"

        hook._parse_extras(mock_conn)
        assert hook.known_hosts == "none"

    def test_parse_extras_host_key(self):
        """Test parsing host_key from connection extras."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")
        mock_conn = mock.MagicMock()
        mock_conn.extra_dejson = {"host_key": "ssh-rsa AAAAB3...", "no_host_key_check": "false"}
        mock_conn.host = "test.host"

        hook._parse_extras(mock_conn)
        assert hook.known_hosts == b"test.host ssh-rsa AAAAB3..."

    def test_parse_extras_host_key_with_no_check_raises(self):
        """Test that host_key with no_host_key_check raises error."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")
        mock_conn = mock.MagicMock()
        mock_conn.extra_dejson = {
            "host_key": "ssh-rsa AAAAB3...",
            "no_host_key_check": "true",
        }
        mock_conn.host = "test.host"

        with pytest.raises(ValueError, match="Host key check was skipped"):
            hook._parse_extras(mock_conn)

    def test_parse_extras_private_key(self):
        """Test parsing private_key from connection extras."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")
        mock_conn = mock.MagicMock()
        test_key = "test-private-key-content"
        mock_conn.extra_dejson = {"private_key": test_key}
        mock_conn.host = "test.host"

        hook._parse_extras(mock_conn)
        assert hook.private_key == test_key

    @pytest.mark.asyncio
    async def test_get_conn_builds_config(self):
        """Test that _get_conn builds correct connection config."""
        hook = SSHHookAsync(
            ssh_conn_id="test_conn",
            host="test.host.com",
            port=22,
            username="testuser",
        )

        mock_conn_obj = mock.MagicMock()
        mock_conn_obj.extra_dejson = {"no_host_key_check": "true"}
        mock_conn_obj.host = None
        mock_conn_obj.port = None
        mock_conn_obj.login = None
        mock_conn_obj.password = None
        mock_conn_obj.extra = "{}"

        mock_ssh_client = mock.AsyncMock()

        with mock.patch("asgiref.sync.sync_to_async") as mock_sync:
            mock_sync.return_value = mock.AsyncMock(return_value=mock_conn_obj)

            with mock.patch("asyncssh.connect", new_callable=mock.AsyncMock) as mock_connect:
                mock_connect.return_value = mock_ssh_client
                result = await hook._get_conn()

                mock_connect.assert_called_once()
                call_kwargs = mock_connect.call_args[1]
                assert call_kwargs["host"] == "test.host.com"
                assert call_kwargs["port"] == 22
                assert call_kwargs["username"] == "testuser"
                assert result == mock_ssh_client

    @pytest.mark.asyncio
    async def test_run_command(self):
        """Test running a command."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        mock_result = mock.MagicMock()
        mock_result.exit_status = 0
        mock_result.stdout = "output"
        mock_result.stderr = ""

        mock_conn = mock.AsyncMock()
        mock_conn.run = mock.AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = mock.AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = mock.AsyncMock(return_value=None)

        with mock.patch.object(hook, "_get_conn", return_value=mock_conn):
            exit_code, stdout, stderr = await hook.run_command("echo test")

            assert exit_code == 0
            assert stdout == "output"
            assert stderr == ""
            mock_conn.run.assert_called_once_with("echo test", timeout=None, check=False)

    @pytest.mark.asyncio
    async def test_run_command_output(self):
        """Test running a command and getting output."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        with mock.patch.object(hook, "run_command", return_value=(0, "test output", "")):
            output = await hook.run_command_output("echo test")
            assert output == "test output"
