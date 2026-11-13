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

import json
import warnings
from unittest import mock

import asyncssh
import pytest
from asyncssh.compression import get_default_compression_algs

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.ssh.hooks.ssh import SSHHookAsync


def _connection(extra: dict, host: str = "test.host") -> Connection:
    return Connection(conn_id="test_conn", conn_type="ssh", host=host, extra=json.dumps(extra))


def _parse_extra_dict(hook: SSHHookAsync, extra: dict, host: str = "test.host") -> None:
    """Parse *extra* as ``_get_conn`` does: the connection plus its extra, read beforehand."""
    hook._parse_extras(_connection(extra, host=host), extra)


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

        _parse_extra_dict(hook, {"key_file": "/path/to/key"})
        assert hook.key_file == "/path/to/key"

    def test_parse_extras_no_host_key_check(self):
        """Test parsing no_host_key_check from connection extras."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        _parse_extra_dict(hook, {"no_host_key_check": "true"})
        assert hook.known_hosts == "none"

    def test_parse_extras_no_host_key_check_defaults_to_false(self):
        """A connection that omits ``no_host_key_check`` keeps host key verification enabled."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        _parse_extra_dict(hook, {})
        assert hook.known_hosts != "none"

    def test_parse_extras_honours_deprecated_alias(self):
        """``ignore_hostkey_verification`` keeps working on the async path too."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        with pytest.warns(AirflowProviderDeprecationWarning, match="ignore_hostkey_verification"):
            _parse_extra_dict(hook, {"ignore_hostkey_verification": True})
        assert hook.known_hosts == "none"

    def test_parse_extras_canonical_key_wins_over_alias(self):
        """``no_host_key_check`` takes precedence and suppresses the deprecation warning."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        with warnings.catch_warnings():
            warnings.simplefilter("error", AirflowProviderDeprecationWarning)
            _parse_extra_dict(hook, {"no_host_key_check": False, "ignore_hostkey_verification": True})
        assert hook.known_hosts != "none"

    def test_parse_extras_host_key_uses_host_override(self):
        """The known_hosts entry names the host actually connected to, not the connection's host."""
        hook = SSHHookAsync(ssh_conn_id="test_conn", host="override.example")

        _parse_extra_dict(hook, {"host_key": "ssh-ed25519 AAAAC3..."}, host="connection.example")
        assert hook.known_hosts == b"override.example ssh-ed25519 AAAAC3..."

    def test_parse_extras_bare_host_key_is_rsa(self):
        """A bare base64 host key is RSA, as on the sync hook, and asyncssh needs the type."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        _parse_extra_dict(hook, {"host_key": "AAAAB3NzaC1yc2E"})
        assert hook.known_hosts == b"test.host ssh-rsa AAAAB3NzaC1yc2E"

    def test_parse_extras_host_key(self):
        """Test parsing host_key from connection extras."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        _parse_extra_dict(hook, {"host_key": " ssh-rsa AAAAB3... user@host ", "no_host_key_check": "false"})
        assert hook.known_hosts == b"test.host ssh-rsa AAAAB3..."

    def test_parse_extras_dss_host_key_raises(self):
        """Test that ssh-dss host_key is rejected."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        with pytest.raises(ValueError, match="DSA/DSS host keys"):
            _parse_extra_dict(hook, {"host_key": "ssh-dss\tAAAAB3...", "no_host_key_check": "false"})

    def test_parse_extras_host_key_with_no_check_raises(self):
        """Test that host_key with no_host_key_check raises error."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        with pytest.raises(ValueError, match="Host key check was skipped"):
            _parse_extra_dict(hook, {"host_key": "ssh-rsa AAAAB3...", "no_host_key_check": "true"})

    def test_parse_extras_private_key(self):
        """Test parsing private_key from connection extras."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")
        test_key = "test-private-key-content"

        _parse_extra_dict(hook, {"private_key": test_key})
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

        mock_conn_obj = Connection(
            conn_id="test_conn", conn_type="ssh", extra=json.dumps({"no_host_key_check": "true"})
        )

        mock_ssh_client = mock.AsyncMock(spec=asyncssh.SSHClientConnection)

        with mock.patch(
            "airflow.providers.ssh.hooks.ssh.get_async_connection",
            autospec=True,
            return_value=mock_conn_obj,
        ):
            with mock.patch("asyncssh.connect", new_callable=mock.AsyncMock) as mock_connect:
                mock_connect.return_value = mock_ssh_client
                result = await hook._get_conn()

                mock_connect.assert_called_once()
                call_kwargs = mock_connect.call_args[1]
                assert call_kwargs["host"] == "test.host.com"
                assert call_kwargs["port"] == 22
                assert call_kwargs["username"] == "testuser"
                assert result == mock_ssh_client

    @mock.patch.object(Connection, "extra_dejson", new_callable=mock.PropertyMock, side_effect=AssertionError)
    @mock.patch(
        "airflow.providers.ssh.hooks.ssh.get_async_extra_dejson",
        autospec=True,
        return_value={"no_host_key_check": "true"},
    )
    # asyncssh.connect is wrapped by @async_context_manager, so autospec would not be awaitable.
    @mock.patch("asyncssh.connect", new_callable=mock.AsyncMock)
    @mock.patch("airflow.providers.ssh.hooks.ssh.get_async_connection", autospec=True)
    @pytest.mark.asyncio
    async def test_get_conn_reads_extra_asynchronously(
        self, mock_get_connection, mock_connect, mock_get_extra, mock_extra_dejson
    ):
        """``extra_dejson`` masks synchronously, so the async hook reads the extra through common.compat."""
        # The connection's own extra is empty: only the asynchronously read extra can disable the check.
        mock_get_connection.return_value = _connection({})

        await SSHHookAsync(ssh_conn_id="test_conn")._get_conn()

        mock_get_extra.assert_awaited_once_with(mock_get_connection.return_value)
        mock_extra_dejson.assert_not_called()
        assert mock_connect.call_args.kwargs["known_hosts"] is None

    @pytest.mark.parametrize(
        ("extra", "compression_algs"),
        [
            pytest.param({}, ["zlib@openssh.com", "zlib", "none"], id="default"),
            pytest.param({"compress": "true"}, ["zlib@openssh.com", "zlib", "none"], id="true"),
            pytest.param({"compress": "false"}, ["none"], id="false-string"),
            pytest.param({"compress": False}, ["none"], id="false-bool"),
        ],
    )
    @mock.patch("asyncssh.connect", new_callable=mock.AsyncMock)
    @mock.patch("airflow.providers.ssh.hooks.ssh.get_async_connection", autospec=True)
    @pytest.mark.asyncio
    async def test_get_conn_compresses_like_sync_hook(
        self, mock_get_connection, mock_connect, extra, compression_algs
    ):
        """zlib is offered unless the ``compress`` extra is false, as ``SSHHook`` does."""
        mock_get_connection.return_value = _connection({"no_host_key_check": "true", **extra})

        await SSHHookAsync(ssh_conn_id="test_conn")._get_conn()

        assert mock_connect.call_args.kwargs["compression_algs"] == compression_algs

    def test_asyncssh_offers_no_compression_first_by_default(self):
        """
        ``_get_conn`` passes its own list because asyncssh prefers ``none`` from 2.18, the floor.

        Older versions preferred zlib, so supporting them means handling both defaults in the hook.
        """
        assert get_default_compression_algs()[0] == b"none"

    @mock.patch("airflow.providers.ssh.hooks.ssh.os.path.isfile", return_value=False)
    @mock.patch("asyncssh.connect", new_callable=mock.AsyncMock)
    @mock.patch(
        "airflow.providers.ssh.hooks.ssh.get_async_connection",
        new_callable=mock.AsyncMock,
    )
    @pytest.mark.asyncio
    async def test_get_conn_omits_missing_default_known_hosts(
        self, mock_get_connection, mock_connect, mock_isfile
    ):
        mock_get_connection.return_value = Connection(host="test.host", extra="{}")

        hook = SSHHookAsync(ssh_conn_id="test_conn")
        await hook._get_conn()

        mock_isfile.assert_called_once_with(hook.known_hosts)
        assert "known_hosts" not in mock_connect.call_args.kwargs

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

    @pytest.mark.asyncio
    async def test_get_tunnel(self):
        """Test that get_tunnel returns an AsyncSSHTunnel."""
        from airflow.providers.ssh.tunnel import AsyncSSHTunnel

        hook = SSHHookAsync(ssh_conn_id="test_conn")

        mock_ssh_conn = mock.MagicMock()

        with mock.patch.object(hook, "_get_conn", new_callable=mock.AsyncMock, return_value=mock_ssh_conn):
            tunnel = await hook.get_tunnel(remote_port=5432)
            assert isinstance(tunnel, AsyncSSHTunnel)

    @pytest.mark.asyncio
    async def test_get_tunnel_async_context_manager(self):
        """Test AsyncSSHTunnel as async context manager with local_bind_port."""

        hook = SSHHookAsync(ssh_conn_id="test_conn")

        mock_listener = mock.MagicMock()
        mock_listener.get_port.return_value = 15432
        mock_listener.close = mock.MagicMock()
        mock_listener.wait_closed = mock.AsyncMock()

        mock_ssh_conn = mock.MagicMock()
        mock_ssh_conn.forward_local_port = mock.AsyncMock(return_value=mock_listener)
        mock_ssh_conn.close = mock.MagicMock()
        mock_ssh_conn.wait_closed = mock.AsyncMock()

        with mock.patch.object(hook, "_get_conn", new_callable=mock.AsyncMock, return_value=mock_ssh_conn):
            async with await hook.get_tunnel(remote_port=5432) as tunnel:
                assert tunnel.local_bind_port == 15432
                mock_ssh_conn.forward_local_port.assert_called_once_with("localhost", 0, "localhost", 5432)

        # Verify cleanup
        mock_listener.close.assert_called_once()
        mock_listener.wait_closed.assert_called_once()
        mock_ssh_conn.close.assert_called_once()
        mock_ssh_conn.wait_closed.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_tunnel_with_local_port(self):
        """Test AsyncSSHTunnel with explicit local port."""
        hook = SSHHookAsync(ssh_conn_id="test_conn")

        mock_listener = mock.MagicMock()
        mock_listener.get_port.return_value = 19999
        mock_listener.close = mock.MagicMock()
        mock_listener.wait_closed = mock.AsyncMock()

        mock_ssh_conn = mock.MagicMock()
        mock_ssh_conn.forward_local_port = mock.AsyncMock(return_value=mock_listener)
        mock_ssh_conn.close = mock.MagicMock()
        mock_ssh_conn.wait_closed = mock.AsyncMock()

        with mock.patch.object(hook, "_get_conn", new_callable=mock.AsyncMock, return_value=mock_ssh_conn):
            async with await hook.get_tunnel(remote_port=5432, local_port=19999) as tunnel:
                assert tunnel.local_bind_port == 19999
                mock_ssh_conn.forward_local_port.assert_called_once_with(
                    "localhost", 19999, "localhost", 5432
                )
