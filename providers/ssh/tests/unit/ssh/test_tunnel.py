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

import socket
import threading
import time
from typing import TYPE_CHECKING
from unittest import mock

import paramiko
import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.ssh.tunnel import SSHTunnel

if TYPE_CHECKING:
    from collections.abc import Callable


@pytest.fixture
def mock_ssh_client():
    client = mock.MagicMock(spec=paramiko.SSHClient)
    transport = mock.MagicMock(spec=paramiko.Transport)
    transport.is_active.return_value = True
    client.get_transport.return_value = transport
    return client


class TestSSHTunnel:
    def test_local_bind_port_is_available_after_init(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        try:
            port = tunnel.local_bind_port
            assert isinstance(port, int)
            assert port > 0
        finally:
            tunnel._stop_forwarding()
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()

    def test_local_bind_address(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        try:
            host, port = tunnel.local_bind_address
            assert host == "localhost"
            assert port == tunnel.local_bind_port
        finally:
            tunnel._stop_forwarding()
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()

    def test_explicit_local_port(self, mock_ssh_client):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("localhost", 0))
        free_port = sock.getsockname()[1]
        sock.close()

        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432, local_port=free_port)
        try:
            assert tunnel.local_bind_port == free_port
        finally:
            tunnel._stop_forwarding()
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()

    def test_context_manager_starts_and_stops(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        with tunnel as t:
            assert t is tunnel
            assert tunnel._running is True
            assert tunnel._thread is not None
            assert tunnel._thread.is_alive()
        assert tunnel._running is False
        assert tunnel._thread is None

    def test_start_emits_deprecation_warning(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        try:
            with pytest.warns(AirflowProviderDeprecationWarning, match="SSHTunnel.start"):
                tunnel.start()
        finally:
            tunnel._stop_forwarding()
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()

    def test_stop_emits_deprecation_warning(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        tunnel._start_forwarding()
        with pytest.warns(AirflowProviderDeprecationWarning, match="SSHTunnel.stop"):
            tunnel.stop()

    def test_getattr_migration_error(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        try:
            with pytest.raises(AttributeError, match="SSHTunnelForwarder has been replaced"):
                tunnel.tunnel_is_up
        finally:
            tunnel._stop_forwarding()
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()

    def test_getattr_unknown_attribute(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        try:
            with pytest.raises(AttributeError, match="has no attribute 'nonexistent'"):
                tunnel.nonexistent
        finally:
            tunnel._stop_forwarding()
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()

    def test_double_start_is_noop(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        try:
            tunnel._start_forwarding()
            thread1 = tunnel._thread
            tunnel._start_forwarding()
            assert tunnel._thread is thread1
        finally:
            tunnel._stop_forwarding()
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()

    def test_stop_without_start_is_noop(self, mock_ssh_client):
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        try:
            tunnel._stop_forwarding()
        finally:
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()

    def test_forwarding_thread_accepts_and_forwards(self, mock_ssh_client):
        """Test that data is forwarded between local socket and SSH channel."""
        channel = mock.MagicMock(spec=paramiko.Channel)
        channel.closed = False
        channel.recv.return_value = b"response"

        transport = mock_ssh_client.get_transport.return_value
        transport.open_channel.return_value = channel

        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432)
        with tunnel:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                client.connect(("localhost", tunnel.local_bind_port))
                client.sendall(b"hello")
                # Give the forwarding thread time to process
                threading.Event().wait(0.2)
            finally:
                client.close()

    def test_custom_logger(self, mock_ssh_client):
        custom_logger = mock.MagicMock()
        tunnel = SSHTunnel(mock_ssh_client, "remotehost", 5432, logger=custom_logger)
        try:
            assert tunnel._logger is custom_logger
        finally:
            tunnel._stop_forwarding()
            if tunnel._server_socket is not None:
                tunnel._server_socket.close()


def _wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> bool:
    """Return whether ``condition`` became true within ``timeout`` seconds, checking every 50 ms."""
    end = time.monotonic() + timeout
    while not condition():
        if time.monotonic() >= end:
            return False
        threading.Event().wait(0.05)
    return True


def _round_trip(port: int, message: bytes) -> bytes:
    with socket.create_connection(("localhost", port), timeout=5) as conn:
        conn.sendall(message)
        return conn.recv(100)


class TestSSHTunnelForwarding:
    @pytest.mark.usefixtures("over_1024_open_fds")
    def test_forwards_with_descriptors_above_fd_setsize(self, in_process_ssh_client):
        with SSHTunnel(in_process_ssh_client, "db.internal", 5432) as tunnel:
            sequential = [_round_trip(tunnel.local_bind_port, b"ping-%d" % i) for i in range(5)]
            concurrent = [
                socket.create_connection(("localhost", tunnel.local_bind_port), timeout=5) for _ in range(5)
            ]
            try:
                for i, conn in enumerate(concurrent):
                    conn.sendall(b"pong-%d" % i)
                replies = [conn.recv(100) for conn in concurrent]
            finally:
                for conn in concurrent:
                    conn.close()

        assert sequential == [b"ping-%d" % i for i in range(5)]
        assert replies == [b"pong-%d" % i for i in range(5)]

    @pytest.mark.parametrize("remote_closes", [False, True], ids=["client-closes", "remote-closes"])
    def test_closed_connections_are_unregistered(
        self, in_process_ssh_client, in_process_ssh_server, remote_closes
    ):
        in_process_ssh_server.close_forwarded_after_echo = remote_closes
        with SSHTunnel(in_process_ssh_client, "db.internal", 5432) as tunnel:
            with socket.create_connection(("localhost", tunnel.local_bind_port), timeout=5) as conn:
                conn.sendall(b"ping")
                assert conn.recv(100) == b"ping"
                if remote_closes:
                    # The tunnel closes the local end once the remote end closes the channel.
                    assert conn.recv(100) == b""
            selector = tunnel._selector
            assert selector is not None
            # The forwarding thread unregisters the connection asynchronously; only the listening
            # socket and the shutdown socket stay registered.
            assert _wait_until(lambda: len(selector.get_map()) == 2)
