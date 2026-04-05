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
"""
SSH tunnel implementations for the Airflow SSH provider.

This module provides ``SSHTunnel`` (sync, paramiko-based) and ``AsyncSSHTunnel``
(async, asyncssh-based) as replacements for the removed ``sshtunnel.SSHTunnelForwarder``.

**SSHTunnel** reuses an already-connected ``paramiko.SSHClient`` from
``SSHHook.get_conn()``, so all authentication and proxy configuration is inherited
automatically. It binds a local TCP socket and forwards connections to a remote
host/port through the SSH transport using ``open_channel('direct-tcpip', ...)``.

**AsyncSSHTunnel** wraps ``asyncssh.forward_local_port()`` and is intended for use
with ``SSHHookAsync``.

Migration from ``sshtunnel.SSHTunnelForwarder``
-----------------------------------------------

Before::

    from sshtunnel import SSHTunnelForwarder

    tunnel = hook.get_tunnel(remote_port=5432)
    tunnel.start()
    # use tunnel.local_bind_port
    tunnel.stop()

After::

    with hook.get_tunnel(remote_port=5432) as tunnel:
        # use tunnel.local_bind_port

The ``.start()`` / ``.stop()`` methods still exist but emit deprecation warnings.
Use the context manager interface instead.
"""

from __future__ import annotations

import logging
import socket
import threading
import warnings
from select import select
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning

if TYPE_CHECKING:
    import asyncssh
    import paramiko

    from airflow.sdk.types import Logger


log = logging.getLogger(__name__)

# Attributes that existed on SSHTunnelForwarder but not on SSHTunnel.
# Used by __getattr__ to provide a helpful migration message.
_SSHTUNNELFORWARDER_ATTRS = frozenset(
    {
        "tunnel_is_up",
        "skip_tunnel_checkup",
        "ssh_host",
        "ssh_port",
        "ssh_username",
        "ssh_password",
        "ssh_pkey",
        "ssh_proxy",
        "local_bind_address",
        "local_bind_addresses",
        "local_bind_host",
        "local_bind_hosts",
        "remote_bind_address",
        "remote_bind_addresses",
        "tunnel_bindings",
        "is_alive",
        "raise_fwd_exc",
        "daemon_forward_servers",
        "daemon_transport",
    }
)


class SSHTunnel:
    """
    Local port-forwarding tunnel over an existing paramiko SSH connection.

    This replaces ``sshtunnel.SSHTunnelForwarder`` by using the SSH client's
    transport directly via ``open_channel('direct-tcpip', ...)``.

    The recommended usage is as a context manager::

        client = hook.get_conn()
        with SSHTunnel(client, "dbhost", 5432) as tunnel:
            connect_to_db("localhost", tunnel.local_bind_port)

    :param ssh_client: An already-connected ``paramiko.SSHClient``.
    :param remote_host: The destination host to forward to (from the SSH server's perspective).
    :param remote_port: The destination port to forward to.
    :param local_port: Local port to bind. ``None`` means an OS-assigned ephemeral port.
    :param logger: Optional logger instance. Falls back to the module logger.
    """

    def __init__(
        self,
        ssh_client: paramiko.SSHClient,
        remote_host: str,
        remote_port: int,
        local_port: int | None = None,
        logger: logging.Logger | Logger | None = None,
    ) -> None:
        self._ssh_client = ssh_client
        self._remote_host = remote_host
        self._remote_port = remote_port
        self._logger: logging.Logger | Logger = logger or log
        self._server_socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        # Self-pipe for waking the select loop on shutdown
        self._shutdown_r: socket.socket | None = None
        self._shutdown_w: socket.socket | None = None
        self._running = False

        # Bind the listening socket eagerly so local_bind_port is available
        # before entering the context manager or calling start().
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._server_socket.bind(("localhost", local_port or 0))
            self._server_socket.listen(5)
        except OSError:
            self._server_socket.close()
            raise
        self._server_socket.setblocking(False)

    # -- Public properties ---------------------------------------------------

    @property
    def local_bind_port(self) -> int:
        """Return the local port the tunnel is listening on."""
        if self._server_socket is None:
            raise RuntimeError("Tunnel socket is not bound")
        return self._server_socket.getsockname()[1]

    @property
    def local_bind_address(self) -> tuple[str, int]:
        """Return ``('localhost', <port>)`` — the local address the tunnel is listening on."""
        return ("localhost", self.local_bind_port)

    # -- Context manager -----------------------------------------------------

    def __enter__(self) -> SSHTunnel:
        self._start_forwarding()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._stop_forwarding()

    # -- Deprecated start/stop -----------------------------------------------

    def start(self) -> None:
        """Start the tunnel. **Deprecated** — use the context manager interface instead."""
        warnings.warn(
            "SSHTunnel.start() is deprecated. Use the context manager interface: "
            "`with hook.get_tunnel(...) as tunnel:`",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        self._start_forwarding()

    def stop(self) -> None:
        """Stop the tunnel. **Deprecated** — use the context manager interface instead."""
        warnings.warn(
            "SSHTunnel.stop() is deprecated. Use the context manager interface: "
            "`with hook.get_tunnel(...) as tunnel:`",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        self._stop_forwarding()

    # -- Migration helper ----------------------------------------------------

    def __getattr__(self, name: str):
        if name in _SSHTUNNELFORWARDER_ATTRS:
            raise AttributeError(
                f"'{type(self).__name__}' has no attribute '{name}'. "
                f"sshtunnel.SSHTunnelForwarder has been replaced by SSHTunnel. "
                f"Use the context manager interface and .local_bind_port instead."
            )
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    # -- Internal ------------------------------------------------------------

    def _start_forwarding(self) -> None:
        """Start the forwarding thread."""
        if self._running:
            return
        self._shutdown_r, self._shutdown_w = socket.socketpair()
        self._running = True
        self._thread = threading.Thread(target=self._serve_forever, daemon=True)
        self._thread.start()

    def _stop_forwarding(self) -> None:
        """Signal the forwarding thread to stop and wait for it."""
        if not self._running:
            return
        self._running = False
        # Wake the select loop
        if self._shutdown_w is not None:
            try:
                self._shutdown_w.send(b"\x00")
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        # Close the shutdown pair
        for sock in (self._shutdown_r, self._shutdown_w):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
        self._shutdown_r = None
        self._shutdown_w = None
        # Close the server socket
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass
            self._server_socket = None

    def _serve_forever(self) -> None:
        """Accept connections on the local socket and forward them through SSH."""
        if self._server_socket is None or self._shutdown_r is None:
            return
        server_socket = self._server_socket
        shutdown_r = self._shutdown_r
        active_channels: list[tuple[socket.socket, paramiko.Channel]] = []
        try:
            while self._running:
                read_fds: list[socket.socket | paramiko.Channel] = [server_socket, shutdown_r]
                for local_sock, chan in active_channels:
                    read_fds.append(local_sock)
                    read_fds.append(chan)

                try:
                    readable, _, _ = select(read_fds, [], [], 1.0)
                except (OSError, ValueError):
                    break

                for fd in readable:
                    if fd is shutdown_r:
                        return
                    if fd is server_socket:
                        self._accept_connection(active_channels)
                    else:
                        self._forward_data(fd, active_channels)

                # Clean up closed channels
                active_channels = [(s, c) for s, c in active_channels if not (s.fileno() == -1 or c.closed)]
        finally:
            for local_sock, chan in active_channels:
                self._close_pair(local_sock, chan)

    def _accept_connection(self, active_channels: list[tuple[socket.socket, paramiko.Channel]]) -> None:
        """Accept a new local connection and open an SSH channel for it."""
        if self._server_socket is None:
            return
        try:
            client_sock, addr = self._server_socket.accept()
        except OSError:
            return

        transport = self._ssh_client.get_transport()
        if transport is None or not transport.is_active():
            self._logger.warning("SSH transport is not active, rejecting connection from %s", addr)
            client_sock.close()
            return

        try:
            channel = transport.open_channel(
                "direct-tcpip",
                (self._remote_host, self._remote_port),
                addr,
            )
        except Exception:
            self._logger.warning(
                "Failed to open SSH channel to %s:%s",
                self._remote_host,
                self._remote_port,
                exc_info=True,
            )
            client_sock.close()
            return

        if channel is None:
            self._logger.warning("SSH channel request was rejected")
            client_sock.close()
            return

        active_channels.append((client_sock, channel))

    def _forward_data(
        self,
        fd: socket.socket | paramiko.Channel,
        active_channels: list[tuple[socket.socket, paramiko.Channel]],
    ) -> None:
        """Forward data between a local socket and its paired SSH channel."""
        for local_sock, chan in active_channels:
            if fd is local_sock:
                try:
                    data = local_sock.recv(16384)
                except OSError:
                    data = b""
                if not data:
                    self._close_pair(local_sock, chan)
                    return
                try:
                    chan.sendall(data)
                except OSError:
                    self._logger.warning("Error sending data to SSH channel, closing connection")
                    self._close_pair(local_sock, chan)
                return
            if fd is chan:
                try:
                    data = chan.recv(16384)
                except OSError:
                    data = b""
                if not data:
                    self._close_pair(local_sock, chan)
                    return
                try:
                    local_sock.sendall(data)
                except OSError:
                    self._logger.warning("Error sending data to local socket, closing connection")
                    self._close_pair(local_sock, chan)
                return

    @staticmethod
    def _close_pair(local_sock: socket.socket, chan: paramiko.Channel) -> None:
        """Close both ends of a forwarded connection."""
        for closeable in (chan, local_sock):
            try:
                closeable.close()
            except OSError:
                pass


class AsyncSSHTunnel:
    """
    Async local port-forwarding tunnel over an asyncssh SSH connection.

    This wraps ``asyncssh.SSHClientConnection.forward_local_port()`` and is
    intended for use with ``SSHHookAsync``.

    Usage::

        async with await hook.get_tunnel(remote_port=5432) as tunnel:
            connect_to_db("localhost", tunnel.local_bind_port)

    On exit, both the forwarding listener and the underlying SSH connection
    are closed.

    :param ssh_conn: An ``asyncssh.SSHClientConnection``.
    :param remote_host: The destination host to forward to.
    :param remote_port: The destination port to forward to.
    :param local_port: Local port to bind. ``None`` means an OS-assigned ephemeral port.
    """

    def __init__(
        self,
        ssh_conn: asyncssh.SSHClientConnection,
        remote_host: str,
        remote_port: int,
        local_port: int | None = None,
    ) -> None:
        self._ssh_conn = ssh_conn
        self._remote_host = remote_host
        self._remote_port = remote_port
        self._local_port = local_port or 0
        self._listener: asyncssh.SSHListener | None = None

    @property
    def local_bind_port(self) -> int:
        """Return the local port the tunnel is listening on."""
        if self._listener is None:
            raise RuntimeError("Tunnel is not started. Use `async with` to start it.")
        return self._listener.get_port()

    async def __aenter__(self) -> AsyncSSHTunnel:
        try:
            self._listener = await self._ssh_conn.forward_local_port(
                "localhost",
                self._local_port,
                self._remote_host,
                self._remote_port,
            )
        except BaseException:
            self._ssh_conn.close()
            await self._ssh_conn.wait_closed()
            raise
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._listener is not None:
            self._listener.close()
            await self._listener.wait_closed()
            self._listener = None
        self._ssh_conn.close()
        await self._ssh_conn.wait_closed()
