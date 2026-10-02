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

import contextlib
import os
import pathlib
import signal
import socket
import subprocess
import sys
import threading
import time
from unittest.mock import ANY, MagicMock, call, patch

import attrs
import psutil
import pytest
from uuid6 import uuid7

from airflow.dag_processing.bundles.base import BaseDagBundle, BundleVersion
from airflow.sdk.api.client import Client, TaskInstanceOperations
from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
from airflow.sdk.coordinators._subprocess import (
    SubprocessCoordinator,
    _accept_connections,
    _connection_owned_by_process_tree,
    _is_connection_from_pid,
    _is_connection_from_process,
    _PopenActivitySubprocess,
    _ResourceTracker,
    _start_server,
    log,
)
from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)


def _make_ti(dag_id: str = "tutorial_dag", queue: str = "socket") -> TaskInstance:
    return TaskInstance(
        id=uuid7(),
        dag_version_id=uuid7(),
        task_id="task_1",
        dag_id=dag_id,
        run_id="run_1",
        try_number=1,
        map_index=-1,
        queue=queue,
    )


class TestStartServer:
    def test_returns_listening_socket(self):
        server = _start_server()
        try:
            host, port = server.getsockname()
        finally:
            server.close()
        assert host == "127.0.0.1"
        assert port > 0

    def test_two_calls_return_different_ports(self):
        s1 = _start_server()
        s2 = _start_server()
        try:
            _, port1 = s1.getsockname()
            _, port2 = s2.getsockname()
        finally:
            s1.close()
            s2.close()
        assert port1 != port2

    def test_accepts_connection(self):
        conn = client = None
        server = _start_server()
        try:
            _, port = server.getsockname()
            client = socket.socket()
            client.connect(("127.0.0.1", port))
            conn, _ = server.accept()
            conn.sendall(b"ping")
            received = client.recv(4)
        finally:
            if conn:
                conn.close()
            if client:
                client.close()
            server.close()
        assert received == b"ping"


class TestAcceptConnections:
    @pytest.fixture(autouse=True)
    def mock_child_connection_check(self):
        with patch(
            "airflow.sdk.coordinators._subprocess._is_connection_from_process",
            return_value=True,
        ) as mock_check:
            yield mock_check

    def _connect_after_delay(self, addr: tuple[str, int], delay: float = 0.0) -> None:
        def _connect():
            time.sleep(delay)
            c = socket.socket()
            with contextlib.suppress(OSError):
                c.connect(addr)

        threading.Thread(target=_connect, daemon=True).start()

    def test_accepts_single_server(self):
        server = _start_server()
        _, port = server.getsockname()
        self._connect_after_delay(("127.0.0.1", port))

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None

        try:
            accepted, _ = _accept_connections({"comm": server}, {}, mock_proc)
            assert server in accepted
            accepted[server].close()
        finally:
            server.close()

    def test_accepts_multiple_servers_keyed_by_server_socket(self):
        comm_server = _start_server()
        logs_server = _start_server()
        _, comm_port = comm_server.getsockname()
        _, logs_port = logs_server.getsockname()

        self._connect_after_delay(("127.0.0.1", comm_port))
        self._connect_after_delay(("127.0.0.1", logs_port))

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None

        try:
            accepted, drained = _accept_connections({"comm": comm_server, "logs": logs_server}, {}, mock_proc)
            assert set(accepted) == {comm_server, logs_server}
            assert drained == {}
            for sock in accepted.values():
                sock.close()
        finally:
            comm_server.close()
            logs_server.close()

    def test_empty_drains_returns_empty_drained_dict(self):
        server = _start_server()
        _, port = server.getsockname()
        self._connect_after_delay(("127.0.0.1", port))

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None
        try:
            _, drained = _accept_connections({"comm": server}, {}, mock_proc)
            assert drained == {}
        finally:
            server.close()

    def test_drain_socket_present_in_drained_dict(self):
        server = _start_server()
        drain_r, drain_w = socket.socketpair()
        _, port = server.getsockname()
        self._connect_after_delay(("127.0.0.1", port))

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None
        try:
            _, drained = _accept_connections({"comm": server}, {"stdout": drain_r}, mock_proc)
            assert drain_r in drained
        finally:
            drain_r.close()
            drain_w.close()
            server.close()

    def test_drain_captures_early_output(self):
        """Bytes written to the drain socket before the comm server accepts
        must be captured and returned in the drained dict."""
        server = _start_server()
        drain_r, drain_w = socket.socketpair()
        _, port = server.getsockname()

        drain_w.sendall(b"early output\n")
        drain_w.shutdown(socket.SHUT_WR)
        self._connect_after_delay(("127.0.0.1", port), delay=0.05)

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None
        try:
            _, drained = _accept_connections({"comm": server}, {"stdout": drain_r}, mock_proc)
            assert drained[drain_r] == b"early output\n"
        finally:
            drain_r.close()
            drain_w.close()
            server.close()

    def test_raises_timeout_when_no_connection(self):
        server = _start_server()
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None
        try:
            with pytest.raises(TimeoutError, match="did not connect within timeout"):
                _accept_connections({"comm": server}, {}, mock_proc, max_wait=0.05)
        finally:
            server.close()

    def test_raises_runtime_error_if_process_exits_before_connecting(self):
        server = _start_server()
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = 1
        mock_proc.returncode = 1
        try:
            with pytest.raises(RuntimeError, match="process exited with 1"):
                _accept_connections({"comm": server}, {}, mock_proc)
        finally:
            server.close()

    def test_returned_sockets_are_connected(self):
        """Accepted sockets should be real, usable connections."""
        server = _start_server()
        _, port = server.getsockname()

        client = socket.socket()
        client.connect(("127.0.0.1", port))

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None

        try:
            accepted, _ = _accept_connections({"comm": server}, {}, mock_proc)
            accepted[server].sendall(b"hello")
            assert client.recv(5) == b"hello"
            accepted[server].close()
            client.close()
        finally:
            server.close()

    def test_accepted_dict_keyed_by_server_socket_object(self):
        """The returned accepted mapping must use server socket objects as keys,
        not the string names passed in the servers dict."""
        server = _start_server()
        _, port = server.getsockname()
        self._connect_after_delay(("127.0.0.1", port))
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None
        try:
            accepted, _ = _accept_connections({"comm": server}, {}, mock_proc)
            # Key must be the socket object itself, not the string "comm"
            assert server in accepted
            assert "comm" not in accepted
            accepted[server].close()
        finally:
            server.close()

    def test_rejects_connections_not_owned_by_child_process(self, mock_child_connection_check):
        server = _start_server()
        _, port = server.getsockname()
        mock_child_connection_check.side_effect = [False, True]
        self._connect_after_delay(("127.0.0.1", port))
        self._connect_after_delay(("127.0.0.1", port), delay=0.05)

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 12345
        mock_proc.poll.return_value = None

        try:
            accepted, _ = _accept_connections({"comm": server}, {}, mock_proc)
            assert mock_child_connection_check.call_count == 2
            assert server in accepted
            accepted[server].close()
        finally:
            server.close()


class TestAcceptConnectionsProcessValidation:
    def _start_connector_process(self, addr: tuple[str, int], *, delay: float = 0.0) -> subprocess.Popen:
        script = """
import socket
import sys
import time

time.sleep(float(sys.argv[3]))
sock = socket.socket()
sock.connect((sys.argv[1], int(sys.argv[2])))
sock.recv(1)
"""
        return subprocess.Popen([sys.executable, "-c", script, addr[0], str(addr[1]), str(delay)])

    def test_rejects_racing_connection_from_other_process(self):
        server = _start_server()
        addr = server.getsockname()
        attacker = socket.socket()
        attacker.connect(addr)
        child_proc = self._start_connector_process(addr, delay=0.05)

        try:
            accepted, _ = _accept_connections({"comm": server}, {}, child_proc)
            accepted[server].sendall(b"x")
            accepted[server].close()
            assert child_proc.wait(timeout=5) == 0
            assert attacker.recv(1) == b""
        finally:
            attacker.close()
            server.close()
            if child_proc.poll() is None:
                child_proc.terminate()
                child_proc.wait(timeout=5)


class TestConnectionFromProcess:
    def test_matches_child_process_tcp_connection(self):
        server = _start_server()
        _, port = server.getsockname()
        client = socket.socket()
        client.connect(("127.0.0.1", port))
        conn, _ = server.accept()
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = os.getpid()

        try:
            assert _is_connection_from_process(conn, mock_proc) is True
        finally:
            conn.close()
            client.close()
            server.close()

    def test_matches_dual_stack_ipv4_mapped_connection(self):
        """A dual-stack (AF_INET6) client connecting to the IPv4 server is accepted.

        Regression test for the Java coordinator (#67781 / #68147): on an
        IPv6-enabled host the JVM connects back over a dual-stack socket, so the
        kernel records its loopback connection as the IPv4-mapped
        ``::ffff:127.0.0.1`` in ``/proc/net/tcp6``. The AF_INET supervisor socket's
        ``getpeername()`` reports plain ``127.0.0.1``, so the ownership check must
        treat the mapped and plain forms as the same address -- otherwise every
        Java task is rejected with "process exited with 1 before connecting".
        """
        server = _start_server()
        _, port = server.getsockname()
        try:
            client = socket.socket(socket.AF_INET6)
            client.connect(("::ffff:127.0.0.1", port))
        except OSError as e:
            server.close()
            pytest.skip(f"IPv6 loopback unavailable: {e}")
        conn, _ = server.accept()
        # Sanity: the client really is using the IPv4-mapped form the JVM exhibits.
        assert client.getsockname()[0] == "::ffff:127.0.0.1"
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = os.getpid()

        try:
            assert _is_connection_from_process(conn, mock_proc) is True
        finally:
            conn.close()
            client.close()
            server.close()

    def test_matches_dual_stack_ipv4_compatible_connection(self):
        """A dual-stack child whose loopback is rendered in IPv4-compatible form is accepted.

        Companion to :meth:`test_matches_dual_stack_ipv4_mapped_connection` for macOS
        (#68938): there the JVM's loopback connection is reported by ``psutil`` as the
        deprecated IPv4-compatible ``::127.0.0.1`` rather than the IPv4-mapped
        ``::ffff:127.0.0.1`` seen on Linux. Both forms must canonicalize to plain
        ``127.0.0.1`` or the ownership check rejects the Java task. The OS will not
        reliably establish a routable ``::`` connection on demand, so ``psutil``'s view of
        the child's connections is mocked to the form macOS actually reports.
        """
        server = _start_server()
        _, server_port = server.getsockname()
        client = socket.socket()
        client.connect(("127.0.0.1", server_port))
        conn, _ = server.accept()
        child_port = conn.getpeername()[1]
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = os.getpid()

        # On macOS psutil reports the child's dual-stack loopback in IPv4-compatible form.
        compat_conn = MagicMock(
            laddr=("::127.0.0.1", child_port),
            raddr=("::127.0.0.1", server_port),
        )
        try:
            with patch("airflow.sdk.coordinators._subprocess.psutil.Process") as mock_process:
                mock_process.return_value.children.return_value = []
                mock_process.return_value.net_connections.return_value = [compat_conn]
                assert _is_connection_from_process(conn, mock_proc) is True
        finally:
            conn.close()
            client.close()
            server.close()

    def test_rejects_tcp_connection_not_owned_by_child_process(self):
        server = _start_server()
        _, port = server.getsockname()
        client = socket.socket()
        client.connect(("127.0.0.1", port))
        conn, _ = server.accept()
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = os.getpid()

        try:
            with patch("airflow.sdk.coordinators._subprocess.psutil.Process") as mock_process:
                mock_process.return_value.children.return_value = []
                mock_process.return_value.net_connections.return_value = []
                assert _is_connection_from_process(conn, mock_proc, verify_timeout=0.0) is False
        finally:
            conn.close()
            client.close()
            server.close()

    def test_matches_descendant_process_tcp_connection(self):
        """A connection owned by a *descendant* of the child process is accepted.

        Regression test for the Java coordinator (#67781): the launched process
        may itself spawn the runtime that connects back, so the peer can belong
        to a descendant of ``proc.pid`` rather than ``proc.pid`` directly.
        """
        server = _start_server()
        host, port = server.getsockname()
        # A real subprocess — a descendant of this test process — opens the connection.
        connector = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import socket, sys, time; s = socket.socket(); "
                "s.connect((sys.argv[1], int(sys.argv[2]))); time.sleep(30)",
                host,
                str(port),
            ],
        )
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = os.getpid()  # connector is a descendant of this process

        try:
            conn, _ = server.accept()
            try:
                assert _is_connection_from_process(conn, mock_proc) is True
            finally:
                conn.close()
        finally:
            connector.terminate()
            connector.wait(timeout=5)
            server.close()

    def test_retries_until_ownership_is_confirmed(self):
        """The lookup is retried while the connection is not yet visible in /proc."""
        conn = MagicMock()
        conn.getpeername.return_value = ("127.0.0.1", 5000)
        conn.getsockname.return_value = ("127.0.0.1", 6000)
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 999

        with patch(
            "airflow.sdk.coordinators._subprocess._connection_owned_by_process_tree",
            side_effect=[False, False, True],
        ) as mock_owned:
            assert _is_connection_from_process(conn, mock_proc, poll_interval=0.0) is True
        assert mock_owned.call_count == 3

    def test_rejects_when_ownership_never_confirmed(self):
        conn = MagicMock()
        conn.getpeername.return_value = ("127.0.0.1", 5000)
        conn.getsockname.return_value = ("127.0.0.1", 6000)
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 999

        with patch(
            "airflow.sdk.coordinators._subprocess._connection_owned_by_process_tree",
            return_value=False,
        ):
            assert (
                _is_connection_from_process(conn, mock_proc, verify_timeout=0.0, poll_interval=0.0) is False
            )

    def test_owned_by_tree_returns_false_when_process_gone(self):
        with patch(
            "airflow.sdk.coordinators._subprocess.psutil.Process",
            side_effect=psutil.NoSuchProcess(999999),
        ):
            assert _connection_owned_by_process_tree(("127.0.0.1", 1), ("127.0.0.1", 2), 999999) is False

    @pytest.mark.parametrize("owned", [True, False])
    @patch("airflow.sdk.coordinators._subprocess._connection_owned_by_process_tree", autospec=True)
    def test_is_connection_from_pid_checks_once(self, mock_owned, owned):
        conn = MagicMock(spec=socket.socket)
        conn.getpeername.return_value = ("127.0.0.1", 5000)
        conn.getsockname.return_value = ("127.0.0.1", 6000)
        mock_owned.return_value = owned

        assert _is_connection_from_pid(conn, 999) is owned
        mock_owned.assert_called_once_with(("127.0.0.1", 5000), ("127.0.0.1", 6000), 999)


class TestResourceTracker:
    """
    Unit tests for the _ResourceTracker context manager.

    _ResourceTracker tracks sockets and Popen objects and ensures they are
    closed/terminated on context-manager exit, unless explicitly untracked
    beforehand.
    """

    def test_track_returns_passed_objects_as_tuple(self):
        tracker = _ResourceTracker(timeout=0.1)
        sock = MagicMock(spec=socket.socket)
        result = tracker.track(sock)
        assert result == (sock,)

    def test_track_multiple_objects_returns_all(self):
        tracker = _ResourceTracker(timeout=0.1)
        sock1 = MagicMock(spec=socket.socket)
        sock2 = MagicMock(spec=socket.socket)
        result = tracker.track(sock1, sock2)
        assert set(result) == {sock1, sock2}

    def test_untrack_returns_objects(self):
        tracker = _ResourceTracker(timeout=0.1)
        sock = MagicMock(spec=socket.socket)
        tracker.track(sock)
        result = tracker.untrack(sock)
        assert result == (sock,)

    def test_context_manager_closes_tracked_socket_on_exit(self):
        sock = MagicMock(spec=socket.socket)
        with _ResourceTracker(timeout=0.1) as tracker:
            tracker.track(sock)
        sock.close.assert_called_once()

    def test_context_manager_terminates_tracked_popen_on_exit(self):
        proc = MagicMock(spec=subprocess.Popen)
        with _ResourceTracker(timeout=0.1) as tracker:
            tracker.track(proc)
        proc.terminate.assert_called_once()

    def test_untracked_socket_not_closed_on_exit(self):
        sock = MagicMock(spec=socket.socket)
        with _ResourceTracker(timeout=0.1) as tracker:
            tracker.track(sock)
            tracker.untrack(sock)
        sock.close.assert_not_called()

    def test_only_remaining_tracked_objects_cleaned_up(self):
        """After untracking one socket the other must still be closed."""
        sock_keep = MagicMock(spec=socket.socket)
        sock_release = MagicMock(spec=socket.socket)
        with _ResourceTracker(timeout=0.1) as tracker:
            tracker.track(sock_keep, sock_release)
            tracker.untrack(sock_release)
        sock_keep.close.assert_called_once()
        sock_release.close.assert_not_called()

    def test_untrack_unknown_object_does_not_raise(self):
        sock = MagicMock(spec=socket.socket)
        tracker = _ResourceTracker(timeout=0.1)
        # Untracking something never tracked must be a no-op, not an error
        tracker.untrack(sock)


@attrs.define(kw_only=True)
class _StubSubprocessCoordinator(SubprocessCoordinator):
    """Minimal SubprocessCoordinator subclass used to exercise the base machinery.

    Roots handed to the command builder are recorded in ``recorded_roots`` so
    wiring can be asserted.
    """

    command: list[str]
    schema_version: str | None = None
    recorded_roots: list[list[pathlib.Path]] = attrs.field(init=False, factory=list)

    def _build_execute_task_command(self, *, what):
        self.recorded_roots.append(list(self._get_scan_roots()))
        return list(self.command), self.schema_version


def _make_bundle(path: pathlib.Path, *, version: str | None = None, name: str = "dags") -> MagicMock:
    bundle = MagicMock(spec=BaseDagBundle, path=path, version=version)
    bundle.name = name
    return bundle


@pytest.fixture
def artifact_bundle(tmp_path):
    """Resolve every task's artifacts to an unversioned bundle at *tmp_path*, without the bundle machinery."""
    with patch.object(
        SubprocessCoordinator,
        "_resolve_artifact_bundle",
        autospec=True,
        return_value=_make_bundle(tmp_path),
    ) as mock_resolve:
        yield mock_resolve


@pytest.fixture
def mock_client(make_ti_context):
    client = MagicMock(spec=Client)
    client.task_instances = MagicMock(spec=TaskInstanceOperations)
    client.task_instances.start.return_value = make_ti_context()
    return client


class TestSubprocessCoordinatorAttributes:
    def test_default_startup_timeout(self):
        coordinator = _StubSubprocessCoordinator(command=["/bin/true"])
        assert coordinator.task_startup_timeout == 10.0

    def test_custom_startup_timeout(self):
        coordinator = _StubSubprocessCoordinator(command=["/bin/true"], task_startup_timeout=2.5)
        assert coordinator.task_startup_timeout == 2.5

    def test_build_execute_task_command_default_raises(self):
        class _Plain(SubprocessCoordinator):
            pass

        with pytest.raises(NotImplementedError):
            _Plain()._build_execute_task_command(what=_make_ti())


@pytest.mark.usefixtures("artifact_bundle")
class TestSubprocessCoordinatorExecuteTask:
    def _captured_popen_cmd(
        self,
        mock_client,
        *,
        command: list[str],
        schema_version: str | None = None,
    ) -> tuple[list[str], str | None]:
        ti = _make_ti()
        coordinator = _StubSubprocessCoordinator(command=command, schema_version=schema_version)

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 12345
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)
        popen_calls: list = []
        cls_kwargs: dict = {}

        def capture_popen(cmd, **kwargs):
            popen_calls.append(cmd)
            return mock_proc

        original_start = _PopenActivitySubprocess.__dict__["start"].__func__

        def spy_start(cls, **kwargs):
            cls_kwargs.update(kwargs)
            return original_start(cls, **kwargs)

        with (
            patch(
                "airflow.sdk.coordinators._subprocess.subprocess.Popen",
                side_effect=capture_popen,
            ),
            patch(
                "airflow.sdk.coordinators._subprocess._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch.object(ActivitySubprocess, "wait", return_value=0),
            patch.object(
                _PopenActivitySubprocess,
                "start",
                classmethod(spy_start),
            ),
        ):
            coordinator.execute_task(
                what=ti,
                dag_rel_path="bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert popen_calls, "subprocess.Popen was not called"
        return popen_calls[0], cls_kwargs.get("subprocess_schema_version")

    def test_command_prefix_preserved(self, mock_client):
        cmd, _ = self._captured_popen_cmd(mock_client, command=["/path/to/runtime", "arg1"])
        assert cmd[:2] == ["/path/to/runtime", "arg1"]

    def test_comm_and_logs_flags_appended(self, mock_client):
        cmd, _ = self._captured_popen_cmd(mock_client, command=["/path/to/runtime"])
        comm_args = [a for a in cmd if a.startswith("--comm=")]
        logs_args = [a for a in cmd if a.startswith("--logs=")]
        assert len(comm_args) == 1
        assert len(logs_args) == 1

    def test_comm_and_logs_contain_port(self, mock_client):
        cmd, _ = self._captured_popen_cmd(mock_client, command=["/path/to/runtime"])
        comm_arg = next(a for a in cmd if a.startswith("--comm="))
        logs_arg = next(a for a in cmd if a.startswith("--logs="))
        # format is host:port
        assert ":" in comm_arg.split("=", 1)[1]
        assert ":" in logs_arg.split("=", 1)[1]

    def test_comm_and_logs_after_user_command(self, mock_client):
        cmd, _ = self._captured_popen_cmd(mock_client, command=["/path/to/runtime", "user-arg"])
        user_idx = cmd.index("user-arg")
        comm_idx = next(i for i, a in enumerate(cmd) if a.startswith("--comm="))
        logs_idx = next(i for i, a in enumerate(cmd) if a.startswith("--logs="))
        assert user_idx < comm_idx
        assert user_idx < logs_idx

    def test_schema_version_forwarded(self, mock_client):
        _, schema = self._captured_popen_cmd(
            mock_client, command=["/path/to/runtime"], schema_version="2026-06-16"
        )
        assert schema == "2026-06-16"

    def test_returns_execution_result(self, mock_client):
        ti = _make_ti()
        coordinator = _StubSubprocessCoordinator(command=["/bin/true"])

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 99999
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch(
                "airflow.sdk.coordinators._subprocess._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch.object(ActivitySubprocess, "wait", return_value=0),
        ):
            result = coordinator.execute_task(
                what=ti,
                dag_rel_path="bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert isinstance(result, BaseCoordinator.ExecutionResult)
        assert result.exit_code == 0


class TestPopenActivitySubprocessStart:
    def _start_with_mocks(self, mock_client, *, command: list[str], schema_version=None):
        ti = _make_ti()
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 12345
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch(
                "airflow.sdk.coordinators._subprocess.subprocess.Popen",
                return_value=mock_proc,
            ) as popen_mock,
            patch(
                "airflow.sdk.coordinators._subprocess._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
        ):
            proc = _PopenActivitySubprocess.start(
                what=ti,
                dag_rel_path="bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                command=command,
                subprocess_schema_version=schema_version,
                subprocess_logs_to_stdout=False,
            )
        return proc, popen_mock, comm_sock

    def test_stdin_is_comm_socket(self, mock_client):
        proc, _, comm_sock = self._start_with_mocks(mock_client, command=["/bin/true"])
        assert proc.stdin is comm_sock

    def test_pid_taken_from_popen(self, mock_client):
        proc, _, _ = self._start_with_mocks(mock_client, command=["/bin/true"])
        assert proc.pid == 12345

    def test_on_child_started_called(self, mock_client):
        ti = _make_ti()
        with (
            patch("airflow.sdk.coordinators._subprocess.subprocess.Popen") as popen_mock,
            patch(
                "airflow.sdk.coordinators._subprocess._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {soc: MagicMock(spec=socket.socket) for soc in servers.values()},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started") as mock_on_started,
        ):
            popen_mock.return_value.pid = 12345
            _PopenActivitySubprocess.start(
                what=ti,
                dag_rel_path="bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                command=["/bin/true"],
                subprocess_logs_to_stdout=False,
            )

        mock_on_started.assert_called_once()
        kwargs = mock_on_started.call_args.kwargs
        assert kwargs["ti"] is ti
        assert kwargs["dag_rel_path"] == "bundle"

    @conf_vars({("logging", "logging_level"): "DEBUG"})
    def test_resolved_log_level_passed_to_subprocess_env(self, mock_client):
        """A language SDK runtime gets the resolved task log level via the environment at launch."""
        _, popen_mock, _ = self._start_with_mocks(mock_client, command=["/bin/true"])
        env = popen_mock.call_args.kwargs["env"]
        assert env["AIRFLOW__LOGGING__LOGGING_LEVEL"] == "DEBUG"

    @conf_vars({("logging", "namespace_levels"): "sqlalchemy=INFO, botocore=WARNING"})
    def test_namespace_levels_passed_to_subprocess_env(self, mock_client):
        """Per-logger levels are propagated verbatim for the runtime to parse."""
        _, popen_mock, _ = self._start_with_mocks(mock_client, command=["/bin/true"])
        env = popen_mock.call_args.kwargs["env"]
        assert env["AIRFLOW__LOGGING__NAMESPACE_LEVELS"] == "sqlalchemy=INFO, botocore=WARNING"

    @conf_vars({("logging", "namespace_levels"): ""})
    def test_namespace_levels_omitted_when_unset(self, mock_client):
        """An empty value has no pairs to parse, so the variable is left out."""
        _, popen_mock, _ = self._start_with_mocks(mock_client, command=["/bin/true"])
        env = popen_mock.call_args.kwargs["env"]
        assert env["AIRFLOW__LOGGING__NAMESPACE_LEVELS"] == ""

    def test_register_pipe_readers_called_with_four_sockets(self, mock_client):
        """Both socketpair read-ends and both TCP sockets must be registered, with a data kwarg."""
        with (
            patch("airflow.sdk.coordinators._subprocess.subprocess.Popen") as popen_mock,
            patch(
                "airflow.sdk.coordinators._subprocess._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {soc: MagicMock(spec=socket.socket) for soc in servers.values()},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers") as mock_register,
            patch.object(ActivitySubprocess, "_on_child_started"),
        ):
            popen_mock.return_value.pid = 12345
            _PopenActivitySubprocess.start(
                what=_make_ti(),
                dag_rel_path="bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                command=["/bin/true"],
                subprocess_logs_to_stdout=False,
            )
        assert mock_register.mock_calls == [call(ANY, ANY, ANY, ANY, data=ANY)]


class TestResolveArtifactBundle:
    """Execute-time resolution of the Dag bundle holding the artifacts."""

    @patch("airflow.sdk.coordinators._subprocess.initialize_ti_bundle", autospec=True)
    def test_unset_name_uses_the_task_bundle(self, mock_initialize, tmp_path):
        resolved = _make_bundle(tmp_path, version="v3")
        mock_initialize.return_value = resolved
        coordinator = _StubSubprocessCoordinator(command=["x"])
        bundle_info = BundleInfo(name="dags", version="v3", version_data={"k": "v"})

        assert coordinator._resolve_artifact_bundle(bundle_info, log) is resolved
        mock_initialize.assert_called_once_with(bundle_info)

    @patch("airflow.sdk.coordinators._subprocess.initialize_ti_bundle", autospec=True)
    def test_version_less_bundle_is_rematerialized_at_its_current_version(self, mock_initialize, tmp_path):
        """A bundle resolved without a version is re-resolved at the version current now.

        Otherwise the subprocess reads the bundle's shared mutable checkout, which no
        version lock can protect and which another task's refresh can reset underneath it.
        """
        shared_checkout = tmp_path / "tracking_repo"
        shared_checkout.mkdir()
        pinned_tree = tmp_path / "versions" / "sha-abc"
        pinned_tree.mkdir(parents=True)

        unpinned = _make_bundle(shared_checkout)
        unpinned.get_current_version.return_value = BundleVersion(version="sha-abc", data={"k": "v"})
        pinned = _make_bundle(pinned_tree, version="sha-abc")
        mock_initialize.side_effect = [unpinned, pinned]

        coordinator = _StubSubprocessCoordinator(command=["x"], task_handler_bundle_name="artifacts")

        assert coordinator._resolve_artifact_bundle(MagicMock(spec=BundleInfo), log) is pinned
        assert mock_initialize.call_args_list == [
            call(BundleInfo(name="artifacts")),
            call(BundleInfo(name="artifacts", version="sha-abc", version_data={"k": "v"})),
        ]

    @patch("airflow.sdk.coordinators._subprocess.initialize_ti_bundle", autospec=True)
    def test_unversioned_bundle_keeps_its_single_path(self, mock_initialize, tmp_path):
        resolved = _make_bundle(tmp_path)
        resolved.get_current_version.return_value = None
        mock_initialize.return_value = resolved

        coordinator = _StubSubprocessCoordinator(command=["x"], task_handler_bundle_name="artifacts")

        assert coordinator._resolve_artifact_bundle(MagicMock(spec=BundleInfo), log) is resolved
        mock_initialize.assert_called_once_with(BundleInfo(name="artifacts"))

    @patch("airflow.sdk.coordinators._subprocess.initialize_ti_bundle", autospec=True)
    def test_missing_resolved_path_raises(self, mock_initialize, tmp_path):
        mock_initialize.return_value = _make_bundle(tmp_path / "nope", version="v1")
        coordinator = _StubSubprocessCoordinator(command=["x"], task_handler_bundle_name="artifacts")

        with pytest.raises(FileNotFoundError, match="does not exist"):
            coordinator._resolve_artifact_bundle(MagicMock(spec=BundleInfo), log)


class TestExecuteTaskBundleWiring:
    """execute_task passes the task bundle, forwards resolved roots, and holds the version lock."""

    @patch("airflow.sdk.coordinators._subprocess.BundleVersionLock", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess.initialize_ti_bundle", autospec=True)
    @patch.object(_PopenActivitySubprocess, "start", autospec=True)
    def test_task_bundle_mode_binds_forwards_roots_and_locks(
        self, mock_start, mock_initialize, mock_lock, mock_client, tmp_path
    ):
        mock_initialize.return_value = _make_bundle(tmp_path, version="v9")
        mock_start.return_value.wait.return_value = 0

        coordinator = _StubSubprocessCoordinator(command=["/runtime"])
        bundle_info = BundleInfo(name="dags", version="v9")

        coordinator.execute_task(
            what=_make_ti(),
            dag_rel_path="dag.py",
            bundle_info=bundle_info,
            client=mock_client,
            subprocess_logs_to_stdout=False,
        )

        mock_initialize.assert_called_once_with(bundle_info)
        assert coordinator.recorded_roots == [[tmp_path]]
        mock_lock.assert_called_once_with(bundle_name="dags", bundle_version="v9")
        mock_lock.return_value.__enter__.assert_called_once()
        mock_lock.return_value.__exit__.assert_called_once()

    @patch("airflow.sdk.coordinators._subprocess.BundleVersionLock", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess.initialize_ti_bundle", autospec=True)
    @patch.object(_PopenActivitySubprocess, "start", autospec=True)
    def test_named_bundle_mode_locks_the_tree_it_scans(
        self, mock_start, mock_initialize, mock_lock, mock_client, tmp_path
    ):
        """The lock names the pinned version whose tree is handed to the subprocess."""
        pinned_tree = tmp_path / "versions" / "sha-abc"
        pinned_tree.mkdir(parents=True)

        unpinned = _make_bundle(tmp_path, name="artifacts")
        unpinned.get_current_version.return_value = BundleVersion(version="sha-abc", data=None)
        pinned = _make_bundle(pinned_tree, version="sha-abc", name="artifacts")
        mock_initialize.side_effect = [unpinned, pinned]
        mock_start.return_value.wait.return_value = 0

        coordinator = _StubSubprocessCoordinator(command=["/runtime"], task_handler_bundle_name="artifacts")

        coordinator.execute_task(
            what=_make_ti(),
            dag_rel_path="dag.py",
            bundle_info=BundleInfo(name="dags", version="v9"),
            client=mock_client,
            subprocess_logs_to_stdout=False,
        )

        assert coordinator.recorded_roots == [[pinned_tree]]
        mock_lock.assert_called_once_with(bundle_name="artifacts", bundle_version="sha-abc")


class TestGetScanRoots:
    def test_returns_bound_roots_and_clears_them_on_exit(self, tmp_path):
        coordinator = _StubSubprocessCoordinator(command=["x"])

        with coordinator._set_scan_roots([tmp_path]):
            assert coordinator._get_scan_roots() == (tmp_path,)

        with pytest.raises(RuntimeError, match="requires an active task"):
            coordinator._get_scan_roots()

    def test_rejects_nested_reentry(self, tmp_path):
        coordinator = _StubSubprocessCoordinator(command=["/bin/true"])
        with coordinator._set_scan_roots([tmp_path]):
            with pytest.raises(RuntimeError, match="not re-entrant"):
                with coordinator._set_scan_roots([tmp_path]):
                    pass

    @pytest.mark.usefixtures("artifact_bundle")
    def test_execute_task_is_not_reentrant(self, mock_client, tmp_path):
        coordinator = _StubSubprocessCoordinator(command=["/bin/true"])
        with coordinator._set_scan_roots([tmp_path]):
            with pytest.raises(RuntimeError, match="not re-entrant"):
                coordinator.execute_task(
                    what=_make_ti(),
                    dag_rel_path="dag.py",
                    bundle_info=MagicMock(),
                    client=mock_client,
                    subprocess_logs_to_stdout=False,
                )


@attrs.define(kw_only=True)
class _TaskHandlerParsingCoordinator(_StubSubprocessCoordinator):
    def _build_parse_task_handler_command(self, *, path):
        self.recorded_roots.append(list(self._get_scan_roots()))
        return [*self.command, os.fspath(path)], self.schema_version


def _parse_task_handler(coordinator: SubprocessCoordinator, tmp_path: pathlib.Path, reported: list) -> None:
    coordinator.parse_task_handler(
        path=tmp_path / "handlers.artifact",
        bundle_path=tmp_path,
        comm_address=("127.0.0.1", 1001),
        logs_address=("127.0.0.1", 1002),
        report_schema_version=reported.append,
    )


def _is_running(process: psutil.Process) -> bool:
    try:
        return process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


class TestParseTaskHandler:
    def test_build_parse_task_handler_command_default_raises(self, tmp_path):
        coordinator = _StubSubprocessCoordinator(command=["x"])

        with pytest.raises(
            NotImplementedError, match="_StubSubprocessCoordinator does not parse task handlers"
        ):
            coordinator._build_parse_task_handler_command(path=tmp_path / "handlers.artifact")

    @patch("airflow.sdk.coordinators._subprocess._set_close_on_exec_above_stderr", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess._set_parent_death_signal", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess.signal.signal", autospec=True)
    @patch(
        "airflow.sdk.coordinators._subprocess.os.execvpe", autospec=True, side_effect=OSError("exec failed")
    )
    def test_resolves_the_command_under_the_bundle_root_then_execs_it(
        self, mock_execvpe, mock_signal, mock_death_signal, mock_close_on_exec, tmp_path
    ):
        coordinator = _TaskHandlerParsingCoordinator(command=["runtime"], schema_version="2026-06-16")
        reported: list[str | None] = []

        with pytest.raises(OSError, match="exec failed"):
            _parse_task_handler(coordinator, tmp_path, reported)

        assert coordinator.recorded_roots == [[tmp_path]]
        assert coordinator._active_scan_roots is None
        assert reported == ["2026-06-16"]
        argv = [
            "runtime",
            os.fspath(tmp_path / "handlers.artifact"),
            "--comm=127.0.0.1:1001",
            "--logs=127.0.0.1:1002",
        ]
        mock_execvpe.assert_called_once_with("runtime", argv, ANY)
        assert "AIRFLOW__LOGGING__LOGGING_LEVEL" in mock_execvpe.call_args.args[2]
        restored = {c.args[0] for c in mock_signal.call_args_list if c.args[1] == signal.SIG_DFL}
        assert {signal.SIGPIPE, signal.SIGXFSZ} <= restored
        mock_death_signal.assert_called_once_with()
        mock_close_on_exec.assert_called_once_with()

    @patch("airflow.sdk.coordinators._subprocess._set_close_on_exec_above_stderr", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess._set_parent_death_signal", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess.signal.signal", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess.os.getppid", autospec=True, side_effect=[4242, 1])
    @patch("airflow.sdk.coordinators._subprocess.os.execvpe", autospec=True)
    def test_does_not_exec_once_the_parent_has_exited(
        self, mock_execvpe, mock_getppid, mock_signal, mock_death_signal, mock_close_on_exec, tmp_path
    ):
        coordinator = _TaskHandlerParsingCoordinator(command=["runtime"])

        with pytest.raises(RuntimeError, match="The process that started the runtime has exited"):
            _parse_task_handler(coordinator, tmp_path, [])

        mock_death_signal.assert_called_once_with()
        mock_execvpe.assert_not_called()

    @patch("airflow.sdk.coordinators._subprocess.os.execvpe", autospec=True)
    def test_rejects_an_unknown_schema_version_before_reporting(self, mock_execvpe, tmp_path):
        coordinator = _TaskHandlerParsingCoordinator(command=["runtime"], schema_version="1999-01-01")
        reported: list[str | None] = []

        with pytest.raises(ValueError, match="'1999-01-01' not found"):
            _parse_task_handler(coordinator, tmp_path, reported)

        assert reported == []
        mock_execvpe.assert_not_called()

    @pytest.mark.skipif(sys.platform != "linux", reason="PR_SET_PDEATHSIG is Linux-only")
    def test_the_runtime_dies_with_the_process_that_started_it(self, tmp_path):
        coordinator = _TaskHandlerParsingCoordinator(command=["/bin/sh", "-c", "exec sleep 60"])
        read_fd, write_fd = os.pipe()
        parent_pid = os.fork()
        if parent_pid == 0:
            # The process reading the runtime's sockets: it starts the runtime, reports its pid and waits.
            try:
                os.close(read_fd)
                runtime_pid = os.fork()
                if runtime_pid == 0:
                    try:
                        _parse_task_handler(coordinator, tmp_path, [])
                    finally:
                        os._exit(1)
                os.write(write_fd, str(runtime_pid).encode())
                os.close(write_fd)
                os.waitpid(runtime_pid, 0)
            finally:
                os._exit(0)

        os.close(write_fd)
        with os.fdopen(read_fd) as reader:
            runtime = psutil.Process(int(reader.read()))
        try:
            # Polls: the runtime is not a child to wait for, and time_machine cannot reach another process.
            deadline = time.monotonic() + 30
            while runtime.name() != "sleep":
                assert time.monotonic() < deadline, "the runtime did not start"
                time.sleep(0.05)

            os.kill(parent_pid, signal.SIGKILL)
            os.waitpid(parent_pid, 0)

            deadline = time.monotonic() + 10
            while _is_running(runtime):
                assert time.monotonic() < deadline, "the runtime outlived the process that started it"
                time.sleep(0.05)
        finally:
            with contextlib.suppress(psutil.NoSuchProcess):
                runtime.kill()
