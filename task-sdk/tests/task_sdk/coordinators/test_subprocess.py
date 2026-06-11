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

from airflow.sdk.api.client import Client, TaskInstanceOperations
from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.coordinators._subprocess import (
    SubprocessCoordinator,
    _accept_connections,
    _connection_owned_by_process_tree,
    _is_connection_from_process,
    _PopenActivitySubprocess,
    _ResourceTracker,
    _start_server,
)
from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess

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
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 999999
        with patch(
            "airflow.sdk.coordinators._subprocess.psutil.Process",
            side_effect=psutil.NoSuchProcess(999999),
        ):
            assert _connection_owned_by_process_tree(("127.0.0.1", 1), ("127.0.0.1", 2), mock_proc) is False


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
    """Minimal SubprocessCoordinator subclass used to exercise the base machinery."""

    command: list[str]
    schema_version: str | None = None

    def _build_execute_task_command(self, *, what):
        return list(self.command), self.schema_version


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
