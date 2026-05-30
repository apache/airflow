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
import re
import socket
import subprocess
import threading
import time
import zipfile
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from uuid6 import uuid7

from airflow.sdk.coordinators.java.coordinator import (
    JavaCoordinator,
    _accept_connections,
    _calculate_classpath,
    _JarInfo,
    _JavaActivitySubprocess,
    _ResourceTracker,
    _start_server,
)
from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess
from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)

METADATA_YAML_PATH = "META-INF/airflow-metadata.yaml"
DAG_CODE_PATH = "dag_source.py"
TEST_MAIN_CLASS = "com.example.MyBundle"


def _make_ti(dag_id: str = "test_dag", queue: str = "java") -> TaskInstanceDTO:
    return TaskInstanceDTO(
        id=uuid7(),
        dag_version_id=uuid7(),
        task_id="task_1",
        dag_id=dag_id,
        run_id="run_1",
        try_number=1,
        map_index=-1,
        pool_slots=1,
        queue=queue,
        priority_weight=1,
    )


def _make_jar(
    path: pathlib.Path,
    *,
    main_class: str | None = "com.example.Main",
    schema_version: str | None = None,
) -> pathlib.Path:
    """Write a minimal JAR with (optionally) a Main-Class manifest entry."""
    lines = ["Manifest-Version: 1.0"]
    if main_class:
        lines.append(f"Main-Class: {main_class}")
    if schema_version:
        lines.append(f"Airflow-Supervisor-Schema-Version: {schema_version}")
    manifest = "\n".join(lines) + "\n\n"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("META-INF/MANIFEST.MF", manifest)
    return path


class TestStartServer:
    def test_returns_listening_socket(self):
        server = _start_server()
        try:
            _, port = server.getsockname()
        finally:
            server.close()
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


class TestCalculateClasspath:
    def test_single_jar(self, tmp_path):
        jar = tmp_path.joinpath("app.jar")
        jar.write_bytes(b"")
        result = _calculate_classpath([tmp_path])
        assert result == jar.as_posix()

    def test_multiple_jars_all_included(self, tmp_path):
        tmp_path.joinpath("a.jar").write_bytes(b"")
        tmp_path.joinpath("b.jar").write_bytes(b"")
        tmp_path.joinpath("c.jar").write_bytes(b"")
        result = _calculate_classpath([tmp_path])
        entries = set(result.split(os.pathsep))
        assert entries == {
            tmp_path.joinpath("a.jar").as_posix(),
            tmp_path.joinpath("b.jar").as_posix(),
            tmp_path.joinpath("c.jar").as_posix(),
        }

    def test_non_jar_files_excluded(self, tmp_path):
        jar = tmp_path.joinpath("app.jar")
        jar.write_bytes(b"")
        tmp_path.joinpath("readme.txt").write_bytes(b"")
        tmp_path.joinpath("config.yaml").write_bytes(b"")
        result = _calculate_classpath([tmp_path])
        assert result == jar.as_posix()

    def test_empty_directory_returns_empty_string(self, tmp_path):
        result = _calculate_classpath([tmp_path])
        assert result == ""


class TestMainJar:
    def test_returns_main_class_from_jar(self, tmp_path):
        _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.Main", schema_version="2026-06-16")
        assert _JarInfo.find([tmp_path], "") == _JarInfo("com.example.Main", "2026-06-16")

    def test_no_jars_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match=re.escape(str(tmp_path.resolve()))):
            _JarInfo.find([tmp_path], "")

    def test_jar_without_main_class_not_returned(self, tmp_path):
        _make_jar(tmp_path.joinpath("app.jar"), main_class=None)
        with pytest.raises(FileNotFoundError):
            _JarInfo.find([tmp_path], "")

    def test_jar_with_main_class_but_no_schema_version_raises(self, tmp_path):
        """A JAR with Main-Class but no Airflow-Supervisor-Schema-Version must raise ValueError."""
        _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.Main")
        with pytest.raises(FileNotFoundError, match="Airflow-Supervisor-Schema-Version"):
            _JarInfo.find([tmp_path], "")

    def test_non_jar_files_skipped(self, tmp_path):
        tmp_path.joinpath("readme.txt").write_bytes(b"not a jar")
        _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.Main", schema_version="2026-06-16")
        assert _JarInfo.find([tmp_path], "") == _JarInfo("com.example.Main", "2026-06-16")

    def test_first_jar_missing_main_class_falls_through_to_second(self, tmp_path):
        # Alphabetically: a.jar (no Main-Class), b.jar (has Main-Class).
        _make_jar(tmp_path.joinpath("a.jar"), main_class=None)
        _make_jar(tmp_path.joinpath("b.jar"), main_class="com.example.Fallback", schema_version="2026-06-16")
        assert _JarInfo.find([tmp_path], "") == _JarInfo("com.example.Fallback", "2026-06-16")

    def test_fully_qualified_class_name_preserved(self, tmp_path):
        _make_jar(
            tmp_path.joinpath("app.jar"),
            main_class="org.apache.airflow.sdk.java.TaskRunner",
            schema_version="2026-06-16",
        )
        assert _JarInfo.find([tmp_path], "") == _JarInfo(
            main_class="org.apache.airflow.sdk.java.TaskRunner",
            schema_version="2026-06-16",
        )

    def test_find_by_explicit_main_class(self, tmp_path):
        """When a main_class filter is given, only the matching JAR is returned."""
        _make_jar(tmp_path.joinpath("a.jar"), main_class="com.example.Alpha", schema_version="2026-06-16")
        _make_jar(tmp_path.joinpath("b.jar"), main_class="com.example.Beta", schema_version="2026-06-16")
        result = _JarInfo.find([tmp_path], "com.example.Beta")
        assert result.main_class == "com.example.Beta"

    def test_find_by_explicit_main_class_not_present_raises(self, tmp_path):
        """When no JAR matches the main_class filter, FileNotFoundError is raised."""
        _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.Main", schema_version="2026-06-16")
        with pytest.raises(FileNotFoundError, match="com.example.Missing"):
            _JarInfo.find([tmp_path], "com.example.Missing")


class TestAcceptConnections:
    def _connect_after_delay(self, addr: tuple[str, int], delay: float = 0.0) -> None:
        def _connect():
            time.sleep(delay)
            c = socket.socket()
            with contextlib.suppress(OSError):  # Server may already be closed in teardown.
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

    def test_accepts_multiple_servers(self):
        comm_server = _start_server()
        logs_server = _start_server()
        _, comm_port = comm_server.getsockname()
        _, logs_port = logs_server.getsockname()

        self._connect_after_delay(("127.0.0.1", comm_port))
        self._connect_after_delay(("127.0.0.1", logs_port))

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None

        try:
            accepted, _ = _accept_connections({"comm": comm_server, "logs": logs_server}, {}, mock_proc)
            assert set(accepted) == {comm_server, logs_server}
            for sock in accepted.values():
                sock.close()
        finally:
            comm_server.close()
            logs_server.close()

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
        # proc has already exited
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

    def test_empty_drains_returns_empty_drained_dict(self):
        """When drains={} the returned drained mapping must also be empty."""
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
        """The drained dict must be keyed by the drain socket objects."""
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
            server.close()
            drain_r.close()
            drain_w.close()

    def test_bytes_written_to_drain_socket_are_returned(self):
        """Bytes written to a drain socket before the connection is accepted
        must be captured and returned in the drained dict."""
        server = _start_server()
        drain_r, drain_w = socket.socketpair()
        _, port = server.getsockname()

        drain_w.sendall(b"early output\n")
        self._connect_after_delay(("127.0.0.1", port), delay=0.05)

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = None
        try:
            _, drained = _accept_connections({"comm": server}, {"stdout": drain_r}, mock_proc)
            assert drained[drain_r] == b"early output\n"
        finally:
            server.close()
            drain_r.close()
            drain_w.close()

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


class TestResourceTracker:
    """Unit tests for the _ResourceTracker context manager introduced in this PR.

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


class TestJavaCoordinatorAttributes:
    def test_default_kwargs(self):
        coordinator = JavaCoordinator(jars_root="/airflow/java-bundles")
        assert coordinator.java_executable == "java"
        assert coordinator.jvm_args == []
        assert coordinator.jars_root == [pathlib.Path("/airflow/java-bundles")]

    def test_custom_kwargs(self):
        coordinator = JavaCoordinator(
            java_executable="/opt/java/bin/java",
            jvm_args=["-Xmx512m", "-Xms256m"],
            jars_root=["/airflow/java-bundles"],
        )
        assert coordinator.java_executable == "/opt/java/bin/java"
        assert coordinator.jvm_args == ["-Xmx512m", "-Xms256m"]
        assert coordinator.jars_root == [pathlib.Path("/airflow/java-bundles")]


@pytest.fixture
def jars_root(tmp_path):
    _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.TaskRunner", schema_version="2026-06-16")
    return tmp_path


@pytest.fixture
def mock_client(make_ti_context):
    client = MagicMock()
    client.task_instances.start.return_value = make_ti_context()
    return client


class TestJavaCoordinatorExecuteTask:
    def _captured_popen_cmd(
        self,
        jars_root: pathlib.Path,
        mock_client,
        *,
        java_executable: str = "java",
        jvm_args: list[str] | None = None,
    ) -> list[str]:
        """Run execute_task with mocked subprocess and return the command list."""
        ti = _make_ti()
        coordinator = JavaCoordinator(
            java_executable=java_executable,
            jvm_args=jvm_args or [],
            jars_root=jars_root,
        )

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 12345
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)
        popen_calls: list = []

        def capture_popen(cmd, **kwargs):
            popen_calls.append(cmd)
            return mock_proc

        with (
            patch(
                "airflow.sdk.coordinators.java.coordinator.subprocess.Popen",
                side_effect=capture_popen,
            ),
            patch(
                "airflow.sdk.coordinators.java.coordinator._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch.object(ActivitySubprocess, "wait", return_value=0),
            patch("psutil.Process"),
        ):
            coordinator.execute_task(
                what=ti,
                dag_rel_path="dags/test.jar",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert popen_calls, "subprocess.Popen was not called"
        return popen_calls[0]

    def test_java_executable_is_first_arg(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(
            jars_root, mock_client, java_executable="/usr/lib/jvm/java-17/bin/java"
        )
        assert cmd[0] == "/usr/lib/jvm/java-17/bin/java"

    def test_classpath_flag_and_value_present(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        assert "-classpath" in cmd
        cp_idx = cmd.index("-classpath")
        classpath = cmd[cp_idx + 1]
        assert jars_root.joinpath("app.jar").as_posix() in classpath

    def test_main_class_present(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        assert "com.example.TaskRunner" in cmd

    def test_comm_and_logs_args_present(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        comm_args = [a for a in cmd if a.startswith("--comm=")]
        logs_args = [a for a in cmd if a.startswith("--logs=")]
        assert len(comm_args) == 1
        assert len(logs_args) == 1

    def test_comm_and_logs_contain_port(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        comm_arg = next(a for a in cmd if a.startswith("--comm="))
        logs_arg = next(a for a in cmd if a.startswith("--logs="))
        # format is host:port
        assert ":" in comm_arg.split("=", 1)[1]
        assert ":" in logs_arg.split("=", 1)[1]

    def test_jvm_args_inserted_before_main_class(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client, jvm_args=["-Xmx512m", "-Dsome.prop=value"])
        main_idx = cmd.index("com.example.TaskRunner")
        for jvm_arg in ["-Xmx512m", "-Dsome.prop=value"]:
            assert jvm_arg in cmd
            assert cmd.index(jvm_arg) < main_idx

    def test_comm_and_logs_after_main_class(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        main_idx = cmd.index("com.example.TaskRunner")
        comm_idx = next(i for i, a in enumerate(cmd) if a.startswith("--comm="))
        logs_idx = next(i for i, a in enumerate(cmd) if a.startswith("--logs="))
        assert comm_idx > main_idx
        assert logs_idx > main_idx

    def test_returns_execution_result(self, jars_root, mock_client):
        ti = _make_ti()
        coordinator = JavaCoordinator(jars_root=jars_root)

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 99999
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch(
                "airflow.sdk.coordinators.java.coordinator._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch.object(ActivitySubprocess, "wait", return_value=0),
            patch("psutil.Process"),
        ):
            result = coordinator.execute_task(
                what=ti,
                dag_rel_path="dags/test.jar",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert isinstance(result, BaseCoordinator.ExecutionResult)
        assert result.exit_code == 0


class TestJavaActivitySubprocessStart:
    """
    Unit tests for _JavaActivitySubprocess.start().

    These tests mock subprocess.Popen and _accept_connections to verify that
    start() wires up the right command and stores the right sockets,
    without requiring a real Java runtime.
    """

    def _start_with_mocks(
        self,
        jars_root: pathlib.Path,
        mock_client,
        *,
        java_executable: str = "java",
        jvm_args: list[str] | None = None,
        ti: TaskInstanceDTO | None = None,
    ):
        """Call _JavaActivitySubprocess.start() with all subprocess machinery mocked out."""
        ti = ti or _make_ti()

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 12345
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch(
                "airflow.sdk.coordinators.java.coordinator.subprocess.Popen",
                return_value=mock_proc,
            ) as popen_mock,
            patch(
                "airflow.sdk.coordinators.java.coordinator._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch("psutil.Process"),
        ):
            proc = _JavaActivitySubprocess.start(
                what=ti,
                dag_rel_path="dags/test.jar",
                bundle_info=MagicMock(),
                client=mock_client,
                java_executable=java_executable,
                jvm_args=jvm_args or [],
                jars_root=[jars_root],
                main_class="",
                subprocess_logs_to_stdout=False,
            )

        return proc, popen_mock

    def test_stdin_is_comm_socket(self, jars_root, mock_client):
        """stdin (used by send_msg) must be the accepted comm socket."""
        ti = _make_ti()
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch("airflow.sdk.coordinators.java.coordinator.subprocess.Popen") as popen_mock,
            patch(
                "airflow.sdk.coordinators.java.coordinator._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch("psutil.Process"),
        ):
            popen_mock.return_value.pid = 12345
            proc = _JavaActivitySubprocess.start(
                what=ti,
                dag_rel_path="dags/test.jar",
                bundle_info=MagicMock(),
                client=MagicMock(),
                java_executable="java",
                jvm_args=[],
                jars_root=[jars_root],
                main_class="",
                subprocess_logs_to_stdout=False,
            )

        assert proc.stdin is comm_sock

    def test_pid_taken_from_popen(self, jars_root, mock_client):
        proc, _ = self._start_with_mocks(jars_root, mock_client)
        assert proc.pid == 12345

    def test_on_child_started_called(self, jars_root, mock_client):
        ti = _make_ti()
        with (
            patch("airflow.sdk.coordinators.java.coordinator.subprocess.Popen") as popen_mock,
            patch(
                "airflow.sdk.coordinators.java.coordinator._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {soc: MagicMock(spec=socket.socket) for soc in servers.values()},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started") as mock_on_started,
            patch("psutil.Process"),
        ):
            popen_mock.return_value.pid = 12345
            _JavaActivitySubprocess.start(
                what=ti,
                dag_rel_path="dags/test.jar",
                bundle_info=MagicMock(),
                client=mock_client,
                java_executable="java",
                jvm_args=[],
                jars_root=[jars_root],
                main_class="",
                subprocess_logs_to_stdout=False,
            )

        mock_on_started.assert_called_once()
        kwargs = mock_on_started.call_args.kwargs
        assert kwargs["ti"] is ti
        assert kwargs["dag_rel_path"] == "dags/test.jar"

    def test_register_pipe_readers_called_with_four_sockets(self, jars_root, mock_client):
        """Both socketpair read-ends and both TCP sockets must be registered, with a data kwarg."""
        with (
            patch("airflow.sdk.coordinators.java.coordinator.subprocess.Popen") as popen_mock,
            patch(
                "airflow.sdk.coordinators.java.coordinator._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {soc: MagicMock(spec=socket.socket) for soc in servers.values()},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers") as mock_register,
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch("psutil.Process"),
        ):
            popen_mock.return_value.pid = 12345
            _JavaActivitySubprocess.start(
                what=_make_ti(),
                dag_rel_path="dags/test.jar",
                bundle_info=MagicMock(),
                client=mock_client,
                java_executable="java",
                jvm_args=[],
                jars_root=[jars_root],
                main_class="",
                subprocess_logs_to_stdout=False,
            )
        assert mock_register.mock_calls == [call(ANY, ANY, ANY, ANY, data=ANY)]
