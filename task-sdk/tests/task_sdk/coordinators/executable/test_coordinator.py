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
import pathlib
import socket
import stat
import struct
import subprocess
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from uuid6 import uuid7

from airflow.sdk.coordinators.executable.coordinator import (
    FOOTER_MAGIC,
    FOOTER_SIZE,
    ExecutableCoordinator,
    _accept_connections,
    _Bundle,
    _ExecutableActivitySubprocess,
    _start_server,
)
from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess
from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)

_DEFAULT_BINARY_PAYLOAD = b"\x7fELF" + b"binary-stub-payload"


def _make_metadata(dag_ids, source_filename: str = "example.go") -> dict:
    return {
        "format_version": "1.0",
        "sdk": {"language": "go", "version": "0.1.0"},
        "source": source_filename,
        "dags": {dag_id: {"tasks": ["task1"]} for dag_id in dag_ids},
    }


def _build_bundle(
    path: Path,
    *,
    dag_ids=("tutorial_dag",),
    source: str | bytes = "package main\n\nfunc main() {}\n",
    source_filename: str = "example.go",
    metadata: dict | bytes | None = None,
    binary_bytes: bytes = _DEFAULT_BINARY_PAYLOAD,
    footer_ver: int = 1,
    magic: bytes = FOOTER_MAGIC,
    reserved: bytes = b"\x00" * 12,
) -> Path:
    if isinstance(source, str):
        source_bytes = source.encode("utf-8")
    else:
        source_bytes = source

    if metadata is None:
        metadata_dict = _make_metadata(dag_ids, source_filename=source_filename)
        metadata_bytes = yaml.safe_dump(metadata_dict, sort_keys=True).encode("utf-8")
    elif isinstance(metadata, (bytes, bytearray)):
        metadata_bytes = bytes(metadata)
    else:
        metadata_bytes = yaml.safe_dump(metadata, sort_keys=True).encode("utf-8")

    if len(reserved) != 12:
        raise ValueError("reserved must be exactly 12 bytes")
    trailer = struct.pack("<III", len(source_bytes), len(metadata_bytes), footer_ver) + reserved + magic
    assert len(trailer) == FOOTER_SIZE

    path.write_bytes(binary_bytes + source_bytes + metadata_bytes + trailer)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


def _make_executable(path: Path) -> Path:
    path.write_bytes(b"#!/bin/sh\nexit 0\n")
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


def _make_ti(dag_id: str = "tutorial_dag", queue: str = "executable") -> TaskInstanceDTO:
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


class TestAcceptConnections:
    def _connect_after_delay(self, addr: tuple[str, int], delay: float = 0.0) -> None:
        def _connect():
            time.sleep(delay)
            c = socket.socket()
            with contextlib.suppress(OSError):  # Server may already be closed in teardown.
                c.connect(addr)

        threading.Thread(target=_connect, daemon=True).start()

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
            accepted = _accept_connections({"comm": comm_server, "logs": logs_server}, mock_proc)
            assert set(accepted) == {"comm", "logs"}
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
                _accept_connections({"comm": server}, mock_proc, max_wait=0.05)
        finally:
            server.close()

    def test_raises_runtime_error_if_process_exits_before_connecting(self):
        server = _start_server()
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = 1
        mock_proc.returncode = 1
        try:
            with pytest.raises(RuntimeError, match="process exited with 1"):
                _accept_connections({"comm": server}, mock_proc)
        finally:
            server.close()


class TestBundleFind:
    def test_finds_matching_dag_id(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag", "other_dag"])

        bundle = _Bundle.find([tmp_path], "tutorial_dag")
        assert bundle.path == binary.resolve()

    def test_picks_matching_bundle_among_many(self, tmp_path):
        _build_bundle(tmp_path / "alpha", dag_ids=["alpha_dag"])
        beta = _build_bundle(tmp_path / "beta", dag_ids=["beta_dag"])
        _build_bundle(tmp_path / "gamma", dag_ids=["gamma_dag"])

        bundle = _Bundle.find([tmp_path], "beta_dag")
        assert bundle.path == beta.resolve()

    def test_searches_multiple_roots(self, tmp_path):
        root_a = tmp_path / "a"
        root_b = tmp_path / "b"
        root_a.mkdir()
        root_b.mkdir()
        _build_bundle(root_a / "alpha", dag_ids=["alpha_dag"])
        target = _build_bundle(root_b / "beta", dag_ids=["beta_dag"])

        bundle = _Bundle.find([root_a, root_b], "beta_dag")
        assert bundle.path == target.resolve()

    def test_skips_non_bundle_files(self, tmp_path):
        (tmp_path / "README.md").write_text("not a bundle")
        _make_executable(tmp_path / "stray_executable")
        binary = _build_bundle(tmp_path / "real_bundle", dag_ids=["tutorial_dag"])

        bundle = _Bundle.find([tmp_path], "tutorial_dag")
        assert bundle.path == binary.resolve()

    def test_skips_non_executable_files(self, tmp_path):
        non_exec = _build_bundle(tmp_path / "non_exec", dag_ids=["tutorial_dag"])
        non_exec.chmod(non_exec.stat().st_mode & ~(stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH))

        with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
            _Bundle.find([tmp_path], "tutorial_dag")

    def test_raises_when_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
            _Bundle.find([tmp_path], "nonexistent_dag")

    def test_raises_when_directory_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
            _Bundle.find([tmp_path / "does_not_exist"], "tutorial_dag")


class TestExecutableCoordinatorAttributes:
    def test_default_kwargs(self):
        coordinator = ExecutableCoordinator()
        assert coordinator.sdk == "executable"
        assert coordinator.file_extension == ""
        assert coordinator.executables_root == []

    def test_executables_root_accepts_single_path(self, tmp_path):
        coordinator = ExecutableCoordinator(executables_root=str(tmp_path))
        assert coordinator.executables_root == [tmp_path]

    def test_executables_root_accepts_list(self, tmp_path):
        other = tmp_path / "other"
        coordinator = ExecutableCoordinator(executables_root=[str(tmp_path), other])
        assert coordinator.executables_root == [tmp_path, other]

    def test_executables_root_none_becomes_empty_list(self):
        coordinator = ExecutableCoordinator(executables_root=None)
        assert coordinator.executables_root == []


class TestResolveExecutable:
    def test_resolves_via_executables_root(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])
        ti = _make_ti(dag_id="tutorial_dag")

        coordinator = ExecutableCoordinator(executables_root=[tmp_path])
        resolved = coordinator._resolve_executable(what=ti)
        assert resolved == str(binary.resolve())

    def test_raises_when_executables_root_missing(self):
        ti = _make_ti(dag_id="tutorial_dag")
        coordinator = ExecutableCoordinator()
        with pytest.raises(ValueError, match="executables_root kwarg must be set"):
            coordinator._resolve_executable(what=ti)

    def test_raises_when_dag_id_not_found(self, tmp_path):
        _build_bundle(tmp_path / "my_bundle", dag_ids=["other_dag"])
        ti = _make_ti(dag_id="tutorial_dag")

        coordinator = ExecutableCoordinator(executables_root=[tmp_path])
        with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
            coordinator._resolve_executable(what=ti)


@pytest.fixture
def bundles_dir(tmp_path):
    _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])
    return tmp_path


@pytest.fixture
def mock_client(make_ti_context):
    client = MagicMock()
    client.task_instances.start.return_value = make_ti_context()
    return client


class TestExecutableCoordinatorExecuteTask:
    def _captured_popen_cmd(self, bundles_dir: pathlib.Path, mock_client) -> list[str]:
        """Run execute_task with mocked subprocess and return the command list."""
        ti = _make_ti(dag_id="tutorial_dag")
        coordinator = ExecutableCoordinator(executables_root=[bundles_dir])

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
                "airflow.sdk.coordinators.executable.coordinator.subprocess.Popen",
                side_effect=capture_popen,
            ),
            patch(
                "airflow.sdk.coordinators.executable.coordinator._accept_connections",
                return_value={"comm": comm_sock, "logs": logs_sock},
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch.object(ActivitySubprocess, "wait", return_value=0),
            patch("psutil.Process"),
        ):
            coordinator.execute_task(
                what=ti,
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert popen_calls, "subprocess.Popen was not called"
        return popen_calls[0]

    def test_executable_path_is_first_arg(self, bundles_dir, mock_client):
        cmd = self._captured_popen_cmd(bundles_dir, mock_client)
        expected = str((bundles_dir / "my_bundle").resolve())
        assert cmd[0] == expected

    def test_comm_and_logs_args_present(self, bundles_dir, mock_client):
        cmd = self._captured_popen_cmd(bundles_dir, mock_client)
        comm_args = [a for a in cmd if a.startswith("--comm=")]
        logs_args = [a for a in cmd if a.startswith("--logs=")]
        assert len(comm_args) == 1
        assert len(logs_args) == 1

    def test_comm_and_logs_contain_port(self, bundles_dir, mock_client):
        cmd = self._captured_popen_cmd(bundles_dir, mock_client)
        comm_arg = next(a for a in cmd if a.startswith("--comm="))
        logs_arg = next(a for a in cmd if a.startswith("--logs="))
        assert ":" in comm_arg.split("=", 1)[1]
        assert ":" in logs_arg.split("=", 1)[1]

    def test_returns_execution_result(self, bundles_dir, mock_client):
        ti = _make_ti(dag_id="tutorial_dag")
        coordinator = ExecutableCoordinator(executables_root=[bundles_dir])

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 99999
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch(
                "airflow.sdk.coordinators.executable.coordinator._accept_connections",
                return_value={"comm": comm_sock, "logs": logs_sock},
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch.object(ActivitySubprocess, "wait", return_value=0),
            patch("psutil.Process"),
        ):
            result = coordinator.execute_task(
                what=ti,
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert isinstance(result, BaseCoordinator.ExecutionResult)
        assert result.exit_code == 0


class TestExecutableActivitySubprocessStart:
    """
    Unit tests for _ExecutableActivitySubprocess.start().

    These tests mock subprocess.Popen and _accept_connections to verify that
    start() wires up the right command and stores the right sockets,
    without requiring a real native binary to launch.
    """

    def _start_with_mocks(
        self,
        executable_path: str,
        mock_client,
        *,
        ti: TaskInstanceDTO | None = None,
    ):
        ti = ti or _make_ti()

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 12345
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch(
                "airflow.sdk.coordinators.executable.coordinator.subprocess.Popen",
                return_value=mock_proc,
            ) as popen_mock,
            patch(
                "airflow.sdk.coordinators.executable.coordinator._accept_connections",
                return_value={"comm": comm_sock, "logs": logs_sock},
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch("psutil.Process"),
        ):
            proc = _ExecutableActivitySubprocess.start(
                what=ti,
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                executable_path=executable_path,
                subprocess_logs_to_stdout=False,
            )
        return proc, popen_mock

    def test_stdout_write_socket_stored_for_cleanup(self, bundles_dir, mock_client):
        proc, _ = self._start_with_mocks(str(bundles_dir / "my_bundle"), mock_client)
        assert proc._stdout_w is not None

    def test_stderr_write_socket_stored_for_cleanup(self, bundles_dir, mock_client):
        proc, _ = self._start_with_mocks(str(bundles_dir / "my_bundle"), mock_client)
        assert proc._stderr_w is not None

    def test_stdout_and_stderr_write_sockets_are_distinct(self, bundles_dir, mock_client):
        proc, _ = self._start_with_mocks(str(bundles_dir / "my_bundle"), mock_client)
        assert proc._stdout_w is not proc._stderr_w

    def test_stdin_is_comm_socket(self, bundles_dir, mock_client):
        """stdin (used by send_msg) must be the accepted comm socket."""
        ti = _make_ti()
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch("airflow.sdk.coordinators.executable.coordinator.subprocess.Popen") as popen_mock,
            patch(
                "airflow.sdk.coordinators.executable.coordinator._accept_connections",
                return_value={"comm": comm_sock, "logs": logs_sock},
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch("psutil.Process"),
        ):
            popen_mock.return_value.pid = 12345
            proc = _ExecutableActivitySubprocess.start(
                what=ti,
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=MagicMock(),
                executable_path=str(bundles_dir / "my_bundle"),
                subprocess_logs_to_stdout=False,
            )

        assert proc.stdin is comm_sock

    def test_pid_taken_from_popen(self, bundles_dir, mock_client):
        proc, _ = self._start_with_mocks(str(bundles_dir / "my_bundle"), mock_client)
        assert proc.pid == 12345

    def test_on_child_started_called(self, bundles_dir, mock_client):
        ti = _make_ti()
        with (
            patch("airflow.sdk.coordinators.executable.coordinator.subprocess.Popen") as popen_mock,
            patch(
                "airflow.sdk.coordinators.executable.coordinator._accept_connections",
                return_value={"comm": MagicMock(), "logs": MagicMock()},
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started") as mock_on_started,
            patch("psutil.Process"),
        ):
            popen_mock.return_value.pid = 12345
            _ExecutableActivitySubprocess.start(
                what=ti,
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                executable_path=str(bundles_dir / "my_bundle"),
                subprocess_logs_to_stdout=False,
            )

        mock_on_started.assert_called_once()
        kwargs = mock_on_started.call_args.kwargs
        assert kwargs["ti"] is ti
        assert kwargs["dag_rel_path"] == "my_bundle"

    def test_register_pipe_readers_called_with_four_sockets(self, bundles_dir, mock_client):
        """Both socketpair read-ends and both TCP sockets must be registered."""
        with (
            patch("airflow.sdk.coordinators.executable.coordinator.subprocess.Popen") as popen_mock,
            patch(
                "airflow.sdk.coordinators.executable.coordinator._accept_connections",
                return_value={"comm": MagicMock(), "logs": MagicMock()},
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers") as mock_register,
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch("psutil.Process"),
        ):
            popen_mock.return_value.pid = 12345
            _ExecutableActivitySubprocess.start(
                what=_make_ti(),
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                executable_path=str(bundles_dir / "my_bundle"),
                subprocess_logs_to_stdout=False,
            )

        mock_register.assert_called_once()
        args = mock_register.call_args.args
        # positional: stdout, stderr, comm, logs — all four must be sockets
        assert len(args) == 4
