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

import pathlib
import socket
import stat
import struct
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from uuid6 import uuid7

from airflow.sdk.coordinators.executable.coordinator import (
    FOOTER_MAGIC,
    FOOTER_SIZE,
    ExecutableCoordinator,
    _Bundle,
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
                "airflow.sdk.coordinators.socket.coordinator.subprocess.Popen",
                side_effect=capture_popen,
            ),
            patch(
                "airflow.sdk.coordinators.socket.coordinator._accept_connections",
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
                "airflow.sdk.coordinators.socket.coordinator._accept_connections",
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
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert isinstance(result, BaseCoordinator.ExecutionResult)
        assert result.exit_code == 0
