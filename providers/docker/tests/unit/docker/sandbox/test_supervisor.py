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
import os
import subprocess
from pathlib import Path
from unittest import mock
from uuid import uuid4

import pytest

from airflow.providers.docker.sandbox import supervisor

_REAL_OS_REPLACE = os.replace


def create_spec(tmp_path: Path) -> tuple[Path, Path, str]:
    request_id = str(uuid4())
    request_dir = tmp_path / request_id
    request_dir.mkdir(mode=0o700)
    request_dir.chmod(0o700)
    spec_path = request_dir / "launch.json"
    status_path = request_dir / "status.json"
    spec_path.write_text(
        json.dumps(
            {
                "command": ["python", "task.py"],
                "env": {"TASK_SECRET": "hidden"},
                "request_id": request_id,
                "schema_version": 1,
                "timeout_seconds": 30,
                "workdir": "/workspace",
            }
        )
    )
    spec_path.chmod(0o600)
    return spec_path, status_path, request_id


def read_status(path: Path) -> dict:
    return json.loads(path.read_text())


@mock.patch("airflow.providers.docker.sandbox.supervisor.os.replace", autospec=True)
@mock.patch("airflow.providers.docker.sandbox.supervisor.subprocess.Popen", autospec=True)
def test_supervisor_executes_argv_and_atomically_publishes_success(
    popen: mock.MagicMock,
    replace: mock.MagicMock,
    tmp_path: Path,
) -> None:
    spec_path, status_path, _ = create_spec(tmp_path)
    process = popen.return_value
    process.wait.return_value = 0
    published_states = []

    def observe_replace(source: Path, destination: Path) -> None:
        published_states.append(json.loads(Path(source).read_text())["state"])
        _REAL_OS_REPLACE(source, destination)

    replace.side_effect = observe_replace

    supervisor.run_supervisor(spec_path, status_path)

    assert published_states == ["running", "succeeded"]
    assert not spec_path.exists()
    assert read_status(status_path)["exit_code"] == 0
    popen.assert_called_once()
    assert popen.call_args.args[0] == ("python", "task.py")
    assert popen.call_args.kwargs["shell"] is False
    assert popen.call_args.kwargs["start_new_session"] is True
    assert popen.call_args.kwargs["env"]["TASK_SECRET"] == "hidden"


@mock.patch("airflow.providers.docker.sandbox.supervisor.subprocess.Popen", autospec=True)
def test_supervisor_publishes_nonzero_exit_as_failure(
    popen: mock.MagicMock,
    tmp_path: Path,
) -> None:
    spec_path, status_path, _ = create_spec(tmp_path)
    popen.return_value.wait.return_value = 7

    supervisor.run_supervisor(spec_path, status_path)

    status = read_status(status_path)
    assert status["state"] == "failed"
    assert status["exit_code"] == 7


@mock.patch("airflow.providers.docker.sandbox.supervisor.os.killpg", autospec=True)
@mock.patch("airflow.providers.docker.sandbox.supervisor.subprocess.Popen", autospec=True)
def test_supervisor_kills_process_group_and_publishes_timeout(
    popen: mock.MagicMock,
    killpg: mock.MagicMock,
    tmp_path: Path,
) -> None:
    spec_path, status_path, _ = create_spec(tmp_path)
    process = popen.return_value
    process.pid = 42
    process.wait.side_effect = [subprocess.TimeoutExpired("task", 30), -9]

    supervisor.run_supervisor(spec_path, status_path)

    killpg.assert_called_once_with(42, supervisor.signal.SIGKILL)
    status = read_status(status_path)
    assert status["state"] == "failed"
    assert status["exit_code"] == 124
    assert "timed out" in status["message"]


@mock.patch("airflow.providers.docker.sandbox.supervisor.subprocess.Popen", autospec=True)
def test_supervisor_publishes_command_start_failure(
    popen: mock.MagicMock,
    tmp_path: Path,
) -> None:
    spec_path, status_path, _ = create_spec(tmp_path)
    popen.side_effect = OSError("not executable")

    supervisor.run_supervisor(spec_path, status_path)

    status = read_status(status_path)
    assert status["state"] == "failed"
    assert status["exit_code"] == 127


def test_supervisor_deletes_malformed_spec_after_reading(tmp_path: Path) -> None:
    spec_path, status_path, _ = create_spec(tmp_path)
    spec_path.write_text("not json")
    spec_path.chmod(0o600)

    with pytest.raises(supervisor.SupervisorProtocolError, match="UTF-8 JSON"):
        supervisor.run_supervisor(spec_path, status_path)

    assert not spec_path.exists()
    assert not status_path.exists()
