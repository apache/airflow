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

from datetime import datetime
from pathlib import Path
from subprocess import Popen
from unittest.mock import patch

import pytest
import time_machine

from airflow.providers.remote.cli.remote_command import (
    Job,
    _check_running_jobs,
    _fetch_job,
    _get_sysinfo,
    _heartbeat,
)
from airflow.providers.remote.models.remote_job import RemoteJob
from airflow.providers.remote.models.remote_worker import RemoteWorkerState
from airflow.utils.state import TaskInstanceState
from tests.test_utils.config import conf_vars

# Ignore the following error for mocking
# mypy: disable-error-code="attr-defined"


def test_get_sysinfo():
    sysinfo = _get_sysinfo()
    assert "airflow_version" in sysinfo
    assert "remote_provider_version" in sysinfo


@pytest.mark.parametrize(
    "reserve_result, fetch_result, expected_calls",
    [
        pytest.param(None, False, (0, 0), id="no_job"),
        pytest.param(
            RemoteJob(
                dag_id="test",
                task_id="test",
                run_id="test",
                map_index=-1,
                try_number=1,
                state=TaskInstanceState.QUEUED,
                queue="test",
                command=["test", "command"],
                queued_dttm=datetime.now(),
                remote_worker=None,
                last_update=None,
            ),
            True,
            (1, 1),
            id="new_job",
        ),
    ],
)
@patch("airflow.providers.remote.models.remote_job.RemoteJob.reserve_task")
@patch("airflow.providers.remote.models.remote_logs.RemoteLogs.logfile_path")
@patch("airflow.providers.remote.models.remote_job.RemoteJob.set_state")
@patch("subprocess.Popen")
def test_fetch_job(
    mock_popen,
    mock_set_state,
    mock_logfile_path,
    mock_reserve_task,
    reserve_result,
    fetch_result,
    expected_calls,
):
    logfile_path_call_count, set_state_call_count = expected_calls
    mock_reserve_task.side_effect = [reserve_result]
    mock_popen.side_effect = ["dummy"]
    joblist = []
    with conf_vars({("remote", "api_url"): "https://mock.server"}):
        got_job = _fetch_job("host", None, joblist)
    mock_reserve_task.assert_called_once()
    assert got_job == fetch_result
    assert mock_logfile_path.call_count == logfile_path_call_count
    assert mock_set_state.call_count == set_state_call_count


@pytest.fixture
def dummy_joblist(tmp_path: Path) -> list[Job]:
    logfile = tmp_path / "file.log"
    logfile.touch()

    class MockPopen(Popen):
        generated_returncode = None

        def __init__(self):
            pass

        def poll(self):
            pass

        @property
        def returncode(self):
            return self.generated_returncode

    return [
        Job(
            remote_job=RemoteJob(
                dag_id="test",
                task_id="test1",
                run_id="test",
                map_index=-1,
                try_number=1,
                state=TaskInstanceState.RUNNING,
                queue="test",
                command=["test", "command"],
                queued_dttm=datetime.now(),
                remote_worker=None,
                last_update=None,
            ),
            process=MockPopen(),
            logfile=logfile,
            logsize=0,
        ),
    ]


def test_check_running_jobs_running(dummy_joblist: list[Job]):
    dummy_joblist[0].process.generated_returncode = None
    with conf_vars({("remote", "api_url"): "https://mock.server"}):
        _check_running_jobs(dummy_joblist)
    assert len(dummy_joblist) == 1


@patch("airflow.providers.remote.models.remote_job.RemoteJob.set_state")
def test_check_running_jobs_success(mock_set_state, dummy_joblist: list[Job]):
    dummy_joblist[0].process.generated_returncode = 0
    job = dummy_joblist[0]
    with conf_vars({("remote", "api_url"): "https://mock.server"}):
        _check_running_jobs(dummy_joblist)
    assert len(dummy_joblist) == 0
    mock_set_state.assert_called_once_with(job.remote_job.key, TaskInstanceState.SUCCESS)


@patch("airflow.providers.remote.models.remote_job.RemoteJob.set_state")
def test_check_running_jobs_failed(mock_set_state, dummy_joblist: list[Job]):
    dummy_joblist[0].process.generated_returncode = 42
    job = dummy_joblist[0]
    with conf_vars({("remote", "api_url"): "https://mock.server"}):
        _check_running_jobs(dummy_joblist)
    assert len(dummy_joblist) == 0
    mock_set_state.assert_called_once_with(job.remote_job.key, TaskInstanceState.FAILED)


@time_machine.travel(datetime.now(), tick=False)
@patch("airflow.providers.remote.models.remote_logs.RemoteLogs.push_logs")
def test_check_running_jobs_log_push(mock_push_logs, dummy_joblist: list[Job]):
    job = dummy_joblist[0]
    job.process.generated_returncode = None
    job.logfile.write_text("some log content")
    with conf_vars({("remote", "api_url"): "https://mock.server"}):
        _check_running_jobs(dummy_joblist)
    assert len(dummy_joblist) == 1
    mock_push_logs.assert_called_once_with(
        task=job.remote_job.key, log_chunk_time=datetime.now(), log_chunk_data="some log content"
    )


@time_machine.travel(datetime.now(), tick=False)
@patch("airflow.providers.remote.models.remote_logs.RemoteLogs.push_logs")
def test_check_running_jobs_log_push_increment(mock_push_logs, dummy_joblist: list[Job]):
    job = dummy_joblist[0]
    job.process.generated_returncode = None
    job.logfile.write_text("hello ")
    job.logsize = job.logfile.stat().st_size
    job.logfile.write_text("hello world")
    with conf_vars({("remote", "api_url"): "https://mock.server"}):
        _check_running_jobs(dummy_joblist)
    assert len(dummy_joblist) == 1
    mock_push_logs.assert_called_once_with(
        task=job.remote_job.key, log_chunk_time=datetime.now(), log_chunk_data="world"
    )


@pytest.mark.parametrize(
    "drain, jobs, expected_state",
    [
        pytest.param(False, True, RemoteWorkerState.RUNNING, id="running_jobs"),
        pytest.param(True, True, RemoteWorkerState.TERMINATING, id="shutting_down"),
        pytest.param(False, False, RemoteWorkerState.IDLE, id="idle"),
    ],
)
@patch("airflow.providers.remote.models.remote_worker.RemoteWorker.set_state")
def test_heartbeat(mock_set_state, drain, jobs, expected_state, dummy_joblist: list[Job]):
    jobs = dummy_joblist if jobs else []
    with conf_vars({("remote", "api_url"): "https://mock.server"}):
        _heartbeat("host", jobs, drain)
    assert mock_set_state.call_args.args[1] == expected_state
