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

import logging
import os
from datetime import datetime
from pathlib import Path
from subprocess import Popen
from unittest.mock import MagicMock, call, patch

import pytest
import time_machine
from requests import HTTPError, Response

from airflow.providers.edge3.cli.dataclasses import Job
from airflow.providers.edge3.cli.edge_command import _EdgeWorkerCli, _write_pid_to_pidfile
from airflow.providers.edge3.models.edge_worker import EdgeWorkerState, EdgeWorkerVersionException
from airflow.providers.edge3.worker_api.datamodels import (
    EdgeJobFetched,
    WorkerRegistrationReturn,
    WorkerSetStateReturn,
)
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

pytest.importorskip("pydantic", minversion="2.0.0")


MOCK_COMMAND = (
    {
        "token": "mock",
        "ti": {
            "id": "4d828a62-a417-4936-a7a6-2b3fabacecab",
            "task_id": "mock",
            "dag_id": "mock",
            "run_id": "mock",
            "try_number": 1,
            "pool_slots": 1,
            "queue": "default",
            "priority_weight": 1,
            "start_date": "2023-01-01T00:00:00+00:00",
            "map_index": -1,
        },
        "dag_rel_path": "mock.py",
        "log_path": "mock.log",
        "bundle_info": {"name": "hello", "version": "abc"},
    }
    if AIRFLOW_V_3_0_PLUS
    else ["test", "command"]  # Airflow 2.10
)


def test_write_pid_to_pidfile_success(caplog, tmp_path):
    with caplog.at_level(logging.DEBUG):
        pid_file_path = tmp_path / "file.pid"
        _write_pid_to_pidfile(pid_file_path)
        assert pid_file_path.exists()
        assert "An existing PID file has been found" not in caplog.text


def test_write_pid_to_pidfile_called_twice(tmp_path):
    pid_file_path = tmp_path / "file.pid"
    _write_pid_to_pidfile(pid_file_path)
    with pytest.raises(SystemExit, match=r"A PID file has already been written"):
        _write_pid_to_pidfile(pid_file_path)
    assert pid_file_path.exists()


def test_write_pid_to_pidfile_created_by_other_instance(tmp_path):
    # write a PID file with the PID of this process
    pid_file_path = tmp_path / "file.pid"
    _write_pid_to_pidfile(pid_file_path)
    # write a PID file, but set the current PID to 0
    with patch("os.getpid", return_value=0):
        with pytest.raises(SystemExit, match=r"contains the PID of another running process"):
            _write_pid_to_pidfile(pid_file_path)


def test_write_pid_to_pidfile_created_by_crashed_instance(tmp_path):
    # write a PID file with process ID 0
    with patch("os.getpid", return_value=0):
        pid_file_path = tmp_path / "file.pid"
        _write_pid_to_pidfile(pid_file_path)
        assert pid_file_path.read_text().strip() == "0"
    # write a PID file with the current process ID, call should not raise an exception
    _write_pid_to_pidfile(pid_file_path)
    assert str(os.getpid()) == pid_file_path.read_text().strip()


class _MockPopen(Popen):
    def __init__(self, returncode=None):
        self.generated_returncode = None

    def poll(self):
        pass

    @property
    def returncode(self):
        return self.generated_returncode


class TestEdgeWorkerCli:
    @pytest.fixture
    def mock_joblist(self, tmp_path: Path) -> list[Job]:
        logfile = tmp_path / "file.log"
        logfile.touch()

        return [
            Job(
                edge_job=EdgeJobFetched(
                    dag_id="test",
                    task_id="test1",
                    run_id="test",
                    map_index=-1,
                    try_number=1,
                    concurrency_slots=1,
                    command=MOCK_COMMAND,  # type: ignore[arg-type]
                ),
                process=_MockPopen(),
                logfile=logfile,
                logsize=0,
            ),
        ]

    @pytest.fixture
    def worker_with_job(self, tmp_path: Path, mock_joblist: list[Job]) -> _EdgeWorkerCli:
        test_worker = _EdgeWorkerCli(str(tmp_path / "mock.pid"), "mock", None, 8, 5, 5)
        _EdgeWorkerCli.jobs = mock_joblist
        return test_worker

    @patch("airflow.providers.edge3.cli.edge_command.Process")
    @patch("airflow.providers.edge3.cli.edge_command.logs_logfile_path")
    @patch("airflow.providers.edge3.cli.edge_command.Popen")
    def test_launch_job(self, mock_popen, mock_logfile_path, mock_process, worker_with_job: _EdgeWorkerCli):
        mock_popen.side_effect = [MagicMock()]
        mock_process_instance = MagicMock()
        mock_process.side_effect = [mock_process_instance]

        edge_job = _EdgeWorkerCli.jobs.pop().edge_job
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            worker_with_job._launch_job(edge_job)

        if AIRFLOW_V_3_0_PLUS:
            assert mock_process.call_count == 1
            assert mock_process_instance.start.call_count == 1
        else:
            assert mock_popen.call_count == 1
            assert mock_logfile_path.call_count == 1

        assert len(_EdgeWorkerCli.jobs) == 1
        assert _EdgeWorkerCli.jobs[0].edge_job == edge_job

    @pytest.mark.parametrize(
        "reserve_result, fetch_result, expected_calls",
        [
            pytest.param(None, False, (0, 0), id="no_job"),
            pytest.param(
                EdgeJobFetched(
                    dag_id="test",
                    task_id="test",
                    run_id="test",
                    map_index=-1,
                    try_number=1,
                    concurrency_slots=1,
                    command=MOCK_COMMAND,  # type: ignore[arg-type]
                ),
                True,
                (1, 1),
                id="new_job",
            ),
        ],
    )
    @patch("airflow.providers.edge3.cli.edge_command.jobs_fetch")
    @patch("airflow.providers.edge3.cli.edge_command.logs_logfile_path")
    @patch("airflow.providers.edge3.cli.edge_command.jobs_set_state")
    @patch("subprocess.Popen")
    def test_fetch_job(
        self,
        mock_popen,
        mock_set_state,
        mock_logfile_path,
        mock_reserve_task,
        reserve_result,
        fetch_result,
        expected_calls,
        worker_with_job: _EdgeWorkerCli,
    ):
        logfile_path_call_count, set_state_call_count = expected_calls
        mock_reserve_task.side_effect = [reserve_result]
        mock_popen.side_effect = ["mock"]
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            got_job = worker_with_job.fetch_job()
        mock_reserve_task.assert_called_once()
        assert got_job == fetch_result
        if AIRFLOW_V_3_0_PLUS:
            # this is only called on Airflow 2.10, AIP-72 includes it
            assert mock_logfile_path.call_count == 0
        else:
            assert mock_logfile_path.call_count == logfile_path_call_count
        assert mock_set_state.call_count == set_state_call_count

    def test_check_running_jobs_running(self, worker_with_job: _EdgeWorkerCli):
        assert worker_with_job.free_concurrency == worker_with_job.concurrency
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            worker_with_job.check_running_jobs()
        assert len(_EdgeWorkerCli.jobs) == 1
        assert (
            worker_with_job.free_concurrency
            == worker_with_job.concurrency - _EdgeWorkerCli.jobs[0].edge_job.concurrency_slots
        )

    @patch("airflow.providers.edge3.cli.edge_command.jobs_set_state")
    def test_check_running_jobs_success(self, mock_set_state, worker_with_job: _EdgeWorkerCli):
        job = _EdgeWorkerCli.jobs[0]
        job.process.generated_returncode = 0  # type: ignore[union-attr]
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            worker_with_job.check_running_jobs()
        assert len(_EdgeWorkerCli.jobs) == 0
        mock_set_state.assert_called_once_with(job.edge_job.key, TaskInstanceState.SUCCESS)
        assert worker_with_job.free_concurrency == worker_with_job.concurrency

    @patch("airflow.providers.edge3.cli.edge_command.jobs_set_state")
    def test_check_running_jobs_failed(self, mock_set_state, worker_with_job: _EdgeWorkerCli):
        job = _EdgeWorkerCli.jobs[0]
        job.process.generated_returncode = 42  # type: ignore[union-attr]
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            worker_with_job.check_running_jobs()
        assert len(_EdgeWorkerCli.jobs) == 0
        mock_set_state.assert_called_once_with(job.edge_job.key, TaskInstanceState.FAILED)
        assert worker_with_job.free_concurrency == worker_with_job.concurrency

    @time_machine.travel(datetime.now(), tick=False)
    @patch("airflow.providers.edge3.cli.edge_command.logs_push")
    def test_check_running_jobs_log_push(self, mock_logs_push, worker_with_job: _EdgeWorkerCli):
        job = _EdgeWorkerCli.jobs[0]
        job.logfile.write_text("some log content")
        with conf_vars(
            {
                ("edge", "api_url"): "https://invalid-api-test-endpoint",
                ("edge", "push_log_chunk_size"): "524288",
            }
        ):
            worker_with_job.check_running_jobs()
        assert len(_EdgeWorkerCli.jobs) == 1
        mock_logs_push.assert_called_once_with(
            task=job.edge_job.key, log_chunk_time=timezone.utcnow(), log_chunk_data="some log content"
        )

    @time_machine.travel(datetime.now(), tick=False)
    @patch("airflow.providers.edge3.cli.edge_command.logs_push")
    def test_check_running_jobs_log_push_increment(self, mock_logs_push, worker_with_job: _EdgeWorkerCli):
        job = _EdgeWorkerCli.jobs[0]
        job.logfile.write_text("hello ")
        job.logsize = job.logfile.stat().st_size
        job.logfile.write_text("hello world")
        with conf_vars(
            {
                ("edge", "api_url"): "https://invalid-api-test-endpoint",
                ("edge", "push_log_chunk_size"): "524288",
            }
        ):
            worker_with_job.check_running_jobs()
        assert len(_EdgeWorkerCli.jobs) == 1
        mock_logs_push.assert_called_once_with(
            task=job.edge_job.key, log_chunk_time=timezone.utcnow(), log_chunk_data="world"
        )

    @time_machine.travel(datetime.now(), tick=False)
    @patch("airflow.providers.edge3.cli.edge_command.logs_push")
    def test_check_running_jobs_log_push_chunks(self, mock_logs_push, worker_with_job: _EdgeWorkerCli):
        job = _EdgeWorkerCli.jobs[0]
        job.logfile.write_bytes("log1log2ülog3".encode("latin-1"))
        with conf_vars(
            {("edge", "api_url"): "https://invalid-api-test-endpoint", ("edge", "push_log_chunk_size"): "4"}
        ):
            worker_with_job.check_running_jobs()
        assert len(_EdgeWorkerCli.jobs) == 1
        calls = mock_logs_push.call_args_list
        assert len(calls) == 4
        assert calls[0] == call(
            task=job.edge_job.key, log_chunk_time=timezone.utcnow(), log_chunk_data="log1"
        )
        assert calls[1] == call(
            task=job.edge_job.key, log_chunk_time=timezone.utcnow(), log_chunk_data="log2"
        )
        assert calls[2] == call(
            task=job.edge_job.key, log_chunk_time=timezone.utcnow(), log_chunk_data="\\xfc"
        )
        assert calls[3] == call(
            task=job.edge_job.key, log_chunk_time=timezone.utcnow(), log_chunk_data="log3"
        )

    @pytest.mark.parametrize(
        "drain, maintenance_mode, jobs, expected_state",
        [
            pytest.param(False, False, False, EdgeWorkerState.IDLE, id="idle"),
            pytest.param(False, False, True, EdgeWorkerState.RUNNING, id="running_jobs"),
            pytest.param(False, True, False, EdgeWorkerState.MAINTENANCE_MODE, id="maintenance_no_job"),
            pytest.param(
                False, True, True, EdgeWorkerState.MAINTENANCE_PENDING, id="maintenance_running_jobs"
            ),
            pytest.param(True, False, False, EdgeWorkerState.OFFLINE, id="shut_down"),
            pytest.param(True, False, True, EdgeWorkerState.TERMINATING, id="terminating"),
            pytest.param(True, True, False, EdgeWorkerState.OFFLINE_MAINTENANCE, id="offline_maintenance"),
            pytest.param(True, True, True, EdgeWorkerState.TERMINATING, id="maintenance_shut_down"),
        ],
    )
    @patch("airflow.providers.edge3.cli.edge_command.worker_set_state")
    def test_heartbeat(
        self, mock_set_state, drain, maintenance_mode, jobs, expected_state, worker_with_job: _EdgeWorkerCli
    ):
        if not jobs:
            _EdgeWorkerCli.jobs = []
        _EdgeWorkerCli.drain = drain
        _EdgeWorkerCli.maintenance_mode = maintenance_mode
        mock_set_state.return_value = WorkerSetStateReturn(
            state=EdgeWorkerState.RUNNING, queues=["queue1", "queue2"]
        )
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            worker_with_job.heartbeat()
        assert mock_set_state.call_args.args[1] == expected_state
        queue_list = worker_with_job.queues or []
        assert len(queue_list) == 2
        assert "queue1" in (queue_list)
        assert "queue2" in (queue_list)

    @patch("airflow.providers.edge3.cli.edge_command.worker_set_state")
    def test_version_mismatch(self, mock_set_state, worker_with_job):
        mock_set_state.side_effect = EdgeWorkerVersionException("")
        worker_with_job.heartbeat()
        assert worker_with_job.drain

    @patch("airflow.providers.edge3.cli.edge_command.worker_register")
    def test_start_missing_apiserver(self, mock_register_worker, worker_with_job: _EdgeWorkerCli):
        mock_response = Response()
        mock_response.status_code = 404
        mock_register_worker.side_effect = HTTPError(
            "Something with 404:NOT FOUND means API is not active", response=mock_response
        )
        with pytest.raises(SystemExit, match=r"API endpoint is not ready"):
            worker_with_job.start()

    @patch("airflow.providers.edge3.cli.edge_command.worker_register")
    def test_start_server_error(self, mock_register_worker, worker_with_job: _EdgeWorkerCli):
        mock_response = Response()
        mock_response.status_code = 500
        mock_register_worker.side_effect = HTTPError(
            "Something other error not FourhundretFour", response=mock_response
        )
        with pytest.raises(SystemExit, match=r"Something other"):
            worker_with_job.start()

    @patch("airflow.providers.edge3.cli.edge_command.worker_register")
    @patch("airflow.providers.edge3.cli.edge_command._EdgeWorkerCli.loop")
    @patch("airflow.providers.edge3.cli.edge_command.worker_set_state")
    def test_start_and_run_one(
        self, mock_set_state, mock_loop, mock_register, worker_with_job: _EdgeWorkerCli
    ):
        def stop_running():
            _EdgeWorkerCli.drain = True
            _EdgeWorkerCli.jobs = []

        mock_loop.side_effect = stop_running
        mock_register.side_effect = [WorkerRegistrationReturn(last_update=datetime.now())]

        worker_with_job.start()

        mock_register.assert_called_once()
        mock_loop.assert_called_once()
        assert mock_set_state.call_count == 2

    def test_get_sysinfo(self, worker_with_job: _EdgeWorkerCli):
        concurrency = 8
        worker_with_job.concurrency = concurrency
        sysinfo = worker_with_job._get_sysinfo()
        assert "airflow_version" in sysinfo
        assert "edge_provider_version" in sysinfo
        assert "concurrency" in sysinfo
        assert sysinfo["concurrency"] == concurrency
