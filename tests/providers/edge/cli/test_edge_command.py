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

from airflow.exceptions import AirflowException
from airflow.providers.edge.cli.edge_command import (
    _EdgeWorkerCli,
    _get_sysinfo,
    _Job,
)
from airflow.providers.edge.models.edge_job import EdgeJob
from airflow.providers.edge.models.edge_worker import EdgeWorker, EdgeWorkerState
from airflow.utils.state import TaskInstanceState
from tests.test_utils.config import conf_vars

pytest.importorskip("pydantic", minversion="2.0.0")

# Ignore the following error for mocking
# mypy: disable-error-code="attr-defined"


def test_get_sysinfo():
    sysinfo = _get_sysinfo()
    assert "airflow_version" in sysinfo
    assert "edge_provider_version" in sysinfo


class TestEdgeWorkerCli:
    @pytest.fixture
    def dummy_joblist(self, tmp_path: Path) -> list[_Job]:
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
            _Job(
                edge_job=EdgeJob(
                    dag_id="test",
                    task_id="test1",
                    run_id="test",
                    map_index=-1,
                    try_number=1,
                    state=TaskInstanceState.RUNNING,
                    queue="test",
                    command=["test", "command"],
                    queued_dttm=datetime.now(),
                    edge_worker=None,
                    last_update=None,
                ),
                process=MockPopen(),
                logfile=logfile,
                logsize=0,
            ),
        ]

    @pytest.fixture
    def worker_with_job(self, tmp_path: Path, dummy_joblist: list[_Job]) -> _EdgeWorkerCli:
        test_worker = _EdgeWorkerCli(tmp_path / "dummy.pid", "dummy", None, 8, 5, 5)
        test_worker.jobs = dummy_joblist
        return test_worker

    @pytest.mark.parametrize(
        "reserve_result, fetch_result, expected_calls",
        [
            pytest.param(None, False, (0, 0), id="no_job"),
            pytest.param(
                EdgeJob(
                    dag_id="test",
                    task_id="test",
                    run_id="test",
                    map_index=-1,
                    try_number=1,
                    state=TaskInstanceState.QUEUED,
                    queue="test",
                    command=["test", "command"],
                    queued_dttm=datetime.now(),
                    edge_worker=None,
                    last_update=None,
                ),
                True,
                (1, 1),
                id="new_job",
            ),
        ],
    )
    @patch("airflow.providers.edge.models.edge_job.EdgeJob.reserve_task")
    @patch("airflow.providers.edge.models.edge_logs.EdgeLogs.logfile_path")
    @patch("airflow.providers.edge.models.edge_job.EdgeJob.set_state")
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
        mock_popen.side_effect = ["dummy"]
        with conf_vars({("edge", "api_url"): "https://mock.server"}):
            got_job = worker_with_job.fetch_job()
        mock_reserve_task.assert_called_once()
        assert got_job == fetch_result
        assert mock_logfile_path.call_count == logfile_path_call_count
        assert mock_set_state.call_count == set_state_call_count

    def test_check_running_jobs_running(self, worker_with_job: _EdgeWorkerCli):
        worker_with_job.jobs[0].process.generated_returncode = None
        with conf_vars({("edge", "api_url"): "https://mock.server"}):
            worker_with_job.check_running_jobs()
        assert len(worker_with_job.jobs) == 1

    @patch("airflow.providers.edge.models.edge_job.EdgeJob.set_state")
    def test_check_running_jobs_success(self, mock_set_state, worker_with_job: _EdgeWorkerCli):
        job = worker_with_job.jobs[0]
        job.process.generated_returncode = 0
        with conf_vars({("edge", "api_url"): "https://mock.server"}):
            worker_with_job.check_running_jobs()
        assert len(worker_with_job.jobs) == 0
        mock_set_state.assert_called_once_with(job.edge_job.key, TaskInstanceState.SUCCESS)

    @patch("airflow.providers.edge.models.edge_job.EdgeJob.set_state")
    def test_check_running_jobs_failed(self, mock_set_state, worker_with_job: _EdgeWorkerCli):
        job = worker_with_job.jobs[0]
        job.process.generated_returncode = 42
        with conf_vars({("edge", "api_url"): "https://mock.server"}):
            worker_with_job.check_running_jobs()
        assert len(worker_with_job.jobs) == 0
        mock_set_state.assert_called_once_with(job.edge_job.key, TaskInstanceState.FAILED)

    @time_machine.travel(datetime.now(), tick=False)
    @patch("airflow.providers.edge.models.edge_logs.EdgeLogs.push_logs")
    def test_check_running_jobs_log_push(self, mock_push_logs, worker_with_job: _EdgeWorkerCli):
        job = worker_with_job.jobs[0]
        job.process.generated_returncode = None
        job.logfile.write_text("some log content")
        with conf_vars({("edge", "api_url"): "https://mock.server"}):
            worker_with_job.check_running_jobs()
        assert len(worker_with_job.jobs) == 1
        mock_push_logs.assert_called_once_with(
            task=job.edge_job.key, log_chunk_time=datetime.now(), log_chunk_data="some log content"
        )

    @time_machine.travel(datetime.now(), tick=False)
    @patch("airflow.providers.edge.models.edge_logs.EdgeLogs.push_logs")
    def test_check_running_jobs_log_push_increment(self, mock_push_logs, worker_with_job: _EdgeWorkerCli):
        job = worker_with_job.jobs[0]
        job.process.generated_returncode = None
        job.logfile.write_text("hello ")
        job.logsize = job.logfile.stat().st_size
        job.logfile.write_text("hello world")
        with conf_vars({("edge", "api_url"): "https://mock.server"}):
            worker_with_job.check_running_jobs()
        assert len(worker_with_job.jobs) == 1
        mock_push_logs.assert_called_once_with(
            task=job.edge_job.key, log_chunk_time=datetime.now(), log_chunk_data="world"
        )

    @pytest.mark.parametrize(
        "drain, jobs, expected_state",
        [
            pytest.param(False, True, EdgeWorkerState.RUNNING, id="running_jobs"),
            pytest.param(True, True, EdgeWorkerState.TERMINATING, id="shutting_down"),
            pytest.param(False, False, EdgeWorkerState.IDLE, id="idle"),
        ],
    )
    @patch("airflow.providers.edge.models.edge_worker.EdgeWorker.set_state")
    def test_heartbeat(self, mock_set_state, drain, jobs, expected_state, worker_with_job: _EdgeWorkerCli):
        if not jobs:
            worker_with_job.jobs = []
        _EdgeWorkerCli.drain = drain
        with conf_vars({("edge", "api_url"): "https://mock.server"}):
            worker_with_job.heartbeat()
        assert mock_set_state.call_args.args[1] == expected_state

    @patch("airflow.providers.edge.models.edge_worker.EdgeWorker.register_worker")
    def test_start_missing_apiserver(self, mock_register_worker, worker_with_job: _EdgeWorkerCli):
        mock_register_worker.side_effect = AirflowException(
            "Something with 404:NOT FOUND means API is not active"
        )
        with pytest.raises(SystemExit, match=r"API endpoint is not ready"):
            worker_with_job.start()

    @patch("airflow.providers.edge.models.edge_worker.EdgeWorker.register_worker")
    def test_start_server_error(self, mock_register_worker, worker_with_job: _EdgeWorkerCli):
        mock_register_worker.side_effect = AirflowException("Something other error not FourhundretFour")
        with pytest.raises(SystemExit, match=r"Something other"):
            worker_with_job.start()

    @patch("airflow.providers.edge.models.edge_worker.EdgeWorker.register_worker")
    @patch("airflow.providers.edge.cli.edge_command._EdgeWorkerCli.loop")
    @patch("airflow.providers.edge.models.edge_worker.EdgeWorker.set_state")
    def test_start_and_run_one(
        self, mock_set_state, mock_loop, mock_register_worker, worker_with_job: _EdgeWorkerCli
    ):
        mock_register_worker.side_effect = [
            EdgeWorker(
                worker_name="test",
                state=EdgeWorkerState.STARTING,
                queues=None,
                first_online=datetime.now(),
                last_update=datetime.now(),
                jobs_active=0,
                jobs_taken=0,
                jobs_success=0,
                jobs_failed=0,
                sysinfo="",
            )
        ]

        def stop_running():
            _EdgeWorkerCli.drain = True
            worker_with_job.jobs = []

        mock_loop.side_effect = stop_running

        worker_with_job.start()

        mock_register_worker.assert_called_once()
        mock_loop.assert_called_once()
        mock_set_state.assert_called_once()
