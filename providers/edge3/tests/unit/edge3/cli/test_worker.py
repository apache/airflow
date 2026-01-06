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
import importlib
import json
from datetime import datetime
from io import StringIO
from multiprocessing import Process, Queue
from pathlib import Path
from unittest import mock
from unittest.mock import call, patch

import pytest
import time_machine
from aiohttp import ClientResponseError, RequestInfo
from yarl import URL

from airflow.cli import cli_parser
from airflow.providers.common.compat.sdk import timezone
from airflow.providers.edge3.cli import edge_command, worker as worker_module
from airflow.providers.edge3.cli.dataclasses import Job
from airflow.providers.edge3.cli.worker import EdgeWorker, _execution_api_server_url
from airflow.providers.edge3.models.edge_worker import (
    EdgeWorkerModel,
    EdgeWorkerState,
    EdgeWorkerVersionException,
)
from airflow.providers.edge3.worker_api.datamodels import (
    EdgeJobFetched,
    WorkerRegistrationReturn,
    WorkerSetStateReturn,
)

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

pytest.importorskip("pydantic", minversion="2.0.0")
pytestmark = [pytest.mark.asyncio]


MOCK_COMMAND = {
    "token": "mock",
    "ti": {
        "id": "4d828a62-a417-4936-a7a6-2b3fabacecab",
        "task_id": "mock",
        "dag_id": "mock",
        "run_id": "mock",
        "try_number": 1,
        "dag_version_id": "01234567-89ab-cdef-0123-456789abcdef",
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


class _MockProcess(Process):
    def __init__(self, returncode=None):
        self.generated_returncode = None

    def poll(self):
        pass

    @property
    def returncode(self):
        return self.generated_returncode


class TestEdgeWorker:
    @pytest.fixture(autouse=True)
    def setup_parser(self):
        if AIRFLOW_V_3_2_PLUS:
            importlib.reload(cli_parser)
            self.parser = cli_parser.get_parser()
        else:
            with patch(
                "airflow.executors.executor_loader.ExecutorLoader.get_executor_names",
            ) as mock_get_executor_names:
                mock_get_executor_names.return_value = [
                    MagicMock(
                        name="EdgeExecutor", module_path="airflow.providers.edge3.executors.EdgeExecutor"
                    )
                ]
                importlib.reload(cli_parser)
                self.parser = cli_parser.get_parser()

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
                process=_MockProcess(),
                logfile=logfile,
                logsize=0,
            ),
        ]

    @pytest.fixture
    def worker_with_job(self, tmp_path: Path, mock_joblist: list[Job]) -> EdgeWorker:
        test_worker = EdgeWorker(str(tmp_path / "mock.pid"), "mock", None, 8, 5, 5)
        EdgeWorker.jobs = mock_joblist
        return test_worker

    @pytest.fixture
    def mock_edgeworker(self) -> EdgeWorkerModel:
        test_edgeworker = EdgeWorkerModel(
            worker_name="test_edge_worker",
            state="idle",
            queues=["default"],
        )
        return test_edgeworker

    @pytest.mark.parametrize(
        ("configs", "expected_url"),
        [
            (
                {("edge", "api_url"): "https://api-host/edge_worker/v1/rpcapi"},
                "https://api-host/execution",
            ),
            (
                {("edge", "api_url"): "https://api:1234/subpath/edge_worker/v1/rpcapi"},
                "https://api:1234/subpath/execution",
            ),
            (
                {
                    ("edge", "api_url"): "https://api-endpoint",
                    ("core", "execution_api_server_url"): "https://other-endpoint",
                },
                "https://other-endpoint",
            ),
        ],
    )
    def test_execution_api_server_url(
        self,
        configs,
        expected_url,
    ):
        with conf_vars(configs):
            _execution_api_server_url.cache_clear()
            url = _execution_api_server_url()
            assert url == expected_url

    @patch("airflow.sdk.execution_time.supervisor.supervise")
    @pytest.mark.asyncio
    async def test_supervise_launch(
        self,
        mock_supervise,
        worker_with_job: EdgeWorker,
    ):
        edge_job = worker_with_job.jobs.pop().edge_job
        q = mock.MagicMock()
        result = worker_with_job._run_job_via_supervisor(edge_job.command, q)

        assert result == 0
        q.put.assert_not_called()

    @patch("airflow.sdk.execution_time.supervisor.supervise")
    @pytest.mark.asyncio
    async def test_supervise_launch_fail(
        self,
        mock_supervise,
        worker_with_job: EdgeWorker,
    ):
        mock_supervise.side_effect = Exception("Supervise failed")
        edge_job = worker_with_job.jobs.pop().edge_job
        q = mock.MagicMock()
        result = worker_with_job._run_job_via_supervisor(edge_job.command, q)

        assert result == 1
        q.put.assert_called_once()

    @patch("airflow.providers.edge3.cli.worker.jobs_fetch")
    @patch("airflow.providers.edge3.cli.worker.EdgeWorker._launch_job", return_value=(Process(), Queue()))
    @pytest.mark.asyncio
    async def test_fetch_and_run_job_no_job(
        self,
        mock_launch_job,
        mock_jobs_fetch,
        worker_with_job: EdgeWorker,
    ):
        mock_jobs_fetch.return_value = None

        await worker_with_job.fetch_and_run_job()

        mock_jobs_fetch.assert_called_once()
        assert len(worker_with_job.jobs) == 1  # no new job added
        mock_launch_job.assert_not_called()

    @patch("airflow.providers.edge3.cli.worker.jobs_fetch")
    @patch("airflow.providers.edge3.cli.worker.EdgeWorker._launch_job", return_value=(Process(), Queue()))
    @patch("airflow.providers.edge3.cli.worker.jobs_set_state")
    @patch("airflow.providers.edge3.cli.worker.EdgeWorker._push_logs_in_chunks")
    @patch("airflow.providers.edge3.cli.worker.logs_push")
    @patch.object(Job, "is_running", property(lambda _: False))
    @patch.object(Job, "is_success", property(lambda _: True))
    @pytest.mark.asyncio
    async def test_fetch_and_run_job_one_job(
        self,
        mock_logs_push,
        mock_push_log_chunks,
        mock_jobs_set_state,
        mock_launch_job,
        mock_jobs_fetch,
        worker_with_job: EdgeWorker,
    ):
        mock_jobs_fetch.side_effect = [
            EdgeJobFetched(
                dag_id="test",
                task_id="test",
                run_id="test",
                map_index=-1,
                try_number=1,
                concurrency_slots=1,
                command=MOCK_COMMAND,  # type: ignore[arg-type]
            ),
            None,
        ]
        worker_with_job.concurrency = 1  # only one job at a time
        assert worker_with_job.free_concurrency == 0

        await worker_with_job.fetch_and_run_job()

        mock_jobs_fetch.assert_called_once()
        mock_launch_job.assert_called_once()
        assert mock_jobs_set_state.call_count == 2
        mock_push_log_chunks.assert_called_once()
        assert len(worker_with_job.jobs) == 1  # no new job added (was removed at the end...)
        mock_logs_push.assert_not_called()

    @patch("airflow.providers.edge3.cli.worker.jobs_fetch")
    @patch("airflow.providers.edge3.cli.worker.EdgeWorker._launch_job", return_value=(Process(), Queue()))
    @patch("airflow.providers.edge3.cli.worker.jobs_set_state")
    @patch("airflow.providers.edge3.cli.worker.EdgeWorker._push_logs_in_chunks")
    @patch("airflow.providers.edge3.cli.worker.logs_push")
    @patch.object(Job, "is_running", property(lambda _: False))
    @patch.object(Job, "is_success", property(lambda _: False))
    @patch("traceback.format_exception", return_value=[])
    @pytest.mark.asyncio
    async def test_fetch_and_run_job_one_job_fail(
        self,
        mock_traceback,
        mock_logs_push,
        mock_push_log_chunks,
        mock_jobs_set_state,
        mock_launch_job,
        mock_jobs_fetch,
        worker_with_job: EdgeWorker,
    ):
        mock_jobs_fetch.side_effect = [
            EdgeJobFetched(
                dag_id="test",
                task_id="test",
                run_id="test",
                map_index=-1,
                try_number=1,
                concurrency_slots=1,
                command=MOCK_COMMAND,  # type: ignore[arg-type]
            ),
            None,
        ]
        worker_with_job.concurrency = 1  # only one job at a time
        assert worker_with_job.free_concurrency == 0

        await worker_with_job.fetch_and_run_job()

        mock_jobs_fetch.assert_called_once()
        mock_launch_job.assert_called_once()
        assert mock_jobs_set_state.call_count == 2
        mock_push_log_chunks.assert_called_once()
        assert len(worker_with_job.jobs) == 1  # no new job added (was removed at the end...)
        mock_logs_push.assert_called_once()

    @time_machine.travel(datetime.now(), tick=False)
    @patch("airflow.providers.edge3.cli.worker.logs_push")
    @pytest.mark.asyncio
    async def test_push_logs_in_chunks(self, mock_logs_push, worker_with_job: EdgeWorker):
        job = EdgeWorker.jobs[0]
        job.logfile.write_text("some log content")
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            await worker_with_job._push_logs_in_chunks(job)

        assert len(EdgeWorker.jobs) == 1
        mock_logs_push.assert_called_once_with(
            task=job.edge_job.key, log_chunk_time=timezone.utcnow(), log_chunk_data="some log content"
        )

    @time_machine.travel(datetime.now(), tick=False)
    @patch("airflow.providers.edge3.cli.worker.logs_push")
    @pytest.mark.asyncio
    async def test_check_running_jobs_log_push_increment(self, mock_logs_push, worker_with_job: EdgeWorker):
        job = EdgeWorker.jobs[0]
        job.logfile.write_text("hello ")
        job.logsize = job.logfile.stat().st_size
        job.logfile.write_text("hello world")
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            await worker_with_job._push_logs_in_chunks(job)
        assert len(EdgeWorker.jobs) == 1
        mock_logs_push.assert_called_once_with(
            task=job.edge_job.key, log_chunk_time=timezone.utcnow(), log_chunk_data="world"
        )

    @time_machine.travel(datetime.now(), tick=False)
    @patch("airflow.providers.edge3.cli.worker.logs_push")
    @patch.object(worker_module, "push_log_chunk_size", 4)
    @pytest.mark.asyncio
    async def test_check_running_jobs_log_push_chunks(self, mock_logs_push, worker_with_job: EdgeWorker):
        job = EdgeWorker.jobs[0]
        job.logfile.write_bytes("log1log2ülog3".encode("latin-1"))
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            await worker_with_job._push_logs_in_chunks(job)
        assert len(EdgeWorker.jobs) == 1
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
        ("drain", "maintenance_mode", "jobs", "expected_state"),
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
    @patch("airflow.providers.edge3.cli.worker.worker_set_state")
    async def test_heartbeat(
        self, mock_set_state, drain, maintenance_mode, jobs, expected_state, worker_with_job: EdgeWorker
    ):
        if not jobs:
            EdgeWorker.jobs = []
        EdgeWorker.drain = drain
        EdgeWorker.maintenance_mode = maintenance_mode
        mock_set_state.return_value = WorkerSetStateReturn(
            state=EdgeWorkerState.RUNNING, queues=["queue1", "queue2"]
        )
        with conf_vars({("edge", "api_url"): "https://invalid-api-test-endpoint"}):
            await worker_with_job.heartbeat()
        assert mock_set_state.call_args.args[1] == expected_state
        queue_list = worker_with_job.queues or []
        assert len(queue_list) == 2
        assert "queue1" in (queue_list)
        assert "queue2" in (queue_list)

    @patch("airflow.providers.edge3.cli.worker.worker_set_state")
    async def test_version_mismatch(self, mock_set_state, worker_with_job):
        mock_set_state.side_effect = EdgeWorkerVersionException("")
        await worker_with_job.heartbeat()
        assert worker_with_job.drain

    @patch("airflow.providers.edge3.cli.worker.worker_register")
    async def test_start_missing_apiserver(self, mock_register_worker, worker_with_job: EdgeWorker):
        mock_register_worker.side_effect = ClientResponseError(
            request_info=RequestInfo(url=URL("mock.com"), method="GET", headers=None),  # type:ignore[arg-type]
            message="Something with 404:NOT FOUND means API is not active",
            status=404,
            history=(),
        )
        with pytest.raises(SystemExit, match=r"API endpoint is not ready"):
            await worker_with_job.start()

    @patch("airflow.providers.edge3.cli.worker.worker_register")
    async def test_start_server_error(self, mock_register_worker, worker_with_job: EdgeWorker):
        mock_register_worker.side_effect = ClientResponseError(
            request_info=RequestInfo(url=URL("mock.com"), method="GET", headers=None),  # type:ignore[arg-type]
            message="Something other error not FourhundretFour",
            status=500,
            history=(),
        )
        with pytest.raises(SystemExit, match=r"Something other"):
            await worker_with_job.start()

    @patch("airflow.providers.edge3.cli.worker.worker_register")
    @patch("airflow.providers.edge3.cli.worker.EdgeWorker.loop")
    @patch("airflow.providers.edge3.cli.worker.worker_set_state")
    async def test_start_and_run_one(
        self, mock_set_state, mock_loop, mock_register, worker_with_job: EdgeWorker
    ):
        def stop_running():
            worker_with_job.drain = True
            worker_with_job.jobs = []

        mock_loop.side_effect = stop_running
        mock_register.side_effect = [WorkerRegistrationReturn(last_update=datetime.now())]

        await worker_with_job.start()

        mock_register.assert_called_once()
        mock_loop.assert_called_once()
        assert mock_set_state.call_count == 1

    def test_get_sysinfo(self, worker_with_job: EdgeWorker):
        concurrency = 8
        worker_with_job.concurrency = concurrency
        sysinfo = worker_with_job._get_sysinfo()
        assert "airflow_version" in sysinfo
        assert "edge_provider_version" in sysinfo
        assert "concurrency" in sysinfo
        assert sysinfo["concurrency"] == concurrency

    @pytest.mark.db_test
    def test_list_edge_workers(self, mock_edgeworker: EdgeWorkerModel):
        args = self.parser.parse_args(["edge", "list-workers", "--output", "json"])
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            with patch(
                "airflow.providers.edge3.models.edge_worker.get_registered_edge_hosts",
                return_value=[mock_edgeworker],
            ):
                edge_command.list_edge_workers(args)
                out = temp_stdout.getvalue()
                edge_workers = json.loads(out)
        for key in [
            "worker_name",
            "state",
            "queues",
            "jobs_active",
            "concurrency",
            "free_concurrency",
            "maintenance_comment",
        ]:
            assert key in edge_workers[0]
        assert any("test_edge_worker" in h["worker_name"] for h in edge_workers)
