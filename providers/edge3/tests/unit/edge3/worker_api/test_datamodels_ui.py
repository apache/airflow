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

from datetime import datetime, timezone as dt_timezone

import pytest
from pydantic import ValidationError

from airflow.providers.edge3.models.edge_worker import EdgeWorkerState
from airflow.providers.edge3.worker_api.datamodels_ui import (
    ConcurrencyRequest,
    Job,
    JobCollectionResponse,
    MaintenanceRequest,
    QueueUpdateRequest,
    Worker,
    WorkerCollectionResponse,
)
from airflow.utils.state import TaskInstanceState


def _make_worker() -> Worker:
    return Worker(worker_name="worker-1", state=EdgeWorkerState.IDLE, sysinfo={"status": 20})


def _make_job(*, queued_dttm: datetime | None = None, edge_worker: str | None = None) -> Job:
    return Job(
        dag_id="test_dag",
        task_id="test_task",
        run_id="test_run",
        map_index=-1,
        try_number=1,
        state=TaskInstanceState.RUNNING,
        queue="default",
        queued_dttm=queued_dttm,
        edge_worker=edge_worker,
    )


class TestWorker:
    def test_defaults(self):
        worker = _make_worker()

        assert worker.first_online is None
        assert worker.last_heartbeat is None

    def test_worker_name_is_required(self):
        with pytest.raises(ValidationError):
            Worker(state=EdgeWorkerState.IDLE, sysinfo={})  # type: ignore[call-arg]


class TestJob:
    def test_defaults(self):
        job = _make_job()

        assert job.queued_dttm is None
        assert job.edge_worker is None
        assert job.last_update is None

    def test_execution_fields_round_trip(self):
        queued = datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc)

        job = _make_job(queued_dttm=queued, edge_worker="worker-1")

        assert job.state == TaskInstanceState.RUNNING
        assert job.queue == "default"
        assert job.queued_dttm == queued
        assert job.edge_worker == "worker-1"


def test_worker_collection_response_holds_workers():
    response = WorkerCollectionResponse(workers=[_make_worker()], total_entries=1)

    assert response.workers[0].worker_name == "worker-1"
    assert response.total_entries == 1


def test_job_collection_response_holds_jobs():
    response = JobCollectionResponse(jobs=[_make_job()], total_entries=1)

    assert response.jobs[0].dag_id == "test_dag"
    assert response.total_entries == 1


def test_maintenance_request_requires_comment():
    assert MaintenanceRequest(maintenance_comment="planned upgrade").maintenance_comment == (
        "planned upgrade"
    )
    with pytest.raises(ValidationError):
        MaintenanceRequest()  # type: ignore[call-arg]


def test_queue_update_request_requires_queue_name():
    assert QueueUpdateRequest(queue_name="gpu").queue_name == "gpu"
    with pytest.raises(ValidationError):
        QueueUpdateRequest()  # type: ignore[call-arg]


class TestConcurrencyRequest:
    def test_positive_concurrency_is_accepted(self):
        assert ConcurrencyRequest(concurrency=4).concurrency == 4

    @pytest.mark.parametrize("concurrency", [0, -5])
    def test_non_positive_concurrency_is_rejected(self, concurrency):
        with pytest.raises(ValidationError):
            ConcurrencyRequest(concurrency=concurrency)
