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

from airflow.providers.common.compat.sdk import TaskInstanceKey
from airflow.providers.edge3.models.edge_worker import EdgeWorkerState
from airflow.providers.edge3.worker_api.datamodels import (
    EdgeJobBase,
    EdgeJobFetched,
    PushLogsBody,
    WorkerQueuesBase,
    WorkerRegistrationReturn,
    WorkerSetStateReturn,
    WorkerStateBody,
)

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
    "type": "ExecuteTask",
}


def _make_job_base(*, map_index: int = -1, try_number: int = 1) -> EdgeJobBase:
    return EdgeJobBase(
        dag_id="test_dag",
        task_id="test_task",
        run_id="test_run",
        map_index=map_index,
        try_number=try_number,
    )


def test_edge_job_base_key_builds_task_instance_key():
    job = _make_job_base(map_index=3, try_number=2)

    assert job.key == TaskInstanceKey("test_dag", "test_task", "test_run", 2, 3)


class TestEdgeJobFetched:
    def test_command_dict_is_coerced_to_workload(self):
        job = EdgeJobFetched(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            concurrency_slots=1,
            command=MOCK_COMMAND,  # type: ignore[arg-type]
        )

        assert job.command.ti.dag_id == "mock"

    def test_identifier_names_all_key_components(self):
        job = EdgeJobFetched(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=3,
            try_number=2,
            concurrency_slots=1,
            command=MOCK_COMMAND,  # type: ignore[arg-type]
        )

        assert job.identifier == (
            "dag_id=test_dag task_id=test_task run_id=test_run map_index=3 try_number=2"
        )


def test_worker_queues_base_defaults():
    body = WorkerQueuesBase()

    assert body.queues is None
    assert body.team_name is None


class TestWorkerStateBody:
    def test_defaults(self):
        body = WorkerStateBody(state=EdgeWorkerState.IDLE, sysinfo={"status": 20})

        assert body.jobs_active == 0
        assert body.queues is None
        assert body.maintenance_comments is None

    def test_state_string_is_coerced_to_enum(self):
        body = WorkerStateBody(state="maintenance mode", sysinfo={})  # type: ignore[arg-type]

        assert body.state == EdgeWorkerState.MAINTENANCE_MODE

    def test_sysinfo_is_required(self):
        with pytest.raises(ValidationError):
            WorkerStateBody(state=EdgeWorkerState.IDLE)  # type: ignore[call-arg]


def test_push_logs_body_parses_iso_timestamp():
    body = PushLogsBody(
        log_chunk_time="2026-01-01T12:00:00+00:00",  # type: ignore[arg-type]
        log_chunk_data="log line",
    )

    assert body.log_chunk_time == datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc)


def test_worker_registration_return_assumes_version_mismatch():
    result = WorkerRegistrationReturn(last_update=datetime(2026, 1, 1, tzinfo=dt_timezone.utc))

    assert result.versions_match is False


def test_worker_set_state_return_defaults():
    result = WorkerSetStateReturn(state=EdgeWorkerState.IDLE, queues=None)

    assert result.versions_match is False
    assert result.maintenance_comments is None
    assert result.concurrency is None
