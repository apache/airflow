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
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from sqlalchemy import delete, select

from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.worker_api.datamodels import WorkerQueuesBody
from airflow.providers.edge3.worker_api.routes.jobs import fetch, state
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

try:
    from airflow.sdk._shared.observability.metrics.dual_stats_manager import DualStatsManager  # noqa: F401

    stats_reference = "airflow.sdk._shared.observability.metrics.dual_stats_manager.DualStatsManager"
    expected_call_count = 1
except ImportError:
    from airflow.providers.common.compat.sdk import Stats

    stats_reference = f"{Stats.__module__}.Stats"
    expected_call_count = 2

pytestmark = pytest.mark.db_test


DAG_ID = "my_dag"
TASK_ID = "my_task"
RUN_ID = "manual__2024-11-24T21:03:01+01:00"
QUEUE = "test"

MOCK_COMMAND_STR = json.dumps(
    {
        "token": "mock",
        "type": "ExecuteTask",
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
)


class TestJobsApiRoutes:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, dag_maker, session: Session):
        session.execute(delete(EdgeJobModel))
        session.commit()

    @patch(f"{stats_reference}.incr")
    def test_state(self, mock_stats_incr, session: Session):
        with create_session() as session:
            job = EdgeJobModel(
                dag_id=DAG_ID,
                task_id=TASK_ID,
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.RUNNING,
                queue=QUEUE,
                concurrency_slots=1,
                command="execute",
            )
            session.add(job)
            session.commit()
            state(
                dag_id=DAG_ID,
                task_id=TASK_ID,
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.RUNNING,
                session=session,
            )

            mock_stats_incr.assert_not_called()

            state(
                dag_id=DAG_ID,
                task_id=TASK_ID,
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.SUCCESS,
                session=session,
            )

            mock_stats_incr.assert_called_with(
                "edge_worker.ti.finish",
                tags={
                    "dag_id": DAG_ID,
                    "queue": QUEUE,
                    "state": TaskInstanceState.SUCCESS,
                    "task_id": TASK_ID,
                },
            )
            assert mock_stats_incr.call_count == expected_call_count

            db_job: EdgeJobModel | None = session.scalar(select(EdgeJobModel))
            assert db_job is not None
            assert db_job.state == TaskInstanceState.SUCCESS

    def test_fetch_filters_by_team_name(self, session: Session):
        with create_session() as session:
            job_team_a = EdgeJobModel(
                dag_id="dag_a",
                task_id="task_a",
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.QUEUED,
                queue=QUEUE,
                concurrency_slots=1,
                command=MOCK_COMMAND_STR,
                team_name="team_a",
            )
            job_team_b = EdgeJobModel(
                dag_id="dag_b",
                task_id="task_b",
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.QUEUED,
                queue=QUEUE,
                concurrency_slots=1,
                command=MOCK_COMMAND_STR,
                team_name="team_b",
            )
            session.add_all([job_team_a, job_team_b])
            session.commit()

            body = WorkerQueuesBody(free_concurrency=1, queues=[QUEUE], team_name="team_a")
            result = fetch("worker1", body, session)
            assert result is not None
            assert result.dag_id == "dag_a"
            assert result.task_id == "task_a"

    def test_fetch_without_team_name_returns_any_team(self, session: Session):
        """When team_name is None, no team filter is applied so any queued job can be returned."""
        with create_session() as session:
            job_team_a = EdgeJobModel(
                dag_id="dag_a",
                task_id="task_a",
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.QUEUED,
                queue=QUEUE,
                concurrency_slots=1,
                command=MOCK_COMMAND_STR,
                team_name="team_a",
            )
            job_no_team = EdgeJobModel(
                dag_id="dag_b",
                task_id="task_b",
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.QUEUED,
                queue=QUEUE,
                concurrency_slots=1,
                command=MOCK_COMMAND_STR,
                team_name=None,
            )
            session.add_all([job_team_a, job_no_team])
            session.commit()

            body = WorkerQueuesBody(free_concurrency=2, queues=[QUEUE], team_name=None)
            result1 = fetch("worker1", body, session)
            assert result1 is not None
            result2 = fetch("worker1", body, session)
            assert result2 is not None
            fetched_dag_ids = {result1.dag_id, result2.dag_id}
            assert fetched_dag_ids == {"dag_a", "dag_b"}
