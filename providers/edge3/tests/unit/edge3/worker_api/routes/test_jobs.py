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

from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.worker_api.routes.jobs import state
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


DAG_ID = "my_dag"
TASK_ID = "my_task"
RUN_ID = "manual__2024-11-24T21:03:01+01:00"
QUEUE = "test"


class TestJobsApiRoutes:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, dag_maker, session: Session):
        session.query(EdgeJobModel).delete()
        session.commit()

    @patch("airflow.observability.stats.Stats.incr")
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
            mock_stats_incr.call_count == 2

            assert session.query(EdgeJobModel).scalar().state == TaskInstanceState.SUCCESS
