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
from uuid import UUID

import pytest
from sqlalchemy import delete

from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel, EdgeWorkerState
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.providers.edge3.worker_api.routes.ui import jobs

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


@pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Plugin endpoint is not used in Airflow 3.0+")
class TestUiApiRoutes:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session: Session):
        session.execute(delete(EdgeWorkerModel))
        session.add(EdgeWorkerModel(worker_name="worker1", queues=["default"], state=EdgeWorkerState.RUNNING))
        session.commit()

    def test_worker(self, session: Session):
        from airflow.providers.edge3.worker_api.routes.ui import worker

        worker_response = worker(session=session)
        assert worker_response is not None
        assert worker_response.total_entries == 1
        assert len(worker_response.workers) == 1
        assert worker_response.workers[0].worker_name == "worker1"

    def test_jobs_preserves_same_coordinate_attempts_and_legacy_identity(self, session: Session):
        attempts = {str(UUID(int=1)): "first", str(UUID(int=2)): "second", "": "legacy"}
        session.add_all(
            EdgeJobModel(
                dag_id="ui_identity",
                task_id="task",
                run_id="run",
                map_index=-1,
                try_number=1,
                task_instance_id=task_instance_id,
                state=TaskInstanceState.RUNNING,
                queue="default",
                concurrency_slots=1,
                command="unused",
                edge_worker=worker,
            )
            for task_instance_id, worker in attempts.items()
        )
        session.flush()

        response = jobs(session=session, dag_id_pattern="ui_identity").model_dump(mode="json")

        assert response["total_entries"] == 3
        assert {job["task_instance_id"]: job["edge_worker"] for job in response["jobs"]} == attempts

    def test_set_worker_concurrency_limit(self, session: Session):
        from airflow.providers.edge3.worker_api.datamodels_ui import ConcurrencyRequest
        from airflow.providers.edge3.worker_api.routes.ui import set_worker_concurrency_limit

        set_worker_concurrency_limit(
            worker_name="worker1",
            concurrency_request=ConcurrencyRequest(concurrency=4),
            session=session,
        )
        worker_model = session.get(EdgeWorkerModel, "worker1")
        assert worker_model is not None
        assert worker_model.concurrency == 4

    def test_set_worker_concurrency_limit_not_found(self, session: Session):
        from fastapi import HTTPException

        from airflow.providers.edge3.worker_api.datamodels_ui import ConcurrencyRequest
        from airflow.providers.edge3.worker_api.routes.ui import set_worker_concurrency_limit

        with pytest.raises(HTTPException) as exc_info:
            set_worker_concurrency_limit(
                worker_name="nonexistent_worker",
                concurrency_request=ConcurrencyRequest(concurrency=4),
                session=session,
            )
        assert exc_info.value.status_code == 404
