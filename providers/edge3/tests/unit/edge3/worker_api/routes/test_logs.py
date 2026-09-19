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

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.providers.common.compat.sdk import conf, timezone
from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
from airflow.providers.edge3.worker_api.auth import jwt_validator
from airflow.providers.edge3.worker_api.datamodels import PushLogsBody
from airflow.providers.edge3.worker_api.routes.logs import logfile_path, logs_router, push_logs
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


DAG_ID = "my_dag"
TASK_ID = "my_task"
RUN_ID = "manual__2024-11-24"


class TestLogsApiRoutes:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, dag_maker, session: Session):
        with dag_maker(DAG_ID):
            EmptyOperator(task_id=TASK_ID)
        dag_maker.create_dagrun(run_id=RUN_ID)

        session.execute(delete(EdgeLogsModel))
        session.commit()

    def test_logfile_path(self, session: Session):
        p: str = logfile_path(dag_id=DAG_ID, task_id=TASK_ID, run_id=RUN_ID, try_number=1, map_index=-1)
        assert p
        assert str(Path(f"dag_id={DAG_ID}") / f"run_id={RUN_ID}" / f"task_id={TASK_ID}" / "attempt=1") in p
        assert "-1" not in Path(p).parts

    @conf_vars({("api_auth", "jwt_secret"): "edge-log-test-secret"})
    def test_logfile_path_missing_ti_returns_404(self):
        app = FastAPI()
        app.include_router(logs_router, prefix="/edge_worker/v1")
        method = f"logs/logfile_path/{DAG_ID}/nonexistent_task/{RUN_ID}/1/-1"
        token = JWTGenerator(
            secret_key=conf.get("api_auth", "jwt_secret"), valid_for=60, audience="api"
        ).generate(extras={"method": method})
        jwt_validator.cache_clear()
        try:
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.get(f"/edge_worker/v1/{method}", headers={"Authorization": token})
        finally:
            jwt_validator.cache_clear()

        assert response.status_code == 404
        assert response.json() == {
            "detail": (
                f"TaskInstance not found for dag_id={DAG_ID}, task_id=nonexistent_task, "
                f"run_id={RUN_ID}, map_index=-1"
            )
        }

    def test_push_logs(self, session: Session):
        log_data = PushLogsBody(
            log_chunk_data="This is Lorem Ipsum log data", log_chunk_time=timezone.utcnow()
        )
        with create_session() as session:
            push_logs(
                dag_id=DAG_ID,
                task_id=TASK_ID,
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                body=log_data,
                session=session,
            )
        logs: Sequence[EdgeLogsModel] = session.scalars(select(EdgeLogsModel)).all()
        assert len(logs) == 1
        assert logs[0].dag_id == DAG_ID
        assert logs[0].task_id == TASK_ID
        assert logs[0].run_id == RUN_ID
        assert logs[0].try_number == 1
        assert logs[0].map_index == -1
        assert "Lorem Ipsum" in logs[0].log_chunk_data
