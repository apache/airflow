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
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.providers.common.compat.sdk import conf, timezone
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
from airflow.providers.edge3.models.types import (
    CALLBACK_JOB_MAP_INDEX,
    CALLBACK_JOB_TRY_NUMBER,
    EXECUTE_CALLBACK_TAG,
    build_callback_run_id,
)
from airflow.providers.edge3.worker_api.auth import jwt_validator
from airflow.providers.edge3.worker_api.datamodels import PushLogsBody
from airflow.providers.edge3.worker_api.routes.logs import _logfile_path, logfile_path, logs_router, push_logs
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


DAG_ID = "my_dag"
TASK_ID = "my_task"
RUN_ID = "manual__2024-11-24"
CALLBACK_ID = "12345678-1234-5678-1234-567812345678"
CALLBACK_LOG_PATH = f"executor_callbacks/{DAG_ID}/{RUN_ID}/{CALLBACK_ID}"
UNKNOWN_TI_ID = "0198f1d2-3c4b-7a5e-8f60-123456789abc"


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

    @pytest.mark.parametrize("method", ["GET", "POST"])
    @pytest.mark.parametrize(
        ("task_id", "sent_ti_id", "stored_ti_id"),
        [
            pytest.param("nonexistent_task", None, None, id="unknown-task"),
            pytest.param(
                TASK_ID,
                UNKNOWN_TI_ID,
                None,
                id="unknown-sent-task-instance-id",
                marks=pytest.mark.skipif(
                    not AIRFLOW_V_3_4_PLUS, reason="Task instance ids are sent only on Airflow 3.4+"
                ),
            ),
            pytest.param(
                "nonexistent_task",
                None,
                UNKNOWN_TI_ID,
                id="unknown-stored-task-instance-id",
                marks=pytest.mark.skipif(
                    not AIRFLOW_V_3_4_PLUS, reason="Task instance ids are stored only on Airflow 3.4+"
                ),
            ),
        ],
    )
    @conf_vars({("api_auth", "jwt_secret"): "edge-log-test-secret"})
    def test_missing_ti_returns_404(
        self,
        method: str,
        task_id: str,
        sent_ti_id: str | None,
        stored_ti_id: str | None,
        session: Session,
    ) -> None:
        if stored_ti_id:
            session.add(
                EdgeJobModel(
                    dag_id=DAG_ID,
                    task_id=task_id,
                    run_id=RUN_ID,
                    map_index=-1,
                    try_number=1,
                    state="running",
                    queue="default",
                    concurrency_slots=1,
                    command="",
                    task_instance_id=stored_ti_id,
                )
            )
            session.commit()
        route_prefix = "logs/logfile_path" if method == "GET" else "logs/push"
        route = f"{route_prefix}/{DAG_ID}/{task_id}/{RUN_ID}/1/-1"
        params = {"task_instance_id": sent_ti_id} if method == "GET" and sent_ti_id else None
        body: dict[str, str] | None = None
        if method == "POST":
            body = {
                "log_chunk_data": "This is Lorem Ipsum log data",
                "log_chunk_time": "2024-11-24T00:00:00Z",
            }
            if sent_ti_id:
                body["task_instance_id"] = sent_ti_id
        app = FastAPI()
        app.include_router(logs_router, prefix="/edge_worker/v1")
        token = JWTGenerator(
            secret_key=conf.get("api_auth", "jwt_secret"),
            valid_for=60,
            audience="api",
        ).generate(extras={"method": route})
        jwt_validator.cache_clear()
        _logfile_path.cache_clear()
        try:
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.request(
                    method=method,
                    url=f"/edge_worker/v1/{route}",
                    headers={"Authorization": token},
                    params=params,
                    json=body,
                )
        finally:
            jwt_validator.cache_clear()
            _logfile_path.cache_clear()
            session.execute(delete(EdgeJobModel))
            session.commit()

        missing_ti_id = sent_ti_id or stored_ti_id
        expected_detail = (
            f"TaskInstance not found for dag_id={DAG_ID}, task_id={task_id}, run_id={RUN_ID}, map_index=-1"
        )
        if missing_ti_id:
            expected_detail += f", task_instance_id={missing_ti_id}"
        assert response.status_code == 404
        assert response.json() == {"detail": expected_detail}

    @pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Loop passes require Airflow 3.4+")
    def test_logfile_path_of_a_loop_pass_is_resolved_through_its_edge_job(self, dag_maker, session: Session):
        from airflow.models.dynamic_region import DynamicRegion
        from airflow.sdk import task_group
        from airflow.sdk.definitions._internal.loop import create_loop

        @task_group
        def body():
            EmptyOperator(task_id="work")

        with dag_maker("loop_dag", serialized=True):
            create_loop(body, max_iterations=3)
        dr = dag_maker.create_dagrun(run_id="loop_run")
        ti = next(ti for ti in dr.task_instances if ti.task_id == "body.work")
        region = DynamicRegion.get_or_create(
            dag_id=dr.dag_id, run_id=dr.run_id, node_id="body", session=session
        )
        session.flush()
        ti.region_id, ti.region_index = region.id, 2
        session.add(
            EdgeJobModel(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                map_index=ti.region_index,
                try_number=1,
                state="running",
                queue="default",
                concurrency_slots=1,
                command="",
                task_instance_id=str(ti.id),
            )
        )
        session.commit()
        session.refresh(ti)
        assert ti.region_index == 2

        path = logfile_path(dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, try_number=1, map_index=2)

        assert "pass=2" in path

    @pytest.fixture
    def mapped_loop_passes(self, dag_maker, session: Session):
        """Two edge jobs of a mapped task in a loop whose passes share every field of the job key."""
        from airflow.models.dynamic_region import DynamicRegion
        from airflow.models.taskinstance import TaskInstance
        from airflow.providers.standard.operators.python import PythonOperator
        from airflow.sdk import task_group
        from airflow.sdk.definitions._internal.loop import create_loop

        @task_group
        def body():
            PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1]])

        with dag_maker("mapped_loop_dag", serialized=True) as dag:
            create_loop(body, max_iterations=3)
        dr = dag_maker.create_dagrun(run_id="mapped_loop_run")
        first = next(ti for ti in dr.task_instances if ti.task_id == "body.mapped")
        loop_region = DynamicRegion.get_or_create(
            dag_id=dr.dag_id, run_id=dr.run_id, node_id="body", session=session
        )
        session.flush()
        tis = []
        for iteration in range(2):
            region = DynamicRegion.get_or_create(
                dag_id=dr.dag_id,
                run_id=dr.run_id,
                node_id="body.mapped",
                parent_region_id=loop_region.id,
                parent_region_index=iteration,
                session=session,
            )
            session.flush()
            ti = (
                first
                if iteration == 0
                else TaskInstance(dag.get_task("body.mapped"), first.dag_version_id, run_id=dr.run_id)
            )
            ti.region_id, ti.region_index = region.id, 0
            session.add(ti)
            session.add(
                EdgeJobModel(
                    dag_id=ti.dag_id,
                    task_id=ti.task_id,
                    run_id=ti.run_id,
                    map_index=0,
                    try_number=1,
                    state="running",
                    queue="default",
                    concurrency_slots=1,
                    command="",
                    task_instance_id=str(ti.id),
                )
            )
            tis.append(ti)
        session.commit()
        return tis

    @pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Loop passes require Airflow 3.4+")
    def test_logfile_path_with_a_task_instance_id_tells_loop_passes_apart(self, mapped_loop_passes):
        paths = [
            logfile_path(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                try_number=1,
                map_index=0,
                task_instance_id=ti.id,
            )
            for ti in mapped_loop_passes
        ]

        assert "pass=0" in paths[0]
        assert "pass=1" in paths[1]

    @pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Loop passes require Airflow 3.4+")
    def test_push_logs_with_a_task_instance_id_writes_to_that_loop_pass(
        self, mapped_loop_passes, session: Session, tmp_path: Path
    ):
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            for ti, text in zip(mapped_loop_passes, ["pass zero", "pass one"]):
                push_logs(
                    dag_id=ti.dag_id,
                    task_id=ti.task_id,
                    run_id=ti.run_id,
                    try_number=1,
                    map_index=0,
                    body=PushLogsBody(
                        log_chunk_data=text, log_chunk_time=timezone.utcnow(), task_instance_id=ti.id
                    ),
                    session=session,
                )

        assert sorted(path.read_text() for path in tmp_path.rglob("*.log")) == ["pass one", "pass zero"]

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

    @pytest.fixture
    def make_callback_job(self, session: Session):
        def make(command: dict) -> EdgeJobModel:
            job = EdgeJobModel(
                dag_id=EXECUTE_CALLBACK_TAG,
                task_id=CALLBACK_ID,
                run_id=build_callback_run_id(CALLBACK_ID),
                map_index=CALLBACK_JOB_MAP_INDEX,
                try_number=CALLBACK_JOB_TRY_NUMBER,
                state=TaskInstanceState.RUNNING,
                queue="default",
                concurrency_slots=1,
                command=json.dumps(command),
            )
            session.add(job)
            session.commit()
            return job

        _logfile_path.cache_clear()
        yield make
        session.execute(delete(EdgeJobModel))
        session.commit()
        _logfile_path.cache_clear()

    def test_logfile_path_for_callback_job(self, make_callback_job):
        job = make_callback_job({"type": EXECUTE_CALLBACK_TAG, "log_path": CALLBACK_LOG_PATH})

        path = logfile_path(
            dag_id=job.dag_id,
            task_id=job.task_id,
            run_id=job.run_id,
            try_number=job.try_number,
            map_index=job.map_index,
        )

        assert path == CALLBACK_LOG_PATH

    def test_push_logs_for_callback_job(self, make_callback_job, session: Session, tmp_path: Path):
        job = make_callback_job({"type": EXECUTE_CALLBACK_TAG, "log_path": CALLBACK_LOG_PATH})
        log_data = PushLogsBody(log_chunk_data="callback log line", log_chunk_time=timezone.utcnow())
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}), create_session() as api_session:
            push_logs(
                dag_id=job.dag_id,
                task_id=job.task_id,
                run_id=job.run_id,
                try_number=job.try_number,
                map_index=job.map_index,
                body=log_data,
                session=api_session,
            )
        logs: Sequence[EdgeLogsModel] = session.scalars(select(EdgeLogsModel)).all()
        assert [(log.dag_id, log.task_id, log.run_id, log.log_chunk_data) for log in logs] == [
            (EXECUTE_CALLBACK_TAG, CALLBACK_ID, build_callback_run_id(CALLBACK_ID), "callback log line")
        ]
        assert (tmp_path / CALLBACK_LOG_PATH).read_text() == "callback log line"

    @pytest.mark.parametrize(
        "stored_command",
        [
            pytest.param(None, id="no-edge-job"),
            pytest.param({"type": EXECUTE_CALLBACK_TAG}, id="no-log-path"),
        ],
    )
    def test_push_logs_for_callback_job_without_log_path(
        self, make_callback_job, stored_command: dict | None, session: Session
    ):
        if stored_command is not None:
            make_callback_job(stored_command)
        log_data = PushLogsBody(log_chunk_data="callback log line", log_chunk_time=timezone.utcnow())
        with pytest.raises(HTTPException) as exc_info, create_session() as api_session:
            push_logs(
                dag_id=EXECUTE_CALLBACK_TAG,
                task_id=CALLBACK_ID,
                run_id=build_callback_run_id(CALLBACK_ID),
                try_number=CALLBACK_JOB_TRY_NUMBER,
                map_index=CALLBACK_JOB_MAP_INDEX,
                body=log_data,
                session=api_session,
            )
        assert exc_info.value.status_code == 404
        assert session.scalars(select(EdgeLogsModel)).all() == []
