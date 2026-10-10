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
from sqlalchemy import delete, select

from airflow.providers.common.compat.sdk import timezone
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
from airflow.providers.edge3.worker_api.datamodels import PushLogsBody
from airflow.providers.edge3.worker_api.routes.logs import logfile_path, push_logs
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

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
