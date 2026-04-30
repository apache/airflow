#
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

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from airflow._shared.timezones import timezone
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.task_state import TaskStateModel

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
TASK_ID = "test_task"
RUN_ID = "scheduled__2026-04-24"


@pytest.fixture(autouse=True)
def clean_tables():
    clear_db_dags()
    clear_db_runs()
    yield
    clear_db_dags()
    clear_db_runs()


@pytest.fixture
def dag_run(session: Session) -> DagRun:
    run = DagRun(
        dag_id=DAG_ID,
        run_id=RUN_ID,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2026, 4, 24),
        run_after=timezone.datetime(2026, 4, 24),
    )
    session.add(run)
    session.flush()
    return run


class TestTaskStateModel:
    def test_insert_and_read(self, session: Session, dag_run: DagRun):
        row = TaskStateModel(
            dag_run_id=dag_run.id,
            task_id=TASK_ID,
            map_index=-1,
            key="remote_job_id",
            dag_id=DAG_ID,
            run_id=RUN_ID,
            value="application_1234",
        )
        session.add(row)
        session.flush()

        result = session.scalar(
            select(TaskStateModel).where(
                TaskStateModel.dag_run_id == dag_run.id,
                TaskStateModel.task_id == TASK_ID,
                TaskStateModel.map_index == -1,
                TaskStateModel.key == "remote_job_id",
            )
        )
        assert result is not None
        assert result.value == "application_1234"
        assert result.dag_id == DAG_ID
        assert result.run_id == RUN_ID

    def test_duplicate_pk_raises(self, session: Session, dag_run: DagRun):
        """Inserting a second row with the same (dag_run_id, task_id, map_index, key) raises IntegrityError."""
        session.add(
            TaskStateModel(
                dag_run_id=dag_run.id,
                task_id=TASK_ID,
                map_index=-1,
                key="remote_job_id",
                dag_id=DAG_ID,
                run_id=RUN_ID,
                value="first",
            )
        )
        session.flush()

        session.add(
            TaskStateModel(
                dag_run_id=dag_run.id,
                task_id=TASK_ID,
                map_index=-1,
                key="remote_job_id",
                dag_id=DAG_ID,
                run_id=RUN_ID,
                value="duplicate",
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()

    def test_retries_share_state(self, session: Session, dag_run: DagRun):
        """Retries of the same task share the same row — updates are visible across retries."""
        row = TaskStateModel(
            dag_run_id=dag_run.id,
            task_id=TASK_ID,
            map_index=-1,
            key="remote_job_id",
            dag_id=DAG_ID,
            run_id=RUN_ID,
            value="application_try1",
        )
        session.add(row)
        session.flush()

        row.value = "application_try2"
        session.flush()

        result = session.scalar(
            select(TaskStateModel).where(
                TaskStateModel.dag_run_id == dag_run.id,
                TaskStateModel.task_id == TASK_ID,
                TaskStateModel.key == "remote_job_id",
            )
        )
        assert result is not None
        assert result.value == "application_try2"

    def test_map_index_isolation(self, session: Session, dag_run: DagRun):
        """Each mapped task instance gets its own independent state namespace."""
        for idx in (0, 1, 2):
            session.add(
                TaskStateModel(
                    dag_run_id=dag_run.id,
                    task_id=TASK_ID,
                    map_index=idx,
                    key="checkpoint",
                    dag_id=DAG_ID,
                    run_id=RUN_ID,
                    value=f"value_{idx}",
                )
            )
        session.flush()

        for idx in (0, 1, 2):
            result = session.scalar(
                select(TaskStateModel).where(
                    TaskStateModel.dag_run_id == dag_run.id,
                    TaskStateModel.task_id == TASK_ID,
                    TaskStateModel.map_index == idx,
                    TaskStateModel.key == "checkpoint",
                )
            )
            assert result is not None
            assert result.value == f"value_{idx}"

    def test_cascade_delete_on_dag_run(self, session: Session, dag_run: DagRun):
        """Deleting a DagRun cascades to its task_state rows — no orphans remain."""
        session.add(
            TaskStateModel(
                dag_run_id=dag_run.id,
                task_id=TASK_ID,
                map_index=-1,
                key="remote_job_id",
                dag_id=DAG_ID,
                run_id=RUN_ID,
                value="application_1234",
            )
        )
        session.flush()

        session.execute(delete(DagRun).where(DagRun.id == dag_run.id))
        session.flush()

        remaining = session.scalars(
            select(TaskStateModel).where(TaskStateModel.dag_run_id == dag_run.id)
        ).all()
        assert remaining == []

    def test_different_dag_runs_are_isolated(self, session: Session, dag_run: DagRun):
        """State written for one DagRun is not visible when querying a different DagRun."""
        run2 = DagRun(
            dag_id=DAG_ID,
            run_id="scheduled__2026-04-25",
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.datetime(2026, 4, 25),
            run_after=timezone.datetime(2026, 4, 25),
        )
        session.add(run2)
        session.flush()

        session.add(
            TaskStateModel(
                dag_run_id=dag_run.id,
                task_id=TASK_ID,
                map_index=-1,
                key="watermark",
                dag_id=DAG_ID,
                run_id=RUN_ID,
                value="run1_value",
            )
        )
        session.flush()

        result = session.scalar(
            select(TaskStateModel).where(
                TaskStateModel.dag_run_id == run2.id,
                TaskStateModel.key == "watermark",
            )
        )
        assert result is None
