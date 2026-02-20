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

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.deadline import Deadline
from airflow.models.deadline_alert import DeadlineAlert
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_deadline,
    clear_db_deadline_alert,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

DAG_ID = "test_deadlines_dag"
RUN_ID = "run_1"
ALERT_NAME = "SLA Breach Alert"
ALERT_DESCRIPTION = "Fires when SLA is breached"

_CALLBACK_PATH = "tests.unit.api_fastapi.core_api.routes.ui.test_deadlines._noop_callback"


async def _noop_callback(**kwargs):
    """No-op callback used to satisfy Deadline creation in tests."""


def _make_callback() -> AsyncCallback:
    return AsyncCallback(_CALLBACK_PATH)


def _clean_db():
    clear_db_deadline()
    clear_db_deadline_alert()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


@pytest.fixture(autouse=True)
@provide_session
def setup(dag_maker, session=None):
    _clean_db()

    with dag_maker(DAG_ID, serialized=True, session=session):
        EmptyOperator(task_id="task")

    logical_date = timezone.datetime(2024, 11, 30)
    data_interval = dag_maker.dag.timetable.infer_manual_data_interval(run_after=logical_date)
    dag_maker.create_dagrun(
        run_id=RUN_ID,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=logical_date,
        data_interval=data_interval,
        triggered_by=DagRunTriggeredByType.TEST,
    )
    session.commit()
    yield
    _clean_db()


class TestGetDagRunDeadlines:
    """Tests for GET /deadlines/{dag_id}/{run_id}."""

    # ------------------------------------------------------------------
    # 200 – happy paths
    # ------------------------------------------------------------------

    def test_no_deadlines_returns_empty_list(self, test_client, session):
        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_ID}")
        assert response.status_code == 200
        assert response.json() == []

    def test_single_deadline_without_alert(self, test_client, session):
        from airflow.models.dagrun import DagRun

        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID))
        deadline_time = timezone.datetime(2025, 1, 1, 12, 0, 0)
        deadline = Deadline(
            deadline_time=deadline_time,
            callback=_make_callback(),
            dagrun_id=dag_run.id,
            deadline_alert_id=None,
        )
        session.add(deadline)
        session.commit()

        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_ID}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["deadline_time"] == "2025-01-01T12:00:00Z"
        assert data[0]["missed"] is False
        assert data[0]["alert_name"] is None
        assert data[0]["alert_description"] is None
        assert "id" in data[0]
        assert "created_at" in data[0]

    def test_missed_deadline_is_reflected(self, test_client, session):
        from airflow.models.dagrun import DagRun

        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID))
        deadline = Deadline(
            deadline_time=timezone.datetime(2024, 12, 1),
            callback=_make_callback(),
            dagrun_id=dag_run.id,
            deadline_alert_id=None,
        )
        deadline.missed = True
        session.add(deadline)
        session.commit()

        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_ID}")
        assert response.status_code == 200
        assert response.json()[0]["missed"] is True

    def test_deadline_with_alert_name_and_description(self, test_client, session):
        from airflow.models.dagrun import DagRun
        from airflow.sdk.definitions.deadline import DeadlineReference

        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID))

        serialized_dag = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == DAG_ID))
        alert = DeadlineAlert(
            serialized_dag_id=serialized_dag.id,
            name=ALERT_NAME,
            description=ALERT_DESCRIPTION,
            reference=DeadlineReference.DAGRUN_QUEUED_AT.serialize_reference(),
            interval=3600.0,
            callback_def={"path": _CALLBACK_PATH},
        )
        session.add(alert)
        session.flush()

        deadline = Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
            callback=_make_callback(),
            dagrun_id=dag_run.id,
            deadline_alert_id=alert.id,
        )
        session.add(deadline)
        session.commit()

        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_ID}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["alert_name"] == ALERT_NAME
        assert data[0]["alert_description"] == ALERT_DESCRIPTION

    def test_deadlines_ordered_by_deadline_time_ascending(self, test_client, session):
        from airflow.models.dagrun import DagRun

        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID))

        times = [
            timezone.datetime(2025, 3, 1),
            timezone.datetime(2025, 1, 1),
            timezone.datetime(2025, 2, 1),
        ]
        for t in times:
            session.add(
                Deadline(
                    deadline_time=t,
                    callback=_make_callback(),
                    dagrun_id=dag_run.id,
                    deadline_alert_id=None,
                )
            )
        session.commit()

        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_ID}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        returned_times = [d["deadline_time"] for d in data]
        assert returned_times == sorted(returned_times)

    def test_only_returns_deadlines_for_requested_run(self, test_client, session, dag_maker):
        from airflow.models.dagrun import DagRun

        # Create a second run in the same DAG
        logical_date_2 = timezone.datetime(2024, 12, 1)
        data_interval_2 = dag_maker.dag.timetable.infer_manual_data_interval(run_after=logical_date_2)
        run_2 = dag_maker.create_dagrun(
            run_id="run_2",
            state=DagRunState.SUCCESS,
            run_type=DagRunType.SCHEDULED,
            logical_date=logical_date_2,
            data_interval=data_interval_2,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        session.commit()

        dag_run_1 = session.scalar(select(DagRun).where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID))

        # Add one deadline to run_1 and one to run_2
        session.add(
            Deadline(
                deadline_time=timezone.datetime(2025, 1, 1),
                callback=_make_callback(),
                dagrun_id=dag_run_1.id,
                deadline_alert_id=None,
            )
        )
        session.add(
            Deadline(
                deadline_time=timezone.datetime(2025, 2, 1),
                callback=_make_callback(),
                dagrun_id=run_2.id,
                deadline_alert_id=None,
            )
        )
        session.commit()

        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_ID}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["deadline_time"] == "2025-01-01T00:00:00Z"

    # ------------------------------------------------------------------
    # 404
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("dag_id", "run_id"),
        [
            pytest.param("nonexistent_dag", RUN_ID, id="wrong_dag_id"),
            pytest.param(DAG_ID, "nonexistent_run", id="wrong_run_id"),
            pytest.param("nonexistent_dag", "nonexistent_run", id="both_wrong"),
        ],
    )
    def test_should_response_404(self, test_client, dag_id, run_id):
        response = test_client.get(f"/deadlines/{dag_id}/{run_id}")
        assert response.status_code == 404

    # ------------------------------------------------------------------
    # 401 / 403
    # ------------------------------------------------------------------

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/deadlines/{DAG_ID}/{RUN_ID}")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/deadlines/{DAG_ID}/{RUN_ID}")
        assert response.status_code == 403
