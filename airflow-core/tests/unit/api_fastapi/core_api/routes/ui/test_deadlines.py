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
from airflow.sdk.definitions.deadline import DeadlineReference
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

# Each run represents a different deadline scenario tested below.
RUN_EMPTY = "run_empty"  # no deadlines
RUN_SINGLE = "run_single"  # 1 deadline, not missed, no alert
RUN_MISSED = "run_missed"  # 1 deadline, missed=True
RUN_ALERT = "run_alert"  # 1 deadline linked to a DeadlineAlert
RUN_MULTI = "run_multi"  # 3 deadlines added out-of-order (ordering test)
RUN_OTHER = "run_other"  # has 1 deadline; used to verify per-run isolation

ALERT_NAME = "SLA Breach Alert"
ALERT_DESCRIPTION = "Fires when SLA is breached"

_CALLBACK_PATH = "tests.unit.api_fastapi.core_api.routes.ui.test_deadlines._noop_callback"


async def _noop_callback(**kwargs):
    """No-op callback used to satisfy Deadline creation in tests."""


def _cb() -> AsyncCallback:
    return AsyncCallback(_CALLBACK_PATH)


@pytest.fixture(autouse=True)
def setup(dag_maker, session):
    clear_db_deadline()
    clear_db_deadline_alert()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    with dag_maker(DAG_ID, serialized=True, session=session):
        EmptyOperator(task_id="task")

    # ---- create runs -------------------------------------------------------
    dag_maker.create_dagrun(
        run_id=RUN_EMPTY,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2024, 11, 1),
        triggered_by=DagRunTriggeredByType.TEST,
    )

    run_single = dag_maker.create_dagrun(
        run_id=RUN_SINGLE,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2024, 11, 2),
        triggered_by=DagRunTriggeredByType.TEST,
    )

    run_missed = dag_maker.create_dagrun(
        run_id=RUN_MISSED,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2024, 11, 3),
        triggered_by=DagRunTriggeredByType.TEST,
    )

    run_alert = dag_maker.create_dagrun(
        run_id=RUN_ALERT,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2024, 11, 4),
        triggered_by=DagRunTriggeredByType.TEST,
    )

    run_multi = dag_maker.create_dagrun(
        run_id=RUN_MULTI,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2024, 11, 5),
        triggered_by=DagRunTriggeredByType.TEST,
    )

    run_other = dag_maker.create_dagrun(
        run_id=RUN_OTHER,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2024, 11, 6),
        triggered_by=DagRunTriggeredByType.TEST,
    )

    # ---- deadlines ---------------------------------------------------------

    # run_empty: intentionally no deadlines

    # run_single: one active, non-missed deadline with no alert
    session.add(
        Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
            callback=_cb(),
            dagrun_id=run_single.id,
            deadline_alert_id=None,
        )
    )

    # run_missed: one missed deadline
    missed_dl = Deadline(
        deadline_time=timezone.datetime(2024, 12, 1),
        callback=_cb(),
        dagrun_id=run_missed.id,
        deadline_alert_id=None,
    )
    missed_dl.missed = True
    session.add(missed_dl)

    # run_alert: one deadline linked to a DeadlineAlert
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
    session.add(
        Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
            callback=_cb(),
            dagrun_id=run_alert.id,
            deadline_alert_id=alert.id,
        )
    )

    # run_multi: three deadlines intentionally added in non-chronological order
    for dl_time in [
        timezone.datetime(2025, 3, 1),
        timezone.datetime(2025, 1, 1),
        timezone.datetime(2025, 2, 1),
    ]:
        session.add(
            Deadline(
                deadline_time=dl_time,
                callback=_cb(),
                dagrun_id=run_multi.id,
                deadline_alert_id=None,
            )
        )

    # run_other: one deadline (for isolation verification)
    session.add(
        Deadline(
            deadline_time=timezone.datetime(2025, 6, 1),
            callback=_cb(),
            dagrun_id=run_other.id,
            deadline_alert_id=None,
        )
    )

    dag_maker.sync_dagbag_to_db()
    session.commit()
    yield
    clear_db_deadline()
    clear_db_deadline_alert()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


class TestGetDagRunDeadlines:
    """Tests for GET /deadlines/{dag_id}/{run_id}."""

    # ------------------------------------------------------------------
    # 200 – happy paths
    # ------------------------------------------------------------------

    def test_no_deadlines_returns_empty_list(self, test_client):
        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_EMPTY}")
        assert response.status_code == 200
        assert response.json() == []

    def test_single_deadline_without_alert(self, test_client):
        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_SINGLE}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["deadline_time"] == "2025-01-01T12:00:00Z"
        assert data[0]["missed"] is False
        assert data[0]["alert_name"] is None
        assert data[0]["alert_description"] is None
        assert "id" in data[0]
        assert "created_at" in data[0]

    def test_missed_deadline_is_reflected(self, test_client):
        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_MISSED}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["missed"] is True

    def test_deadline_with_alert_name_and_description(self, test_client):
        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_ALERT}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["alert_name"] == ALERT_NAME
        assert data[0]["alert_description"] == ALERT_DESCRIPTION

    def test_deadlines_ordered_by_deadline_time_ascending(self, test_client):
        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_MULTI}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        returned_times = [d["deadline_time"] for d in data]
        assert returned_times == sorted(returned_times)

    def test_only_returns_deadlines_for_requested_run(self, test_client):
        """Deadlines belonging to a different run must not appear in the response."""
        # RUN_EMPTY has no deadlines; RUN_OTHER has one — querying RUN_EMPTY must return [].
        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_EMPTY}")
        assert response.status_code == 200
        assert response.json() == []

        # And querying RUN_OTHER returns only its own deadline.
        response = test_client.get(f"/deadlines/{DAG_ID}/{RUN_OTHER}")
        assert response.status_code == 200
        assert len(response.json()) == 1

    # ------------------------------------------------------------------
    # 404
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("dag_id", "run_id"),
        [
            pytest.param("nonexistent_dag", RUN_EMPTY, id="wrong_dag_id"),
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
        response = unauthenticated_test_client.get(f"/deadlines/{DAG_ID}/{RUN_EMPTY}")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/deadlines/{DAG_ID}/{RUN_EMPTY}")
        assert response.status_code == 403
