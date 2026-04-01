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

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_deadline,
    clear_db_deadline_alert,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

DAG_ID = "test_deadlines_dag"
DAG_ID_2 = "test_deadlines_dag_2"

# Each run represents a different deadline scenario tested below.
RUN_EMPTY = "run_empty"  # no deadlines
RUN_SINGLE = "run_single"  # 1 deadline, not missed, no alert
RUN_MISSED = "run_missed"  # 1 deadline, missed=True
RUN_ALERT = "run_alert"  # 1 deadline linked to a DeadlineAlert
RUN_MULTI = "run_multi"  # 3 deadlines added out-of-order (ordering test)
RUN_OTHER = "run_other"  # has 1 deadline; used to verify per-run isolation
RUN_DAG2 = "run_dag2"  # belongs to DAG_ID_2, which is used for cross-dag filter tests

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

    # Second DAG with two deadlines (for cross-DAG filter verification)
    with dag_maker(DAG_ID_2, serialized=True, session=session):
        EmptyOperator(task_id="task")

    run_dag2 = dag_maker.create_dagrun(
        run_id=RUN_DAG2,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2024, 11, 7),
        triggered_by=DagRunTriggeredByType.TEST,
    )
    # Two non-missed deadlines in Jan 2025 (avoids disturbing deadline_time filter tests)
    for dl_time in [timezone.datetime(2025, 1, 10), timezone.datetime(2025, 1, 20)]:
        session.add(
            Deadline(
                deadline_time=dl_time,
                callback=_cb(),
                dagrun_id=run_dag2.id,
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
    """Tests for GET /dags/{dag_id}/dagRuns/{dag_run_id}/deadlines."""

    def test_no_deadlines_returns_empty_list(self, test_client):
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_EMPTY}/deadlines")
        assert response.status_code == 200
        assert response.json() == {"deadlines": [], "total_entries": 0}

    def test_single_deadline_without_alert(self, test_client):
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_SINGLE}/deadlines")
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 1
        deadline1 = data["deadlines"][0]
        assert deadline1["deadline_time"] == "2025-01-01T12:00:00Z"
        assert deadline1["missed"] is False
        assert deadline1["alert_name"] is None
        assert deadline1["alert_description"] is None
        assert deadline1["dag_id"] == DAG_ID
        assert deadline1["dag_run_id"] == RUN_SINGLE
        assert "id" in deadline1
        assert "created_at" in deadline1

    def test_missed_deadline_is_reflected(self, test_client):
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_MISSED}/deadlines")
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 1
        assert data["deadlines"][0]["missed"] is True

    def test_deadline_with_alert_name_and_description(self, test_client):
        with assert_queries_count(4):
            response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_ALERT}/deadlines")
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 1
        assert data["deadlines"][0]["alert_name"] == ALERT_NAME
        assert data["deadlines"][0]["alert_description"] == ALERT_DESCRIPTION

    def test_deadlines_ordered_by_deadline_time_ascending(self, test_client):
        with assert_queries_count(4):
            response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_MULTI}/deadlines")
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 3
        returned_times = [d["deadline_time"] for d in data["deadlines"]]
        assert returned_times == sorted(returned_times)

    @pytest.mark.parametrize(
        "order_by",
        ["deadline_time", "id", "created_at", "alert_name"],
        ids=["deadline_time", "id", "created_at", "alert_name"],
    )
    def test_should_response_200_order_by(self, test_client, order_by):
        url = f"/dags/{DAG_ID}/dagRuns/{RUN_MULTI}/deadlines"
        with assert_queries_count(8):
            response_asc = test_client.get(url, params={"order_by": order_by})
            response_desc = test_client.get(url, params={"order_by": f"-{order_by}"})
        assert response_asc.status_code == 200
        assert response_desc.status_code == 200
        ids_asc = [d["id"] for d in response_asc.json()["deadlines"]]
        ids_desc = [d["id"] for d in response_desc.json()["deadlines"]]
        assert ids_desc == list(reversed(ids_asc))

    def test_only_returns_deadlines_for_requested_run(self, test_client):
        """Deadlines belonging to a different run must not appear in the response."""
        # RUN_EMPTY has no deadlines; RUN_OTHER has one — querying RUN_EMPTY must return [].
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_EMPTY}/deadlines")
        assert response.status_code == 200
        assert response.json() == {"deadlines": [], "total_entries": 0}

        # And querying RUN_OTHER returns only its own deadline.
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_OTHER}/deadlines")
        assert response.status_code == 200
        assert response.json()["total_entries"] == 1

    @pytest.mark.parametrize(
        ("dag_id", "run_id"),
        [
            pytest.param("nonexistent_dag", RUN_EMPTY, id="wrong_dag_id"),
            pytest.param(DAG_ID, "nonexistent_run", id="wrong_run_id"),
            pytest.param("nonexistent_dag", "nonexistent_run", id="both_wrong"),
        ],
    )
    def test_should_response_404(self, test_client, dag_id, run_id):
        response = test_client.get(f"/dags/{dag_id}/dagRuns/{run_id}/deadlines")
        assert response.status_code == 404

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_EMPTY}/deadlines")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_EMPTY}/deadlines")
        assert response.status_code == 403


class TestGetDeadlines:
    """Tests for GET /dags/{dag_id}/dagRuns/{dag_run_id}/deadlines with ~ wildcards."""

    def test_returns_all_deadlines(self, test_client):
        """All deadlines across all DAGs and runs are returned"""
        response = test_client.get("/dags/~/dagRuns/~/deadlines")
        assert response.status_code == 200
        data = response.json()
        # DAG_ID: 7 deadlines (run_single, run_missed, run_alert, 3×run_multi, run_other)
        # DAG_ID_2: 2 deadlines (run_dag2)
        assert data["total_entries"] == 9
        assert len(data["deadlines"]) == 9
        assert {dl["dag_id"] for dl in data["deadlines"]} == {DAG_ID, DAG_ID_2}
        for dl in data["deadlines"]:
            assert "id" in dl
            assert "dag_run_id" in dl
            assert "deadline_time" in dl
            assert "missed" in dl

    def test_filter_by_dag_id(self, test_client):
        """Specifying a dag_id returns only that DAG's deadlines, not other DAGs'."""
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/~/deadlines")
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 7
        for dl in data["deadlines"]:
            assert dl["dag_id"] == DAG_ID

        # Cross-check: the second DAG's deadlines are isolated
        dag2_response = test_client.get(f"/dags/{DAG_ID_2}/dagRuns/~/deadlines")
        assert dag2_response.json()["total_entries"] == 2
        for dl in dag2_response.json()["deadlines"]:
            assert dl["dag_id"] == DAG_ID_2

    def test_filter_missed_true(self, test_client):
        """Only missed deadlines are returned when missed=true."""
        response = test_client.get("/dags/~/dagRuns/~/deadlines", params={"missed": "true"})
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 1
        assert data["deadlines"][0]["missed"] is True

    def test_filter_missed_false(self, test_client):
        """Only non-missed (pending) deadlines are returned when missed=false."""
        response = test_client.get("/dags/~/dagRuns/~/deadlines", params={"missed": "false"})
        assert response.status_code == 200
        data = response.json()
        # DAG_ID non-missed: 6; DAG_ID_2 non-missed: 2
        assert data["total_entries"] == 8
        for dl in data["deadlines"]:
            assert dl["missed"] is False

    def test_filter_deadline_time_gte(self, test_client):
        """deadline_time_gte filters out deadlines before the given time."""
        response = test_client.get(
            "/dags/~/dagRuns/~/deadlines", params={"deadline_time_gte": "2025-02-01T00:00:00Z"}
        )
        assert response.status_code == 200
        data = response.json()
        # Deadline times at/after 2025-02-01: run_multi 2025-02-01, 2025-03-01, run_other 2025-06-01 = 3
        assert data["total_entries"] == 3

    def test_filter_deadline_time_lte(self, test_client):
        """deadline_time_lte filters out deadlines after the given time."""
        response = test_client.get(
            "/dags/~/dagRuns/~/deadlines", params={"deadline_time_lte": "2024-12-31T23:59:59Z"}
        )
        assert response.status_code == 200
        data = response.json()
        # Only run_missed's 2024-12-01 is before 2025
        assert data["total_entries"] == 1

    def test_filter_last_updated_at_gte(self, test_client):
        """last_updated_at_gte filters deadlines updated before the given time."""
        response = test_client.get(
            "/dags/~/dagRuns/~/deadlines", params={"last_updated_at_gte": "2099-01-01T00:00:00Z"}
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == 0

    def test_filter_last_updated_at_lte(self, test_client):
        """last_updated_at_lte filters deadlines updated after the given time."""
        response = test_client.get(
            "/dags/~/dagRuns/~/deadlines", params={"last_updated_at_lte": "2099-01-01T00:00:00Z"}
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == 9

    def test_ordered_by_deadline_time_ascending_by_default(self, test_client):
        """Deadlines are ordered by deadline_time ascending by default."""
        response = test_client.get("/dags/~/dagRuns/~/deadlines")
        assert response.status_code == 200
        times = [dl["deadline_time"] for dl in response.json()["deadlines"]]
        assert times == sorted(times)

    def test_pagination(self, test_client):
        """limit and offset work correctly."""
        all_resp = test_client.get("/dags/~/dagRuns/~/deadlines")
        all_ids = [dl["id"] for dl in all_resp.json()["deadlines"]]

        page1 = test_client.get("/dags/~/dagRuns/~/deadlines", params={"limit": 3, "offset": 0}).json()[
            "deadlines"
        ]
        page2 = test_client.get("/dags/~/dagRuns/~/deadlines", params={"limit": 3, "offset": 3}).json()[
            "deadlines"
        ]

        assert [dl["id"] for dl in page1] == all_ids[:3]
        assert [dl["id"] for dl in page2] == all_ids[3:6]

    def test_alert_name_present_when_linked(self, test_client):
        """Deadlines linked to a DeadlineAlert include alert_name and alert_description."""
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/~/deadlines")
        assert response.status_code == 200
        deadlines = response.json()["deadlines"]
        alerts = [dl for dl in deadlines if dl["alert_name"] is not None]
        assert len(alerts) == 1
        assert alerts[0]["alert_name"] == ALERT_NAME
        assert alerts[0]["alert_description"] == ALERT_DESCRIPTION

    def test_filter_nonexistent_dag_returns_empty(self, test_client):
        """Filtering by a dag_id that doesn't exist returns an empty list."""
        response = test_client.get("/dags/nonexistent_dag/dagRuns/~/deadlines")
        assert response.status_code == 200
        assert response.json() == {"deadlines": [], "total_entries": 0}

    def test_dag_run_id_without_dag_id_returns_400(self, test_client):
        """Specifying a concrete dag_run_id with dag_id=~ is a bad request."""
        response = test_client.get(f"/dags/~/dagRuns/{RUN_SINGLE}/deadlines")
        assert response.status_code == 400

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags/~/dagRuns/~/deadlines")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/~/dagRuns/~/deadlines")
        assert response.status_code == 403


class TestGetDagDeadlineAlerts:
    """Tests for GET /dags/{dag_id}/deadlineAlerts."""

    def test_returns_deadline_alerts_for_dag(self, test_client):
        """Returns all deadline alerts defined on the DAG."""
        response = test_client.get(f"/dags/{DAG_ID}/deadlineAlerts")
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 1
        assert len(data["deadline_alerts"]) == 1

    def test_alert_response_fields(self, test_client):
        """Alert response includes expected fields with correct values."""
        response = test_client.get(f"/dags/{DAG_ID}/deadlineAlerts")
        assert response.status_code == 200
        alert = response.json()["deadline_alerts"][0]
        assert alert["name"] == ALERT_NAME
        assert alert["description"] == ALERT_DESCRIPTION
        assert alert["interval"] == 3600.0
        assert alert["reference_type"] == "DagRunQueuedAtDeadline"
        assert "id" in alert
        assert "created_at" in alert

    def test_dag_with_no_alerts_returns_empty_list(self, test_client, dag_maker, session):
        """A DAG with no deadline alerts returns an empty list."""
        with dag_maker("dag_no_alerts", serialized=True, session=session):
            EmptyOperator(task_id="task")
        dag_maker.sync_dagbag_to_db()
        session.commit()

        response = test_client.get("/dags/dag_no_alerts/deadlineAlerts")
        assert response.status_code == 200
        assert response.json() == {"deadline_alerts": [], "total_entries": 0}

    def test_should_response_404_for_nonexistent_dag(self, test_client):
        response = test_client.get("/dags/nonexistent_dag/deadlineAlerts")
        assert response.status_code == 404

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/dags/{DAG_ID}/deadlineAlerts")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/dags/{DAG_ID}/deadlineAlerts")
        assert response.status_code == 403
