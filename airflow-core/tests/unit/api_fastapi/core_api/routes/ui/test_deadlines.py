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

from datetime import timedelta

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.deadline import Deadline
from airflow.models.deadline_alert import DeadlineAlert
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.sdk.definitions.deadline import DeadlineReference, VariableInterval
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
        assert deadline1["alert_id"] is None
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

    def test_deadline_with_alert_name(self, test_client, session):
        alert = session.scalar(select(DeadlineAlert).where(DeadlineAlert.name == ALERT_NAME))
        with assert_queries_count(4):
            response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_ALERT}/deadlines")
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 1
        dl = data["deadlines"][0]
        assert dl["alert_name"] == ALERT_NAME
        assert dl["alert_id"] == str(alert.id)

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

    def test_alert_name_present_when_linked(self, test_client, session):
        """Deadlines linked to a DeadlineAlert include both alert_name and alert_id."""
        alert = session.scalar(select(DeadlineAlert).where(DeadlineAlert.name == ALERT_NAME))
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/~/deadlines")
        assert response.status_code == 200
        deadlines = response.json()["deadlines"]
        linked = [dl for dl in deadlines if dl["alert_name"] is not None]
        assert len(linked) == 1
        assert linked[0]["alert_name"] == ALERT_NAME
        assert linked[0]["alert_id"] == str(alert.id)
        # Unlinked deadlines must have null alert_id
        unlinked = [dl for dl in deadlines if dl["alert_name"] is None]
        assert all(dl["alert_id"] is None for dl in unlinked)

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

    @pytest.mark.parametrize("order_by", ["id", "created_at", "name", "-id", "-name"])
    def test_valid_order_by_keys_succeed(self, test_client, order_by):
        response = test_client.get(f"/dags/{DAG_ID}/deadlineAlerts", params={"order_by": order_by})
        assert response.status_code == 200

    @pytest.mark.parametrize("order_by", ["interval", "-interval"])
    def test_order_by_interval_is_rejected(self, test_client, order_by):
        """``interval`` is a serialized-JSON column (timedelta/VariableInterval dict), so DB-level
        ordering by it is meaningless (sorts by JSON structure, not duration). It must NOT be an
        allowed sort key — the endpoint should reject it with a 400 rather than silently returning
        an arbitrarily-ordered list.
        """
        response = test_client.get(f"/dags/{DAG_ID}/deadlineAlerts", params={"order_by": order_by})
        assert response.status_code == 400
        assert "interval" in response.json()["detail"]

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/dags/{DAG_ID}/deadlineAlerts")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/dags/{DAG_ID}/deadlineAlerts")
        assert response.status_code == 403


class TestDeadlineAlertsIntervalSerialization:
    """Regression tests for the ``interval`` column shape on the deadlineAlerts endpoint.

    ``DeadlineAlert.interval`` is a JSON column that, in production, holds the
    Airflow-*serialized* interval — ``encode_deadline_alert`` stores
    ``serialize(self.interval)``, not a plain number. A fixed ``timedelta`` becomes
    ``{"__classname__": "datetime.timedelta", "__version__": 2, "__data__": <seconds>}``
    and a dynamic ``VariableInterval`` becomes
    ``{"__classname__": ".../VariableInterval", "__data__": {"key": ...}}``.

    The response model originally typed ``interval`` as a bare ``float``, so Pydantic
    raised on that dict and the endpoint returned 500 — which broke the run-page
    ``DeadlineStatus`` badge. The existing fixtures masked this by seeding a bare float
    (``interval=3600.0``), a value the real creation path never produces. These tests
    seed the realistic serialized forms.
    """

    @pytest.fixture
    def dag_with_serialized_intervals(self, dag_maker, session):
        from airflow.sdk.serde import serialize

        dag_id = "dag_serialized_interval"
        with dag_maker(dag_id, serialized=True, session=session):
            EmptyOperator(task_id="task")
        dag_maker.sync_dagbag_to_db()
        session.commit()

        serialized_dag = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id))
        # Fixed interval: stored as the serialized timedelta dict, exactly as
        # encode_deadline_alert would persist it.
        session.add(
            DeadlineAlert(
                serialized_dag_id=serialized_dag.id,
                name="fixed_interval_alert",
                reference=DeadlineReference.DAGRUN_QUEUED_AT.serialize_reference(),
                interval=serialize(timedelta(seconds=300)),
                callback_def={"path": _CALLBACK_PATH},
            )
        )
        # Dynamic interval: a VariableInterval serializes to a dict with no fixed
        # seconds — the value is only resolved at scheduler evaluation time.
        session.add(
            DeadlineAlert(
                serialized_dag_id=serialized_dag.id,
                name="dynamic_interval_alert",
                reference=DeadlineReference.DAGRUN_QUEUED_AT.serialize_reference(),
                interval=serialize(VariableInterval("deadline_seconds")),
                callback_def={"path": _CALLBACK_PATH},
            )
        )
        session.commit()
        return dag_id

    def test_serialized_timedelta_interval_does_not_500(self, test_client, dag_with_serialized_intervals):
        """A fixed timedelta interval is coerced to its seconds value (not a 500)."""
        response = test_client.get(f"/dags/{dag_with_serialized_intervals}/deadlineAlerts")
        assert response.status_code == 200
        alerts = {a["name"]: a for a in response.json()["deadline_alerts"]}
        assert alerts["fixed_interval_alert"]["interval"] == 300.0

    def test_dynamic_variable_interval_serializes_as_null(self, test_client, dag_with_serialized_intervals):
        """A dynamic VariableInterval has no fixed seconds, so interval is null (not a 500)."""
        response = test_client.get(f"/dags/{dag_with_serialized_intervals}/deadlineAlerts")
        assert response.status_code == 200
        alerts = {a["name"]: a for a in response.json()["deadline_alerts"]}
        assert alerts["dynamic_interval_alert"]["interval"] is None

    @pytest.mark.parametrize(
        ("stored_interval", "expected"),
        [
            pytest.param({"__classname__": "datetime.timedelta", "__data__": 300.0}, 300.0, id="timedelta"),
            pytest.param(3600, 3600.0, id="legacy_bare_int"),
            pytest.param(3600.0, 3600.0, id="legacy_bare_float"),
            pytest.param({"__classname__": "x", "__data__": {"key": "k"}}, None, id="variable_interval"),
            # Malformed / corrupted shapes must degrade to null, never 500.
            pytest.param(
                {"__classname__": "datetime.timedelta", "__data__": "oops"}, None, id="data_not_number"
            ),
            pytest.param({"__classname__": "datetime.timedelta"}, None, id="no_data_key"),
            pytest.param({}, None, id="empty_dict"),
            pytest.param({"__classname__": "datetime.timedelta", "__data__": None}, None, id="data_none"),
        ],
    )
    def test_interval_validator_never_500s(self, dag_maker, session, stored_interval, expected):
        """The interval coercion degrades gracefully for every stored shape, including
        corrupted rows and legacy bare numbers — the endpoint must not 500."""
        dag_id = f"dag_interval_shape_{abs(hash(str(stored_interval)))}"
        with dag_maker(dag_id, serialized=True, session=session):
            EmptyOperator(task_id="task")
        dag_maker.sync_dagbag_to_db()
        session.commit()

        serialized_dag = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id))
        session.add(
            DeadlineAlert(
                serialized_dag_id=serialized_dag.id,
                name="shape_alert",
                reference=DeadlineReference.DAGRUN_QUEUED_AT.serialize_reference(),
                interval=stored_interval,
                callback_def={"path": _CALLBACK_PATH},
            )
        )
        session.commit()

        from airflow.api_fastapi.core_api.datamodels.ui.deadline import DeadlineAlertResponse

        alert = session.scalar(select(DeadlineAlert).where(DeadlineAlert.name == "shape_alert"))
        response = DeadlineAlertResponse.model_validate(alert)
        # The actual 500 trigger is JSON serialization, so exercise that too.
        response.model_dump_json()
        assert response.interval == expected
