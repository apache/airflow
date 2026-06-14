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

import pendulum
import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

DAG_ID_MORNING = "test_schedule_overview_dag_morning"
DAG_ID_EVENING = "test_schedule_overview_dag_evening"
DAG_ID_NO_RUNS = "test_schedule_overview_dag_no_runs"
TASK_ID = "task"


def _utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> pendulum.DateTime:
    return pendulum.DateTime(year, month, day, hour, minute, tzinfo=pendulum.UTC)


@pytest.fixture(autouse=True)
@provide_session
def setup(dag_maker, session=None) -> None:
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST}

    # Dag that consistently runs in the morning UTC.
    with dag_maker(dag_id=DAG_ID_MORNING, serialized=True, session=session):
        EmptyOperator(task_id=TASK_ID)
    morning_logicals = [
        _utc(2025, 1, 1, 6),
        _utc(2025, 1, 2, 6, 5),
        _utc(2025, 1, 3, 5, 55),
        _utc(2025, 1, 4, 6, 10),
    ]
    for i, logical in enumerate(morning_logicals):
        run = dag_maker.create_dagrun(
            run_id=f"morning_run_{i}",
            state=DagRunState.SUCCESS,
            run_type=DagRunType.SCHEDULED,
            logical_date=logical,
            **triggered_by_kwargs,
        )
        run.start_date = logical
        run.end_date = logical.add(minutes=30)
        for ti in run.task_instances:
            ti.state = DagRunState.SUCCESS
            ti.start_date = logical
            ti.end_date = logical.add(minutes=30)

    # Dag that consistently runs in the evening UTC, with longer duration.
    with dag_maker(dag_id=DAG_ID_EVENING, serialized=True, session=session):
        EmptyOperator(task_id=TASK_ID)
    evening_logicals = [
        _utc(2025, 1, 1, 18),
        _utc(2025, 1, 2, 18, 5),
        _utc(2025, 1, 3, 17, 55),
    ]
    for i, logical in enumerate(evening_logicals):
        run = dag_maker.create_dagrun(
            run_id=f"evening_run_{i}",
            state=DagRunState.SUCCESS,
            run_type=DagRunType.SCHEDULED,
            logical_date=logical,
            **triggered_by_kwargs,
        )
        run.start_date = logical
        run.end_date = logical.add(hours=2)
        for ti in run.task_instances:
            ti.state = DagRunState.SUCCESS
            ti.start_date = logical
            ti.end_date = logical.add(hours=2)

    # Add a failed evening run that must NOT affect the morning Dag's
    # statistics. (Dag is different so it doesn't pollute, but it also
    # ensures we filter on state=SUCCESS.)
    failed_evening = dag_maker.create_dagrun(
        run_id="evening_failed",
        state=DagRunState.FAILED,
        run_type=DagRunType.SCHEDULED,
        logical_date=_utc(2025, 1, 5, 18),
        **triggered_by_kwargs,
    )
    failed_evening.start_date = _utc(2025, 1, 5, 18)
    failed_evening.end_date = _utc(2025, 1, 5, 19)
    for ti in failed_evening.task_instances:
        ti.state = DagRunState.FAILED
        ti.start_date = _utc(2025, 1, 5, 18)
        ti.end_date = _utc(2025, 1, 5, 19)

    # Dag that exists but has no successful runs.
    with dag_maker(dag_id=DAG_ID_NO_RUNS, serialized=True, session=session):
        EmptyOperator(task_id=TASK_ID)
    failed_only = dag_maker.create_dagrun(
        run_id="no_runs_failed",
        state=DagRunState.FAILED,
        run_type=DagRunType.MANUAL,
        logical_date=_utc(2025, 1, 1, 0),
        **triggered_by_kwargs,
    )
    for ti in failed_only.task_instances:
        ti.state = DagRunState.FAILED

    dag_maker.sync_dagbag_to_db()
    session.commit()


@pytest.fixture(autouse=True)
def _clean():
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


class TestDagScheduleOverviewEndpoint:
    def test_returns_200_and_includes_all_dags(self, test_client):
        response = test_client.get("/dag_schedule_overview")
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 3
        ids = [entry["dag_id"] for entry in body["entries"]]
        assert DAG_ID_MORNING in ids
        assert DAG_ID_EVENING in ids
        assert DAG_ID_NO_RUNS in ids
        # Order: by dag_id ascending.
        assert ids == sorted(ids)

    def test_morning_dag_statistics(self, test_client):
        body = test_client.get("/dag_schedule_overview").json()
        morning = next(e for e in body["entries"] if e["dag_id"] == DAG_ID_MORNING)
        # Four successful runs at 06:00, 06:05, 05:55, 06:10 UTC.
        # start_seconds sorted: 21300, 21600, 21900, 22200.
        # mean = (21300 + 21600 + 21900 + 22200) / 4 = 21750.
        # median = (21600 + 21900) / 2 = 21750.
        assert morning["recent_runs_count"] == 4
        assert morning["start_mean_seconds"] == 21750
        assert morning["start_median_seconds"] == 21750
        # End is start + 30 minutes for every run.
        assert morning["end_mean_seconds"] == morning["start_mean_seconds"] + 30 * 60
        assert morning["end_median_seconds"] == morning["start_median_seconds"] + 30 * 60
        # Duration is exactly 30 minutes for every run.
        assert morning["duration_mean_seconds"] == 30 * 60
        assert morning["duration_median_seconds"] == 30 * 60

    def test_evening_dag_statistics(self, test_client):
        body = test_client.get("/dag_schedule_overview").json()
        evening = next(e for e in body["entries"] if e["dag_id"] == DAG_ID_EVENING)
        # 3 successful runs; the FAILED one should be excluded.
        assert evening["recent_runs_count"] == 3
        # End is start + 2h for every successful run.
        assert evening["end_mean_seconds"] == evening["start_mean_seconds"] + 2 * 3600
        assert evening["end_median_seconds"] == evening["start_median_seconds"] + 2 * 3600
        assert evening["duration_mean_seconds"] == 2 * 3600
        assert evening["duration_median_seconds"] == 2 * 3600

    def test_dag_with_no_successful_runs(self, test_client):
        body = test_client.get("/dag_schedule_overview").json()
        no_runs = next(e for e in body["entries"] if e["dag_id"] == DAG_ID_NO_RUNS)
        assert no_runs["recent_runs_count"] == 0
        assert no_runs["start_mean_seconds"] is None
        assert no_runs["start_median_seconds"] is None
        assert no_runs["end_mean_seconds"] is None
        assert no_runs["end_median_seconds"] is None
        assert no_runs["duration_mean_seconds"] is None
        assert no_runs["duration_median_seconds"] is None
        assert no_runs["oldest_logical_date"] is None
        assert no_runs["newest_logical_date"] is None

    def test_dag_id_pattern_filter(self, test_client):
        response = test_client.get(
            "/dag_schedule_overview",
            params={"dag_id_pattern": "%_morning"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 1
        assert body["entries"][0]["dag_id"] == DAG_ID_MORNING

    def test_dag_display_name_pattern_filter(self, test_client):
        # The default dag_display_name is the dag_id unless overridden; using
        # the same pattern should still return the morning dag.
        response = test_client.get(
            "/dag_schedule_overview",
            params={"dag_display_name_pattern": "%_morning"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 1
        assert body["entries"][0]["dag_id"] == DAG_ID_MORNING

    def test_run_after_window_excludes_old_runs(self, test_client):
        # run_after_gte > all run_after values for the morning dag should drop
        # its recent_runs_count to zero.
        response = test_client.get(
            "/dag_schedule_overview",
            params={"run_after_gte": "2030-01-01T00:00:00Z"},
        )
        assert response.status_code == 200
        body = response.json()
        for entry in body["entries"]:
            assert entry["recent_runs_count"] == 0
            assert entry["start_mean_seconds"] is None

    def test_run_after_window_lte_excludes_recent_runs(self, test_client):
        # run_after_lte < all morning dag run_after should drop the morning
        # dag to zero counts but leave the evening dag (which is also
        # scheduled but at a different time) unaffected only if it falls
        # within the window. To keep this test focused we just check that
        # the morning dag has zero counts when lte < morning's earliest run.
        response = test_client.get(
            "/dag_schedule_overview",
            params={"run_after_lte": "2024-12-31T23:59:59Z"},
        )
        assert response.status_code == 200
        body = response.json()
        for entry in body["entries"]:
            assert entry["recent_runs_count"] == 0

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dag_schedule_overview")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dag_schedule_overview")
        assert response.status_code == 403

    def test_response_clamps_to_max_dags(self, test_client):
        # Sanity: with three dags in the database, the response should
        # include all of them (max_dags=500 default).
        body = test_client.get("/dag_schedule_overview").json()
        assert body["total_entries"] == 3
