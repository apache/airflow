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

from datetime import timedelta

import pendulum
import pytest

from airflow.models.dag import DagModel
from airflow.models.dagbag import DBDagBag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag():
    return DBDagBag()


@pytest.fixture(autouse=True)
def clean():
    clear_db_runs()
    yield
    clear_db_runs()


# freeze time fixture so that it is applied before `make_dag_runs` is!
@pytest.fixture
def freeze_time_for_dagruns(time_machine):
    time_machine.move_to("2023-05-02T00:00:00+00:00", tick=False)


@pytest.fixture
def make_dag_runs(dag_maker, session, time_machine):
    with dag_maker(
        dag_id="test_dag_id",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    date = dag_maker.dag.start_date

    run1 = dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=date,
        start_date=date,
    )

    run2 = dag_maker.create_dagrun(
        run_id="run_2",
        state=DagRunState.FAILED,
        run_type=DagRunType.ASSET_TRIGGERED,
        logical_date=date + timedelta(days=1),
        start_date=date + timedelta(days=1),
    )

    run3 = dag_maker.create_dagrun(
        run_id="run_3",
        state=DagRunState.RUNNING,
        run_type=DagRunType.SCHEDULED,
        logical_date=date + timedelta(days=2),
        start_date=date + timedelta(days=2),
    )

    run3.end_date = None

    dag_maker.create_dagrun(
        run_id="run_4",
        state=DagRunState.QUEUED,
        run_type=DagRunType.SCHEDULED,
        logical_date=date + timedelta(days=3),
    )

    for ti in run1.task_instances:
        ti.state = TaskInstanceState.SUCCESS

    for ti in run2.task_instances:
        ti.state = TaskInstanceState.FAILED

    dag_maker.sync_dagbag_to_db()
    time_machine.move_to("2023-07-02T00:00:00+00:00", tick=False)


@pytest.fixture
def make_failed_dag_runs(dag_maker, session):
    with dag_maker(
        dag_id="test_failed_dag",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    date = dag_maker.dag.start_date

    dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.FAILED,
        run_type=DagRunType.SCHEDULED,
        logical_date=date,
        start_date=date,
    )

    dag_maker.sync_dagbag_to_db()


@pytest.fixture
def make_queued_dag_runs(dag_maker, session):
    with dag_maker(
        dag_id="test_queued_dag",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    date = dag_maker.dag.start_date

    dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.QUEUED,
        run_type=DagRunType.SCHEDULED,
        logical_date=date,
        start_date=date,
    )

    dag_maker.sync_dagbag_to_db()


@pytest.fixture
def make_multiple_dags(dag_maker, session):
    with dag_maker(
        dag_id="test_running_dag",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    date = dag_maker.dag.start_date
    dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.RUNNING,
        run_type=DagRunType.SCHEDULED,
        logical_date=date,
        start_date=date,
    )

    with dag_maker(
        dag_id="test_failed_dag",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    date = dag_maker.dag.start_date
    dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.FAILED,
        run_type=DagRunType.SCHEDULED,
        logical_date=date,
        start_date=date,
    )

    with dag_maker(
        dag_id="test_queued_dag",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    date = dag_maker.dag.start_date
    dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.QUEUED,
        run_type=DagRunType.SCHEDULED,
        logical_date=date,
        start_date=date,
    )

    with dag_maker(
        dag_id="no_dag_runs",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    with dag_maker(
        dag_id="stale_dag",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    date = dag_maker.dag.start_date
    dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.QUEUED,
        run_type=DagRunType.SCHEDULED,
        logical_date=date,
        start_date=date,
    )
    dag_maker.sync_dagbag_to_db()

    session.get(DagModel, "stale_dag").is_stale = True
    session.commit()


class TestHistoricalMetricsDataEndpoint:
    @pytest.mark.parametrize(
        ("params", "expected"),
        [
            (
                {"start_date": "2023-01-01T00:00", "end_date": "2023-08-02T00:00"},
                {
                    "dag_run_states": {"failed": 1, "queued": 1, "running": 1, "success": 1},
                    "dag_run_types": {"backfill": 0, "asset_triggered": 1, "manual": 0, "scheduled": 3},
                    "task_instance_states": {
                        "deferred": 0,
                        "failed": 2,
                        "no_status": 4,
                        "queued": 0,
                        "removed": 0,
                        "restarting": 0,
                        "running": 0,
                        "scheduled": 0,
                        "skipped": 0,
                        "success": 2,
                        "up_for_reschedule": 0,
                        "up_for_retry": 0,
                        "upstream_failed": 0,
                    },
                },
            ),
            (
                {"start_date": "2023-02-02T00:00", "end_date": "2023-06-02T00:00"},
                {
                    "dag_run_states": {"failed": 1, "queued": 0, "running": 0, "success": 0},
                    "dag_run_types": {"backfill": 0, "asset_triggered": 1, "manual": 0, "scheduled": 0},
                    "task_instance_states": {
                        "deferred": 0,
                        "failed": 2,
                        "no_status": 0,
                        "queued": 0,
                        "removed": 0,
                        "restarting": 0,
                        "running": 0,
                        "scheduled": 0,
                        "skipped": 0,
                        "success": 0,
                        "up_for_reschedule": 0,
                        "up_for_retry": 0,
                        "upstream_failed": 0,
                    },
                },
            ),
            (
                {"start_date": "2023-02-02T00:00"},
                {
                    "dag_run_states": {"failed": 1, "queued": 1, "running": 1, "success": 0},
                    "dag_run_types": {"backfill": 0, "asset_triggered": 1, "manual": 0, "scheduled": 2},
                    "task_instance_states": {
                        "deferred": 0,
                        "failed": 2,
                        "no_status": 4,
                        "queued": 0,
                        "removed": 0,
                        "restarting": 0,
                        "running": 0,
                        "scheduled": 0,
                        "skipped": 0,
                        "success": 0,
                        "up_for_reschedule": 0,
                        "up_for_retry": 0,
                        "upstream_failed": 0,
                    },
                },
            ),
        ],
    )
    @pytest.mark.usefixtures("freeze_time_for_dagruns", "make_dag_runs")
    def test_should_response_200(self, test_client, params, expected):
        with assert_queries_count(4):
            response = test_client.get("/dashboard/historical_metrics_data", params=params)
        assert response.status_code == 200
        assert response.json() == expected

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dashboard/historical_metrics_data", params={"start_date": "2023-02-02T00:00"}
        )
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dashboard/historical_metrics_data", params={"start_date": "2023-02-02T00:00"}
        )
        assert response.status_code == 403


class TestDagStatsEndpoint:
    @pytest.mark.usefixtures("freeze_time_for_dagruns", "make_multiple_dags")
    def test_should_response_200_multiple_dags(self, test_client):
        with assert_queries_count(3):
            response = test_client.get("/dashboard/dag_stats")
        assert response.status_code == 200
        assert response.json() == {
            "active_dag_count": 4,
            "failed_dag_count": 1,
            "running_dag_count": 1,
            "queued_dag_count": 1,
        }

    @pytest.mark.usefixtures("freeze_time_for_dagruns", "make_dag_runs")
    def test_should_response_200_single_dag(self, test_client):
        with assert_queries_count(3):
            response = test_client.get("/dashboard/dag_stats")
        assert response.status_code == 200
        assert response.json() == {
            "active_dag_count": 1,
            "failed_dag_count": 0,
            "running_dag_count": 0,
            "queued_dag_count": 1,
        }

    @pytest.mark.usefixtures("freeze_time_for_dagruns", "make_failed_dag_runs")
    def test_should_response_200_failed_dag(self, test_client):
        with assert_queries_count(3):
            response = test_client.get("/dashboard/dag_stats")
        assert response.status_code == 200
        assert response.json() == {
            "active_dag_count": 1,
            "failed_dag_count": 1,
            "running_dag_count": 0,
            "queued_dag_count": 0,
        }

    @pytest.mark.usefixtures("freeze_time_for_dagruns", "make_queued_dag_runs")
    def test_should_response_200_queued_dag(self, test_client):
        with assert_queries_count(3):
            response = test_client.get("/dashboard/dag_stats")
        assert response.status_code == 200
        assert response.json() == {
            "active_dag_count": 1,
            "failed_dag_count": 0,
            "running_dag_count": 0,
            "queued_dag_count": 1,
        }

    @pytest.mark.usefixtures("freeze_time_for_dagruns")
    def test_should_response_200_no_dag_runs(self, test_client):
        with assert_queries_count(3):
            response = test_client.get("/dashboard/dag_stats")
        assert response.status_code == 200
        assert response.json() == {
            "active_dag_count": 0,
            "failed_dag_count": 0,
            "running_dag_count": 0,
            "queued_dag_count": 0,
        }

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dashboard/dag_stats")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dashboard/dag_stats")
        assert response.status_code == 403
