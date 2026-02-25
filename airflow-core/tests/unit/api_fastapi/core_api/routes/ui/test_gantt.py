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

from operator import attrgetter

import pendulum
import pytest

from airflow._shared.timezones import timezone
from airflow.models.dagbag import DBDagBag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_assets, clear_db_dags, clear_db_runs, clear_db_serialized_dags
from tests_common.test_utils.mock_operators import MockOperator

pytestmark = pytest.mark.db_test

DAG_ID = "test_gantt_dag"
DAG_ID_2 = "test_gantt_dag_2"
DAG_ID_3 = "test_gantt_dag_3"
TASK_ID = "task"
TASK_ID_2 = "task2"
TASK_ID_3 = "task3"
TASK_DISPLAY_NAME = "task_display_name"
TASK_DISPLAY_NAME_2 = "task2_display_name"
TASK_DISPLAY_NAME_3 = "task3_display_name"
MAPPED_TASK_ID = "mapped_task"

GANTT_TASK_1 = {
    "task_id": "task",
    "task_display_name": TASK_DISPLAY_NAME,
    "try_number": 1,
    "state": "success",
    "start_date": "2024-11-30T10:00:00Z",
    "end_date": "2024-11-30T10:05:00Z",
    "is_group": False,
    "is_mapped": False,
}

GANTT_TASK_2 = {
    "task_id": "task2",
    "task_display_name": TASK_DISPLAY_NAME_2,
    "try_number": 1,
    "state": "failed",
    "start_date": "2024-11-30T10:05:00Z",
    "end_date": "2024-11-30T10:10:00Z",
    "is_group": False,
    "is_mapped": False,
}

GANTT_TASK_3 = {
    "task_id": "task3",
    "task_display_name": TASK_DISPLAY_NAME_3,
    "try_number": 1,
    "state": "running",
    "start_date": "2024-11-30T10:10:00Z",
    "end_date": None,
    "is_group": False,
    "is_mapped": False,
}


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag():
    return DBDagBag()


@pytest.fixture(autouse=True)
@provide_session
def setup(dag_maker, session=None):
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST}

    # DAG 1: Multiple tasks with different states (success, failed, running)
    with dag_maker(dag_id=DAG_ID, serialized=True, session=session) as dag:
        EmptyOperator(task_id=TASK_ID, task_display_name=TASK_DISPLAY_NAME)
        EmptyOperator(task_id=TASK_ID_2, task_display_name=TASK_DISPLAY_NAME_2)
        EmptyOperator(task_id=TASK_ID_3, task_display_name=TASK_DISPLAY_NAME_3)

    logical_date = timezone.datetime(2024, 11, 30)
    data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)

    run_1 = dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.RUNNING,
        run_type=DagRunType.MANUAL,
        logical_date=logical_date,
        data_interval=data_interval,
        **triggered_by_kwargs,
    )

    for ti in sorted(run_1.task_instances, key=attrgetter("task_id")):
        if ti.task_id == TASK_ID:
            ti.state = TaskInstanceState.SUCCESS
            ti.try_number = 1
            ti.start_date = pendulum.DateTime(2024, 11, 30, 10, 0, 0, tzinfo=pendulum.UTC)
            ti.end_date = pendulum.DateTime(2024, 11, 30, 10, 5, 0, tzinfo=pendulum.UTC)
        elif ti.task_id == TASK_ID_2:
            ti.state = TaskInstanceState.FAILED
            ti.try_number = 1
            ti.start_date = pendulum.DateTime(2024, 11, 30, 10, 5, 0, tzinfo=pendulum.UTC)
            ti.end_date = pendulum.DateTime(2024, 11, 30, 10, 10, 0, tzinfo=pendulum.UTC)
        elif ti.task_id == TASK_ID_3:
            ti.state = TaskInstanceState.RUNNING
            ti.try_number = 1
            ti.start_date = pendulum.DateTime(2024, 11, 30, 10, 10, 0, tzinfo=pendulum.UTC)
            ti.end_date = None

    # DAG 2: With mapped tasks (only non-mapped should be returned)
    with dag_maker(dag_id=DAG_ID_2, serialized=True, session=session) as dag_2:
        EmptyOperator(task_id=TASK_ID)
        MockOperator.partial(task_id=MAPPED_TASK_ID).expand(arg1=["a", "b", "c"])

    logical_date_2 = timezone.datetime(2024, 12, 1)
    data_interval_2 = dag_2.timetable.infer_manual_data_interval(run_after=logical_date_2)

    run_2 = dag_maker.create_dagrun(
        run_id="run_2",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.MANUAL,
        logical_date=logical_date_2,
        data_interval=data_interval_2,
        **triggered_by_kwargs,
    )

    for ti in run_2.task_instances:
        ti.state = TaskInstanceState.SUCCESS
        ti.try_number = 1
        ti.start_date = pendulum.DateTime(2024, 12, 1, 10, 0, 0, tzinfo=pendulum.UTC)
        ti.end_date = pendulum.DateTime(2024, 12, 1, 10, 5, 0, tzinfo=pendulum.UTC)

    # DAG 3: With UP_FOR_RETRY state (should be excluded from results)
    with dag_maker(dag_id=DAG_ID_3, serialized=True, session=session) as dag_3:
        EmptyOperator(task_id=TASK_ID)
        EmptyOperator(task_id=TASK_ID_2)

    logical_date_3 = timezone.datetime(2024, 12, 2)
    data_interval_3 = dag_3.timetable.infer_manual_data_interval(run_after=logical_date_3)

    run_3 = dag_maker.create_dagrun(
        run_id="run_3",
        state=DagRunState.RUNNING,
        run_type=DagRunType.MANUAL,
        logical_date=logical_date_3,
        data_interval=data_interval_3,
        **triggered_by_kwargs,
    )

    for ti in sorted(run_3.task_instances, key=attrgetter("task_id")):
        if ti.task_id == TASK_ID:
            ti.state = TaskInstanceState.SUCCESS
            ti.try_number = 1
            ti.start_date = pendulum.DateTime(2024, 12, 2, 10, 0, 0, tzinfo=pendulum.UTC)
            ti.end_date = pendulum.DateTime(2024, 12, 2, 10, 5, 0, tzinfo=pendulum.UTC)
        elif ti.task_id == TASK_ID_2:
            # UP_FOR_RETRY should be excluded (historical tries are in TaskInstanceHistory)
            ti.state = TaskInstanceState.UP_FOR_RETRY
            ti.try_number = 2
            ti.start_date = pendulum.DateTime(2024, 12, 2, 10, 5, 0, tzinfo=pendulum.UTC)
            ti.end_date = pendulum.DateTime(2024, 12, 2, 10, 10, 0, tzinfo=pendulum.UTC)

    session.commit()


@pytest.fixture(autouse=True)
def _clean():
    clear_db_runs()
    clear_db_assets()
    yield
    clear_db_runs()
    clear_db_assets()


@pytest.mark.usefixtures("setup")
class TestGetGanttDataEndpoint:
    def test_should_response_200(self, test_client):
        with assert_queries_count(3):
            response = test_client.get(f"/gantt/{DAG_ID}/run_1")
        assert response.status_code == 200
        data = response.json()
        assert data["dag_id"] == DAG_ID
        assert data["run_id"] == "run_1"
        actual = sorted(data["task_instances"], key=lambda x: x["task_id"])
        assert actual == [GANTT_TASK_1, GANTT_TASK_2, GANTT_TASK_3]

    @pytest.mark.parametrize(
        ("dag_id", "run_id", "expected_task_ids", "expected_states", "expected_task_display_names"),
        [
            pytest.param(
                DAG_ID,
                "run_1",
                ["task", "task2", "task3"],
                {"success", "failed", "running"},
                {TASK_ID: TASK_DISPLAY_NAME, TASK_ID_2: TASK_DISPLAY_NAME_2, TASK_ID_3: TASK_DISPLAY_NAME_3},
                id="dag1_multiple_states",
            ),
            pytest.param(
                DAG_ID_2,
                "run_2",
                ["task"],
                {"success"},
                {TASK_ID: TASK_ID},
                id="dag2_filters_mapped_tasks",
            ),
            pytest.param(
                DAG_ID_3,
                "run_3",
                ["task"],
                {"success"},
                {TASK_ID: TASK_ID},
                id="dag3_excludes_up_for_retry",
            ),
        ],
    )
    def test_task_filtering_and_states(
        self, test_client, dag_id, run_id, expected_task_ids, expected_states, expected_task_display_names
    ):
        with assert_queries_count(3):
            response = test_client.get(f"/gantt/{dag_id}/{run_id}")
        assert response.status_code == 200
        data = response.json()

        actual_task_ids = sorted([ti["task_id"] for ti in data["task_instances"]])
        assert actual_task_ids == expected_task_ids

        actual_states = {ti["state"] for ti in data["task_instances"]}
        assert actual_states == expected_states

        for ti in data["task_instances"]:
            assert ti["task_display_name"] == expected_task_display_names[ti["task_id"]]

    @pytest.mark.parametrize(
        ("dag_id", "run_id", "task_id", "expected_start", "expected_end", "expected_state"),
        [
            pytest.param(
                DAG_ID,
                "run_1",
                "task",
                "2024-11-30T10:00:00Z",
                "2024-11-30T10:05:00Z",
                "success",
                id="success_task_has_dates",
            ),
            pytest.param(
                DAG_ID,
                "run_1",
                "task2",
                "2024-11-30T10:05:00Z",
                "2024-11-30T10:10:00Z",
                "failed",
                id="failed_task_has_dates",
            ),
            pytest.param(
                DAG_ID,
                "run_1",
                "task3",
                "2024-11-30T10:10:00Z",
                None,
                "running",
                id="running_task_null_end_date",
            ),
        ],
    )
    def test_task_dates_and_states(
        self, test_client, dag_id, run_id, task_id, expected_start, expected_end, expected_state
    ):
        with assert_queries_count(3):
            response = test_client.get(f"/gantt/{dag_id}/{run_id}")
        assert response.status_code == 200
        data = response.json()
        ti = next((t for t in data["task_instances"] if t["task_id"] == task_id), None)
        assert ti is not None
        assert ti["start_date"] == expected_start
        assert ti["end_date"] == expected_end
        assert ti["state"] == expected_state

    def test_sorted_by_task_id_and_try_number(self, test_client):
        with assert_queries_count(3):
            response = test_client.get(f"/gantt/{DAG_ID}/run_1")
        assert response.status_code == 200
        data = response.json()
        task_instances = data["task_instances"]
        sorted_tis = sorted(task_instances, key=lambda x: (x["task_id"], x["try_number"]))
        assert task_instances == sorted_tis

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/gantt/{DAG_ID}/run_1")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/gantt/{DAG_ID}/run_1")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("dag_id", "run_id"),
        [
            pytest.param("invalid_dag", "run_1", id="invalid_dag"),
            pytest.param(DAG_ID, "invalid_run", id="invalid_run"),
            pytest.param("invalid_dag", "invalid_run", id="both_invalid"),
        ],
    )
    def test_should_response_404(self, test_client, dag_id, run_id):
        with assert_queries_count(3):
            response = test_client.get(f"/gantt/{dag_id}/{run_id}")
        assert response.status_code == 404
