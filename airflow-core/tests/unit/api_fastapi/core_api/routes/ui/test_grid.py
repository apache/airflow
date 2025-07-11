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
from operator import attrgetter

import pendulum
import pytest
from sqlalchemy import select

from airflow.models import DagBag
from airflow.models.dag import DagModel
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task_group
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import clear_db_assets, clear_db_dags, clear_db_runs, clear_db_serialized_dags
from tests_common.test_utils.mock_operators import MockOperator

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
DAG_ID_2 = "test_dag_2"
DAG_ID_3 = "test_dag_3"
DAG_ID_4 = "test_dag_4"
TASK_ID = "task"
TASK_ID_2 = "task2"
TASK_ID_3 = "task3"
TASK_ID_4 = "task4"
SUB_TASK_ID = "subtask"
MAPPED_TASK_ID = "mapped_task"
MAPPED_TASK_ID_2 = "mapped_task_2"
TASK_GROUP_ID = "task_group"
INNER_TASK_GROUP = "inner_task_group"
INNER_TASK_GROUP_SUB_TASK = "inner_task_group_sub_task"

GRID_RUN_1 = {
    "dag_id": "test_dag",
    "duration": 0,
    "end_date": "2024-12-31T00:00:00Z",
    "run_after": "2024-11-30T00:00:00Z",
    "run_id": "run_1",
    "run_type": "scheduled",
    "start_date": "2016-01-01T00:00:00Z",
    "state": "success",
}

GRID_RUN_2 = {
    "dag_id": "test_dag",
    "duration": 0,
    "end_date": "2024-12-31T00:00:00Z",
    "run_after": "2024-11-30T00:00:00Z",
    "run_id": "run_2",
    "run_type": "manual",
    "start_date": "2016-01-01T00:00:00Z",
    "state": "failed",
}

GRID_NODES = [
    {
        "children": [{"id": "mapped_task_group.subtask", "is_mapped": True, "label": "subtask"}],
        "id": "mapped_task_group",
        "is_mapped": True,
        "label": "mapped_task_group",
    },
    {"id": "task", "label": "task"},
    {
        "children": [
            {
                "children": [
                    {
                        "id": "task_group.inner_task_group.inner_task_group_sub_task",
                        "is_mapped": True,
                        "label": "inner_task_group_sub_task",
                    }
                ],
                "id": "task_group.inner_task_group",
                "label": "inner_task_group",
            },
            {"id": "task_group.mapped_task", "is_mapped": True, "label": "mapped_task"},
        ],
        "id": "task_group",
        "label": "task_group",
    },
    {"id": "mapped_task_2", "is_mapped": True, "label": "mapped_task_2"},
]


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag():
    # Speed up: We don't want example dags for this module
    return DagBag(include_examples=False, read_dags_from_db=True)


@pytest.fixture(autouse=True)
@provide_session
def setup(dag_maker, session=None):
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    # DAG 1
    with dag_maker(dag_id=DAG_ID, serialized=True, session=session) as dag:
        task = EmptyOperator(task_id=TASK_ID)

        @task_group
        def mapped_task_group(arg1):
            return MockOperator(task_id=SUB_TASK_ID, arg1=arg1)

        mapped_task_group.expand(arg1=["a", "b", "c"])

        with TaskGroup(group_id=TASK_GROUP_ID):
            MockOperator.partial(task_id=MAPPED_TASK_ID).expand(arg1=["a", "b", "c", "d"])
            with TaskGroup(group_id=INNER_TASK_GROUP):
                MockOperator.partial(task_id=INNER_TASK_GROUP_SUB_TASK).expand(arg1=["a", "b"])

        # Mapped but never expanded. API should not crash, but count this as one no-status ti.
        MockOperator.partial(task_id=MAPPED_TASK_ID_2).expand(arg1=task.output)

    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST}
    logical_date = timezone.datetime(2024, 11, 30)
    data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)
    run_1 = dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=logical_date,
        data_interval=data_interval,
        **triggered_by_kwargs,
    )
    run_2 = dag_maker.create_dagrun(
        run_id="run_2",
        run_type=DagRunType.MANUAL,
        state=DagRunState.FAILED,
        logical_date=logical_date + timedelta(days=1),
        data_interval=data_interval,
        **triggered_by_kwargs,
    )
    for ti in run_1.task_instances:
        ti.state = TaskInstanceState.SUCCESS
    for ti in sorted(run_2.task_instances, key=lambda ti: (ti.task_id, ti.map_index)):
        if ti.task_id == TASK_ID:
            ti.state = TaskInstanceState.SUCCESS
        elif ti.task_id == "mapped_task_group.subtask":
            if ti.map_index == 0:
                ti.state = TaskInstanceState.SUCCESS
                ti.start_date = pendulum.DateTime(2024, 12, 30, 1, 0, 0, tzinfo=pendulum.UTC)
                ti.end_date = pendulum.DateTime(2024, 12, 30, 1, 2, 3, tzinfo=pendulum.UTC)
            elif ti.map_index == 1:
                ti.state = TaskInstanceState.RUNNING
                ti.start_date = pendulum.DateTime(2024, 12, 30, 2, 3, 4, tzinfo=pendulum.UTC)
                ti.end_date = None

    # DAG 2
    with dag_maker(dag_id=DAG_ID_2, serialized=True, session=session):
        EmptyOperator(task_id=TASK_ID_2)

    # DAG 3 for testing removed task
    with dag_maker(dag_id=DAG_ID_3, serialized=True, session=session) as dag_3:
        EmptyOperator(task_id=TASK_ID_3)
        EmptyOperator(task_id=TASK_ID_4)
        with TaskGroup(group_id=TASK_GROUP_ID):
            EmptyOperator(task_id="inner_task")

    logical_date = timezone.datetime(2024, 11, 30)
    data_interval = dag_3.timetable.infer_manual_data_interval(run_after=logical_date)
    run_3 = dag_maker.create_dagrun(
        run_id="run_3",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        start_date=logical_date,
        logical_date=logical_date,
        data_interval=data_interval,
        **triggered_by_kwargs,
    )

    # Serialize DAG with only one task
    with dag_maker(dag_id=DAG_ID_3, serialized=True, session=session):
        EmptyOperator(task_id=TASK_ID_3)

    run_4 = dag_maker.create_dagrun(
        run_id="run_4",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.MANUAL,
        start_date=logical_date,
        logical_date=logical_date + timedelta(days=1),
        data_interval=data_interval,
        **triggered_by_kwargs,
    )

    for ti in run_3.task_instances:
        ti.state = TaskInstanceState.SUCCESS
        ti.end_date = None
    for ti in run_4.task_instances:
        ti.state = TaskInstanceState.SUCCESS
        ti.end_date = None

    # DAG 4 for testing removed task
    with dag_maker(dag_id=DAG_ID_4, serialized=True, session=session) as dag_4:
        t1 = EmptyOperator(task_id="t1")
        t2 = EmptyOperator(task_id="t2")
        with TaskGroup(group_id=f"{TASK_GROUP_ID}-1") as tg1:
            with TaskGroup(group_id=f"{TASK_GROUP_ID}-2") as tg2:
                EmptyOperator(task_id="t3")
                EmptyOperator(task_id="t4")
                EmptyOperator(task_id="t5")
            t6 = EmptyOperator(task_id="t6")
            tg2 >> t6
        t7 = EmptyOperator(task_id="t7")
        t1 >> t2 >> tg1 >> t7

    logical_date = timezone.datetime(2024, 11, 30)
    data_interval = dag_4.timetable.infer_manual_data_interval(run_after=logical_date)
    run_4 = dag_maker.create_dagrun(
        run_id="run_4-1",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        start_date=logical_date,
        logical_date=logical_date,
        data_interval=data_interval,
        **triggered_by_kwargs,
    )
    end_date = pendulum.datetime(2025, 3, 2)
    start_date = end_date.add(seconds=-2)
    for ti in sorted(run_4.task_instances, key=attrgetter("task_id")):
        ti.state = "success"
        ti.start_date = start_date
        ti.end_date = end_date
        start_date = end_date
        end_date = start_date.add(seconds=2)
    session.commit()


@pytest.fixture(autouse=True)
def _clean():
    clear_db_runs()
    clear_db_assets()
    yield
    clear_db_runs()
    clear_db_assets()


# Create this as a fixture so that it is applied before the `dag_with_runs` fixture is!
@pytest.fixture(autouse=True)
def _freeze_time_for_dagruns(time_machine):
    time_machine.move_to("2024-12-31T00:00:00+00:00", tick=False)


@pytest.mark.usefixtures("_freeze_time_for_dagruns")
class TestGetGridDataEndpoint:
    def test_should_response_200(self, test_client):
        response = test_client.get(f"/grid/runs/{DAG_ID}")
        assert response.status_code == 200
        assert response.json() == [
            GRID_RUN_1,
            GRID_RUN_2,
        ]

    @pytest.mark.parametrize(
        "order_by,expected",
        [
            (
                "logical_date",
                [
                    GRID_RUN_1,
                    GRID_RUN_2,
                ],
            ),
            (
                "-logical_date",
                [
                    GRID_RUN_2,
                    GRID_RUN_1,
                ],
            ),
            (
                "run_after",
                [
                    GRID_RUN_1,
                    GRID_RUN_2,
                ],
            ),
            (
                "-run_after",
                [
                    GRID_RUN_2,
                    GRID_RUN_1,
                ],
            ),
        ],
    )
    def test_should_response_200_order_by(self, test_client, order_by, expected):
        response = test_client.get(f"/grid/runs/{DAG_ID}", params={"order_by": order_by})
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "limit, expected",
        [
            (
                1,
                [GRID_RUN_1],
            ),
            (
                2,
                [GRID_RUN_1, GRID_RUN_2],
            ),
        ],
    )
    def test_should_response_200_limit(self, test_client, limit, expected):
        response = test_client.get(f"/grid/runs/{DAG_ID}", params={"limit": limit})
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "params, expected",
        [
            (
                {
                    "run_after_gte": timezone.datetime(2024, 11, 30),
                    "run_after_lte": timezone.datetime(2024, 11, 30),
                },
                [GRID_RUN_1, GRID_RUN_2],
            ),
            (
                {
                    "run_after_gte": timezone.datetime(2024, 10, 30),
                    "run_after_lte": timezone.datetime(2024, 10, 30),
                },
                [],
            ),
        ],
    )
    def test_runs_should_response_200_date_filters(self, test_client, params, expected):
        response = test_client.get(
            f"/grid/runs/{DAG_ID}",
            params=params,
        )
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "params, expected",
        [
            (
                {
                    "run_after_gte": timezone.datetime(2024, 11, 30),
                    "run_after_lte": timezone.datetime(2024, 11, 30),
                },
                GRID_NODES,
            ),
            (
                {
                    "run_after_gte": timezone.datetime(2024, 10, 30),
                    "run_after_lte": timezone.datetime(2024, 10, 30),
                },
                GRID_NODES,
            ),
        ],
    )
    def test_structure_should_response_200_date_filters(self, test_client, params, expected):
        response = test_client.get(
            f"/grid/structure/{DAG_ID}",
            params=params,
        )
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize("endpoint", ["runs", "structure", "latest_run"])
    def test_should_response_401(self, unauthenticated_test_client, endpoint):
        response = unauthenticated_test_client.get(f"/grid/{endpoint}/{DAG_ID_3}")
        assert response.status_code == 401

    @pytest.mark.parametrize("endpoint", ["runs", "structure", "latest_run"])
    def test_should_response_403(self, unauthorized_test_client, endpoint):
        response = unauthorized_test_client.get(f"/grid/{endpoint}/{DAG_ID_3}")
        assert response.status_code == 403

    @pytest.mark.parametrize("endpoint", ["runs", "structure"])
    def test_should_response_404(self, test_client, endpoint):
        response = test_client.get(f"/grid/{endpoint}/invalid_dag")
        assert response.status_code == 404
        assert response.json() == {"detail": "Dag with id invalid_dag was not found"}

    def test_structure_should_response_200_without_dag_run(self, test_client):
        response = test_client.get(f"/grid/structure/{DAG_ID_2}")
        assert response.status_code == 200
        assert response.json() == [{"id": "task2", "label": "task2"}]

    def test_runs_should_response_200_without_dag_run(self, test_client):
        response = test_client.get(f"/grid/runs/{DAG_ID_2}")
        assert response.status_code == 200
        assert response.json() == []

    def test_should_response_200_with_deleted_task_and_taskgroup(self, session, test_client):
        # Mark one of the TI of the previous runs as "REMOVED" to simulate clearing an older DagRun.
        # https://github.com/apache/airflow/issues/48670
        ti = session.scalar(
            select(TaskInstance).where(TaskInstance.run_id == "run_3", TaskInstance.task_id == TASK_ID_4)
        )
        ti.state = TaskInstanceState.REMOVED
        ti.dag_version = session.scalar(select(DagModel).where(DagModel.dag_id == DAG_ID_3)).dag_versions[-1]
        session.commit()

        response = test_client.get(f"/grid/structure/{DAG_ID_3}")
        assert response.status_code == 200
        assert response.json() == [
            {"id": "task3", "label": "task3"},
            {"id": "task4", "label": "task4"},
            {
                "children": [{"id": "task_group.inner_task", "label": "inner_task"}],
                "id": "task_group",
                "label": "task_group",
            },
        ]

    def test_get_dag_structure(self, session, test_client):
        session.commit()
        response = test_client.get(f"/grid/structure/{DAG_ID}?limit=5")
        assert response.status_code == 200
        assert response.json() == [
            {
                "children": [{"id": "mapped_task_group.subtask", "is_mapped": True, "label": "subtask"}],
                "id": "mapped_task_group",
                "is_mapped": True,
                "label": "mapped_task_group",
            },
            {"id": "task", "label": "task"},
            {
                "children": [
                    {
                        "children": [
                            {
                                "id": "task_group.inner_task_group.inner_task_group_sub_task",
                                "is_mapped": True,
                                "label": "inner_task_group_sub_task",
                            }
                        ],
                        "id": "task_group.inner_task_group",
                        "label": "inner_task_group",
                    },
                    {"id": "task_group.mapped_task", "is_mapped": True, "label": "mapped_task"},
                ],
                "id": "task_group",
                "label": "task_group",
            },
            {"id": "mapped_task_2", "is_mapped": True, "label": "mapped_task_2"},
        ]

    def test_get_grid_runs(self, session, test_client):
        session.commit()
        response = test_client.get(f"/grid/runs/{DAG_ID}?limit=5")
        assert response.status_code == 200
        assert response.json() == [
            {
                "dag_id": "test_dag",
                "duration": 0,
                "end_date": "2024-12-31T00:00:00Z",
                "run_after": "2024-11-30T00:00:00Z",
                "run_id": "run_1",
                "run_type": "scheduled",
                "start_date": "2016-01-01T00:00:00Z",
                "state": "success",
            },
            {
                "dag_id": "test_dag",
                "duration": 0,
                "end_date": "2024-12-31T00:00:00Z",
                "run_after": "2024-11-30T00:00:00Z",
                "run_id": "run_2",
                "run_type": "manual",
                "start_date": "2016-01-01T00:00:00Z",
                "state": "failed",
            },
        ]

    def test_grid_ti_summaries_group(self, session, test_client):
        run_id = "run_4-1"
        session.commit()
        response = test_client.get(f"/grid/ti_summaries/{DAG_ID_4}/{run_id}")
        assert response.status_code == 200
        actual = response.json()
        expected = {
            "dag_id": "test_dag_4",
            "run_id": "run_4-1",
            "task_instances": [
                {
                    "state": "success",
                    "task_id": "t1",
                    "child_states": None,
                    "max_end_date": None,
                    "min_start_date": None,
                },
                {
                    "state": "success",
                    "task_id": "t2",
                    "child_states": None,
                    "max_end_date": None,
                    "min_start_date": None,
                },
                {
                    "state": "success",
                    "task_id": "t7",
                    "child_states": None,
                    "max_end_date": None,
                    "min_start_date": None,
                },
                {
                    "child_states": {"success": 2},
                    "max_end_date": "2025-03-02T00:00:12Z",
                    "min_start_date": "2025-03-02T00:00:04Z",
                    "state": "success",
                    "task_id": "task_group-1",
                },
                {
                    "state": "success",
                    "task_id": "task_group-1.t6",
                    "child_states": None,
                    "max_end_date": None,
                    "min_start_date": None,
                },
                {
                    "child_states": {"success": 3},
                    "max_end_date": "2025-03-02T00:00:12Z",
                    "min_start_date": "2025-03-02T00:00:06Z",
                    "state": "success",
                    "task_id": "task_group-1.task_group-2",
                },
                {
                    "state": "success",
                    "task_id": "task_group-1.task_group-2.t3",
                    "child_states": None,
                    "max_end_date": None,
                    "min_start_date": None,
                },
                {
                    "state": "success",
                    "task_id": "task_group-1.task_group-2.t4",
                    "child_states": None,
                    "max_end_date": None,
                    "min_start_date": None,
                },
                {
                    "state": "success",
                    "task_id": "task_group-1.task_group-2.t5",
                    "child_states": None,
                    "max_end_date": None,
                    "min_start_date": None,
                },
            ],
        }
        for obj in actual, expected:
            tis = obj["task_instances"]
            tis[:] = sorted(tis, key=lambda x: x["task_id"])
        assert actual == expected

    def test_grid_ti_summaries_mapped(self, session, test_client):
        run_id = "run_2"
        session.commit()
        response = test_client.get(f"/grid/ti_summaries/{DAG_ID}/{run_id}")
        assert response.status_code == 200
        data = response.json()
        actual = data["task_instances"]

        def sort_dict(in_dict):
            in_dict = sorted(in_dict, key=lambda x: x["task_id"])
            out = []
            for d in in_dict:
                n = {k: d[k] for k in sorted(d, reverse=True)}
                out.append(n)
            return out

        expected = [
            {
                "child_states": {"None": 1},
                "task_id": "mapped_task_2",
                "max_end_date": None,
                "min_start_date": None,
                "state": None,
            },
            {
                "child_states": {"running": 1},
                "max_end_date": "2024-12-30T01:02:03Z",
                "min_start_date": "2024-12-30T01:00:00Z",
                "state": "running",
                "task_id": "mapped_task_group",
            },
            {
                "state": "running",
                "task_id": "mapped_task_group.subtask",
                "child_states": None,
                "max_end_date": None,
                "min_start_date": None,
            },
            {
                "state": "success",
                "task_id": "task",
                "child_states": None,
                "max_end_date": None,
                "min_start_date": None,
            },
            {
                "child_states": {"None": 2},
                "task_id": "task_group",
                "max_end_date": None,
                "min_start_date": None,
                "state": None,
            },
            {
                "child_states": {"None": 1},
                "task_id": "task_group.inner_task_group",
                "max_end_date": None,
                "min_start_date": None,
                "state": None,
            },
            {
                "child_states": {"None": 2},
                "task_id": "task_group.inner_task_group.inner_task_group_sub_task",
                "max_end_date": None,
                "min_start_date": None,
                "state": None,
            },
            {
                "child_states": {"None": 4},
                "task_id": "task_group.mapped_task",
                "max_end_date": None,
                "min_start_date": None,
                "state": None,
            },
        ]
        expected = sort_dict(expected)
        actual = sort_dict(actual)
        assert actual == expected
