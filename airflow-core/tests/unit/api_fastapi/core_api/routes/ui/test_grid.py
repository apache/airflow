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
TASK_ID = "task"
TASK_ID_2 = "task2"
TASK_ID_3 = "task3"
TASK_ID_4 = "task4"
SUB_TASK_ID = "subtask"
MAPPED_TASK_ID = "mapped_task"
TASK_GROUP_ID = "task_group"
INNER_TASK_GROUP = "inner_task_group"
INNER_TASK_GROUP_SUB_TASK = "inner_task_group_sub_task"

GRID_RUN_1 = {
    "dag_run_id": "run_1",
    "data_interval_end": "2024-11-30T00:00:00Z",
    "data_interval_start": "2024-11-29T00:00:00Z",
    "end_date": "2024-12-31T00:00:00Z",
    "logical_date": "2024-11-30T00:00:00Z",
    "note": None,
    "queued_at": None,
    "run_type": "scheduled",
    "start_date": "2016-01-01T00:00:00Z",
    "run_after": "2024-11-30T00:00:00Z",
    "state": "success",
    "task_instances": [
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 0,
                "queued": 0,
                "removed": 0,
                "restarting": 0,
                "running": 0,
                "scheduled": 0,
                "skipped": 0,
                "success": 3,
                "up_for_reschedule": 0,
                "up_for_retry": 0,
                "upstream_failed": 0,
            },
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": "success",
            "task_count": 3,
            "task_id": "mapped_task_group",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 0,
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
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": "success",
            "task_count": 2,
            "task_id": "task_group.inner_task_group",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 0,
                "queued": 0,
                "removed": 0,
                "restarting": 0,
                "running": 0,
                "scheduled": 0,
                "skipped": 0,
                "success": 5,
                "up_for_reschedule": 0,
                "up_for_retry": 0,
                "upstream_failed": 0,
            },
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": "success",
            "task_count": 5,
            "task_id": "task_group",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 0,
                "queued": 0,
                "removed": 0,
                "restarting": 0,
                "running": 0,
                "scheduled": 0,
                "skipped": 0,
                "success": 3,
                "up_for_reschedule": 0,
                "up_for_retry": 0,
                "upstream_failed": 0,
            },
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": "success",
            "task_count": 3,
            "task_id": "mapped_task_group.subtask",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 0,
                "queued": 0,
                "removed": 0,
                "restarting": 0,
                "running": 0,
                "scheduled": 0,
                "skipped": 0,
                "success": 1,
                "up_for_reschedule": 0,
                "up_for_retry": 0,
                "upstream_failed": 0,
            },
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": "success",
            "task_count": 1,
            "task_id": "task",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 0,
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
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": "success",
            "task_count": 2,
            "task_id": "task_group.inner_task_group.inner_task_group_sub_task",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 0,
                "queued": 0,
                "removed": 0,
                "restarting": 0,
                "running": 0,
                "scheduled": 0,
                "skipped": 0,
                "success": 4,
                "up_for_reschedule": 0,
                "up_for_retry": 0,
                "upstream_failed": 0,
            },
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": "success",
            "task_count": 4,
            "task_id": "task_group.mapped_task",
            "try_number": 0,
        },
    ],
}

GRID_RUN_2 = {
    "dag_run_id": "run_2",
    "data_interval_end": "2024-11-30T00:00:00Z",
    "data_interval_start": "2024-11-29T00:00:00Z",
    "end_date": "2024-12-31T00:00:00Z",
    "logical_date": "2024-12-01T00:00:00Z",
    "note": None,
    "queued_at": None,
    "run_after": "2024-11-30T00:00:00Z",
    "run_type": "manual",
    "start_date": "2016-01-01T00:00:00Z",
    "state": "failed",
    "task_instances": [
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 1,
                "queued": 0,
                "removed": 0,
                "restarting": 0,
                "running": 1,
                "scheduled": 0,
                "skipped": 0,
                "success": 1,
                "up_for_reschedule": 0,
                "up_for_retry": 0,
                "upstream_failed": 0,
            },
            "end_date": "2024-12-30T01:02:03Z",
            "note": None,
            "queued_dttm": None,
            "start_date": "2024-12-30T01:00:00Z",
            "state": "running",
            "task_count": 3,
            "task_id": "mapped_task_group",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 2,
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
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": None,
            "task_count": 2,
            "task_id": "task_group.inner_task_group",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 5,
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
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": None,
            "task_count": 5,
            "task_id": "task_group",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 1,
                "queued": 0,
                "removed": 0,
                "restarting": 0,
                "running": 1,
                "scheduled": 0,
                "skipped": 0,
                "success": 1,
                "up_for_reschedule": 0,
                "up_for_retry": 0,
                "upstream_failed": 0,
            },
            "end_date": "2024-12-30T01:02:03Z",
            "note": None,
            "queued_dttm": None,
            "start_date": "2024-12-30T01:00:00Z",
            "state": "running",
            "task_count": 3,
            "task_id": "mapped_task_group.subtask",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 0,
                "queued": 0,
                "removed": 0,
                "restarting": 0,
                "running": 0,
                "scheduled": 0,
                "skipped": 0,
                "success": 1,
                "up_for_reschedule": 0,
                "up_for_retry": 0,
                "upstream_failed": 0,
            },
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": "success",
            "task_count": 1,
            "task_id": "task",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
                "no_status": 2,
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
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": None,
            "task_count": 2,
            "task_id": "task_group.inner_task_group.inner_task_group_sub_task",
            "try_number": 0,
        },
        {
            "child_states": {
                "deferred": 0,
                "failed": 0,
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
            "end_date": None,
            "note": None,
            "queued_dttm": None,
            "start_date": None,
            "state": None,
            "task_count": 4,
            "task_id": "task_group.mapped_task",
            "try_number": 0,
        },
    ],
}

STRUCTURE = {
    "edges": [],
    "nodes": [
        {
            "asset_condition_type": None,
            "children": [
                {
                    "asset_condition_type": None,
                    "children": None,
                    "id": "mapped_task_group.subtask",
                    "is_mapped": True,
                    "label": "subtask",
                    "operator": "MockOperator",
                    "setup_teardown_type": None,
                    "tooltip": None,
                    "type": "task",
                },
            ],
            "id": "mapped_task_group",
            "is_mapped": True,
            "label": "mapped_task_group",
            "operator": None,
            "setup_teardown_type": None,
            "tooltip": "",
            "type": "task",
        },
        {
            "asset_condition_type": None,
            "children": None,
            "id": "task",
            "is_mapped": None,
            "label": "task",
            "operator": "EmptyOperator",
            "setup_teardown_type": None,
            "tooltip": None,
            "type": "task",
        },
        {
            "asset_condition_type": None,
            "children": [
                {
                    "asset_condition_type": None,
                    "children": [
                        {
                            "asset_condition_type": None,
                            "children": None,
                            "id": "task_group.inner_task_group.inner_task_group_sub_task",
                            "is_mapped": True,
                            "label": "inner_task_group_sub_task",
                            "operator": "MockOperator",
                            "setup_teardown_type": None,
                            "tooltip": None,
                            "type": "task",
                        },
                    ],
                    "id": "task_group.inner_task_group",
                    "is_mapped": False,
                    "label": "inner_task_group",
                    "operator": None,
                    "setup_teardown_type": None,
                    "tooltip": "",
                    "type": "task",
                },
                {
                    "asset_condition_type": None,
                    "children": None,
                    "id": "task_group.mapped_task",
                    "is_mapped": True,
                    "label": "mapped_task",
                    "operator": "MockOperator",
                    "setup_teardown_type": None,
                    "tooltip": None,
                    "type": "task",
                },
            ],
            "id": "task_group",
            "is_mapped": False,
            "label": "task_group",
            "operator": None,
            "setup_teardown_type": None,
            "tooltip": "",
            "type": "task",
        },
    ],
}


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
        EmptyOperator(task_id=TASK_ID)

        @task_group
        def mapped_task_group(arg1):
            return MockOperator(task_id=SUB_TASK_ID, arg1=arg1)

        mapped_task_group.expand(arg1=["a", "b", "c"])

        with TaskGroup(group_id=TASK_GROUP_ID):
            MockOperator.partial(task_id=MAPPED_TASK_ID).expand(arg1=["a", "b", "c", "d"])
            with TaskGroup(group_id=INNER_TASK_GROUP):
                MockOperator.partial(task_id=INNER_TASK_GROUP_SUB_TASK).expand(arg1=["a", "b"])

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
        response = test_client.get(f"/grid/{DAG_ID}")
        assert response.status_code == 200
        assert response.json() == {
            "structure": STRUCTURE,
            "dag_runs": [GRID_RUN_1, GRID_RUN_2],
        }

    @pytest.mark.parametrize(
        "order_by,expected",
        [
            (
                "logical_date",
                {
                    "structure": STRUCTURE,
                    "dag_runs": [
                        GRID_RUN_1,
                        GRID_RUN_2,
                    ],
                },
            ),
            (
                "-logical_date",
                {
                    "structure": STRUCTURE,
                    "dag_runs": [
                        GRID_RUN_2,
                        GRID_RUN_1,
                    ],
                },
            ),
            (
                "run_after",
                {
                    "structure": STRUCTURE,
                    "dag_runs": [
                        GRID_RUN_1,
                        GRID_RUN_2,
                    ],
                },
            ),
            (
                "-run_after",
                {
                    "structure": STRUCTURE,
                    "dag_runs": [
                        GRID_RUN_2,
                        GRID_RUN_1,
                    ],
                },
            ),
        ],
    )
    def test_should_response_200_order_by(self, test_client, order_by, expected):
        response = test_client.get(f"/grid/{DAG_ID}", params={"order_by": order_by})
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "include_upstream, include_downstream, expected",
        [
            (
                "true",
                "false",
                {
                    "structure": STRUCTURE,
                    "dag_runs": [
                        {
                            **GRID_RUN_1,
                            "task_instances": [
                                {
                                    "child_states": {
                                        "deferred": 0,
                                        "failed": 0,
                                        "no_status": 0,
                                        "queued": 0,
                                        "removed": 0,
                                        "restarting": 0,
                                        "running": 0,
                                        "scheduled": 0,
                                        "skipped": 0,
                                        "success": 3,
                                        "up_for_reschedule": 0,
                                        "up_for_retry": 0,
                                        "upstream_failed": 0,
                                    },
                                    "end_date": None,
                                    "note": None,
                                    "queued_dttm": None,
                                    "start_date": None,
                                    "state": "success",
                                    "task_count": 3,
                                    "task_id": "mapped_task_group",
                                    "try_number": 0,
                                },
                                {
                                    "child_states": {
                                        "deferred": 0,
                                        "failed": 0,
                                        "no_status": 0,
                                        "queued": 0,
                                        "removed": 0,
                                        "restarting": 0,
                                        "running": 0,
                                        "scheduled": 0,
                                        "skipped": 0,
                                        "success": 3,
                                        "up_for_reschedule": 0,
                                        "up_for_retry": 0,
                                        "upstream_failed": 0,
                                    },
                                    "end_date": None,
                                    "note": None,
                                    "queued_dttm": None,
                                    "start_date": None,
                                    "state": "success",
                                    "task_count": 3,
                                    "task_id": "mapped_task_group.subtask",
                                    "try_number": 0,
                                },
                            ],
                        },
                        {
                            **GRID_RUN_2,
                            "task_instances": [
                                {
                                    "child_states": {
                                        "deferred": 0,
                                        "failed": 0,
                                        "no_status": 1,
                                        "queued": 0,
                                        "removed": 0,
                                        "restarting": 0,
                                        "running": 1,
                                        "scheduled": 0,
                                        "skipped": 0,
                                        "success": 1,
                                        "up_for_reschedule": 0,
                                        "up_for_retry": 0,
                                        "upstream_failed": 0,
                                    },
                                    "end_date": "2024-12-30T01:02:03Z",
                                    "note": None,
                                    "queued_dttm": None,
                                    "start_date": "2024-12-30T01:00:00Z",
                                    "state": "running",
                                    "task_count": 3,
                                    "task_id": "mapped_task_group",
                                    "try_number": 0,
                                },
                                {
                                    "child_states": {
                                        "deferred": 0,
                                        "failed": 0,
                                        "no_status": 1,
                                        "queued": 0,
                                        "removed": 0,
                                        "restarting": 0,
                                        "running": 1,
                                        "scheduled": 0,
                                        "skipped": 0,
                                        "success": 1,
                                        "up_for_reschedule": 0,
                                        "up_for_retry": 0,
                                        "upstream_failed": 0,
                                    },
                                    "end_date": "2024-12-30T01:02:03Z",
                                    "note": None,
                                    "queued_dttm": None,
                                    "start_date": "2024-12-30T01:00:00Z",
                                    "state": "running",
                                    "task_count": 3,
                                    "task_id": "mapped_task_group.subtask",
                                    "try_number": 0,
                                },
                            ],
                        },
                    ],
                },
            ),
            (
                "false",
                "true",
                {
                    "structure": STRUCTURE,
                    "dag_runs": [
                        {
                            **GRID_RUN_1,
                            "task_instances": [
                                {
                                    "child_states": {
                                        "deferred": 0,
                                        "failed": 0,
                                        "no_status": 0,
                                        "queued": 0,
                                        "removed": 0,
                                        "restarting": 0,
                                        "running": 0,
                                        "scheduled": 0,
                                        "skipped": 0,
                                        "success": 3,
                                        "up_for_reschedule": 0,
                                        "up_for_retry": 0,
                                        "upstream_failed": 0,
                                    },
                                    "end_date": None,
                                    "note": None,
                                    "queued_dttm": None,
                                    "start_date": None,
                                    "state": "success",
                                    "task_count": 3,
                                    "task_id": "mapped_task_group",
                                    "try_number": 0,
                                },
                                {
                                    "child_states": {
                                        "deferred": 0,
                                        "failed": 0,
                                        "no_status": 0,
                                        "queued": 0,
                                        "removed": 0,
                                        "restarting": 0,
                                        "running": 0,
                                        "scheduled": 0,
                                        "skipped": 0,
                                        "success": 3,
                                        "up_for_reschedule": 0,
                                        "up_for_retry": 0,
                                        "upstream_failed": 0,
                                    },
                                    "end_date": None,
                                    "note": None,
                                    "queued_dttm": None,
                                    "start_date": None,
                                    "state": "success",
                                    "task_count": 3,
                                    "task_id": "mapped_task_group.subtask",
                                    "try_number": 0,
                                },
                            ],
                        },
                        {
                            **GRID_RUN_2,
                            "task_instances": [
                                {
                                    "child_states": {
                                        "deferred": 0,
                                        "failed": 0,
                                        "no_status": 1,
                                        "queued": 0,
                                        "removed": 0,
                                        "restarting": 0,
                                        "running": 1,
                                        "scheduled": 0,
                                        "skipped": 0,
                                        "success": 1,
                                        "up_for_reschedule": 0,
                                        "up_for_retry": 0,
                                        "upstream_failed": 0,
                                    },
                                    "end_date": "2024-12-30T01:02:03Z",
                                    "note": None,
                                    "queued_dttm": None,
                                    "start_date": "2024-12-30T01:00:00Z",
                                    "state": "running",
                                    "task_count": 3,
                                    "task_id": "mapped_task_group",
                                    "try_number": 0,
                                },
                                {
                                    "child_states": {
                                        "deferred": 0,
                                        "failed": 0,
                                        "no_status": 1,
                                        "queued": 0,
                                        "removed": 0,
                                        "restarting": 0,
                                        "running": 1,
                                        "scheduled": 0,
                                        "skipped": 0,
                                        "success": 1,
                                        "up_for_reschedule": 0,
                                        "up_for_retry": 0,
                                        "upstream_failed": 0,
                                    },
                                    "end_date": "2024-12-30T01:02:03Z",
                                    "note": None,
                                    "queued_dttm": None,
                                    "start_date": "2024-12-30T01:00:00Z",
                                    "state": "running",
                                    "task_count": 3,
                                    "task_id": "mapped_task_group.subtask",
                                    "try_number": 0,
                                },
                            ],
                        },
                    ],
                },
            ),
        ],
    )
    def test_should_response_200_include_upstream_downstream(
        self, test_client, include_upstream, include_downstream, expected
    ):
        response = test_client.get(
            f"/grid/{DAG_ID}",
            params={
                "root": SUB_TASK_ID,
                "include_upstream": include_upstream,
                "include_downstream": include_downstream,
            },
        )
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "limit, expected",
        [
            (
                1,
                {
                    "structure": STRUCTURE,
                    "dag_runs": [GRID_RUN_1],
                },
            ),
            (
                2,
                {
                    "structure": STRUCTURE,
                    "dag_runs": [GRID_RUN_1, GRID_RUN_2],
                },
            ),
        ],
    )
    def test_should_response_200_limit(self, test_client, limit, expected):
        response = test_client.get(f"/grid/{DAG_ID}", params={"limit": limit})
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "params, expected",
        [
            (
                {
                    "logical_date_gte": timezone.datetime(2024, 11, 30),
                    "logical_date_lte": timezone.datetime(2024, 11, 30),
                },
                {
                    "structure": STRUCTURE,
                    "dag_runs": [GRID_RUN_1],
                },
            ),
            (
                {
                    "logical_date_gte": timezone.datetime(2024, 10, 30),
                    "logical_date_lte": timezone.datetime(2024, 10, 30),
                },
                {"dag_runs": [], "structure": STRUCTURE},
            ),
            (
                {
                    "run_after_gte": timezone.datetime(2024, 11, 30),
                    "run_after_lte": timezone.datetime(2024, 11, 30),
                },
                {
                    "structure": STRUCTURE,
                    "dag_runs": [GRID_RUN_1, GRID_RUN_2],
                },
            ),
            (
                {
                    "run_after_gte": timezone.datetime(2024, 10, 30),
                    "run_after_lte": timezone.datetime(2024, 10, 30),
                },
                {"dag_runs": [], "structure": STRUCTURE},
            ),
        ],
    )
    def test_should_response_200_date_filters(self, test_client, params, expected):
        response = test_client.get(
            f"/grid/{DAG_ID}",
            params=params,
        )
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "run_type, expected",
        [
            (
                ["manual"],
                {
                    "structure": STRUCTURE,
                    "dag_runs": [GRID_RUN_2],
                },
            ),
            (
                ["scheduled"],
                {
                    "structure": STRUCTURE,
                    "dag_runs": [GRID_RUN_1],
                },
            ),
        ],
    )
    def test_should_response_200_run_types(self, test_client, run_type, expected):
        response = test_client.get(f"/grid/{DAG_ID}", params={"run_type": run_type})
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "run_type, expected",
        [
            (
                ["invalid"],
                {"detail": f"Invalid value for run type. Valid values are {', '.join(DagRunType)}"},
            )
        ],
    )
    def test_should_response_200_run_types_invalid(self, test_client, run_type, expected):
        response = test_client.get(f"/grid/{DAG_ID}", params={"run_type": run_type})
        assert response.status_code == 422
        assert response.json() == expected

    @pytest.mark.parametrize(
        "state, expected",
        [
            (
                ["success"],
                {
                    "structure": STRUCTURE,
                    "dag_runs": [GRID_RUN_1],
                },
            ),
            (
                ["failed"],
                {
                    "structure": STRUCTURE,
                    "dag_runs": [GRID_RUN_2],
                },
            ),
            (
                ["running"],
                {"dag_runs": [], "structure": STRUCTURE},
            ),
        ],
    )
    def test_should_response_200_states(self, test_client, state, expected):
        response = test_client.get(f"/grid/{DAG_ID}", params={"state": state})
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        "state, expected",
        [
            (
                ["invalid"],
                {"detail": f"Invalid value for state. Valid values are {', '.join(DagRunState)}"},
            )
        ],
    )
    def test_should_response_200_states_invalid(self, test_client, state, expected):
        response = test_client.get(f"/grid/{DAG_ID}", params={"state": state})
        assert response.status_code == 422
        assert response.json() == expected

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/grid/{DAG_ID_3}")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/grid/{DAG_ID_3}")
        assert response.status_code == 403

    def test_should_response_404(self, test_client):
        response = test_client.get("/grid/invalid_dag")
        assert response.status_code == 404
        assert response.json() == {"detail": "Dag with id invalid_dag was not found"}

    def test_should_response_200_without_dag_run(self, test_client):
        response = test_client.get(f"/grid/{DAG_ID_2}")
        assert response.status_code == 200
        assert response.json() == {
            "dag_runs": [],
            "structure": {
                "nodes": [
                    {
                        "asset_condition_type": None,
                        "children": None,
                        "id": "task2",
                        "is_mapped": None,
                        "label": "task2",
                        "operator": "EmptyOperator",
                        "setup_teardown_type": None,
                        "tooltip": None,
                        "type": "task",
                    },
                ],
                "edges": [],
            },
        }

    def test_should_response_200_with_deleted_task_and_taskgroup(self, session, test_client):
        # Mark one of the TI of the previous runs as "REMOVED" to simulate clearing an older DagRun.
        # https://github.com/apache/airflow/issues/48670
        ti = session.scalar(
            select(TaskInstance).where(TaskInstance.run_id == "run_3", TaskInstance.task_id == TASK_ID_4)
        )
        ti.state = TaskInstanceState.REMOVED
        ti.dag_version = session.scalar(select(DagModel).where(DagModel.dag_id == DAG_ID_3)).dag_versions[-1]
        session.commit()

        response = test_client.get(f"/grid/{DAG_ID_3}")
        assert response.status_code == 200
        assert response.json() == {
            "structure": {
                "edges": [],
                "nodes": [
                    {
                        "asset_condition_type": None,
                        "children": None,
                        "id": "task3",
                        "is_mapped": None,
                        "label": "task3",
                        "operator": "EmptyOperator",
                        "setup_teardown_type": None,
                        "tooltip": None,
                        "type": "task",
                    },
                    {
                        "asset_condition_type": None,
                        "children": None,
                        "id": "task4",
                        "is_mapped": None,
                        "label": "task4",
                        "operator": "EmptyOperator",
                        "setup_teardown_type": None,
                        "tooltip": None,
                        "type": "task",
                    },
                    {
                        "asset_condition_type": None,
                        "children": [
                            {
                                "asset_condition_type": None,
                                "children": None,
                                "id": "task_group.inner_task",
                                "is_mapped": None,
                                "label": "inner_task",
                                "operator": "EmptyOperator",
                                "setup_teardown_type": None,
                                "tooltip": None,
                                "type": "task",
                            },
                        ],
                        "id": "task_group",
                        "is_mapped": False,
                        "label": "task_group",
                        "operator": None,
                        "setup_teardown_type": None,
                        "tooltip": "",
                        "type": "task",
                    },
                ],
            },
            "dag_runs": [
                {
                    "dag_run_id": "run_3",
                    "data_interval_end": "2024-11-30T00:00:00Z",
                    "data_interval_start": "2024-11-29T00:00:00Z",
                    "end_date": None,
                    "logical_date": "2024-11-30T00:00:00Z",
                    "note": None,
                    "queued_at": "2024-12-31T00:00:00Z",
                    "run_after": "2024-11-30T00:00:00Z",
                    "run_type": "scheduled",
                    "start_date": None,
                    "state": "queued",
                    "task_instances": [
                        {
                            "child_states": {
                                "deferred": 0,
                                "failed": 0,
                                "no_status": 0,
                                "queued": 0,
                                "removed": 0,
                                "restarting": 0,
                                "running": 0,
                                "scheduled": 0,
                                "skipped": 0,
                                "success": 1,
                                "up_for_reschedule": 0,
                                "up_for_retry": 0,
                                "upstream_failed": 0,
                            },
                            "end_date": None,
                            "note": None,
                            "queued_dttm": None,
                            "start_date": None,
                            "state": "success",
                            "task_count": 1,
                            "task_id": "task_group",
                            "try_number": 0,
                        },
                        {
                            "child_states": {
                                "deferred": 0,
                                "failed": 0,
                                "no_status": 0,
                                "queued": 0,
                                "removed": 0,
                                "restarting": 0,
                                "running": 0,
                                "scheduled": 0,
                                "skipped": 0,
                                "success": 1,
                                "up_for_reschedule": 0,
                                "up_for_retry": 0,
                                "upstream_failed": 0,
                            },
                            "end_date": None,
                            "note": None,
                            "queued_dttm": None,
                            "start_date": None,
                            "state": "success",
                            "task_count": 1,
                            "task_id": "task3",
                            "try_number": 0,
                        },
                        {
                            "child_states": {
                                "deferred": 0,
                                "failed": 0,
                                "no_status": 0,
                                "queued": 0,
                                "removed": 0,
                                "restarting": 0,
                                "running": 0,
                                "scheduled": 0,
                                "skipped": 0,
                                "success": 1,
                                "up_for_reschedule": 0,
                                "up_for_retry": 0,
                                "upstream_failed": 0,
                            },
                            "end_date": None,
                            "note": None,
                            "queued_dttm": None,
                            "start_date": None,
                            "state": "success",
                            "task_count": 1,
                            "task_id": "task_group.inner_task",
                            "try_number": 0,
                        },
                    ],
                },
                {
                    "dag_run_id": "run_4",
                    "data_interval_end": "2024-11-30T00:00:00Z",
                    "data_interval_start": "2024-11-29T00:00:00Z",
                    "end_date": "2024-12-31T00:00:00Z",
                    "logical_date": "2024-12-01T00:00:00Z",
                    "note": None,
                    "queued_at": None,
                    "run_after": "2024-11-30T00:00:00Z",
                    "run_type": "manual",
                    "start_date": "2024-11-30T00:00:00Z",
                    "state": "success",
                    "task_instances": [
                        {
                            "child_states": {
                                "deferred": 0,
                                "failed": 0,
                                "no_status": 0,
                                "queued": 0,
                                "removed": 0,
                                "restarting": 0,
                                "running": 0,
                                "scheduled": 0,
                                "skipped": 0,
                                "success": 1,
                                "up_for_reschedule": 0,
                                "up_for_retry": 0,
                                "upstream_failed": 0,
                            },
                            "end_date": None,
                            "note": None,
                            "queued_dttm": None,
                            "start_date": None,
                            "state": "success",
                            "task_count": 1,
                            "task_id": "task3",
                            "try_number": 0,
                        },
                    ],
                },
            ],
        }
