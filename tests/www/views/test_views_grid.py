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
from typing import TYPE_CHECKING

import pendulum
import pytest
from dateutil.tz import UTC

from airflow.assets import Asset
from airflow.decorators import task_group
from airflow.lineage.entities import File
from airflow.models import DagBag
from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType
from airflow.www.views import dag_to_grid
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.db import clear_db_assets, clear_db_runs
from tests.test_utils.mock_operators import MockOperator

pytestmark = pytest.mark.db_test

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun

DAG_ID = "test"


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag():
    # Speed up: We don't want example dags for this module
    return DagBag(include_examples=False, read_dags_from_db=True)


@pytest.fixture(autouse=True)
def clean():
    clear_db_runs()
    clear_db_assets()
    yield
    clear_db_runs()
    clear_db_assets()


@pytest.fixture
def dag_without_runs(dag_maker, session, app, monkeypatch):
    with monkeypatch.context() as m:
        # Remove global operator links for this test
        m.setattr("airflow.plugins_manager.global_operator_extra_links", [])
        m.setattr("airflow.plugins_manager.operator_extra_links", [])
        m.setattr("airflow.plugins_manager.registered_operator_link_classes", {})

        with dag_maker(dag_id=DAG_ID, serialized=True, session=session):
            EmptyOperator(task_id="task1")

            @task_group
            def mapped_task_group(arg1):
                return MockOperator(task_id="subtask2", arg1=arg1)

            mapped_task_group.expand(arg1=["a", "b", "c"])
            with TaskGroup(group_id="group"):
                MockOperator.partial(task_id="mapped").expand(arg1=["a", "b", "c", "d"])

        m.setattr(app, "dag_bag", dag_maker.dagbag)
        yield dag_maker


@pytest.fixture
def dag_with_runs(dag_without_runs):
    date = dag_without_runs.dag.start_date
    run_1 = dag_without_runs.create_dagrun(
        run_id="run_1", state=DagRunState.SUCCESS, run_type=DagRunType.SCHEDULED, execution_date=date
    )
    run_2 = dag_without_runs.create_dagrun(
        run_id="run_2",
        run_type=DagRunType.SCHEDULED,
        execution_date=date + timedelta(days=1),
    )

    return run_1, run_2


def test_no_runs(admin_client, dag_without_runs):
    resp = admin_client.get(f"/object/grid_data?dag_id={DAG_ID}", follow_redirects=True)
    assert resp.status_code == 200, resp.json
    assert resp.json == {
        "dag_runs": [],
        "groups": {
            "children": [
                {
                    "extra_links": [],
                    "has_outlet_datasets": False,
                    "id": "task1",
                    "instances": [],
                    "is_mapped": False,
                    "label": "task1",
                    "operator": "EmptyOperator",
                    "trigger_rule": "all_success",
                },
                {
                    "children": [
                        {
                            "extra_links": [],
                            "has_outlet_datasets": False,
                            "id": "mapped_task_group.subtask2",
                            "instances": [],
                            "is_mapped": True,
                            "label": "subtask2",
                            "operator": "MockOperator",
                            "trigger_rule": "all_success",
                        }
                    ],
                    "is_mapped": True,
                    "id": "mapped_task_group",
                    "instances": [],
                    "label": "mapped_task_group",
                    "tooltip": "",
                },
                {
                    "children": [
                        {
                            "extra_links": [],
                            "has_outlet_datasets": False,
                            "id": "group.mapped",
                            "instances": [],
                            "is_mapped": True,
                            "label": "mapped",
                            "operator": "MockOperator",
                            "trigger_rule": "all_success",
                        }
                    ],
                    "id": "group",
                    "instances": [],
                    "label": "group",
                    "tooltip": "",
                },
            ],
            "id": None,
            "instances": [],
            "label": None,
        },
        "ordering": ["data_interval_end", "execution_date"],
        "errors": [],
    }


def test_grid_data_filtered_on_run_type_and_run_state(admin_client, dag_with_runs):
    for uri_params, expected_run_types, expected_run_states in [
        ("run_state=success&run_state=queued", ["scheduled"], ["success"]),
        ("run_state=running&run_state=failed", ["scheduled"], ["running"]),
        ("run_type=scheduled&run_type=manual", ["scheduled", "scheduled"], ["success", "running"]),
        ("run_type=backfill&run_type=manual", [], []),
        ("run_state=running&run_type=failed&run_type=backfill&run_type=manual", [], []),
        (
            "run_state=running&run_type=failed&run_type=scheduled&run_type=backfill&run_type=manual",
            ["scheduled"],
            ["running"],
        ),
    ]:
        resp = admin_client.get(f"/object/grid_data?dag_id={DAG_ID}&{uri_params}", follow_redirects=True)
        assert resp.status_code == 200, resp.json
        actual_run_types = list(map(lambda x: x["run_type"], resp.json["dag_runs"]))
        actual_run_states = list(map(lambda x: x["state"], resp.json["dag_runs"]))
        assert actual_run_types == expected_run_types
        assert actual_run_states == expected_run_states


# Create this as a fixture so that it is applied before the `dag_with_runs` fixture is!
@pytest.fixture
def freeze_time_for_dagruns(time_machine):
    time_machine.move_to("2022-01-02T00:00:00+00:00", tick=False)


@pytest.mark.usefixtures("freeze_time_for_dagruns")
def test_one_run(admin_client, dag_with_runs: list[DagRun], session):
    """
    Test a DAG with complex interaction of states:
    - One run successful
    - One run partly success, partly running
    - One TI not yet finished
    """
    run1, run2 = dag_with_runs

    for ti in run1.task_instances:
        ti.state = TaskInstanceState.SUCCESS
    for ti in sorted(run2.task_instances, key=lambda ti: (ti.task_id, ti.map_index)):
        if ti.task_id == "task1":
            ti.state = TaskInstanceState.SUCCESS
        elif ti.task_id == "group.mapped":
            if ti.map_index == 0:
                ti.state = TaskInstanceState.SUCCESS
                ti.start_date = pendulum.DateTime(2021, 7, 1, 1, 0, 0, tzinfo=pendulum.UTC)
                ti.end_date = pendulum.DateTime(2021, 7, 1, 1, 2, 3, tzinfo=pendulum.UTC)
            elif ti.map_index == 1:
                ti.state = TaskInstanceState.RUNNING
                ti.start_date = pendulum.DateTime(2021, 7, 1, 2, 3, 4, tzinfo=pendulum.UTC)
                ti.end_date = None

    session.flush()

    resp = admin_client.get(f"/object/grid_data?dag_id={DAG_ID}", follow_redirects=True)

    assert resp.status_code == 200, resp.json

    assert resp.json == {
        "dag_runs": [
            {
                "conf": None,
                "conf_is_json": False,
                "data_interval_end": "2016-01-02T00:00:00+00:00",
                "data_interval_start": "2016-01-01T00:00:00+00:00",
                "end_date": timezone.utcnow().isoformat(),
                "execution_date": "2016-01-01T00:00:00+00:00",
                "external_trigger": False,
                "last_scheduling_decision": None,
                "note": None,
                "queued_at": None,
                "run_id": "run_1",
                "run_type": "scheduled",
                "start_date": "2016-01-01T00:00:00+00:00",
                "state": "success",
                "triggered_by": "test",
            },
            {
                "conf": None,
                "conf_is_json": False,
                "data_interval_end": "2016-01-03T00:00:00+00:00",
                "data_interval_start": "2016-01-02T00:00:00+00:00",
                "end_date": None,
                "execution_date": "2016-01-02T00:00:00+00:00",
                "external_trigger": False,
                "last_scheduling_decision": None,
                "note": None,
                "queued_at": None,
                "run_id": "run_2",
                "run_type": "scheduled",
                "start_date": "2016-01-01T00:00:00+00:00",
                "state": "running",
                "triggered_by": "test",
            },
        ],
        "groups": {
            "children": [
                {
                    "extra_links": [],
                    "has_outlet_datasets": False,
                    "id": "task1",
                    "instances": [
                        {
                            "run_id": "run_1",
                            "queued_dttm": None,
                            "start_date": None,
                            "end_date": None,
                            "note": None,
                            "state": "success",
                            "task_id": "task1",
                            "try_number": 0,
                        },
                        {
                            "run_id": "run_2",
                            "queued_dttm": None,
                            "start_date": None,
                            "end_date": None,
                            "note": None,
                            "state": "success",
                            "task_id": "task1",
                            "try_number": 0,
                        },
                    ],
                    "is_mapped": False,
                    "label": "task1",
                    "operator": "EmptyOperator",
                    "trigger_rule": "all_success",
                },
                {
                    "children": [
                        {
                            "extra_links": [],
                            "has_outlet_datasets": False,
                            "id": "mapped_task_group.subtask2",
                            "instances": [
                                {
                                    "run_id": "run_1",
                                    "mapped_states": {"success": 3},
                                    "queued_dttm": None,
                                    "start_date": None,
                                    "end_date": None,
                                    "state": "success",
                                    "task_id": "mapped_task_group.subtask2",
                                },
                                {
                                    "run_id": "run_2",
                                    "mapped_states": {"no_status": 3},
                                    "queued_dttm": None,
                                    "start_date": None,
                                    "end_date": None,
                                    "state": None,
                                    "task_id": "mapped_task_group.subtask2",
                                },
                            ],
                            "is_mapped": True,
                            "label": "subtask2",
                            "operator": "MockOperator",
                            "trigger_rule": "all_success",
                        }
                    ],
                    "is_mapped": True,
                    "id": "mapped_task_group",
                    "instances": [
                        {
                            "end_date": None,
                            "run_id": "run_1",
                            "mapped_states": {"success": 3},
                            "queued_dttm": None,
                            "start_date": None,
                            "state": "success",
                            "task_id": "mapped_task_group",
                        },
                        {
                            "run_id": "run_2",
                            "queued_dttm": None,
                            "start_date": None,
                            "end_date": None,
                            "state": None,
                            "mapped_states": {"no_status": 3},
                            "task_id": "mapped_task_group",
                        },
                    ],
                    "label": "mapped_task_group",
                    "tooltip": "",
                },
                {
                    "children": [
                        {
                            "extra_links": [],
                            "has_outlet_datasets": False,
                            "id": "group.mapped",
                            "instances": [
                                {
                                    "run_id": "run_1",
                                    "mapped_states": {"success": 4},
                                    "queued_dttm": None,
                                    "start_date": None,
                                    "end_date": None,
                                    "state": "success",
                                    "task_id": "group.mapped",
                                },
                                {
                                    "run_id": "run_2",
                                    "mapped_states": {"no_status": 2, "running": 1, "success": 1},
                                    "queued_dttm": None,
                                    "start_date": "2021-07-01T01:00:00+00:00",
                                    "end_date": "2021-07-01T01:02:03+00:00",
                                    "state": "running",
                                    "task_id": "group.mapped",
                                },
                            ],
                            "is_mapped": True,
                            "label": "mapped",
                            "operator": "MockOperator",
                            "trigger_rule": "all_success",
                        },
                    ],
                    "id": "group",
                    "instances": [
                        {
                            "end_date": None,
                            "run_id": "run_1",
                            "queued_dttm": None,
                            "start_date": None,
                            "state": "success",
                            "task_id": "group",
                        },
                        {
                            "run_id": "run_2",
                            "queued_dttm": None,
                            "start_date": "2021-07-01T01:00:00+00:00",
                            "end_date": "2021-07-01T01:02:03+00:00",
                            "state": "running",
                            "task_id": "group",
                        },
                    ],
                    "label": "group",
                    "tooltip": "",
                },
            ],
            "id": None,
            "instances": [],
            "label": None,
        },
        "ordering": ["data_interval_end", "execution_date"],
        "errors": [],
    }


def test_query_count(dag_with_runs, session):
    run1, run2 = dag_with_runs
    with assert_queries_count(2):
        dag_to_grid(run1.dag, (run1, run2), session)


def test_has_outlet_asset_flag(admin_client, dag_maker, session, app, monkeypatch):
    with monkeypatch.context() as m:
        # Remove global operator links for this test
        m.setattr("airflow.plugins_manager.global_operator_extra_links", [])
        m.setattr("airflow.plugins_manager.operator_extra_links", [])
        m.setattr("airflow.plugins_manager.registered_operator_link_classes", {})

        with dag_maker(dag_id=DAG_ID, serialized=True, session=session):
            lineagefile = File("/tmp/does_not_exist")
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2", outlets=[lineagefile])
            EmptyOperator(task_id="task3", outlets=[Asset("foo"), lineagefile])
            EmptyOperator(task_id="task4", outlets=[Asset("foo")])

        m.setattr(app, "dag_bag", dag_maker.dagbag)
        resp = admin_client.get(f"/object/grid_data?dag_id={DAG_ID}", follow_redirects=True)

    def _expected_task_details(task_id, has_outlet_datasets):
        return {
            "extra_links": [],
            "has_outlet_datasets": has_outlet_datasets,
            "id": task_id,
            "instances": [],
            "is_mapped": False,
            "label": task_id,
            "operator": "EmptyOperator",
            "trigger_rule": "all_success",
        }

    assert resp.status_code == 200, resp.json
    assert resp.json == {
        "dag_runs": [],
        "groups": {
            "children": [
                _expected_task_details("task1", False),
                _expected_task_details("task2", False),
                _expected_task_details("task3", True),
                _expected_task_details("task4", True),
            ],
            "id": None,
            "instances": [],
            "label": None,
        },
        "ordering": ["data_interval_end", "execution_date"],
        "errors": [],
    }


@pytest.mark.need_serialized_dag
def test_next_run_datasets(admin_client, dag_maker, session, app, monkeypatch):
    with monkeypatch.context() as m:
        assets = [Asset(uri=f"s3://bucket/key/{i}") for i in [1, 2]]

        with dag_maker(dag_id=DAG_ID, schedule=assets, serialized=True, session=session):
            EmptyOperator(task_id="task1")

        m.setattr(app, "dag_bag", dag_maker.dagbag)

        asset1_id = session.query(AssetModel.id).filter_by(uri=assets[0].uri).scalar()
        asset2_id = session.query(AssetModel.id).filter_by(uri=assets[1].uri).scalar()
        adrq = AssetDagRunQueue(
            target_dag_id=DAG_ID, dataset_id=asset1_id, created_at=pendulum.DateTime(2022, 8, 2, tzinfo=UTC)
        )
        session.add(adrq)
        asset_events = [
            AssetEvent(
                dataset_id=asset1_id,
                extra={},
                timestamp=pendulum.DateTime(2022, 8, 1, 1, tzinfo=UTC),
            ),
            AssetEvent(
                dataset_id=asset1_id,
                extra={},
                timestamp=pendulum.DateTime(2022, 8, 2, 1, tzinfo=UTC),
            ),
            AssetEvent(
                dataset_id=asset1_id,
                extra={},
                timestamp=pendulum.DateTime(2022, 8, 2, 2, tzinfo=UTC),
            ),
        ]
        session.add_all(asset_events)
        session.commit()

        resp = admin_client.get(f"/object/next_run_datasets/{DAG_ID}", follow_redirects=True)

    assert resp.status_code == 200, resp.json
    assert resp.json == {
        "dataset_expression": {"all": ["s3://bucket/key/1", "s3://bucket/key/2"]},
        "events": [
            {"id": asset1_id, "uri": "s3://bucket/key/1", "lastUpdate": "2022-08-02T02:00:00+00:00"},
            {"id": asset2_id, "uri": "s3://bucket/key/2", "lastUpdate": None},
        ],
    }


def test_next_run_datasets_404(admin_client):
    resp = admin_client.get("/object/next_run_datasets/missingdag", follow_redirects=True)
    assert resp.status_code == 404, resp.json
    assert resp.json == {"error": "can't find dag missingdag"}
