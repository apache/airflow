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

import pendulum
import pytest
from dateutil.tz import UTC

from airflow.datasets import Dataset
from airflow.decorators import task_group
from airflow.lineage.entities import File
from airflow.models import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.dataset import DatasetDagRunQueue, DatasetEvent, DatasetModel
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType
from airflow.www.views import dag_to_grid
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.db import clear_db_datasets, clear_db_runs
from tests.test_utils.mock_operators import MockOperator

DAG_ID = "test"


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag():
    # Speed up: We don't want example dags for this module
    return DagBag(include_examples=False, read_dags_from_db=True)


@pytest.fixture(autouse=True)
def clean():
    clear_db_runs()
    clear_db_datasets()
    yield
    clear_db_runs()
    clear_db_datasets()


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
        execution_date=dag_without_runs.dag.next_dagrun_info(date).logical_date,
    )

    yield run_1, run_2


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
    }


# Create this as a fixture so that it is applied before the `dag_with_runs` fixture is!
@pytest.fixture
def freeze_time_for_dagruns(time_machine):
    time_machine.move_to("2022-01-02T00:00:00+00:00", tick=False)
    yield


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
                            "start_date": None,
                            "end_date": None,
                            "note": None,
                            "state": "success",
                            "task_id": "task1",
                            "try_number": 0,
                        },
                        {
                            "run_id": "run_2",
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
                                    "start_date": None,
                                    "end_date": None,
                                    "state": "success",
                                    "task_id": "mapped_task_group.subtask2",
                                },
                                {
                                    "run_id": "run_2",
                                    "mapped_states": {"no_status": 3},
                                    "start_date": None,
                                    "end_date": None,
                                    "state": None,
                                    "task_id": "mapped_task_group.subtask2",
                                },
                            ],
                            "is_mapped": True,
                            "label": "subtask2",
                            "operator": "MockOperator",
                        }
                    ],
                    "is_mapped": True,
                    "id": "mapped_task_group",
                    "instances": [
                        {
                            "end_date": None,
                            "run_id": "run_1",
                            "mapped_states": {"success": 3},
                            "start_date": None,
                            "state": "success",
                            "task_id": "mapped_task_group",
                        },
                        {
                            "run_id": "run_2",
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
                                    "start_date": None,
                                    "end_date": None,
                                    "state": "success",
                                    "task_id": "group.mapped",
                                },
                                {
                                    "run_id": "run_2",
                                    "mapped_states": {"no_status": 2, "running": 1, "success": 1},
                                    "start_date": "2021-07-01T01:00:00+00:00",
                                    "end_date": "2021-07-01T01:02:03+00:00",
                                    "state": "running",
                                    "task_id": "group.mapped",
                                },
                            ],
                            "is_mapped": True,
                            "label": "mapped",
                            "operator": "MockOperator",
                        },
                    ],
                    "id": "group",
                    "instances": [
                        {
                            "end_date": None,
                            "run_id": "run_1",
                            "start_date": None,
                            "state": "success",
                            "task_id": "group",
                        },
                        {
                            "run_id": "run_2",
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
    }


def test_query_count(dag_with_runs, session):
    run1, run2 = dag_with_runs
    with assert_queries_count(2):
        dag_to_grid(run1.dag, (run1, run2), session)


def test_has_outlet_dataset_flag(admin_client, dag_maker, session, app, monkeypatch):
    with monkeypatch.context() as m:
        # Remove global operator links for this test
        m.setattr("airflow.plugins_manager.global_operator_extra_links", [])
        m.setattr("airflow.plugins_manager.operator_extra_links", [])
        m.setattr("airflow.plugins_manager.registered_operator_link_classes", {})

        with dag_maker(dag_id=DAG_ID, serialized=True, session=session):
            lineagefile = File("/tmp/does_not_exist")
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2", outlets=[lineagefile])
            EmptyOperator(task_id="task3", outlets=[Dataset("foo"), lineagefile])
            EmptyOperator(task_id="task4", outlets=[Dataset("foo")])

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
    }


@pytest.mark.need_serialized_dag
def test_next_run_datasets(admin_client, dag_maker, session, app, monkeypatch):
    with monkeypatch.context() as m:
        datasets = [Dataset(uri=f"s3://bucket/key/{i}") for i in [1, 2]]

        with dag_maker(dag_id=DAG_ID, schedule=datasets, serialized=True, session=session):
            EmptyOperator(task_id="task1")

        m.setattr(app, "dag_bag", dag_maker.dagbag)

        ds1_id = session.query(DatasetModel.id).filter_by(uri=datasets[0].uri).scalar()
        ds2_id = session.query(DatasetModel.id).filter_by(uri=datasets[1].uri).scalar()
        ddrq = DatasetDagRunQueue(
            target_dag_id=DAG_ID, dataset_id=ds1_id, created_at=pendulum.DateTime(2022, 8, 2, tzinfo=UTC)
        )
        session.add(ddrq)
        dataset_events = [
            DatasetEvent(
                dataset_id=ds1_id,
                extra={},
                timestamp=pendulum.DateTime(2022, 8, 1, 1, tzinfo=UTC),
            ),
            DatasetEvent(
                dataset_id=ds1_id,
                extra={},
                timestamp=pendulum.DateTime(2022, 8, 2, 1, tzinfo=UTC),
            ),
            DatasetEvent(
                dataset_id=ds1_id,
                extra={},
                timestamp=pendulum.DateTime(2022, 8, 2, 2, tzinfo=UTC),
            ),
        ]
        session.add_all(dataset_events)
        session.commit()

        resp = admin_client.get(f"/object/next_run_datasets/{DAG_ID}", follow_redirects=True)

    assert resp.status_code == 200, resp.json
    assert resp.json == [
        {"id": ds1_id, "uri": "s3://bucket/key/1", "lastUpdate": "2022-08-02T02:00:00+00:00"},
        {"id": ds2_id, "uri": "s3://bucket/key/2", "lastUpdate": None},
    ]


def test_next_run_datasets_404(admin_client):
    resp = admin_client.get("/object/next_run_datasets/missingdag", follow_redirects=True)
    assert resp.status_code == 404, resp.json
    assert resp.json == {"error": "can't find dag missingdag"}
