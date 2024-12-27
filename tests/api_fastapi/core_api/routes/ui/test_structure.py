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
from deepdiff import DeepDiff

from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.definitions.asset import Asset, AssetAlias, Dataset

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag_id"
DAG_ID_EXTERNAL_TRIGGER = "external_trigger"


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag():
    # Speed up: We don't want example dags for this module

    return DagBag(include_examples=False, read_dags_from_db=True)


@pytest.fixture(autouse=True)
def clean():
    clear_db_runs()
    yield
    clear_db_runs()


@pytest.fixture
def make_dag(dag_maker, session, time_machine):
    with dag_maker(
        dag_id=DAG_ID_EXTERNAL_TRIGGER,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        TriggerDagRunOperator(task_id="trigger_dag_run_operator", trigger_dag_id=DAG_ID)

    dag_maker.dagbag.sync_to_db()

    with dag_maker(
        dag_id=DAG_ID,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
        schedule=(
            Asset(uri="s3://bucket/next-run-asset/1", name="asset1")
            & Asset(uri="s3://bucket/next-run-asset/2", name="asset2")
            & AssetAlias("example-alias")
        ),
    ):
        (
            EmptyOperator(task_id="task_1", outlets=[Dataset(uri="s3://dataset-bucket/example.csv")])
            >> ExternalTaskSensor(task_id="external_task_sensor", external_dag_id=DAG_ID)
            >> EmptyOperator(task_id="task_2")
        )

    dag_maker.dagbag.sync_to_db()


class TestStructureDataEndpoint:
    @pytest.mark.parametrize(
        "params, expected",
        [
            (
                {"dag_id": DAG_ID},
                {
                    "edges": [
                        {
                            "is_setup_teardown": None,
                            "is_source_asset": None,
                            "label": None,
                            "source_id": "external_task_sensor",
                            "target_id": "task_2",
                        },
                        {
                            "is_setup_teardown": None,
                            "is_source_asset": None,
                            "label": None,
                            "source_id": "task_1",
                            "target_id": "external_task_sensor",
                        },
                    ],
                    "nodes": [
                        {
                            "asset_condition_type": None,
                            "children": None,
                            "id": "task_1",
                            "is_mapped": None,
                            "label": "task_1",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "operator": "EmptyOperator",
                        },
                        {
                            "asset_condition_type": None,
                            "children": None,
                            "id": "external_task_sensor",
                            "is_mapped": None,
                            "label": "external_task_sensor",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "operator": "ExternalTaskSensor",
                        },
                        {
                            "asset_condition_type": None,
                            "children": None,
                            "id": "task_2",
                            "is_mapped": None,
                            "label": "task_2",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "operator": "EmptyOperator",
                        },
                    ],
                    "arrange": "LR",
                },
            ),
            (
                {
                    "dag_id": DAG_ID,
                    "root": "unknown_task",
                },
                {"arrange": "LR", "edges": [], "nodes": []},
            ),
            (
                {
                    "dag_id": DAG_ID,
                    "root": "task_1",
                    "filter_upstream": False,
                    "filter_downstream": False,
                },
                {
                    "arrange": "LR",
                    "edges": [],
                    "nodes": [
                        {
                            "asset_condition_type": None,
                            "children": None,
                            "id": "task_1",
                            "is_mapped": None,
                            "label": "task_1",
                            "operator": "EmptyOperator",
                            "setup_teardown_type": None,
                            "tooltip": None,
                            "type": "task",
                        },
                    ],
                },
            ),
            (
                {
                    "dag_id": DAG_ID,
                    "external_dependencies": True,
                },
                {
                    "edges": [
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "and-gate-0",
                            "target_id": "task_1",
                            "is_source_asset": True,
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "asset1",
                            "target_id": "and-gate-0",
                            "is_source_asset": None,
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "asset2",
                            "target_id": "and-gate-0",
                            "is_source_asset": None,
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "example-alias",
                            "target_id": "and-gate-0",
                            "is_source_asset": None,
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "trigger:external_trigger:test_dag_id:trigger_dag_run_operator",
                            "target_id": "task_1",
                            "is_source_asset": None,
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "sensor:test_dag_id:test_dag_id:external_task_sensor",
                            "target_id": "task_1",
                            "is_source_asset": None,
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "external_task_sensor",
                            "target_id": "task_2",
                            "is_source_asset": None,
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "task_1",
                            "target_id": "external_task_sensor",
                            "is_source_asset": None,
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "task_2",
                            "target_id": "asset:s3://dataset-bucket/example.csv",
                            "is_source_asset": None,
                        },
                    ],
                    "nodes": [
                        {
                            "children": None,
                            "id": "task_1",
                            "is_mapped": None,
                            "label": "task_1",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "operator": "EmptyOperator",
                            "asset_condition_type": None,
                        },
                        {
                            "children": None,
                            "id": "external_task_sensor",
                            "is_mapped": None,
                            "label": "external_task_sensor",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "operator": "ExternalTaskSensor",
                            "asset_condition_type": None,
                        },
                        {
                            "children": None,
                            "id": "task_2",
                            "is_mapped": None,
                            "label": "task_2",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "operator": "EmptyOperator",
                            "asset_condition_type": None,
                        },
                        {
                            "children": None,
                            "id": "trigger:external_trigger:test_dag_id:trigger_dag_run_operator",
                            "is_mapped": None,
                            "label": "trigger_dag_run_operator",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "trigger",
                            "operator": None,
                            "asset_condition_type": None,
                        },
                        {
                            "children": None,
                            "id": "asset:s3://dataset-bucket/example.csv",
                            "is_mapped": None,
                            "label": "s3://dataset-bucket/example.csv",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "asset",
                            "operator": None,
                            "asset_condition_type": None,
                        },
                        {
                            "children": None,
                            "id": "sensor:test_dag_id:test_dag_id:external_task_sensor",
                            "is_mapped": None,
                            "label": "external_task_sensor",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "sensor",
                            "operator": None,
                            "asset_condition_type": None,
                        },
                        {
                            "children": None,
                            "id": "and-gate-0",
                            "is_mapped": None,
                            "label": "and-gate-0",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "asset-condition",
                            "operator": None,
                            "asset_condition_type": "and-gate",
                        },
                        {
                            "children": None,
                            "id": "asset1",
                            "is_mapped": None,
                            "label": "asset1",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "asset",
                            "operator": None,
                            "asset_condition_type": None,
                        },
                        {
                            "children": None,
                            "id": "asset2",
                            "is_mapped": None,
                            "label": "asset2",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "asset",
                            "operator": None,
                            "asset_condition_type": None,
                        },
                        {
                            "children": None,
                            "id": "example-alias",
                            "is_mapped": None,
                            "label": "example-alias",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "asset-alias",
                            "operator": None,
                            "asset_condition_type": None,
                        },
                    ],
                    "arrange": "LR",
                },
            ),
            (
                {"dag_id": DAG_ID_EXTERNAL_TRIGGER, "external_dependencies": True},
                {
                    "edges": [
                        {
                            "is_source_asset": None,
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "trigger_dag_run_operator",
                            "target_id": "trigger:external_trigger:test_dag_id:trigger_dag_run_operator",
                        }
                    ],
                    "nodes": [
                        {
                            "asset_condition_type": None,
                            "children": None,
                            "id": "trigger_dag_run_operator",
                            "is_mapped": None,
                            "label": "trigger_dag_run_operator",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "operator": "TriggerDagRunOperator",
                        },
                        {
                            "asset_condition_type": None,
                            "children": None,
                            "id": "trigger:external_trigger:test_dag_id:trigger_dag_run_operator",
                            "is_mapped": None,
                            "label": "trigger_dag_run_operator",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "trigger",
                            "operator": None,
                        },
                    ],
                    "arrange": "LR",
                },
            ),
        ],
    )
    @pytest.mark.usefixtures("make_dag")
    def test_should_return_200(self, test_client, params, expected):
        response = test_client.get("/ui/structure/structure_data", params=params)
        assert response.status_code == 200
        assert not DeepDiff(response.json(), expected, ignore_order=True)

    def test_should_return_404(self, test_client):
        response = test_client.get("/ui/structure/structure_data", params={"dag_id": "not_existing"})
        assert response.status_code == 404
        assert response.json()["detail"] == "Dag with id not_existing was not found"
