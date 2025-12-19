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

import copy

import pendulum
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow._shared.timezones import timezone
from airflow.models.asset import AssetAliasModel, AssetEvent, AssetModel
from airflow.models.dagbag import DBDagBag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Metadata, task
from airflow.sdk.definitions.asset import Asset, AssetAlias, Dataset

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_assets, clear_db_runs

pytestmark = pytest.mark.db_test

DAG_ID = "dag_with_multiple_versions"
DAG_ID_EXTERNAL_TRIGGER = "external_trigger"
DAG_ID_RESOLVED_ASSET_ALIAS = "dag_with_resolved_asset_alias"
LATEST_VERSION_DAG_RESPONSE: dict = {
    "edges": [],
    "nodes": [
        {
            "children": None,
            "id": "task1",
            "is_mapped": None,
            "label": "task1",
            "tooltip": None,
            "setup_teardown_type": None,
            "type": "task",
            "operator": "EmptyOperator",
            "asset_condition_type": None,
        },
        {
            "children": None,
            "id": "task2",
            "is_mapped": None,
            "label": "task2",
            "tooltip": None,
            "setup_teardown_type": None,
            "type": "task",
            "operator": "EmptyOperator",
            "asset_condition_type": None,
        },
        {
            "children": None,
            "id": "task3",
            "is_mapped": None,
            "label": "task3",
            "tooltip": None,
            "setup_teardown_type": None,
            "type": "task",
            "operator": "EmptyOperator",
            "asset_condition_type": None,
        },
    ],
}
SECOND_VERSION_DAG_RESPONSE: dict = copy.deepcopy(LATEST_VERSION_DAG_RESPONSE)
SECOND_VERSION_DAG_RESPONSE["nodes"] = [
    node for node in SECOND_VERSION_DAG_RESPONSE["nodes"] if node["id"] != "task3"
]
FIRST_VERSION_DAG_RESPONSE: dict = copy.deepcopy(SECOND_VERSION_DAG_RESPONSE)
FIRST_VERSION_DAG_RESPONSE["nodes"] = [
    node for node in FIRST_VERSION_DAG_RESPONSE["nodes"] if node["id"] != "task2"
]


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag() -> DBDagBag:
    return DBDagBag()


@pytest.fixture(autouse=True)
def clean():
    clear_db_runs()
    clear_db_assets()
    yield
    clear_db_runs()
    clear_db_assets()


@pytest.fixture
def asset1() -> Asset:
    return Asset(uri="s3://bucket/next-run-asset/1", name="asset1")


@pytest.fixture
def asset2() -> Asset:
    return Asset(uri="s3://bucket/next-run-asset/2", name="asset2")


@pytest.fixture
def asset3() -> Dataset:
    return Dataset(uri="s3://dataset-bucket/example.csv")


@pytest.fixture
def make_dags(dag_maker, session, time_machine, asset1: Asset, asset2: Asset, asset3: Dataset) -> None:
    with dag_maker(
        dag_id=DAG_ID_EXTERNAL_TRIGGER,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        TriggerDagRunOperator(task_id="trigger_dag_run_operator", trigger_dag_id=DAG_ID)
    dag_maker.sync_dagbag_to_db()

    with dag_maker(
        dag_id=DAG_ID,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
        schedule=(asset1 & asset2 & AssetAlias("example-alias")),
    ):
        (
            EmptyOperator(task_id="task_1", outlets=[asset3])
            >> ExternalTaskSensor(task_id="external_task_sensor", external_dag_id=DAG_ID)
            >> EmptyOperator(task_id="task_2")
        )
    dag_maker.sync_dagbag_to_db()

    with dag_maker(
        dag_id=DAG_ID_RESOLVED_ASSET_ALIAS,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):

        @task(outlets=[AssetAlias("example-alias-resolved")])
        def task_1(**context):
            yield Metadata(
                asset=Asset("resolved_example_asset_alias"),
                extra={"k": "v"},  # extra has to be provided, can be {}
                alias=AssetAlias("example-alias-resolved"),
            )

        task_1() >> EmptyOperator(task_id="task_2")

    dr = dag_maker.create_dagrun()
    asset_alias = session.scalar(
        select(AssetAliasModel).where(AssetAliasModel.name == "example-alias-resolved")
    )
    asset_model = AssetModel(name="resolved_example_asset_alias")
    session.add(asset_model)
    session.flush()
    asset_alias.assets.append(asset_model)
    asset_alias.asset_events.append(
        AssetEvent(
            id=1,
            timestamp=timezone.parse("2021-01-01T00:00:00"),
            asset_id=asset_model.id,
            source_dag_id=DAG_ID_RESOLVED_ASSET_ALIAS,
            source_task_id="task_1",
            source_run_id=dr.run_id,
            source_map_index=-1,
        )
    )
    session.commit()
    dag_maker.sync_dagbag_to_db()


def _fetch_asset_id(asset: Asset, session: Session) -> str:
    return str(
        session.scalar(
            select(AssetModel.id).where(AssetModel.name == asset.name, AssetModel.uri == asset.uri)
        )
    )


@pytest.fixture
def asset1_id(make_dags, asset1, session: Session) -> str:
    return _fetch_asset_id(asset1, session)


@pytest.fixture
def asset2_id(make_dags, asset2, session) -> str:
    return _fetch_asset_id(asset2, session)


@pytest.fixture
def asset3_id(make_dags, asset3, session) -> str:
    return _fetch_asset_id(asset3, session)


class TestStructureDataEndpoint:
    @pytest.mark.parametrize(
        ("params", "expected", "expected_queries_count"),
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
                },
                3,
            ),
            (
                {
                    "dag_id": DAG_ID,
                    "root": "unknown_task",
                },
                {"edges": [], "nodes": []},
                3,
            ),
            (
                {
                    "dag_id": DAG_ID,
                    "root": "task_1",
                    "filter_upstream": False,
                    "filter_downstream": False,
                },
                {
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
                3,
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
                            "target_id": "trigger:external_trigger:dag_with_multiple_versions:trigger_dag_run_operator",
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
                            "id": "trigger:external_trigger:dag_with_multiple_versions:trigger_dag_run_operator",
                            "is_mapped": None,
                            "label": "trigger_dag_run_operator",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "trigger",
                            "operator": None,
                        },
                    ],
                },
                10,
            ),
        ],
    )
    @pytest.mark.usefixtures("make_dags")
    def test_should_return_200(self, test_client, params, expected, expected_queries_count):
        with assert_queries_count(expected_queries_count):
            response = test_client.get("/structure/structure_data", params=params)
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.usefixtures("make_dags")
    def test_should_return_200_with_asset(self, test_client, asset1_id, asset2_id, asset3_id):
        params = {
            "dag_id": DAG_ID,
            "external_dependencies": True,
        }
        expected = {
            "edges": [
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
                    "source_id": "and-gate-0",
                    "target_id": "task_1",
                    "is_source_asset": True,
                },
                {
                    "is_setup_teardown": None,
                    "label": None,
                    "source_id": asset1_id,
                    "target_id": "and-gate-0",
                    "is_source_asset": None,
                },
                {
                    "is_setup_teardown": None,
                    "label": None,
                    "source_id": asset2_id,
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
                    "source_id": "sensor:dag_with_multiple_versions:dag_with_multiple_versions:external_task_sensor",
                    "target_id": "task_1",
                    "is_source_asset": None,
                },
                {
                    "is_setup_teardown": None,
                    "label": None,
                    "source_id": "trigger:external_trigger:dag_with_multiple_versions:trigger_dag_run_operator",
                    "target_id": "task_1",
                    "is_source_asset": None,
                },
                {
                    "is_setup_teardown": None,
                    "label": None,
                    "source_id": "task_1",
                    "target_id": f"asset:{asset3_id}",
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
                    "id": f"asset:{asset3_id}",
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
                    "id": "sensor:dag_with_multiple_versions:dag_with_multiple_versions:external_task_sensor",
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
                    "id": "trigger:external_trigger:dag_with_multiple_versions:trigger_dag_run_operator",
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
                    "id": asset1_id,
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
                    "id": asset2_id,
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
        }

        with assert_queries_count(10):
            response = test_client.get("/structure/structure_data", params=params)
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.usefixtures("make_dags")
    def test_should_return_200_with_resolved_asset_alias_attached_to_the_corrrect_producing_task(
        self, test_client, session
    ):
        resolved_asset = session.scalar(
            select(AssetModel).where(AssetModel.name == "resolved_example_asset_alias")
        )
        params = {
            "dag_id": DAG_ID_RESOLVED_ASSET_ALIAS,
            "external_dependencies": True,
        }
        expected = {
            "edges": [
                {
                    "source_id": "task_1",
                    "target_id": "task_2",
                    "is_setup_teardown": None,
                    "label": None,
                    "is_source_asset": None,
                },
                {
                    "source_id": "task_1",
                    "target_id": f"asset:{resolved_asset.id}",
                    "is_setup_teardown": None,
                    "label": None,
                    "is_source_asset": None,
                },
            ],
            "nodes": [
                {
                    "id": "task_1",
                    "label": "task_1",
                    "type": "task",
                    "children": None,
                    "is_mapped": None,
                    "tooltip": None,
                    "setup_teardown_type": None,
                    "operator": "@task",
                    "asset_condition_type": None,
                },
                {
                    "id": "task_2",
                    "label": "task_2",
                    "type": "task",
                    "children": None,
                    "is_mapped": None,
                    "tooltip": None,
                    "setup_teardown_type": None,
                    "operator": "EmptyOperator",
                    "asset_condition_type": None,
                },
                {
                    "id": f"asset:{resolved_asset.id}",
                    "label": "resolved_example_asset_alias",
                    "type": "asset",
                    "children": None,
                    "is_mapped": None,
                    "tooltip": None,
                    "setup_teardown_type": None,
                    "operator": None,
                    "asset_condition_type": None,
                },
            ],
        }

        response = test_client.get("/structure/structure_data", params=params)
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        ("params", "expected"),
        [
            pytest.param(
                {"dag_id": DAG_ID},
                LATEST_VERSION_DAG_RESPONSE,
                id="get_default_version",
            ),
            pytest.param(
                {"dag_id": DAG_ID, "version_number": 1},
                FIRST_VERSION_DAG_RESPONSE,
                id="get_oldest_version",
            ),
            pytest.param(
                {"dag_id": DAG_ID, "version_number": 2},
                SECOND_VERSION_DAG_RESPONSE,
                id="get_specific_version",
            ),
            pytest.param(
                {"dag_id": DAG_ID, "version_number": 3},
                LATEST_VERSION_DAG_RESPONSE,
                id="get_latest_version",
            ),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_should_return_200_with_multiple_versions(self, test_client, params, expected):
        response = test_client.get("/structure/structure_data", params=params)
        assert response.status_code == 200
        assert response.json() == expected

    def test_delete_dag_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/structure/structure_data", params={"dag_id": DAG_ID})
        assert response.status_code == 401

    def test_delete_dag_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/structure/structure_data", params={"dag_id": DAG_ID})
        assert response.status_code == 403

    def test_should_return_404(self, test_client):
        response = test_client.get("/structure/structure_data", params={"dag_id": "not_existing"})
        assert response.status_code == 404
        assert response.json()["detail"] == "Dag with id not_existing was not found"

    def test_should_return_404_when_dag_version_not_found(self, test_client):
        response = test_client.get(
            "/structure/structure_data", params={"dag_id": DAG_ID, "version_number": 999}
        )
        assert response.status_code == 404
        assert (
            response.json()["detail"]
            == "Dag with id dag_with_multiple_versions and version number 999 was not found"
        )

    def test_mapped_operator_graph_view(self, dag_maker, test_client, session):
        """
        Ensures structure_data endpoint handles MappedOperator without AttributeError.
        """
        from airflow.providers.standard.operators.bash import BashOperator

        with dag_maker(
            dag_id="test_mapped_operator_dag",
            serialized=True,
            session=session,
            start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
        ):
            task1 = EmptyOperator(task_id="task1")
            mapped_task = BashOperator.partial(
                task_id="mapped_bash_task",
                do_xcom_push=False,
            ).expand(bash_command=["echo 1", "echo 2", "echo 3"])
            task2 = EmptyOperator(task_id="task2")

            task1 >> mapped_task >> task2

        dag_maker.sync_dagbag_to_db()
        response = test_client.get("/structure/structure_data", params={"dag_id": "test_mapped_operator_dag"})
        assert response.status_code == 200
        data = response.json()

        mapped_node = next(node for node in data["nodes"] if node["id"] == "mapped_bash_task")
        assert mapped_node["is_mapped"] is True
        assert mapped_node["operator"] == "BashOperator"
        assert len(data["edges"]) == 2

    def test_mapped_operator_in_task_group(self, dag_maker, test_client, session):
        """
        Test that mapped operators within task groups are handled correctly.
        Specifically tests task_group_to_dict function with MappedOperator instances.
        """
        from airflow.providers.standard.operators.python import PythonOperator
        from airflow.sdk.definitions.taskgroup import TaskGroup

        with dag_maker(
            dag_id="test_mapped_in_group_dag",
            serialized=True,
            session=session,
            start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
        ):
            with TaskGroup(group_id="processing_group"):
                prep = EmptyOperator(task_id="prep")
                mapped = PythonOperator.partial(
                    task_id="process",
                    python_callable=lambda x: print(f"Processing {x}"),
                ).expand(op_args=[[1], [2], [3], [4]])

                prep >> mapped

        dag_maker.sync_dagbag_to_db()
        response = test_client.get("/structure/structure_data", params={"dag_id": "test_mapped_in_group_dag"})

        assert response.status_code == 200
        data = response.json()
        group_node = next(node for node in data["nodes"] if node["id"] == "processing_group")
        assert group_node["children"] is not None

        mapped_in_group = next(
            child for child in group_node["children"] if child["id"] == "processing_group.process"
        )
        assert mapped_in_group["is_mapped"] is True
        assert mapped_in_group["operator"] == "PythonOperator"
