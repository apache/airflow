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

from airflow.models.dagbag import DBDagBag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.definitions.taskgroup import TaskGroup

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_assets, clear_db_runs

pytestmark = pytest.mark.db_test

DAG_ID = "dag_with_multiple_versions"
DAG_ID_LINEAR_DEPTH = "linear_depth_dag"
DAG_ID_NONLINEAR_DEPTH = "nonlinear_depth_dag"
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
            "team": None,
            "operator": "EmptyOperator",
            "asset_condition_type": None,
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
        },
        {
            "children": None,
            "id": "task2",
            "is_mapped": None,
            "label": "task2",
            "tooltip": None,
            "setup_teardown_type": None,
            "type": "task",
            "team": None,
            "operator": "EmptyOperator",
            "asset_condition_type": None,
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
        },
        {
            "children": None,
            "id": "task3",
            "is_mapped": None,
            "label": "task3",
            "tooltip": None,
            "setup_teardown_type": None,
            "type": "task",
            "team": None,
            "operator": "EmptyOperator",
            "asset_condition_type": None,
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
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
def make_dags(dag_maker, session, time_machine) -> None:
    with dag_maker(
        dag_id=DAG_ID,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        (
            EmptyOperator(task_id="task_1")
            >> ExternalTaskSensor(task_id="external_task_sensor", external_dag_id=DAG_ID)
            >> EmptyOperator(task_id="task_2")
        )
    dag_maker.sync_dagbag_to_db()

    # Linear DAG with 5 tasks for depth testing
    with dag_maker(
        dag_id=DAG_ID_LINEAR_DEPTH,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        task_a = EmptyOperator(task_id="task_a")
        task_b = EmptyOperator(task_id="task_b")
        task_c = EmptyOperator(task_id="task_c")
        task_d = EmptyOperator(task_id="task_d")
        task_e = EmptyOperator(task_id="task_e")
        # Linear chain: task_a >> task_b >> task_c >> task_d >> task_e
        task_a >> task_b >> task_c >> task_d >> task_e
    dag_maker.sync_dagbag_to_db()

    # Non-linear DAG for depth testing with branching and merging
    with dag_maker(
        dag_id=DAG_ID_NONLINEAR_DEPTH,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        start = EmptyOperator(task_id="start")
        branch_a = EmptyOperator(task_id="branch_a")
        branch_b = EmptyOperator(task_id="branch_b")
        intermediate = EmptyOperator(task_id="intermediate")
        merge = EmptyOperator(task_id="merge")
        end = EmptyOperator(task_id="end")
        # Non-linear structure
        start >> [branch_a, branch_b]
        branch_a >> intermediate >> merge
        branch_b >> merge
        merge >> end
    dag_maker.sync_dagbag_to_db()


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
                            "label": None,
                            "source_id": "external_task_sensor",
                            "target_id": "task_2",
                        },
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "task_1",
                            "target_id": "external_task_sensor",
                        },
                    ],
                    "nodes": [
                        {
                            "asset_condition_type": None,
                            "ui_color": "#e8f7e4",
                            "ui_fgcolor": "#000",
                            "children": None,
                            "id": "task_1",
                            "is_mapped": None,
                            "label": "task_1",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "team": None,
                            "operator": "EmptyOperator",
                        },
                        {
                            "asset_condition_type": None,
                            "ui_color": "#4db7db",
                            "ui_fgcolor": "#000",
                            "children": None,
                            "id": "external_task_sensor",
                            "is_mapped": None,
                            "label": "external_task_sensor",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "team": None,
                            "operator": "ExternalTaskSensor",
                        },
                        {
                            "asset_condition_type": None,
                            "ui_color": "#e8f7e4",
                            "ui_fgcolor": "#000",
                            "children": None,
                            "id": "task_2",
                            "is_mapped": None,
                            "label": "task_2",
                            "tooltip": None,
                            "setup_teardown_type": None,
                            "type": "task",
                            "team": None,
                            "operator": "EmptyOperator",
                        },
                    ],
                },
                7,
            ),
            (
                {
                    "dag_id": DAG_ID,
                    "root": "unknown_task",
                },
                {"edges": [], "nodes": []},
                7,
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
                            "ui_color": "#e8f7e4",
                            "ui_fgcolor": "#000",
                            "children": None,
                            "id": "task_1",
                            "is_mapped": None,
                            "label": "task_1",
                            "operator": "EmptyOperator",
                            "setup_teardown_type": None,
                            "tooltip": None,
                            "type": "task",
                            "team": None,
                        },
                    ],
                },
                7,
            ),
        ],
    )
    @pytest.mark.usefixtures("make_dags")
    def test_should_return_200(self, test_client, params, expected, expected_queries_count):
        with assert_queries_count(expected_queries_count):
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

    def test_ui_colors_passed_through_to_graph(self, dag_maker, test_client, session):
        """Both raw hex colors and Chakra palette tokens reach the graph unchanged, for operators and groups."""

        class TokenOperator(EmptyOperator):
            ui_color = "blue.500"
            ui_fgcolor = "red.700"

        class HexOperator(EmptyOperator):
            ui_color = "#e8b7e4"
            ui_fgcolor = "#000000"

        with dag_maker(
            dag_id="test_ui_colors_dag",
            serialized=True,
            session=session,
            start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
        ):
            TokenOperator(task_id="token")
            HexOperator(task_id="hex")
            with TaskGroup(group_id="grp", ui_color="teal.400", ui_fgcolor="#ffffff"):
                EmptyOperator(task_id="inner")

        dag_maker.sync_dagbag_to_db()
        response = test_client.get("/structure/structure_data", params={"dag_id": "test_ui_colors_dag"})
        assert response.status_code == 200
        nodes = {node["id"]: node for node in response.json()["nodes"]}

        assert nodes["token"]["ui_color"] == "blue.500"
        assert nodes["token"]["ui_fgcolor"] == "red.700"
        assert nodes["hex"]["ui_color"] == "#e8b7e4"
        assert nodes["hex"]["ui_fgcolor"] == "#000000"
        assert nodes["grp"]["ui_color"] == "teal.400"
        assert nodes["grp"]["ui_fgcolor"] == "#ffffff"

    @pytest.mark.parametrize(
        ("params", "expected_task_ids", "description"),
        [
            pytest.param(
                {"dag_id": DAG_ID_LINEAR_DEPTH, "root": "task_a", "include_downstream": True, "depth": 1},
                ["task_a", "task_b"],
                "depth=1 downstream from task_a should return task_a and task_b only",
                id="downstream_depth_1",
            ),
            pytest.param(
                {"dag_id": DAG_ID_LINEAR_DEPTH, "root": "task_a", "include_downstream": True, "depth": 2},
                ["task_a", "task_b", "task_c"],
                "depth=2 downstream from task_a should return task_a, task_b, and task_c",
                id="downstream_depth_2",
            ),
            pytest.param(
                {"dag_id": DAG_ID_LINEAR_DEPTH, "root": "task_e", "include_upstream": True, "depth": 1},
                ["task_d", "task_e"],
                "depth=1 upstream from task_e should return task_e and task_d only",
                id="upstream_depth_1",
            ),
            pytest.param(
                {"dag_id": DAG_ID_LINEAR_DEPTH, "root": "task_e", "include_upstream": True, "depth": 2},
                ["task_c", "task_d", "task_e"],
                "depth=2 upstream from task_e should return task_e, task_d, and task_c",
                id="upstream_depth_2",
            ),
            pytest.param(
                {
                    "dag_id": DAG_ID_LINEAR_DEPTH,
                    "root": "task_c",
                    "include_upstream": True,
                    "include_downstream": True,
                    "depth": 1,
                },
                ["task_b", "task_c", "task_d"],
                "depth=1 both directions from task_c should return task_b, task_c, and task_d",
                id="both_directions_depth_1",
            ),
            pytest.param(
                {
                    "dag_id": DAG_ID_NONLINEAR_DEPTH,
                    "root": "start",
                    "include_downstream": True,
                    "depth": 1,
                },
                ["branch_a", "branch_b", "start"],
                "depth=1 downstream from start in nonlinear DAG should return start and both branches",
                id="nonlinear_downstream_depth_1",
            ),
            pytest.param(
                {
                    "dag_id": DAG_ID_NONLINEAR_DEPTH,
                    "root": "merge",
                    "include_upstream": True,
                    "depth": 1,
                },
                ["branch_b", "intermediate", "merge"],
                "depth=1 upstream from merge in nonlinear DAG should return merge, branch_b, and intermediate",
                id="nonlinear_upstream_depth_1",
            ),
        ],
    )
    @pytest.mark.usefixtures("make_dags")
    def test_structure_with_depth(self, test_client, params, expected_task_ids, description):
        """Test that depth parameter limits the number of levels returned in various scenarios."""
        response = test_client.get("/structure/structure_data", params=params)
        assert response.status_code == 200
        data = response.json()
        task_ids = sorted([node["id"] for node in data["nodes"]])
        assert task_ids == expected_task_ids, description
