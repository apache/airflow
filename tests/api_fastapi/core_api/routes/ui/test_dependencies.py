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
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.definitions.asset import Asset

from tests_common.test_utils.db import clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def cleanup():
    clear_db_dags()
    clear_db_serialized_dags()


@pytest.fixture
def make_primary_connected_component(dag_maker):
    with dag_maker(
        dag_id="external_trigger_dag_id",
        serialized=True,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        TriggerDagRunOperator(task_id="trigger_dag_run_operator", trigger_dag_id="downstream")

    with dag_maker(dag_id="other_dag", serialized=True):
        EmptyOperator(task_id="task1")

    asset = Asset(uri="s3://bucket/next-run-asset/1", name="asset1")
    with dag_maker(dag_id="upstream", serialized=True):
        EmptyOperator(task_id="task2", outlets=[asset])

    with dag_maker(
        dag_id="downstream",
        schedule=[asset],
        serialized=True,
    ):
        EmptyOperator(task_id="task1") >> ExternalTaskSensor(
            task_id="external_task_sensor", external_dag_id="other_dag"
        )

    dag_maker.sync_dagbag_to_db()


@pytest.fixture
def expected_primary_component_response():
    return {
        "edges": [
            {
                "source_id": "asset:asset1",
                "target_id": "dag:downstream",
            },
            {
                "source_id": "dag:external_trigger_dag_id",
                "target_id": "trigger:external_trigger_dag_id:downstream:trigger_dag_run_operator",
            },
            {
                "source_id": "dag:other_dag",
                "target_id": "sensor:other_dag:downstream:external_task_sensor",
            },
            {
                "source_id": "dag:upstream",
                "target_id": "asset:asset1",
            },
            {
                "source_id": "sensor:other_dag:downstream:external_task_sensor",
                "target_id": "dag:downstream",
            },
            {
                "source_id": "trigger:external_trigger_dag_id:downstream:trigger_dag_run_operator",
                "target_id": "dag:downstream",
            },
        ],
        "nodes": [
            {
                "id": "dag:downstream",
                "label": "downstream",
                "type": "dag",
            },
            {
                "id": "asset:asset1",
                "label": "asset1",
                "type": "asset",
            },
            {
                "id": "sensor:other_dag:downstream:external_task_sensor",
                "label": "external_task_sensor",
                "type": "sensor",
            },
            {
                "id": "dag:external_trigger_dag_id",
                "label": "external_trigger_dag_id",
                "type": "dag",
            },
            {
                "id": "trigger:external_trigger_dag_id:downstream:trigger_dag_run_operator",
                "label": "trigger_dag_run_operator",
                "type": "trigger",
            },
            {
                "id": "dag:upstream",
                "label": "upstream",
                "type": "dag",
            },
        ],
    }


@pytest.fixture
def make_secondary_connected_component(dag_maker):
    asset = Asset(uri="s3://bucket/next-run-asset/2", name="asset2")

    with dag_maker(dag_id="upstream_secondary", serialized=True):
        EmptyOperator(task_id="task2", outlets=[asset])

    with dag_maker(
        dag_id="downstream_secondary",
        schedule=[asset],
        serialized=True,
    ):
        EmptyOperator(task_id="task1")

    dag_maker.sync_dagbag_to_db()


@pytest.fixture
def expected_secondary_component_response():
    return {
        "edges": [
            {
                "source_id": "asset:asset2",
                "target_id": "dag:downstream_secondary",
            },
            {
                "source_id": "dag:upstream_secondary",
                "target_id": "asset:asset2",
            },
        ],
        "nodes": [
            {
                "id": "dag:downstream_secondary",
                "label": "downstream_secondary",
                "type": "dag",
            },
            {
                "id": "asset:asset2",
                "label": "asset2",
                "type": "asset",
            },
            {
                "id": "dag:upstream_secondary",
                "label": "upstream_secondary",
                "type": "dag",
            },
        ],
    }


@pytest.mark.usefixtures("make_primary_connected_component")
def test_get_dependencies(test_client, expected_primary_component_response):
    response = test_client.get("/ui/dependencies")
    assert response.status_code == 200

    assert response.json() == expected_primary_component_response


@pytest.mark.parametrize(
    "node_id, expected_response_fixture",
    [
        # Primary Component
        ("asset:asset1", "expected_primary_component_response"),
        ("dag:downstream", "expected_primary_component_response"),
        ("sensor:other_dag:downstream:external_task_sensor", "expected_primary_component_response"),
        ("dag:external_trigger_dag_id", "expected_primary_component_response"),
        (
            "trigger:external_trigger_dag_id:downstream:trigger_dag_run_operator",
            "expected_primary_component_response",
        ),
        ("dag:upstream", "expected_primary_component_response"),
        # Secondary Component
        ("asset:asset2", "expected_secondary_component_response"),
        ("dag:downstream_secondary", "expected_secondary_component_response"),
        ("dag:upstream_secondary", "expected_secondary_component_response"),
    ],
)
@pytest.mark.usefixtures("make_primary_connected_component", "make_secondary_connected_component")
def test_get_dependencies_with_node_id_filter(test_client, node_id, expected_response_fixture, request):
    expected_response = request.getfixturevalue(expected_response_fixture)
    response = test_client.get("/ui/dependencies", params={"node_id": node_id})
    assert response.status_code == 200

    assert response.json() == expected_response


@pytest.mark.usefixtures("make_primary_connected_component", "make_secondary_connected_component")
def test_get_dependencies_with_node_id_filter_not_found(test_client):
    response = test_client.get("/ui/dependencies", params={"node_id": "missing_node_id"})
    assert response.status_code == 404

    assert response.json() == {
        "detail": "Unique connected component not found, got [] for connected components of node missing_node_id, expected only 1 connected component.",
    }
