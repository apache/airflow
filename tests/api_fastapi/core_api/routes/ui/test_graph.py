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

from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag_id"


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
        dag_id=DAG_ID,
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    dag_maker.dagbag.sync_to_db()


class TestGraphDataEndpoint:
    @pytest.mark.parametrize(
        "params, expected",
        [
            (
                {"dag_id": DAG_ID},
                {
                    "arrange": "LR",
                    "edges": [
                        {
                            "is_setup_teardown": None,
                            "label": None,
                            "source_id": "task_1",
                            "target_id": "task_2",
                        },
                    ],
                    "nodes": {
                        "children": [
                            {
                                "children": None,
                                "id": "task_1",
                                "value": {
                                    "clusterLabelPos": None,
                                    "isMapped": None,
                                    "label": "task_1",
                                    "labelStyle": "fill:#000;",
                                    "rx": 5,
                                    "ry": 5,
                                    "setupTeardownType": None,
                                    "style": "fill:#e8f7e4;",
                                    "tooltip": None,
                                },
                            },
                            {
                                "children": None,
                                "id": "task_2",
                                "value": {
                                    "clusterLabelPos": None,
                                    "isMapped": None,
                                    "label": "task_2",
                                    "labelStyle": "fill:#000;",
                                    "rx": 5,
                                    "ry": 5,
                                    "setupTeardownType": None,
                                    "style": "fill:#e8f7e4;",
                                    "tooltip": None,
                                },
                            },
                        ],
                        "id": None,
                        "value": {
                            "clusterLabelPos": "top",
                            "isMapped": False,
                            "label": None,
                            "labelStyle": "fill:#000;",
                            "rx": 5,
                            "ry": 5,
                            "setupTeardownType": None,
                            "style": "fill:CornflowerBlue",
                            "tooltip": "",
                        },
                    },
                },
            ),
            (
                {
                    "dag_id": DAG_ID,
                    "root": "unknown_task",
                },
                {
                    "arrange": "LR",
                    "edges": [],
                    "nodes": {
                        "children": [],
                        "id": None,
                        "value": {
                            "clusterLabelPos": "top",
                            "isMapped": False,
                            "label": None,
                            "labelStyle": "fill:#000;",
                            "rx": 5,
                            "ry": 5,
                            "setupTeardownType": None,
                            "style": "fill:CornflowerBlue",
                            "tooltip": "",
                        },
                    },
                },
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
                    "nodes": {
                        "children": [
                            {
                                "children": None,
                                "id": "task_1",
                                "value": {
                                    "clusterLabelPos": None,
                                    "isMapped": None,
                                    "label": "task_1",
                                    "labelStyle": "fill:#000;",
                                    "rx": 5,
                                    "ry": 5,
                                    "setupTeardownType": None,
                                    "style": "fill:#e8f7e4;",
                                    "tooltip": None,
                                },
                            },
                        ],
                        "id": None,
                        "value": {
                            "clusterLabelPos": "top",
                            "isMapped": False,
                            "label": None,
                            "labelStyle": "fill:#000;",
                            "rx": 5,
                            "ry": 5,
                            "setupTeardownType": None,
                            "style": "fill:CornflowerBlue",
                            "tooltip": "",
                        },
                    },
                },
            ),
        ],
    )
    @pytest.mark.usefixtures("make_dag")
    def test_historical_metrics_data(self, test_client, params, expected):
        response = test_client.get("/ui/graph/graph_data", params=params)
        assert response.status_code == 200
        assert response.json() == expected
