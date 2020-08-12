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

import pendulum

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.www.views import task_group_to_dict

EXPECTED_JSON = {
    "id": None,
    "value": {
        "label": None,
        "labelStyle": "fill:#000;",
        "style": "fill:CornflowerBlue",
        "rx": 5,
        "ry": 5,
        "clusterLabelPos": "top",
    },
    "children": [
        {
            "id": "group234",
            "value": {
                "label": "group234",
                "labelStyle": "fill:#000;",
                "style": "fill:CornflowerBlue",
                "rx": 5,
                "ry": 5,
                "clusterLabelPos": "top",
            },
            "children": [
                {
                    "id": "group234.group34",
                    "value": {
                        "label": "group34",
                        "labelStyle": "fill:#000;",
                        "style": "fill:CornflowerBlue",
                        "rx": 5,
                        "ry": 5,
                        "clusterLabelPos": "top",
                    },
                    "children": [
                        {
                            "id": "group234.group34.task3",
                            "value": {
                                "label": "task3",
                                "labelStyle": "fill:#000;",
                                "style": "fill:#e8f7e4;",
                                "rx": 5,
                                "ry": 5,
                            },
                        },
                        {
                            "id": "group234.group34.task4",
                            "value": {
                                "label": "task4",
                                "labelStyle": "fill:#000;",
                                "style": "fill:#e8f7e4;",
                                "rx": 5,
                                "ry": 5,
                            },
                        },
                    ],
                },
                {
                    "id": "group234.task2",
                    "value": {
                        "label": "task2",
                        "labelStyle": "fill:#000;",
                        "style": "fill:#e8f7e4;",
                        "rx": 5,
                        "ry": 5,
                    },
                },
            ],
        },
        {
            "id": "task1",
            "value": {
                "label": "task1",
                "labelStyle": "fill:#000;",
                "style": "fill:#e8f7e4;",
                "rx": 5,
                "ry": 5,
            },
        },
        {
            "id": "task5",
            "value": {
                "label": "task5",
                "labelStyle": "fill:#000;",
                "style": "fill:#e8f7e4;",
                "rx": 5,
                "ry": 5,
            },
        },
    ],
}


def test_build_task_group_context_manager():
    execution_date = pendulum.parse("20200101")
    with DAG("test_build_task_group_context_manager", start_date=execution_date) as dag:
        task1 = DummyOperator(task_id="task1")
        with TaskGroup("group234") as group234:
            task2 = DummyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                task3 = DummyOperator(task_id="task3")
                task4 = DummyOperator(task_id="task4")

        task5 = DummyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    assert task1.task_id == "task1"
    assert task2.task_id == "group234.task2"
    assert task3.task_id == "group234.group34.task3"
    assert task3.label == "task3"
    assert task4.task_id == "group234.group34.task4"
    assert task4.label == "task4"
    assert task5.task_id == "task5"
    assert task5.label == "task5"
    assert task1.get_direct_relative_ids(upstream=False) == {
        "group234.task2",
        "group234.group34.task3",
        "group234.group34.task4",
    }
    assert task5.get_direct_relative_ids(upstream=True) == {
        "group234.group34.task3",
        "group234.group34.task4",
    }

    assert dag.task_group.parent_group is None
    assert set(dag.task_group.children.keys()) == {"task1", "group234", "task5"}
    assert group34.group_id == "group234.group34"

    assert task_group_to_dict(dag.task_group) == EXPECTED_JSON


def test_build_task_group():
    """
    This is an alternative syntax to use TaskGroup. It should result in the same TaskGroup
    as using context manager.
    """
    execution_date = pendulum.parse("20200101")
    dag = DAG("test_build_task_group", start_date=execution_date)
    task1 = DummyOperator(task_id="task1", dag=dag)
    group234 = TaskGroup("group234", dag=dag)
    _ = DummyOperator(task_id="task2", dag=dag, task_group=group234)
    group34 = TaskGroup("group34", dag=dag, parent_group=group234)
    _ = DummyOperator(task_id="task3", dag=dag, task_group=group34)
    _ = DummyOperator(task_id="task4", dag=dag, task_group=group34)
    task5 = DummyOperator(task_id="task5", dag=dag)

    task1 >> group234
    group34 >> task5

    assert task_group_to_dict(dag.task_group) == EXPECTED_JSON


def test_sub_dag_task_group():
    execution_date = pendulum.parse("20200101")
    with DAG("test_test_task_group_sub_dag", start_date=execution_date) as dag:
        task1 = DummyOperator(task_id="task1")
        with TaskGroup("group234") as group234:
            task2 = DummyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                task3 = DummyOperator(task_id="task3")
                task4 = DummyOperator(task_id="task4")

        task5 = DummyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    subdag = dag.sub_dag(
        task_regex="task2", include_upstream=True, include_downstream=False
    )

    assert task_group_to_dict(subdag.task_group) == {
        "id": None,
        "value": {
            "label": None,
            "labelStyle": "fill:#000;",
            "style": "fill:CornflowerBlue",
            "rx": 5,
            "ry": 5,
            "clusterLabelPos": "top",
        },
        "children": [
            {
                "id": "group234",
                "value": {
                    "label": "group234",
                    "labelStyle": "fill:#000;",
                    "style": "fill:CornflowerBlue",
                    "rx": 5,
                    "ry": 5,
                    "clusterLabelPos": "top",
                },
                "children": [
                    {
                        "id": "group234.task2",
                        "value": {
                            "label": "task2",
                            "labelStyle": "fill:#000;",
                            "style": "fill:#e8f7e4;",
                            "rx": 5,
                            "ry": 5,
                        },
                    },
                ],
            },
            {
                "id": "task1",
                "value": {
                    "label": "task1",
                    "labelStyle": "fill:#000;",
                    "style": "fill:#e8f7e4;",
                    "rx": 5,
                    "ry": 5,
                },
            },
        ],
    }

