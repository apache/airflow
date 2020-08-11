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


def test_build_task_group():
    execution_date = pendulum.parse("20200101")
    with DAG("test_build_task_group", start_date=execution_date):
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

    root = TaskGroup.build_task_group([task1, task2, task3, task4, task5])
    assert root.parent_group is None
    assert set(root.children.keys()) == {"task1", "group234", "task5"}
    assert group34.group_id == "group234.group34"

    json_dict = task_group_to_dict(root)
    assert json_dict == {
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
                ],
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
