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
import pytest

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.www.views import dag_edges, task_group_to_dict

EXPECTED_JSON = {
    'id': None,
    'value': {
        'label': None,
        'labelStyle': 'fill:#000;',
        'style': 'fill:CornflowerBlue',
        'rx': 5,
        'ry': 5,
        'clusterLabelPos': 'top',
    },
    'tooltip': '',
    'children': [
        {
            'id': 'group234',
            'value': {
                'label': 'group234',
                'labelStyle': 'fill:#000;',
                'style': 'fill:CornflowerBlue',
                'rx': 5,
                'ry': 5,
                'clusterLabelPos': 'top',
            },
            'tooltip': '',
            'children': [
                {
                    'id': 'group34',
                    'value': {
                        'label': 'group34',
                        'labelStyle': 'fill:#000;',
                        'style': 'fill:CornflowerBlue',
                        'rx': 5,
                        'ry': 5,
                        'clusterLabelPos': 'top',
                    },
                    'tooltip': '',
                    'children': [
                        {
                            'id': 'task3',
                            'value': {
                                'label': 'task3',
                                'labelStyle': 'fill:#000;',
                                'style': 'fill:#e8f7e4;',
                                'rx': 5,
                                'ry': 5,
                            },
                        },
                        {
                            'id': 'task4',
                            'value': {
                                'label': 'task4',
                                'labelStyle': 'fill:#000;',
                                'style': 'fill:#e8f7e4;',
                                'rx': 5,
                                'ry': 5,
                            },
                        },
                        {
                            'id': 'group34_downstream_join_id',
                            'value': {
                                'label': '',
                                'labelStyle': 'fill:#000;',
                                'style': 'fill:CornflowerBlue;',
                                'shape': 'circle',
                            },
                        },
                    ],
                },
                {
                    'id': 'task2',
                    'value': {
                        'label': 'task2',
                        'labelStyle': 'fill:#000;',
                        'style': 'fill:#e8f7e4;',
                        'rx': 5,
                        'ry': 5,
                    },
                },
                {
                    'id': 'group234_upstream_join_id',
                    'value': {
                        'label': '',
                        'labelStyle': 'fill:#000;',
                        'style': 'fill:CornflowerBlue;',
                        'shape': 'circle',
                    },
                },
            ],
        },
        {
            'id': 'task1',
            'value': {
                'label': 'task1',
                'labelStyle': 'fill:#000;',
                'style': 'fill:#e8f7e4;',
                'rx': 5,
                'ry': 5,
            },
        },
        {
            'id': 'task5',
            'value': {
                'label': 'task5',
                'labelStyle': 'fill:#000;',
                'style': 'fill:#e8f7e4;',
                'rx': 5,
                'ry': 5,
            },
        },
    ],
}


def test_build_task_group_context_manager():
    execution_date = pendulum.parse("20200101")
    with DAG("test_build_task_group_context_manager", start_date=execution_date) as dag:
        task1 = DummyOperator(task_id="task1")
        with TaskGroup("group234") as group234:
            _ = DummyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                _ = DummyOperator(task_id="task3")
                _ = DummyOperator(task_id="task4")

        task5 = DummyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    assert task1.get_direct_relative_ids(upstream=False) == {
        "task2",
        "task3",
        "task4",
    }
    assert task5.get_direct_relative_ids(upstream=True) == {
        "task3",
        "task4",
    }

    assert dag.task_group.group_id is None
    assert dag.task_group.is_root
    assert set(dag.task_group.children.keys()) == {"task1", "group234", "task5"}
    assert group34.group_id == "group34"

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
    """
    Tests dag.sub_dag() updates task_group correctly.
    """
    execution_date = pendulum.parse("20200101")
    with DAG("test_test_task_group_sub_dag", start_date=execution_date) as dag:
        task1 = DummyOperator(task_id="task1")
        with TaskGroup("group234") as group234:
            _ = DummyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                _ = DummyOperator(task_id="task3")
                _ = DummyOperator(task_id="task4")

        task5 = DummyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    subdag = dag.sub_dag(task_regex="task2", include_upstream=True, include_downstream=False)

    assert task_group_to_dict(subdag.task_group) == {
        'id': None,
        'value': {
            'label': None,
            'labelStyle': 'fill:#000;',
            'style': 'fill:CornflowerBlue',
            'rx': 5,
            'ry': 5,
            'clusterLabelPos': 'top',
        },
        'tooltip': '',
        'children': [
            {
                'id': 'group234',
                'value': {
                    'label': 'group234',
                    'labelStyle': 'fill:#000;',
                    'style': 'fill:CornflowerBlue',
                    'rx': 5,
                    'ry': 5,
                    'clusterLabelPos': 'top',
                },
                'tooltip': '',
                'children': [
                    {
                        'id': 'task2',
                        'value': {
                            'label': 'task2',
                            'labelStyle': 'fill:#000;',
                            'style': 'fill:#e8f7e4;',
                            'rx': 5,
                            'ry': 5,
                        },
                    },
                    {
                        'id': 'group234_upstream_join_id',
                        'value': {
                            'label': '',
                            'labelStyle': 'fill:#000;',
                            'style': 'fill:CornflowerBlue;',
                            'shape': 'circle',
                        },
                    },
                ],
            },
            {
                'id': 'task1',
                'value': {
                    'label': 'task1',
                    'labelStyle': 'fill:#000;',
                    'style': 'fill:#e8f7e4;',
                    'rx': 5,
                    'ry': 5,
                },
            },
        ],
    }


def extract_node_id(node):
    ret = {"id": node["id"]}
    if "children" in node:
        children = []
        for child in node["children"]:
            children.append(extract_node_id(child))

        ret["children"] = children

    return ret


def test_dag_edges():
    execution_date = pendulum.parse("20200101")
    with DAG("test_dag_edges", start_date=execution_date) as dag:
        task1 = DummyOperator(task_id="task1")
        with TaskGroup("group_a") as group_a:
            with TaskGroup("group_b") as group_b:
                task2 = DummyOperator(task_id="task2")
                task3 = DummyOperator(task_id="task3")
                task4 = DummyOperator(task_id="task4")
                task2 >> [task3, task4]

            task5 = DummyOperator(task_id="task5")

            task5 << group_b

        task1 >> group_a

        with TaskGroup("group_c") as group_c:
            task6 = DummyOperator(task_id="task6")
            task7 = DummyOperator(task_id="task7")
            task8 = DummyOperator(task_id="task8")
            [task6, task7] >> task8
            group_a >> group_c

        task5 >> task8

        task9 = DummyOperator(task_id="task9")
        task10 = DummyOperator(task_id="task10")

        group_c >> [task9, task10]

        with TaskGroup("group_d") as group_d:
            task11 = DummyOperator(task_id="task11")
            task12 = DummyOperator(task_id="task12")
            task11 >> task12

        group_d << group_c

    nodes = task_group_to_dict(dag.task_group)
    edges = dag_edges(dag)

    assert extract_node_id(nodes) == {
        'id': None,
        'children': [
            {
                'id': 'group_a',
                'children': [
                    {
                        'id': 'group_b',
                        'children': [
                            {'id': 'task2'},
                            {'id': 'task3'},
                            {'id': 'task4'},
                            {'id': 'group_b_downstream_join_id'},
                        ],
                    },
                    {'id': 'task5'},
                    {'id': 'group_a_upstream_join_id'},
                    {'id': 'group_a_downstream_join_id'},
                ],
            },
            {
                'id': 'group_c',
                'children': [
                    {'id': 'task6'},
                    {'id': 'task7'},
                    {'id': 'task8'},
                    {'id': 'group_c_upstream_join_id'},
                    {'id': 'group_c_downstream_join_id'},
                ],
            },
            {
                'id': 'group_d',
                'children': [{'id': 'task11'}, {'id': 'task12'}, {'id': 'group_d_upstream_join_id'},],
            },
            {'id': 'task1'},
            {'id': 'task10'},
            {'id': 'task9'},
        ],
    }

    assert sorted((e["source_id"], e["target_id"]) for e in edges) == [
        ('group_a_downstream_join_id', 'group_c_upstream_join_id'),
        ('group_a_upstream_join_id', 'task2'),
        ('group_b_downstream_join_id', 'task5'),
        ('group_c_downstream_join_id', 'group_d_upstream_join_id'),
        ('group_c_downstream_join_id', 'task10'),
        ('group_c_downstream_join_id', 'task9'),
        ('group_c_upstream_join_id', 'task6'),
        ('group_c_upstream_join_id', 'task7'),
        ('group_d_upstream_join_id', 'task11'),
        ('task1', 'group_a_upstream_join_id'),
        ('task11', 'task12'),
        ('task2', 'task3'),
        ('task2', 'task4'),
        ('task3', 'group_b_downstream_join_id'),
        ('task4', 'group_b_downstream_join_id'),
        ('task5', 'group_a_downstream_join_id'),
        ('task5', 'task8'),
        ('task6', 'task8'),
        ('task7', 'task8'),
        ('task8', 'group_c_downstream_join_id'),
    ]


def test_duplicate_group_id():
    from airflow.exceptions import DuplicateTaskIdFound

    execution_date = pendulum.parse("20200101")

    with pytest.raises(DuplicateTaskIdFound, match=r".* 'task1' .*"):
        with DAG("test_duplicate_group_id", start_date=execution_date):
            _ = DummyOperator(task_id="task1")
            with TaskGroup("task1"):
                pass

    with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1' .*"):
        with DAG("test_duplicate_group_id", start_date=execution_date):
            _ = DummyOperator(task_id="task1")
            with TaskGroup("group1"):
                with TaskGroup("group1"):
                    pass

    with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1' .*"):
        with DAG("test_duplicate_group_id", start_date=execution_date):
            with TaskGroup("group1"):
                _ = DummyOperator(task_id="group1")

    with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1_downstream_join_id' .*"):
        with DAG("test_duplicate_group_id", start_date=execution_date):
            _ = DummyOperator(task_id="task1")
            with TaskGroup("group1"):
                _ = DummyOperator(task_id="group1_downstream_join_id")

    with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1_upstream_join_id' .*"):
        with DAG("test_duplicate_group_id", start_date=execution_date):
            _ = DummyOperator(task_id="task1")
            with TaskGroup("group1"):
                _ = DummyOperator(task_id="group1_upstream_join_id")
