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

from airflow.decorators import task_group as task_group_decorator
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
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
                    'id': 'group234.group34',
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
                            'id': 'group234.group34.task3',
                            'value': {
                                'label': 'task3',
                                'labelStyle': 'fill:#000;',
                                'style': 'fill:#e8f7e4;',
                                'rx': 5,
                                'ry': 5,
                            },
                        },
                        {
                            'id': 'group234.group34.task4',
                            'value': {
                                'label': 'task4',
                                'labelStyle': 'fill:#000;',
                                'style': 'fill:#e8f7e4;',
                                'rx': 5,
                                'ry': 5,
                            },
                        },
                        {
                            'id': 'group234.group34.downstream_join_id',
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
                    'id': 'group234.task2',
                    'value': {
                        'label': 'task2',
                        'labelStyle': 'fill:#000;',
                        'style': 'fill:#e8f7e4;',
                        'rx': 5,
                        'ry': 5,
                    },
                },
                {
                    'id': 'group234.upstream_join_id',
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
        'group234.group34.task4',
        'group234.group34.task3',
        'group234.task2',
    }
    assert task5.get_direct_relative_ids(upstream=True) == {
        'group234.group34.task4',
        'group234.group34.task3',
    }

    assert dag.task_group.group_id is None
    assert dag.task_group.is_root
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


def extract_node_id(node, include_label=False):
    ret = {"id": node["id"]}
    if include_label:
        ret["label"] = node["value"]["label"]
    if "children" in node:
        children = []
        for child in node["children"]:
            children.append(extract_node_id(child, include_label=include_label))

        ret["children"] = children

    return ret


def test_build_task_group_with_prefix():
    """
    Tests that prefix_group_id turns on/off prefixing of task_id with group_id.
    """
    execution_date = pendulum.parse("20200101")
    with DAG("test_build_task_group_with_prefix", start_date=execution_date) as dag:
        task1 = DummyOperator(task_id="task1")
        with TaskGroup("group234", prefix_group_id=False) as group234:
            task2 = DummyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                task3 = DummyOperator(task_id="task3")

                with TaskGroup("group4", prefix_group_id=False) as group4:
                    task4 = DummyOperator(task_id="task4")

        task5 = DummyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    assert task2.task_id == "task2"
    assert group34.group_id == "group34"
    assert task3.task_id == "group34.task3"
    assert group4.group_id == "group34.group4"
    assert task4.task_id == "task4"
    assert task5.task_id == "task5"
    assert group234.get_child_by_label("task2") == task2
    assert group234.get_child_by_label("group34") == group34
    assert group4.get_child_by_label("task4") == task4

    assert extract_node_id(task_group_to_dict(dag.task_group), include_label=True) == {
        'id': None,
        'label': None,
        'children': [
            {
                'id': 'group234',
                'label': 'group234',
                'children': [
                    {
                        'id': 'group34',
                        'label': 'group34',
                        'children': [
                            {
                                'id': 'group34.group4',
                                'label': 'group4',
                                'children': [{'id': 'task4', 'label': 'task4'}],
                            },
                            {'id': 'group34.task3', 'label': 'task3'},
                            {'id': 'group34.downstream_join_id', 'label': ''},
                        ],
                    },
                    {'id': 'task2', 'label': 'task2'},
                    {'id': 'group234.upstream_join_id', 'label': ''},
                ],
            },
            {'id': 'task1', 'label': 'task1'},
            {'id': 'task5', 'label': 'task5'},
        ],
    }


def test_build_task_group_with_task_decorator():
    """
    Test that TaskGroup can be used with the @task decorator.
    """
    from airflow.decorators import task

    @task
    def task_1():
        print("task_1")

    @task
    def task_2():
        return "task_2"

    @task
    def task_3():
        return "task_3"

    @task
    def task_4(task_2_output, task_3_output):
        print(task_2_output, task_3_output)

    @task
    def task_5():
        print("task_5")

    execution_date = pendulum.parse("20200101")
    with DAG("test_build_task_group_with_task_decorator", start_date=execution_date) as dag:
        tsk_1 = task_1()

        with TaskGroup("group234") as group234:
            tsk_2 = task_2()
            tsk_3 = task_3()
            tsk_4 = task_4(tsk_2, tsk_3)

        tsk_5 = task_5()

        tsk_1 >> group234 >> tsk_5

    # pylint: disable=no-member
    assert tsk_1.operator in tsk_2.operator.upstream_list
    assert tsk_1.operator in tsk_3.operator.upstream_list
    assert tsk_5.operator in tsk_4.operator.downstream_list
    # pylint: enable=no-member

    assert extract_node_id(task_group_to_dict(dag.task_group)) == {
        'id': None,
        'children': [
            {
                'id': 'group234',
                'children': [
                    {'id': 'group234.task_2'},
                    {'id': 'group234.task_3'},
                    {'id': 'group234.task_4'},
                    {'id': 'group234.upstream_join_id'},
                    {'id': 'group234.downstream_join_id'},
                ],
            },
            {'id': 'task_1'},
            {'id': 'task_5'},
        ],
    }

    edges = dag_edges(dag)
    assert sorted((e["source_id"], e["target_id"]) for e in edges) == [
        ('group234.downstream_join_id', 'task_5'),
        ('group234.task_2', 'group234.task_4'),
        ('group234.task_3', 'group234.task_4'),
        ('group234.task_4', 'group234.downstream_join_id'),
        ('group234.upstream_join_id', 'group234.task_2'),
        ('group234.upstream_join_id', 'group234.task_3'),
        ('task_1', 'group234.upstream_join_id'),
    ]


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

        with TaskGroup("group6") as group6:
            _ = DummyOperator(task_id="task6")

        task7 = DummyOperator(task_id="task7")
        task5 = DummyOperator(task_id="task5")

        task1 >> group234
        group34 >> task5
        group234 >> group6
        group234 >> task7

    subdag = dag.sub_dag(task_ids_or_regex="task5", include_upstream=True, include_downstream=False)

    assert extract_node_id(task_group_to_dict(subdag.task_group)) == {
        'id': None,
        'children': [
            {
                'id': 'group234',
                'children': [
                    {
                        'id': 'group234.group34',
                        'children': [
                            {'id': 'group234.group34.task3'},
                            {'id': 'group234.group34.task4'},
                            {'id': 'group234.group34.downstream_join_id'},
                        ],
                    },
                    {'id': 'group234.upstream_join_id'},
                ],
            },
            {'id': 'task1'},
            {'id': 'task5'},
        ],
    }

    edges = dag_edges(subdag)
    assert sorted((e["source_id"], e["target_id"]) for e in edges) == [
        ('group234.group34.downstream_join_id', 'task5'),
        ('group234.group34.task3', 'group234.group34.downstream_join_id'),
        ('group234.group34.task4', 'group234.group34.downstream_join_id'),
        ('group234.upstream_join_id', 'group234.group34.task3'),
        ('group234.upstream_join_id', 'group234.group34.task4'),
        ('task1', 'group234.upstream_join_id'),
    ]

    subdag_task_groups = subdag.task_group.get_task_group_dict()
    assert subdag_task_groups.keys() == {None, "group234", "group234.group34"}

    included_group_ids = {"group234", "group234.group34"}
    included_task_ids = {'group234.group34.task3', 'group234.group34.task4', 'task1', 'task5'}

    for task_group in subdag_task_groups.values():
        assert task_group.upstream_group_ids.issubset(included_group_ids)
        assert task_group.downstream_group_ids.issubset(included_group_ids)
        assert task_group.upstream_task_ids.issubset(included_task_ids)
        assert task_group.downstream_task_ids.issubset(included_task_ids)

    for task in subdag.task_group:
        assert task.upstream_task_ids.issubset(included_task_ids)
        assert task.downstream_task_ids.issubset(included_task_ids)


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
                        'id': 'group_a.group_b',
                        'children': [
                            {'id': 'group_a.group_b.task2'},
                            {'id': 'group_a.group_b.task3'},
                            {'id': 'group_a.group_b.task4'},
                            {'id': 'group_a.group_b.downstream_join_id'},
                        ],
                    },
                    {'id': 'group_a.task5'},
                    {'id': 'group_a.upstream_join_id'},
                    {'id': 'group_a.downstream_join_id'},
                ],
            },
            {
                'id': 'group_c',
                'children': [
                    {'id': 'group_c.task6'},
                    {'id': 'group_c.task7'},
                    {'id': 'group_c.task8'},
                    {'id': 'group_c.upstream_join_id'},
                    {'id': 'group_c.downstream_join_id'},
                ],
            },
            {
                'id': 'group_d',
                'children': [
                    {'id': 'group_d.task11'},
                    {'id': 'group_d.task12'},
                    {'id': 'group_d.upstream_join_id'},
                ],
            },
            {'id': 'task1'},
            {'id': 'task10'},
            {'id': 'task9'},
        ],
    }

    assert sorted((e["source_id"], e["target_id"]) for e in edges) == [
        ('group_a.downstream_join_id', 'group_c.upstream_join_id'),
        ('group_a.group_b.downstream_join_id', 'group_a.task5'),
        ('group_a.group_b.task2', 'group_a.group_b.task3'),
        ('group_a.group_b.task2', 'group_a.group_b.task4'),
        ('group_a.group_b.task3', 'group_a.group_b.downstream_join_id'),
        ('group_a.group_b.task4', 'group_a.group_b.downstream_join_id'),
        ('group_a.task5', 'group_a.downstream_join_id'),
        ('group_a.task5', 'group_c.task8'),
        ('group_a.upstream_join_id', 'group_a.group_b.task2'),
        ('group_c.downstream_join_id', 'group_d.upstream_join_id'),
        ('group_c.downstream_join_id', 'task10'),
        ('group_c.downstream_join_id', 'task9'),
        ('group_c.task6', 'group_c.task8'),
        ('group_c.task7', 'group_c.task8'),
        ('group_c.task8', 'group_c.downstream_join_id'),
        ('group_c.upstream_join_id', 'group_c.task6'),
        ('group_c.upstream_join_id', 'group_c.task7'),
        ('group_d.task11', 'group_d.task12'),
        ('group_d.upstream_join_id', 'group_d.task11'),
        ('task1', 'group_a.upstream_join_id'),
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
            with TaskGroup("group1", prefix_group_id=False):
                with TaskGroup("group1"):
                    pass

    with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1' .*"):
        with DAG("test_duplicate_group_id", start_date=execution_date):
            with TaskGroup("group1", prefix_group_id=False):
                _ = DummyOperator(task_id="group1")

    with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1.downstream_join_id' .*"):
        with DAG("test_duplicate_group_id", start_date=execution_date):
            _ = DummyOperator(task_id="task1")
            with TaskGroup("group1"):
                _ = DummyOperator(task_id="downstream_join_id")

    with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1.upstream_join_id' .*"):
        with DAG("test_duplicate_group_id", start_date=execution_date):
            _ = DummyOperator(task_id="task1")
            with TaskGroup("group1"):
                _ = DummyOperator(task_id="upstream_join_id")


def test_task_without_dag():
    """
    Test that if a task doesn't have a DAG when it's being set as the relative of another task which
    has a DAG, the task should be added to the root TaskGroup of the other task's DAG.
    """
    dag = DAG(dag_id='test_task_without_dag', start_date=pendulum.parse("20200101"))
    op1 = DummyOperator(task_id='op1', dag=dag)
    op2 = DummyOperator(task_id='op2')
    op3 = DummyOperator(task_id="op3")
    op1 >> op2
    op3 >> op2

    assert op1.dag == op2.dag == op3.dag
    assert dag.task_group.children.keys() == {"op1", "op2", "op3"}
    assert dag.task_group.children.keys() == dag.task_dict.keys()


# taskgroup decorator tests


def test_build_task_group_deco_context_manager():
    """
    Tests Following :
    1. Nested TaskGroup creation using taskgroup decorator should create same TaskGroup which can be
    created using TaskGroup context manager.
    2. TaskGroup consisting Tasks created using task decorator.
    3. Node Ids of dags created with taskgroup decorator.
    """

    from airflow.decorators import task

    # Creating Tasks
    @task
    def task_start():
        """Dummy Task which is First Task of Dag """
        return '[Task_start]'

    @task
    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print('[ Task_End ]')

    @task
    def task_1(value):
        """ Dummy Task1"""
        return f'[ Task1 {value} ]'

    @task
    def task_2(value):
        """ Dummy Task2"""
        print(f'[ Task2 {value} ]')

    @task
    def task_3(value):
        """ Dummy Task3"""
        return f'[ Task3 {value} ]'

    @task
    def task_4(value):
        """ Dummy Task3"""
        print(f'[ Task4 {value} ]')

    # Creating TaskGroups
    @task_group_decorator
    def section_1(value):
        """ TaskGroup for grouping related Tasks"""

        @task_group_decorator()
        def section_2(value2):
            """ TaskGroup for grouping related Tasks"""
            return task_4(task_3(value2))

        op1 = task_2(task_1(value))
        return section_2(op1)

    execution_date = pendulum.parse("20201109")
    with DAG(
        dag_id="example_nested_task_group_decorator", start_date=execution_date, tags=["example"]
    ) as dag:
        t_start = task_start()
        sec_1 = section_1(t_start)
        sec_1.set_downstream(task_end())

    # Testing TaskGroup created using taskgroup decorator
    assert set(dag.task_group.children.keys()) == {"task_start", "task_end", "section_1"}
    assert set(dag.task_group.children['section_1'].children.keys()) == {
        'section_1.task_1',
        'section_1.task_2',
        'section_1.section_2',
    }

    # Testing TaskGroup consisting Tasks created using task decorator
    assert dag.task_dict['task_start'].downstream_task_ids == {'section_1.task_1'}
    assert dag.task_dict['section_1.task_2'].downstream_task_ids == {'section_1.section_2.task_3'}
    assert dag.task_dict['section_1.section_2.task_4'].downstream_task_ids == {'task_end'}

    # Node IDs test
    node_ids = {
        'id': None,
        'children': [
            {
                'id': 'section_1',
                'children': [
                    {
                        'id': 'section_1.section_2',
                        'children': [
                            {'id': 'section_1.section_2.task_3'},
                            {'id': 'section_1.section_2.task_4'},
                        ],
                    },
                    {'id': 'section_1.task_1'},
                    {'id': 'section_1.task_2'},
                    {'id': 'section_1.downstream_join_id'},
                ],
            },
            {'id': 'task_end'},
            {'id': 'task_start'},
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids


def test_build_task_group_with_operators():
    """  Tests DAG with Tasks created with *Operators and TaskGroup created with taskgroup decorator """

    from airflow.decorators import task

    def task_start():
        """Dummy Task which is First Task of Dag """
        return '[Task_start]'

    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print('[ Task_End  ]')

    # Creating Tasks
    @task
    def task_1(value):
        """ Dummy Task1"""
        return f'[ Task1 {value} ]'

    @task
    def task_2(value):
        """ Dummy Task2"""
        return f'[ Task2 {value} ]'

    @task
    def task_3(value):
        """ Dummy Task3"""
        print(f'[ Task3 {value} ]')

    # Creating TaskGroups
    @task_group_decorator(group_id='section_1')
    def section_a(value):
        """ TaskGroup for grouping related Tasks"""
        return task_3(task_2(task_1(value)))

    execution_date = pendulum.parse("20201109")
    with DAG(dag_id="example_task_group_decorator_mix", start_date=execution_date, tags=["example"]) as dag:
        t_start = PythonOperator(task_id='task_start', python_callable=task_start, dag=dag)
        sec_1 = section_a(t_start.output)
        t_end = PythonOperator(task_id='task_end', python_callable=task_end, dag=dag)
        sec_1.set_downstream(t_end)

    # Testing Tasks ing DAG
    assert set(dag.task_group.children.keys()) == {'section_1', 'task_start', 'task_end'}
    assert set(dag.task_group.children['section_1'].children.keys()) == {
        'section_1.task_2',
        'section_1.task_3',
        'section_1.task_1',
    }

    # Testing Tasks downstream
    assert dag.task_dict['task_start'].downstream_task_ids == {'section_1.task_1'}
    assert dag.task_dict['section_1.task_3'].downstream_task_ids == {'task_end'}


def test_task_group_context_mix():
    """ Test cases to check nested TaskGroup context manager with taskgroup decorator"""

    from airflow.decorators import task

    def task_start():
        """Dummy Task which is First Task of Dag """
        return '[Task_start]'

    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print('[ Task_End  ]')

    # Creating Tasks
    @task
    def task_1(value):
        """ Dummy Task1"""
        return f'[ Task1 {value} ]'

    @task
    def task_2(value):
        """ Dummy Task2"""
        return f'[ Task2 {value} ]'

    @task
    def task_3(value):
        """ Dummy Task3"""
        print(f'[ Task3 {value} ]')

    # Creating TaskGroups
    @task_group_decorator
    def section_2(value):
        """ TaskGroup for grouping related Tasks"""
        return task_3(task_2(task_1(value)))

    execution_date = pendulum.parse("20201109")
    with DAG(dag_id="example_task_group_decorator_mix", start_date=execution_date, tags=["example"]) as dag:
        t_start = PythonOperator(task_id='task_start', python_callable=task_start, dag=dag)

        with TaskGroup("section_1", tooltip="section_1") as section_1:
            sec_2 = section_2(t_start.output)
            task_s1 = DummyOperator(task_id="task_1")
            task_s2 = BashOperator(task_id="task_2", bash_command='echo 1')
            task_s3 = DummyOperator(task_id="task_3")

            sec_2.set_downstream(task_s1)
            task_s1 >> [task_s2, task_s3]

        t_end = PythonOperator(task_id='task_end', python_callable=task_end, dag=dag)
        t_start >> section_1 >> t_end

    node_ids = {
        'id': None,
        'children': [
            {
                'id': 'section_1',
                'children': [
                    {
                        'id': 'section_1.section_2',
                        'children': [
                            {'id': 'section_1.section_2.task_1'},
                            {'id': 'section_1.section_2.task_2'},
                            {'id': 'section_1.section_2.task_3'},
                            {'id': 'section_1.section_2.downstream_join_id'},
                        ],
                    },
                    {'id': 'section_1.task_1'},
                    {'id': 'section_1.task_2'},
                    {'id': 'section_1.task_3'},
                    {'id': 'section_1.upstream_join_id'},
                    {'id': 'section_1.downstream_join_id'},
                ],
            },
            {'id': 'task_end'},
            {'id': 'task_start'},
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids


def test_duplicate_task_group_id():
    """ Testing automatic suffix assignment for duplicate group_id"""

    from airflow.decorators import task

    @task(task_id='start_task')
    def task_start():
        """Dummy Task which is First Task of Dag """
        print('[Task_start]')

    @task(task_id='end_task')
    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print('[Task_End]')

    # Creating Tasks
    @task(task_id='task')
    def task_1():
        """ Dummy Task1"""
        print('[Task1]')

    @task(task_id='task')
    def task_2():
        """ Dummy Task2"""
        print('[Task2]')

    @task(task_id='task1')
    def task_3():
        """ Dummy Task3"""
        print('[Task3]')

    @task_group_decorator('task_group1')
    def task_group1():
        task_start()
        task_1()
        task_2()

    @task_group_decorator(group_id='task_group1')
    def task_group2():
        task_3()

    @task_group_decorator(group_id='task_group1')
    def task_group3():
        task_end()

    execution_date = pendulum.parse("20201109")
    with DAG(dag_id="example_duplicate_task_group_id", start_date=execution_date, tags=["example"]) as dag:
        task_group1()
        task_group2()
        task_group3()
    node_ids = {
        'id': None,
        'children': [
            {
                'id': 'task_group1',
                'children': [
                    {'id': 'task_group1.start_task'},
                    {'id': 'task_group1.task'},
                    {'id': 'task_group1.task__1'},
                ],
            },
            {'id': 'task_group1__1', 'children': [{'id': 'task_group1__1.task1'}]},
            {'id': 'task_group1__2', 'children': [{'id': 'task_group1__2.end_task'}]},
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids


def test_call_taskgroup_twice():
    """Test for using same taskgroup decorated function twice"""
    from airflow.decorators import task

    @task(task_id='start_task')
    def task_start():
        """Dummy Task which is First Task of Dag """
        print('[Task_start]')

    @task(task_id='end_task')
    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print('[Task_End]')

    # Creating Tasks
    @task(task_id='task')
    def task_1():
        """ Dummy Task1"""
        print('[Task1]')

    @task_group_decorator
    def task_group1(name: str):
        print(f'Starting taskgroup {name}')
        task_start()
        task_1()
        task_end()

    execution_date = pendulum.parse("20201109")
    with DAG(dag_id="example_multi_call_task_groups", start_date=execution_date, tags=["example"]) as dag:
        task_group1('Call1')
        task_group1('Call2')

    node_ids = {
        'id': None,
        'children': [
            {
                'id': 'task_group1',
                'children': [
                    {'id': 'task_group1.end_task'},
                    {'id': 'task_group1.start_task'},
                    {'id': 'task_group1.task'},
                ],
            },
            {
                'id': 'task_group1__1',
                'children': [
                    {'id': 'task_group1__1.end_task'},
                    {'id': 'task_group1__1.start_task'},
                    {'id': 'task_group1__1.task'},
                ],
            },
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids
