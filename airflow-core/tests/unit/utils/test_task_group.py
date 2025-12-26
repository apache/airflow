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

from airflow.api_fastapi.core_api.services.ui.task_group import task_group_to_dict
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import (
    DAG,
    BaseOperator,
    TaskGroup,
    setup,
    task as task_decorator,
    task_group as task_group_decorator,
    teardown,
)
from airflow.utils.dag_edges import dag_edges

from tests_common.test_utils.dag import create_scheduler_dag
from unit.models import DEFAULT_DATE

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


EXPECTED_JSON_LEGACY = {
    "id": None,
    "value": {
        "label": None,
        "labelStyle": "fill:#000;",
        "style": "fill:CornflowerBlue",
        "rx": 5,
        "ry": 5,
        "clusterLabelPos": "top",
        "isMapped": False,
        "tooltip": "",
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
                "tooltip": "",
                "isMapped": False,
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
                        "tooltip": "",
                        "isMapped": False,
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
                        {
                            "id": "group234.group34.downstream_join_id",
                            "value": {
                                "label": "",
                                "labelStyle": "fill:#000;",
                                "style": "fill:CornflowerBlue;",
                                "shape": "circle",
                            },
                        },
                    ],
                },
                {
                    "id": "group234.upstream_join_id",
                    "value": {
                        "label": "",
                        "labelStyle": "fill:#000;",
                        "style": "fill:CornflowerBlue;",
                        "shape": "circle",
                    },
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

EXPECTED_JSON = {
    "children": [
        {"id": "task1", "label": "task1", "operator": "EmptyOperator", "type": "task"},
        {
            "children": [
                {
                    "children": [
                        {
                            "id": "group234.group34.task3",
                            "label": "task3",
                            "operator": "EmptyOperator",
                            "type": "task",
                        },
                        {
                            "id": "group234.group34.task4",
                            "label": "task4",
                            "operator": "EmptyOperator",
                            "type": "task",
                        },
                        {"id": "group234.group34.downstream_join_id", "label": "", "type": "join"},
                    ],
                    "id": "group234.group34",
                    "is_mapped": False,
                    "label": "group34",
                    "tooltip": "",
                    "type": "task",
                },
                {
                    "id": "group234.task2",
                    "label": "task2",
                    "operator": "EmptyOperator",
                    "type": "task",
                },
                {"id": "group234.upstream_join_id", "label": "", "type": "join"},
            ],
            "id": "group234",
            "is_mapped": False,
            "label": "group234",
            "tooltip": "",
            "type": "task",
        },
        {"id": "task5", "label": "task5", "operator": "EmptyOperator", "type": "task"},
    ],
    "id": None,
    "is_mapped": False,
    "label": "",
    "tooltip": "",
    "type": "task",
}


def test_task_group_to_dict_serialized_dag(dag_maker):
    logical_date = pendulum.parse("20200101")
    with dag_maker("test_task_group_to_dict", schedule=None, start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group234") as group234:
            _ = EmptyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                _ = EmptyOperator(task_id="task3")
                _ = EmptyOperator(task_id="task4")

        task5 = EmptyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    assert task_group_to_dict(dag.task_group) == EXPECTED_JSON


def test_task_group_to_dict_alternative_syntax():
    logical_date = pendulum.parse("20200101")
    dag = DAG("test_task_group_to_dict_alt", schedule=None, start_date=logical_date)
    task1 = EmptyOperator(task_id="task1", dag=dag)
    group234 = TaskGroup("group234", dag=dag)
    _ = EmptyOperator(task_id="task2", dag=dag, task_group=group234)
    group34 = TaskGroup("group34", dag=dag, parent_group=group234)
    _ = EmptyOperator(task_id="task3", dag=dag, task_group=group34)
    _ = EmptyOperator(task_id="task4", dag=dag, task_group=group34)
    task5 = EmptyOperator(task_id="task5", dag=dag)

    task1 >> group234
    group34 >> task5

    serialized_dag = create_scheduler_dag(dag)

    assert task_group_to_dict(serialized_dag.task_group) == EXPECTED_JSON


def extract_node_id(node, include_label=False):
    ret = {"id": node["id"]}
    if include_label:
        ret["label"] = node["label"]
    if "children" in node:
        children = []
        for child in node["children"]:
            children.append(extract_node_id(child, include_label=include_label))

        ret["children"] = children

    return ret


def test_task_group_to_dict_with_prefix(dag_maker):
    logical_date = pendulum.parse("20200101")
    with dag_maker("test_task_group_to_dict_prefix", start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group234", prefix_group_id=False) as group234:
            EmptyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                EmptyOperator(task_id="task3")

                with TaskGroup("group4", prefix_group_id=False):
                    EmptyOperator(task_id="task4")

        task5 = EmptyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    expected_node_id = {
        "children": [
            {"id": "task1", "label": "task1"},
            {
                "id": "group234",
                "label": "group234",
                "children": [
                    {
                        "children": [
                            {
                                "children": [{"id": "task4", "label": "task4"}],
                                "id": "group34.group4",
                                "label": "group4",
                            },
                            {"id": "group34.task3", "label": "task3"},
                            {"id": "group34.downstream_join_id", "label": ""},
                        ],
                        "id": "group34",
                        "label": "group34",
                    },
                    {"id": "task2", "label": "task2"},
                    {"id": "group234.upstream_join_id", "label": ""},
                ],
            },
            {"id": "task5", "label": "task5"},
        ],
        "id": None,
        "label": "",
    }

    assert extract_node_id(task_group_to_dict(dag.task_group), include_label=True) == expected_node_id


def test_task_group_to_dict_with_task_decorator(dag_maker):
    from airflow.sdk import task

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

    logical_date = pendulum.parse("20200101")
    with dag_maker("test_build_task_group_with_task_decorator", start_date=logical_date) as dag:
        tsk_1 = task_1()

        with TaskGroup("group234") as group234:
            tsk_2 = task_2()
            tsk_3 = task_3()
            task_4(tsk_2, tsk_3)

        tsk_5 = task_5()

        tsk_1 >> group234 >> tsk_5

    expected_node_id = {
        "id": None,
        "children": [
            {"id": "task_1"},
            {
                "id": "group234",
                "children": [
                    {"id": "group234.task_2"},
                    {"id": "group234.task_3"},
                    {"id": "group234.task_4"},
                    {"id": "group234.upstream_join_id"},
                    {"id": "group234.downstream_join_id"},
                ],
            },
            {"id": "task_5"},
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == expected_node_id

    edges = dag_edges(dag)
    assert sorted((e["source_id"], e["target_id"]) for e in edges) == [
        ("group234.downstream_join_id", "task_5"),
        ("group234.task_2", "group234.task_4"),
        ("group234.task_3", "group234.task_4"),
        ("group234.task_4", "group234.downstream_join_id"),
        ("group234.upstream_join_id", "group234.task_2"),
        ("group234.upstream_join_id", "group234.task_3"),
        ("task_1", "group234.upstream_join_id"),
    ]


def test_task_group_to_dict_sub_dag(dag_maker):
    logical_date = pendulum.parse("20200101")
    with dag_maker("test_test_task_group_sub_dag", schedule=None, start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group234") as group234:
            _ = EmptyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                _ = EmptyOperator(task_id="task3")
                _ = EmptyOperator(task_id="task4")

        with TaskGroup("group6") as group6:
            _ = EmptyOperator(task_id="task6")

        task7 = EmptyOperator(task_id="task7")
        task5 = EmptyOperator(task_id="task5")

        task1 >> group234
        group34 >> task5
        group234 >> group6
        group234 >> task7

    subset = dag.partial_subset(task_ids="task5", include_upstream=True, include_downstream=False)

    expected_node_id = {
        "id": None,
        "children": [
            {"id": "task1"},
            {
                "id": "group234",
                "children": [
                    {
                        "id": "group234.group34",
                        "children": [
                            {"id": "group234.group34.task3"},
                            {"id": "group234.group34.task4"},
                            {"id": "group234.group34.downstream_join_id"},
                        ],
                    },
                    {"id": "group234.upstream_join_id"},
                ],
            },
            {"id": "task5"},
        ],
    }

    assert extract_node_id(task_group_to_dict(subset.task_group)) == expected_node_id

    edges = dag_edges(subset)
    assert sorted((e["source_id"], e["target_id"]) for e in edges) == [
        ("group234.group34.downstream_join_id", "task5"),
        ("group234.group34.task3", "group234.group34.downstream_join_id"),
        ("group234.group34.task4", "group234.group34.downstream_join_id"),
        ("group234.upstream_join_id", "group234.group34.task3"),
        ("group234.upstream_join_id", "group234.group34.task4"),
        ("task1", "group234.upstream_join_id"),
    ]


def test_task_group_to_dict_and_dag_edges(dag_maker):
    logical_date = pendulum.parse("20200101")
    with dag_maker("test_dag_edges", schedule=None, start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group_a") as group_a:
            with TaskGroup("group_b") as group_b:
                task2 = EmptyOperator(task_id="task2")
                task3 = EmptyOperator(task_id="task3")
                task4 = EmptyOperator(task_id="task4")
                task2 >> [task3, task4]

            task5 = EmptyOperator(task_id="task5")

            task5 << group_b

        task1 >> group_a

        with TaskGroup("group_c") as group_c:
            task6 = EmptyOperator(task_id="task6")
            task7 = EmptyOperator(task_id="task7")
            task8 = EmptyOperator(task_id="task8")
            [task6, task7] >> task8
            group_a >> group_c

        task5 >> task8

        task9 = EmptyOperator(task_id="task9")
        task10 = EmptyOperator(task_id="task10")

        group_c >> [task9, task10]

        with TaskGroup("group_d") as group_d:
            task11 = EmptyOperator(task_id="task11")
            task12 = EmptyOperator(task_id="task12")
            task11 >> task12

        group_d << group_c

    nodes = task_group_to_dict(dag.task_group)
    edges = dag_edges(dag)

    expected_node_id = {
        "id": None,
        "children": [
            {
                "id": "group_d",
                "children": [
                    {"id": "group_d.task11"},
                    {"id": "group_d.task12"},
                    {"id": "group_d.upstream_join_id"},
                ],
            },
            {"id": "task1"},
            {
                "id": "group_a",
                "children": [
                    {
                        "id": "group_a.group_b",
                        "children": [
                            {"id": "group_a.group_b.task2"},
                            {"id": "group_a.group_b.task3"},
                            {"id": "group_a.group_b.task4"},
                            {"id": "group_a.group_b.downstream_join_id"},
                        ],
                    },
                    {"id": "group_a.task5"},
                    {"id": "group_a.upstream_join_id"},
                    {"id": "group_a.downstream_join_id"},
                ],
            },
            {
                "id": "group_c",
                "children": [
                    {"id": "group_c.task6"},
                    {"id": "group_c.task7"},
                    {"id": "group_c.task8"},
                    {"id": "group_c.upstream_join_id"},
                    {"id": "group_c.downstream_join_id"},
                ],
            },
            {"id": "task10"},
            {"id": "task9"},
        ],
    }

    assert extract_node_id(nodes) == expected_node_id

    assert sorted((e["source_id"], e["target_id"]) for e in edges) == [
        ("group_a.downstream_join_id", "group_c.upstream_join_id"),
        ("group_a.group_b.downstream_join_id", "group_a.task5"),
        ("group_a.group_b.task2", "group_a.group_b.task3"),
        ("group_a.group_b.task2", "group_a.group_b.task4"),
        ("group_a.group_b.task3", "group_a.group_b.downstream_join_id"),
        ("group_a.group_b.task4", "group_a.group_b.downstream_join_id"),
        ("group_a.task5", "group_a.downstream_join_id"),
        ("group_a.task5", "group_c.task8"),
        ("group_a.upstream_join_id", "group_a.group_b.task2"),
        ("group_c.downstream_join_id", "group_d.upstream_join_id"),
        ("group_c.downstream_join_id", "task10"),
        ("group_c.downstream_join_id", "task9"),
        ("group_c.task6", "group_c.task8"),
        ("group_c.task7", "group_c.task8"),
        ("group_c.task8", "group_c.downstream_join_id"),
        ("group_c.upstream_join_id", "group_c.task6"),
        ("group_c.upstream_join_id", "group_c.task7"),
        ("group_d.task11", "group_d.task12"),
        ("group_d.upstream_join_id", "group_d.task11"),
        ("task1", "group_a.upstream_join_id"),
    ]


def test_dag_edges_setup_teardown(dag_maker):
    logical_date = pendulum.parse("20200101")
    with dag_maker("test_dag_edges", schedule=None, start_date=logical_date) as dag:
        setup1 = EmptyOperator(task_id="setup1").as_setup()
        teardown1 = EmptyOperator(task_id="teardown1").as_teardown()

        with setup1 >> teardown1:
            EmptyOperator(task_id="task1")

        with TaskGroup("group_a"):
            setup2 = EmptyOperator(task_id="setup2").as_setup()
            teardown2 = EmptyOperator(task_id="teardown2").as_teardown()

            with setup2 >> teardown2:
                EmptyOperator(task_id="task2")

    edges = dag_edges(dag)

    assert sorted((e["source_id"], e["target_id"], e.get("is_setup_teardown")) for e in edges) == [
        ("group_a.setup2", "group_a.task2", None),
        ("group_a.setup2", "group_a.teardown2", True),
        ("group_a.task2", "group_a.teardown2", None),
        ("setup1", "task1", None),
        ("setup1", "teardown1", True),
        ("task1", "teardown1", None),
    ]


def test_dag_edges_setup_teardown_nested(dag_maker):
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import task, task_group

    logical_date = pendulum.parse("20200101")

    with dag_maker(dag_id="s_t_dag", schedule=None, start_date=logical_date) as dag:

        @task
        def test_task():
            print("Hello world!")

        @task_group
        def inner():
            inner_start = EmptyOperator(task_id="start")
            inner_end = EmptyOperator(task_id="end")

            test_task_r = test_task.override(task_id="work")()
            inner_start >> test_task_r >> inner_end.as_teardown(setups=inner_start)

        @task_group
        def outer():
            outer_work = EmptyOperator(task_id="work")
            inner_group = inner()
            inner_group >> outer_work

        dag_start = EmptyOperator(task_id="dag_start")
        dag_end = EmptyOperator(task_id="dag_end")
        dag_start >> outer() >> dag_end

    edges = dag_edges(dag)

    actual = sorted((e["source_id"], e["target_id"], e.get("is_setup_teardown")) for e in edges)
    assert actual == [
        ("dag_start", "outer.upstream_join_id", None),
        ("outer.downstream_join_id", "dag_end", None),
        ("outer.inner.downstream_join_id", "outer.work", None),
        ("outer.inner.start", "outer.inner.end", True),
        ("outer.inner.start", "outer.inner.work", None),
        ("outer.inner.work", "outer.inner.downstream_join_id", None),
        ("outer.inner.work", "outer.inner.end", None),
        ("outer.upstream_join_id", "outer.inner.start", None),
        ("outer.work", "outer.downstream_join_id", None),
    ]


# taskgroup decorator tests


def test_build_task_group_deco_context_manager(dag_maker):
    """
    Tests Following :
    1. Nested TaskGroup creation using taskgroup decorator should create same TaskGroup which can be
    created using TaskGroup context manager.
    2. TaskGroup consisting Tasks created using task decorator.
    3. Node Ids of dags created with taskgroup decorator.
    """
    from airflow.sdk import task

    # Creating Tasks
    @task
    def task_start():
        """Dummy Task which is First Task of Dag"""
        return "[Task_start]"

    @task
    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print("[ Task_End ]")

    @task
    def task_1(value):
        """Dummy Task1"""
        return f"[ Task1 {value} ]"

    @task
    def task_2(value):
        """Dummy Task2"""
        print(f"[ Task2 {value} ]")

    @task
    def task_3(value):
        """Dummy Task3"""
        return f"[ Task3 {value} ]"

    @task
    def task_4(value):
        """Dummy Task3"""
        print(f"[ Task4 {value} ]")

    # Creating TaskGroups
    @task_group_decorator
    def section_1(value):
        """TaskGroup for grouping related Tasks"""

        @task_group_decorator()
        def section_2(value2):
            """TaskGroup for grouping related Tasks"""
            return task_4(task_3(value2))

        op1 = task_2(task_1(value))
        return section_2(op1)

    logical_date = pendulum.parse("20201109")
    with dag_maker(
        dag_id="example_nested_task_group_decorator",
        schedule=None,
        start_date=logical_date,
        tags=["example"],
    ) as dag:
        t_start = task_start()
        sec_1 = section_1(t_start)
        sec_1.set_downstream(task_end())

    # Testing TaskGroup created using taskgroup decorator
    assert set(dag.task_group.children.keys()) == {"task_start", "task_end", "section_1"}
    assert set(dag.task_group.children["section_1"].children.keys()) == {
        "section_1.task_1",
        "section_1.task_2",
        "section_1.section_2",
    }

    # Testing TaskGroup consisting Tasks created using task decorator
    assert dag.task_dict["task_start"].downstream_task_ids == {"section_1.task_1"}
    assert dag.task_dict["section_1.task_2"].downstream_task_ids == {"section_1.section_2.task_3"}
    assert dag.task_dict["section_1.section_2.task_4"].downstream_task_ids == {"task_end"}

    # Node IDs test
    node_ids = {
        "id": None,
        "children": [
            {
                "id": "section_1",
                "children": [
                    {
                        "id": "section_1.section_2",
                        "children": [
                            {"id": "section_1.section_2.task_3"},
                            {"id": "section_1.section_2.task_4"},
                        ],
                    },
                    {"id": "section_1.task_1"},
                    {"id": "section_1.task_2"},
                ],
            },
            {"id": "task_end"},
            {"id": "task_start"},
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids


def test_task_group_context_mix(dag_maker):
    """Test cases to check nested TaskGroup context manager with taskgroup decorator"""
    from airflow.sdk import task

    def task_start():
        """Dummy Task which is First Task of Dag"""
        return "[Task_start]"

    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print("[ Task_End  ]")

    # Creating Tasks
    @task
    def task_1(value):
        """Dummy Task1"""
        return f"[ Task1 {value} ]"

    @task
    def task_2(value):
        """Dummy Task2"""
        return f"[ Task2 {value} ]"

    @task
    def task_3(value):
        """Dummy Task3"""
        print(f"[ Task3 {value} ]")

    # Creating TaskGroups
    @task_group_decorator
    def section_2(value):
        """TaskGroup for grouping related Tasks"""
        return task_3(task_2(task_1(value)))

    logical_date = pendulum.parse("20201109")
    with dag_maker(
        dag_id="example_task_group_decorator_mix",
        schedule=None,
        start_date=logical_date,
        tags=["example"],
    ) as dag:
        t_start = PythonOperator(task_id="task_start", python_callable=task_start)

        with TaskGroup("section_1", tooltip="section_1") as section_1:
            sec_2 = section_2(t_start.output)
            task_s1 = EmptyOperator(task_id="task_1")
            task_s2 = BashOperator(task_id="task_2", bash_command="echo 1")
            task_s3 = EmptyOperator(task_id="task_3")

            sec_2.set_downstream(task_s1)
            task_s1 >> [task_s2, task_s3]

        t_end = PythonOperator(task_id="task_end", python_callable=task_end)
        t_start >> section_1 >> t_end

    node_ids = {
        "id": None,
        "children": [
            {"id": "task_start"},
            {
                "id": "section_1",
                "children": [
                    {
                        "id": "section_1.section_2",
                        "children": [
                            {"id": "section_1.section_2.task_1"},
                            {"id": "section_1.section_2.task_2"},
                            {"id": "section_1.section_2.task_3"},
                        ],
                    },
                    {"id": "section_1.task_1"},
                    {"id": "section_1.task_2"},
                    {"id": "section_1.task_3"},
                    {"id": "section_1.upstream_join_id"},
                    {"id": "section_1.downstream_join_id"},
                ],
            },
            {"id": "task_end"},
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids


def test_duplicate_task_group_id(dag_maker):
    """Testing automatic suffix assignment for duplicate group_id"""
    from airflow.sdk import task

    @task(task_id="start_task")
    def task_start():
        """Dummy Task which is First Task of Dag"""
        print("[Task_start]")

    @task(task_id="end_task")
    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print("[Task_End]")

    # Creating Tasks
    @task(task_id="task")
    def task_1():
        """Dummy Task1"""
        print("[Task1]")

    @task(task_id="task")
    def task_2():
        """Dummy Task2"""
        print("[Task2]")

    @task(task_id="task1")
    def task_3():
        """Dummy Task3"""
        print("[Task3]")

    @task_group_decorator("task_group1")
    def task_group1():
        task_start()
        task_1()
        task_2()

    @task_group_decorator(group_id="task_group1")
    def task_group2():
        task_3()

    @task_group_decorator(group_id="task_group1")
    def task_group3():
        task_end()

    logical_date = pendulum.parse("20201109")
    with dag_maker(
        dag_id="example_duplicate_task_group_id",
        schedule=None,
        start_date=logical_date,
        tags=["example"],
    ) as dag:
        task_group1()
        task_group2()
        task_group3()
    node_ids = {
        "id": None,
        "children": [
            {
                "id": "task_group1",
                "children": [
                    {"id": "task_group1.start_task"},
                    {"id": "task_group1.task"},
                    {"id": "task_group1.task__1"},
                ],
            },
            {"id": "task_group1__1", "children": [{"id": "task_group1__1.task1"}]},
            {"id": "task_group1__2", "children": [{"id": "task_group1__2.end_task"}]},
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids


def test_call_taskgroup_twice(dag_maker):
    from airflow.sdk import task

    @task(task_id="start_task")
    def task_start():
        """Dummy Task which is First Task of Dag"""
        print("[Task_start]")

    @task(task_id="end_task")
    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print("[Task_End]")

    # Creating Tasks
    @task(task_id="task")
    def task_1():
        """Dummy Task1"""
        print("[Task1]")

    @task_group_decorator
    def task_group1(name: str):
        print(f"Starting taskgroup {name}")
        task_start()
        task_1()
        task_end()

    logical_date = pendulum.parse("20201109")
    with dag_maker(
        dag_id="example_multi_call_task_groups",
        schedule=None,
        start_date=logical_date,
        tags=["example"],
    ) as dag:
        task_group1("Call1")
        task_group1("Call2")

    node_ids = {
        "id": None,
        "children": [
            {
                "id": "task_group1",
                "children": [
                    {"id": "task_group1.end_task"},
                    {"id": "task_group1.start_task"},
                    {"id": "task_group1.task"},
                ],
            },
            {
                "id": "task_group1__1",
                "children": [
                    {"id": "task_group1__1.end_task"},
                    {"id": "task_group1__1.start_task"},
                    {"id": "task_group1__1.task"},
                ],
            },
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids


def test_topological_sort1():
    dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

    # A -> B
    # A -> C -> D
    # ordered: B, D, C, A or D, B, C, A or D, C, B, A
    with dag:
        op1 = EmptyOperator(task_id="A")
        op2 = EmptyOperator(task_id="B")
        op3 = EmptyOperator(task_id="C")
        op4 = EmptyOperator(task_id="D")
        [op2, op3] >> op1
        op3 >> op4

    topological_list = dag.task_group.topological_sort()

    tasks = [op2, op3, op4]
    assert topological_list[0] in tasks
    tasks.remove(topological_list[0])
    assert topological_list[1] in tasks
    tasks.remove(topological_list[1])
    assert topological_list[2] in tasks
    tasks.remove(topological_list[2])
    assert topological_list[3] == op1


def test_topological_sort2():
    dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

    # C -> (A u B) -> D
    # C -> E
    # ordered: E | D, A | B, C
    with dag:
        op1 = EmptyOperator(task_id="A")
        op2 = EmptyOperator(task_id="B")
        op3 = EmptyOperator(task_id="C")
        op4 = EmptyOperator(task_id="D")
        op5 = EmptyOperator(task_id="E")
        op3 << [op1, op2]
        op4 >> [op1, op2]
        op5 >> op3

    topological_list = dag.task_group.topological_sort()

    set1 = [op4, op5]
    assert topological_list[0] in set1
    set1.remove(topological_list[0])

    set2 = [op1, op2]
    set2.extend(set1)
    assert topological_list[1] in set2
    set2.remove(topological_list[1])

    assert topological_list[2] in set2
    set2.remove(topological_list[2])

    assert topological_list[3] in set2

    assert topological_list[4] == op3


def test_topological_nested_groups():
    logical_date = pendulum.parse("20200101")
    with DAG("test_dag_edges", schedule=None, start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        task5 = EmptyOperator(task_id="task5")
        with TaskGroup("group_a") as group_a:
            with TaskGroup("group_b"):
                task2 = EmptyOperator(task_id="task2")
                task3 = EmptyOperator(task_id="task3")
                task4 = EmptyOperator(task_id="task4")
                task2 >> [task3, task4]

        task1 >> group_a
        group_a >> task5

    def nested_topo(group):
        return [
            nested_topo(node) if isinstance(node, TaskGroup) else node for node in group.topological_sort()
        ]

    topological_list = nested_topo(dag.task_group)

    assert topological_list == [
        task1,
        [
            [
                task2,
                task3,
                task4,
            ],
        ],
        task5,
    ]


def test_hierarchical_alphabetical_sort():
    logical_date = pendulum.parse("20200101")
    with DAG("test_dag_edges", schedule=None, start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        task5 = EmptyOperator(task_id="task5")
        with TaskGroup("group_c"):
            task7 = EmptyOperator(task_id="task7")
        with TaskGroup("group_b"):
            task6 = EmptyOperator(task_id="task6")
        with TaskGroup("group_a"):
            with TaskGroup("group_d"):
                task2 = EmptyOperator(task_id="task2")
                task3 = EmptyOperator(task_id="task3")
                task4 = EmptyOperator(task_id="task4")
            task9 = EmptyOperator(task_id="task9")
            task8 = EmptyOperator(task_id="task8")

    def nested(group):
        return [
            nested(node) if isinstance(node, TaskGroup) else node
            for node in group.hierarchical_alphabetical_sort()
        ]

    sorted_list = nested(dag.task_group)

    assert sorted_list == [
        [  # group_a
            [  # group_d
                task2,
                task3,
                task4,
            ],
            task8,
            task9,
        ],
        [task6],  # group_b
        [task7],  # group_c
        task1,
        task5,
    ]


def test_topological_group_dep():
    logical_date = pendulum.parse("20200101")
    with DAG("test_dag_edges", schedule=None, start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        task6 = EmptyOperator(task_id="task6")
        with TaskGroup("group_a") as group_a:
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")
        with TaskGroup("group_b") as group_b:
            task4 = EmptyOperator(task_id="task4")
            task5 = EmptyOperator(task_id="task5")

        task1 >> group_a >> group_b >> task6

    def nested_topo(group):
        return [
            nested_topo(node) if isinstance(node, TaskGroup) else node for node in group.topological_sort()
        ]

    topological_list = nested_topo(dag.task_group)

    assert topological_list == [
        task1,
        [
            task2,
            task3,
        ],
        [
            task4,
            task5,
        ],
        task6,
    ]


def test_task_group_arrow_with_setup_group():
    with DAG(dag_id="setup_group_teardown_group") as dag:
        with TaskGroup("group_1") as g1:

            @setup
            def setup_1(): ...

            @setup
            def setup_2(): ...

            s1 = setup_1()
            s2 = setup_2()

        with TaskGroup("group_2") as g2:

            @teardown
            def teardown_1(): ...

            @teardown
            def teardown_2(): ...

            t1 = teardown_1()
            t2 = teardown_2()

        @task_decorator
        def work(): ...

        w1 = work()
        g1 >> w1 >> g2
        t1.as_teardown(setups=s1)
        t2.as_teardown(setups=s2)
    assert set(s1.operator.downstream_task_ids) == {"work", "group_2.teardown_1"}
    assert set(s2.operator.downstream_task_ids) == {"work", "group_2.teardown_2"}
    assert set(w1.operator.downstream_task_ids) == {"group_2.teardown_1", "group_2.teardown_2"}
    assert set(t1.operator.downstream_task_ids) == set()
    assert set(t2.operator.downstream_task_ids) == set()

    def get_nodes(group):
        serialized_dag = create_scheduler_dag(dag)
        group = serialized_dag.task_group_dict[g1.group_id]
        d = task_group_to_dict(group)
        new_d = {}
        new_d["id"] = d["id"]
        new_d["children"] = [{"id": x["id"]} for x in d["children"]]
        return new_d

    assert get_nodes(g1) == {
        "id": "group_1",
        "children": [
            {"id": "group_1.setup_1"},
            {"id": "group_1.setup_2"},
            {"id": "group_1.downstream_join_id"},
        ],
    }


def test_task_group_display_name_used_as_label(dag_maker):
    """Test that the group_display_name for TaskGroup is used as the label for display on the UI."""
    with dag_maker(dag_id="display_name", schedule=None, start_date=pendulum.datetime(2022, 1, 1)) as dag:
        with TaskGroup(group_id="tg", group_display_name="my_custom_name") as tg:
            task1 = BaseOperator(task_id="task1")
            task2 = BaseOperator(task_id="task2")
            task1 >> task2

    assert tg.group_id == "tg"
    assert tg.label == "my_custom_name"
    expected_node_id = {
        "id": None,
        "label": "",
        "children": [
            {
                "id": "tg",
                "label": "my_custom_name",
                "children": [
                    {"id": "tg.task1", "label": "task1"},
                    {"id": "tg.task2", "label": "task2"},
                ],
            },
        ],
    }

    assert extract_node_id(task_group_to_dict(dag.task_group), include_label=True) == expected_node_id
