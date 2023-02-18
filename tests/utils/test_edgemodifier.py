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

from datetime import datetime, timedelta

import pytest

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.www.views import dag_edges

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": datetime.today(),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@pytest.fixture
def test_dag():
    """Creates a test DAG with a few operators to test on."""

    def f(task_id):
        return f"OP:{task_id}"

    with DAG(dag_id="test_xcom_dag", default_args=DEFAULT_ARGS) as dag:
        operators = [PythonOperator(python_callable=f, task_id="test_op_%i" % i) for i in range(4)]
        return dag, operators


@pytest.fixture
def test_taskgroup_dag():
    """Creates a test DAG with a few operators to test on, with some in a task group."""

    def f(task_id):
        return f"OP:{task_id}"

    with DAG(dag_id="test_xcom_dag", default_args=DEFAULT_ARGS) as dag:
        op1 = PythonOperator(python_callable=f, task_id="test_op_1")
        op4 = PythonOperator(python_callable=f, task_id="test_op_4")
        with TaskGroup("group_1") as group:
            op2 = PythonOperator(python_callable=f, task_id="test_op_2")
            op3 = PythonOperator(python_callable=f, task_id="test_op_3")
            return dag, group, (op1, op2, op3, op4)


@pytest.fixture
def test_complex_taskgroup_dag():
    """Creates a test DAG with many operators and a task group."""

    def f(task_id):
        return f"OP:{task_id}"

    with DAG(dag_id="test_complex_dag", default_args=DEFAULT_ARGS) as dag:
        with TaskGroup("group_1") as group:
            group_emp1 = EmptyOperator(task_id="group_empty1")
            group_emp2 = EmptyOperator(task_id="group_empty2")
            group_emp3 = EmptyOperator(task_id="group_empty3")
        emp_in1 = EmptyOperator(task_id="empty_in1")
        emp_in2 = EmptyOperator(task_id="empty_in2")
        emp_in3 = EmptyOperator(task_id="empty_in3")
        emp_in4 = EmptyOperator(task_id="empty_in4")
        emp_out1 = EmptyOperator(task_id="empty_out1")
        emp_out2 = EmptyOperator(task_id="empty_out2")
        emp_out3 = EmptyOperator(task_id="empty_out3")
        emp_out4 = EmptyOperator(task_id="empty_out4")
        op_in1 = PythonOperator(python_callable=f, task_id="op_in1")
        op_out1 = PythonOperator(python_callable=f, task_id="op_out1")

        return (
            dag,
            group,
            (
                group_emp1,
                group_emp2,
                group_emp3,
                emp_in1,
                emp_in2,
                emp_in3,
                emp_in4,
                emp_out1,
                emp_out2,
                emp_out3,
                emp_out4,
                op_in1,
                op_out1,
            ),
        )


@pytest.fixture
def test_multiple_taskgroups_dag():
    """Creates a test DAG with many operators and multiple task groups."""

    def f(task_id):
        return f"OP:{task_id}"

    with DAG(dag_id="test_multiple_task_group_dag", default_args=DEFAULT_ARGS) as dag:
        with TaskGroup("group1") as group1:
            group1_emp1 = EmptyOperator(task_id="group1_empty1")
            group1_emp2 = EmptyOperator(task_id="group1_empty2")
            group1_emp3 = EmptyOperator(task_id="group1_empty3")
        with TaskGroup("group2") as group2:
            group2_emp1 = EmptyOperator(task_id="group2_empty1")
            group2_emp2 = EmptyOperator(task_id="group2_empty2")
            group2_emp3 = EmptyOperator(task_id="group2_empty3")
            group2_op1 = PythonOperator(python_callable=f, task_id="group2_op1")
            group2_op2 = PythonOperator(python_callable=f, task_id="group2_op2")

            with TaskGroup("group3") as group3:
                group3_emp1 = EmptyOperator(task_id="group3_empty1")
                group3_emp2 = EmptyOperator(task_id="group3_empty2")
                group3_emp3 = EmptyOperator(task_id="group3_empty3")
        emp_in1 = EmptyOperator(task_id="empty_in1")
        emp_in2 = EmptyOperator(task_id="empty_in2")
        emp_in3 = EmptyOperator(task_id="empty_in3")
        emp_in4 = EmptyOperator(task_id="empty_in4")
        emp_out1 = EmptyOperator(task_id="empty_out1")
        emp_out2 = EmptyOperator(task_id="empty_out2")
        emp_out3 = EmptyOperator(task_id="empty_out3")
        emp_out4 = EmptyOperator(task_id="empty_out4")
        op_in1 = PythonOperator(python_callable=f, task_id="op_in1")
        op_out1 = PythonOperator(python_callable=f, task_id="op_out1")

        return (
            dag,
            group1,
            group2,
            group3,
            (
                group1_emp1,
                group1_emp2,
                group1_emp3,
                group2_emp1,
                group2_emp2,
                group2_emp3,
                group2_op1,
                group2_op2,
                group3_emp1,
                group3_emp2,
                group3_emp3,
                emp_in1,
                emp_in2,
                emp_in3,
                emp_in4,
                emp_out1,
                emp_out2,
                emp_out3,
                emp_out4,
                op_in1,
                op_out1,
            ),
        )


@pytest.fixture
def simple_dag_expected_edges():
    return [
        {"source_id": "group_1.downstream_join_id", "target_id": "test_op_4"},
        {"source_id": "group_1.test_op_2", "target_id": "group_1.downstream_join_id"},
        {"source_id": "group_1.test_op_3", "target_id": "group_1.downstream_join_id"},
        {"source_id": "group_1.upstream_join_id", "target_id": "group_1.test_op_2"},
        {"source_id": "group_1.upstream_join_id", "target_id": "group_1.test_op_3"},
        {"label": "Label", "source_id": "test_op_1", "target_id": "group_1.upstream_join_id"},
    ]


@pytest.fixture
def complex_dag_expected_edges():
    return [
        {"source_id": "empty_in1", "target_id": "group_1.upstream_join_id"},
        {
            "label": "label emp_in2 <=> group",
            "source_id": "empty_in2",
            "target_id": "group_1.upstream_join_id",
        },
        {
            "label": "label emp_in3/emp_in4 <=> group",
            "source_id": "empty_in3",
            "target_id": "group_1.upstream_join_id",
        },
        {
            "label": "label emp_in3/emp_in4 <=> group",
            "source_id": "empty_in4",
            "target_id": "group_1.upstream_join_id",
        },
        {"source_id": "group_1.downstream_join_id", "target_id": "empty_out1"},
        {
            "label": "label group <=> emp_out2",
            "source_id": "group_1.downstream_join_id",
            "target_id": "empty_out2",
        },
        {
            "label": "label group <=> emp_out3/emp_out4",
            "source_id": "group_1.downstream_join_id",
            "target_id": "empty_out3",
        },
        {
            "label": "label group <=> emp_out3/emp_out4",
            "source_id": "group_1.downstream_join_id",
            "target_id": "empty_out4",
        },
        {
            "label": "label group <=> op_out1",
            "source_id": "group_1.downstream_join_id",
            "target_id": "op_out1",
        },
        {"source_id": "group_1.group_empty1", "target_id": "group_1.downstream_join_id"},
        {"source_id": "group_1.group_empty2", "target_id": "group_1.group_empty1"},
        {"source_id": "group_1.group_empty3", "target_id": "group_1.group_empty1"},
        {"source_id": "group_1.upstream_join_id", "target_id": "group_1.group_empty2"},
        {"source_id": "group_1.upstream_join_id", "target_id": "group_1.group_empty3"},
        {
            "label": "label op_in1 <=> group",
            "source_id": "op_in1",
            "target_id": "group_1.upstream_join_id",
        },
    ]


@pytest.fixture
def multiple_taskgroups_dag_expected_edges():
    return [
        {"source_id": "empty_in1", "target_id": "group1.upstream_join_id"},
        {
            "label": "label emp_in2 <=> group1",
            "source_id": "empty_in2",
            "target_id": "group1.upstream_join_id",
        },
        {
            "label": "label emp_in3/emp_in4 <=> group1",
            "source_id": "empty_in3",
            "target_id": "group1.upstream_join_id",
        },
        {
            "label": "label emp_in3/emp_in4 <=> group1",
            "source_id": "empty_in4",
            "target_id": "group1.upstream_join_id",
        },
        {
            "label": "label group1 <=> group2",
            "source_id": "group1.downstream_join_id",
            "target_id": "group2.upstream_join_id",
        },
        {
            "label": "label group1.group1_emp1 <=> group1.group1_emp2",
            "source_id": "group1.group1_empty1",
            "target_id": "group1.group1_empty3",
        },
        {"source_id": "group1.group1_empty2", "target_id": "group1.downstream_join_id"},
        {"source_id": "group1.group1_empty3", "target_id": "group1.downstream_join_id"},
        {"source_id": "group1.upstream_join_id", "target_id": "group1.group1_empty1"},
        {"source_id": "group1.upstream_join_id", "target_id": "group1.group1_empty2"},
        {
            "label": "label group2.group2_emp1 <=> group2.group2_emp2/group2.group2_emp3",
            "source_id": "group2.group2_empty1",
            "target_id": "group2.group2_empty2",
        },
        {
            "label": "label group2.group2_emp1 <=> group2.group2_emp2/group2.group2_emp3",
            "source_id": "group2.group2_empty1",
            "target_id": "group2.group2_empty3",
        },
        {
            "label": "label group2.group2_emp1/group2.group2_emp2 <=> group2.group2_emp3",
            "source_id": "group2.group2_empty2",
            "target_id": "group2.group2_empty3",
        },
        {
            "label": "label group2.group2_emp3 <=> group3",
            "source_id": "group2.group2_empty3",
            "target_id": "group2.group3.upstream_join_id",
        },
        {
            "label": "label group2.group2_op1 <=> group2.group2_op2",
            "source_id": "group2.group2_op1",
            "target_id": "group2.group2_op2",
        },
        {
            "label": "label group2.group2_op2 <=> group3",
            "source_id": "group2.group2_op2",
            "target_id": "group2.group3.upstream_join_id",
        },
        {"source_id": "group2.group3.downstream_join_id", "target_id": "empty_out1"},
        {
            "label": "label group3 <=> emp_out2",
            "source_id": "group2.group3.downstream_join_id",
            "target_id": "empty_out2",
        },
        {
            "label": "label group3 <=> emp_out3/emp_out4",
            "source_id": "group2.group3.downstream_join_id",
            "target_id": "empty_out3",
        },
        {
            "label": "label group3 <=> emp_out3/emp_out4",
            "source_id": "group2.group3.downstream_join_id",
            "target_id": "empty_out4",
        },
        {
            "label": "label group3 <=> op_out1",
            "source_id": "group2.group3.downstream_join_id",
            "target_id": "op_out1",
        },
        {"source_id": "group2.group3.group3_empty1", "target_id": "group2.group3.downstream_join_id"},
        {"source_id": "group2.group3.group3_empty2", "target_id": "group2.group3.downstream_join_id"},
        {"source_id": "group2.group3.group3_empty3", "target_id": "group2.group3.downstream_join_id"},
        {"source_id": "group2.group3.upstream_join_id", "target_id": "group2.group3.group3_empty1"},
        {"source_id": "group2.group3.upstream_join_id", "target_id": "group2.group3.group3_empty2"},
        {"source_id": "group2.group3.upstream_join_id", "target_id": "group2.group3.group3_empty3"},
        {"source_id": "group2.upstream_join_id", "target_id": "group2.group2_empty1"},
        {"source_id": "group2.upstream_join_id", "target_id": "group2.group2_op1"},
        {"label": "label op_in1 <=> group1", "source_id": "op_in1", "target_id": "group1.upstream_join_id"},
    ]


def compare_dag_edges(current, expected):
    assert len(current) == len(expected)

    for i in current:
        assert current.count(i) == expected.count(i), f"The unexpected DAG edge: {i}"


class TestEdgeModifierBuilding:
    """
    Tests that EdgeModifiers work when composed with Tasks (either via >>
    or set_upstream styles)
    """

    def test_operator_set(self, test_dag):
        """Tests the set_upstream/downstream style with a plain operator"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1.set_downstream(op2, Label("Label 1"))
        op3.set_upstream(op2, Label("Label 2"))
        op4.set_upstream(op2)
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op2.task_id, op3.task_id) == {"label": "Label 2"}
        assert dag.get_edge_info(op2.task_id, op4.task_id) == {}

    def test_tasklist_set(self, test_dag):
        """Tests the set_upstream/downstream style with a list of operators"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1.set_downstream([op2, op3], Label("Label 1"))
        op4.set_upstream(op2)
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op1.task_id, op3.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op2.task_id, op4.task_id) == {}

    def test_xcomarg_set(self, test_dag):
        """Tests the set_upstream/downstream style with an XComArg"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1_arg = XComArg(op1, "test_key")
        op1_arg.set_downstream(op2, Label("Label 1"))
        op1.set_downstream([op3, op4])
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op1.task_id, op4.task_id) == {}

    def test_taskgroup_set(self, test_taskgroup_dag):
        """Tests the set_upstream/downstream style with a TaskGroup"""
        # Unpack the fixture
        dag, group, (op1, op2, op3, op4) = test_taskgroup_dag
        # Arrange them with a Label in the middle
        op1.set_downstream(group, Label("Group label"))
        group.set_downstream(op4)
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Group label"}
        assert dag.get_edge_info(op1.task_id, op3.task_id) == {"label": "Group label"}
        assert dag.get_edge_info(op3.task_id, op4.task_id) == {}

    def test_operator_shift(self, test_dag):
        """Tests the >> / << style with a plain operator"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1 >> Label("Label 1") >> op2
        op3 << Label("Label 2") << op2 >> op4
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op2.task_id, op3.task_id) == {"label": "Label 2"}
        assert dag.get_edge_info(op2.task_id, op4.task_id) == {}

    def test_tasklist_shift(self, test_dag):
        """Tests the >> / << style with a list of operators"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1 >> Label("Label 1") >> [op2, op3] << Label("Label 2") << op4
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op1.task_id, op3.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op4.task_id, op2.task_id) == {"label": "Label 2"}

    def test_xcomarg_shift(self, test_dag):
        """Tests the >> / << style with an XComArg"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1_arg = XComArg(op1, "test_key")
        op1_arg >> Label("Label 1") >> [op2, op3]
        op1_arg >> op4
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op1.task_id, op4.task_id) == {}

    def test_taskgroup_shift(self, test_taskgroup_dag):
        """Tests the set_upstream/downstream style with a TaskGroup"""
        # Unpack the fixture
        dag, group, (op1, op2, op3, op4) = test_taskgroup_dag
        # Arrange them with a Label in the middle
        op1 >> Label("Group label") >> group >> op4
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Group label"}
        assert dag.get_edge_info(op1.task_id, op3.task_id) == {"label": "Group label"}
        assert dag.get_edge_info(op3.task_id, op4.task_id) == {}

    def test_simple_dag(self, test_taskgroup_dag, simple_dag_expected_edges):
        """Tests the simple dag with a TaskGroup and a Label"""
        dag, group, (op1, op2, op3, op4) = test_taskgroup_dag
        op1 >> Label("Label") >> group >> op4
        compare_dag_edges(dag_edges(dag), simple_dag_expected_edges)

    def test_simple_reversed_dag(self, test_taskgroup_dag, simple_dag_expected_edges):
        """Tests the simple reversed dag with a TaskGroup and a Label"""
        dag, group, (op1, op2, op3, op4) = test_taskgroup_dag
        op4 << group << Label("Label") << op1
        compare_dag_edges(dag_edges(dag), simple_dag_expected_edges)

    def test_complex_dag(self, test_complex_taskgroup_dag, complex_dag_expected_edges):
        """Tests the complex dag with a TaskGroup and a Label"""
        (
            dag,
            group,
            (
                group_emp1,
                group_emp2,
                group_emp3,
                emp_in1,
                emp_in2,
                emp_in3,
                emp_in4,
                emp_out1,
                emp_out2,
                emp_out3,
                emp_out4,
                op_in1,
                op_out1,
            ),
        ) = test_complex_taskgroup_dag

        [group_emp2, group_emp3] >> group_emp1

        emp_in1 >> group
        emp_in2 >> Label("label emp_in2 <=> group") >> group
        [emp_in3, emp_in4] >> Label("label emp_in3/emp_in4 <=> group") >> group
        XComArg(op_in1, "test_key") >> Label("label op_in1 <=> group") >> group

        group >> emp_out1
        group >> Label("label group <=> emp_out2") >> emp_out2
        group >> Label("label group <=> emp_out3/emp_out4") >> [emp_out3, emp_out4]
        group >> Label("label group <=> op_out1") >> XComArg(op_out1, "test_key")

        compare_dag_edges(dag_edges(dag), complex_dag_expected_edges)

    def test_complex_reversed_dag(self, test_complex_taskgroup_dag, complex_dag_expected_edges):
        """Tests the complex reversed dag with a TaskGroup and a Label"""
        (
            dag,
            group,
            (
                group_emp1,
                group_emp2,
                group_emp3,
                emp_in1,
                emp_in2,
                emp_in3,
                emp_in4,
                emp_out1,
                emp_out2,
                emp_out3,
                emp_out4,
                op_in1,
                op_out1,
            ),
        ) = test_complex_taskgroup_dag

        group_emp1 << [group_emp2, group_emp3]

        group << emp_in1
        group << Label("label emp_in2 <=> group") << emp_in2
        group << Label("label emp_in3/emp_in4 <=> group") << [emp_in3, emp_in4]
        group << Label("label op_in1 <=> group") << XComArg(op_in1, "test_key")

        emp_out1 << group
        emp_out2 << Label("label group <=> emp_out2") << group
        [emp_out3, emp_out4] << Label("label group <=> emp_out3/emp_out4") << group
        XComArg(op_out1, "test_key") << Label("label group <=> op_out1") << group

        compare_dag_edges(dag_edges(dag), complex_dag_expected_edges)

    def test_multiple_task_groups_dag(
        self, test_multiple_taskgroups_dag, multiple_taskgroups_dag_expected_edges
    ):
        """Tests multiple task groups and labels"""
        (
            dag,
            group1,
            group2,
            group3,
            (
                group1_emp1,
                group1_emp2,
                group1_emp3,
                group2_emp1,
                group2_emp2,
                group2_emp3,
                group2_op1,
                group2_op2,
                group3_emp1,
                group3_emp2,
                group3_emp3,
                emp_in1,
                emp_in2,
                emp_in3,
                emp_in4,
                emp_out1,
                emp_out2,
                emp_out3,
                emp_out4,
                op_in1,
                op_out1,
            ),
        ) = test_multiple_taskgroups_dag

        group1_emp1 >> Label("label group1.group1_emp1 <=> group1.group1_emp2") >> group1_emp3

        emp_in1 >> group1
        emp_in2 >> Label("label emp_in2 <=> group1") >> group1
        [emp_in3, emp_in4] >> Label("label emp_in3/emp_in4 <=> group1") >> group1
        XComArg(op_in1, "test_key") >> Label("label op_in1 <=> group1") >> group1

        (
            [group2_emp1, group2_emp2]
            >> Label("label group2.group2_emp1/group2.group2_emp2 <=> group2.group2_emp3")
            >> group2_emp3
        )
        (
            group2_emp1
            >> Label("label group2.group2_emp1 <=> group2.group2_emp2/group2.group2_emp3")
            >> [group2_emp2, group2_emp3]
        )
        group2_emp3 >> Label("label group2.group2_emp3 <=> group3") >> group3

        (
            XComArg(group2_op1, "test_key")
            >> Label("label group2.group2_op1 <=> group2.group2_op2")
            >> XComArg(group2_op2, "test_key")
        )
        XComArg(group2_op2, "test_key") >> Label("label group2.group2_op2 <=> group3") >> group3

        group3 >> emp_out1
        group3 >> Label("label group3 <=> emp_out2") >> emp_out2
        group3 >> Label("label group3 <=> emp_out3/emp_out4") >> [emp_out3, emp_out4]
        group3 >> Label("label group3 <=> op_out1") >> XComArg(op_out1, "test_key")

        group1 >> Label("label group1 <=> group2") >> group2

        compare_dag_edges(dag_edges(dag), multiple_taskgroups_dag_expected_edges)

    def test_multiple_task_groups_reversed_dag(
        self, test_multiple_taskgroups_dag, multiple_taskgroups_dag_expected_edges
    ):
        """Tests multiple task groups and labels"""
        (
            dag,
            group1,
            group2,
            group3,
            (
                group1_emp1,
                group1_emp2,
                group1_emp3,
                group2_emp1,
                group2_emp2,
                group2_emp3,
                group2_op1,
                group2_op2,
                group3_emp1,
                group3_emp2,
                group3_emp3,
                emp_in1,
                emp_in2,
                emp_in3,
                emp_in4,
                emp_out1,
                emp_out2,
                emp_out3,
                emp_out4,
                op_in1,
                op_out1,
            ),
        ) = test_multiple_taskgroups_dag

        group1_emp3 << Label("label group1.group1_emp1 <=> group1.group1_emp2") << group1_emp1

        group1 << emp_in1
        group1 << Label("label emp_in2 <=> group1") << emp_in2
        group1 << Label("label emp_in3/emp_in4 <=> group1") << [emp_in3, emp_in4]
        group1 << Label("label op_in1 <=> group1") << XComArg(op_in1, "test_key")

        (
            group2_emp3
            << Label("label group2.group2_emp1/group2.group2_emp2 <=> group2.group2_emp3")
            << [group2_emp1, group2_emp2]
        )
        (
            [group2_emp2, group2_emp3]
            << Label("label group2.group2_emp1 <=> group2.group2_emp2/group2.group2_emp3")
            << group2_emp1
        )
        group3 << Label("label group2.group2_emp3 <=> group3") << group2_emp3

        (
            XComArg(group2_op2, "test_key")
            << Label("label group2.group2_op1 <=> group2.group2_op2")
            << XComArg(group2_op1, "test_key")
        )
        group3 << Label("label group2.group2_op2 <=> group3") << XComArg(group2_op2, "test_key")

        emp_out1 << group3
        emp_out2 << Label("label group3 <=> emp_out2") << group3
        [emp_out3, emp_out4] << Label("label group3 <=> emp_out3/emp_out4") << group3
        XComArg(op_out1, "test_key") << Label("label group3 <=> op_out1") << group3

        group2 << Label("label group1 <=> group2") << group1

        compare_dag_edges(dag_edges(dag), multiple_taskgroups_dag_expected_edges)
