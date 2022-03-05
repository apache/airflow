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

from datetime import datetime, timedelta

import pytest

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.dummy import DummyOperator
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
            group_dm1 = DummyOperator(task_id="group_dummy1")
            group_dm2 = DummyOperator(task_id="group_dummy2")
            group_dm3 = DummyOperator(task_id="group_dummy3")
        dm_in1 = DummyOperator(task_id="dummy_in1")
        dm_in2 = DummyOperator(task_id="dummy_in2")
        dm_in3 = DummyOperator(task_id="dummy_in3")
        dm_in4 = DummyOperator(task_id="dummy_in4")
        dm_out1 = DummyOperator(task_id="dummy_out1")
        dm_out2 = DummyOperator(task_id="dummy_out2")
        dm_out3 = DummyOperator(task_id="dummy_out3")
        dm_out4 = DummyOperator(task_id="dummy_out4")
        op_in1 = PythonOperator(python_callable=f, task_id="op_in1")
        op_out1 = PythonOperator(python_callable=f, task_id="op_out1")

        return (
            dag,
            group,
            (
                group_dm1,
                group_dm2,
                group_dm3,
                dm_in1,
                dm_in2,
                dm_in3,
                dm_in4,
                dm_out1,
                dm_out2,
                dm_out3,
                dm_out4,
                op_in1,
                op_out1,
            ),
        )


@pytest.fixture
def simple_dag_expected_edges():
    return [
        {'source_id': 'group_1.downstream_join_id', 'target_id': 'test_op_4'},
        {'source_id': 'group_1.test_op_2', 'target_id': 'group_1.downstream_join_id'},
        {'source_id': 'group_1.test_op_3', 'target_id': 'group_1.downstream_join_id'},
        {'source_id': 'group_1.upstream_join_id', 'target_id': 'group_1.test_op_2'},
        {'source_id': 'group_1.upstream_join_id', 'target_id': 'group_1.test_op_3'},
        {'label': 'Label', 'source_id': 'test_op_1', 'target_id': 'group_1.upstream_join_id'},
    ]


@pytest.fixture
def complex_dag_expected_edges():
    return [
        {'source_id': 'dummy_in1', 'target_id': 'group_1.upstream_join_id'},
        {
            'label': 'label dm_in2 <=> group',
            'source_id': 'dummy_in2',
            'target_id': 'group_1.upstream_join_id',
        },
        {
            'label': 'label dm_in3/dm_in4 <=> group',
            'source_id': 'dummy_in3',
            'target_id': 'group_1.upstream_join_id',
        },
        {
            'label': 'label dm_in3/dm_in4 <=> group',
            'source_id': 'dummy_in4',
            'target_id': 'group_1.upstream_join_id',
        },
        {'source_id': 'group_1.downstream_join_id', 'target_id': 'dummy_out1'},
        {
            'label': 'label group <=> dm_out2',
            'source_id': 'group_1.downstream_join_id',
            'target_id': 'dummy_out2',
        },
        {
            'label': 'label group <=> dm_out3/dm_out4',
            'source_id': 'group_1.downstream_join_id',
            'target_id': 'dummy_out3',
        },
        {
            'label': 'label group <=> dm_out3/dm_out4',
            'source_id': 'group_1.downstream_join_id',
            'target_id': 'dummy_out4',
        },
        {
            'source_id': 'group_1.group_dummy1',
            'target_id': 'group_1.downstream_join_id',
        },
        {'source_id': 'group_1.group_dummy2', 'target_id': 'group_1.group_dummy1'},
        {'source_id': 'group_1.group_dummy3', 'target_id': 'group_1.group_dummy1'},
        {'source_id': 'group_1.upstream_join_id', 'target_id': 'group_1.group_dummy2'},
        {'source_id': 'group_1.upstream_join_id', 'target_id': 'group_1.group_dummy3'},
        {
            'label': 'label op_in1 <=> group',
            'source_id': 'op_in1',
            'target_id': 'group_1.upstream_join_id',
        },
        {
            'label': 'label group <=> op_out1',
            'source_id': 'group_1.downstream_join_id',
            'target_id': 'op_out1',
        },
    ]


def compare_dag_edges(current, expected):
    assert len(current) == len(expected)

    for i in current:
        assert current.count(i) == expected.count(i), f'The unexpected DAG edge: {i}'


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
                group_dm1,
                group_dm2,
                group_dm3,
                dm_in1,
                dm_in2,
                dm_in3,
                dm_in4,
                dm_out1,
                dm_out2,
                dm_out3,
                dm_out4,
                op_in1,
                op_out1,
            ),
        ) = test_complex_taskgroup_dag

        [group_dm2, group_dm3] >> group_dm1

        dm_in1 >> group
        dm_in2 >> Label('label dm_in2 <=> group') >> group
        [dm_in3, dm_in4] >> Label('label dm_in3/dm_in4 <=> group') >> group
        XComArg(op_in1, 'test_key') >> Label('label op_in1 <=> group') >> group

        group >> dm_out1
        group >> Label('label group <=> dm_out2') >> dm_out2
        group >> Label('label group <=> dm_out3/dm_out4') >> [dm_out3, dm_out4]
        group >> Label('label group <=> op_out1') >> XComArg(op_out1, 'test_key')

        compare_dag_edges(dag_edges(dag), complex_dag_expected_edges)

    def test_complex_reversed_dag(self, test_complex_taskgroup_dag, complex_dag_expected_edges):
        """Tests the complex reversed dag with a TaskGroup and a Label"""
        (
            dag,
            group,
            (
                group_dm1,
                group_dm2,
                group_dm3,
                dm_in1,
                dm_in2,
                dm_in3,
                dm_in4,
                dm_out1,
                dm_out2,
                dm_out3,
                dm_out4,
                op_in1,
                op_out1,
            ),
        ) = test_complex_taskgroup_dag

        group_dm1 << [group_dm2, group_dm3]

        group << dm_in1
        group << Label('label dm_in2 <=> group') << dm_in2
        group << Label('label dm_in3/dm_in4 <=> group') << [dm_in3, dm_in4]
        group << Label('label op_in1 <=> group') << XComArg(op_in1, 'test_key')

        dm_out1 << group
        dm_out2 << Label('label group <=> dm_out2') << group
        [dm_out3, dm_out4] << Label('label group <=> dm_out3/dm_out4') << group
        XComArg(op_out1, 'test_key') << Label('label group <=> op_out1') << group

        compare_dag_edges(dag_edges(dag), complex_dag_expected_edges)
