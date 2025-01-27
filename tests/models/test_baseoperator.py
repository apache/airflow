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
from collections import defaultdict
from datetime import datetime
from unittest import mock

import pytest

from airflow.decorators import task as task_decorator
from airflow.exceptions import AirflowException, TaskDeferralTimeout
from airflow.models.baseoperator import (
    BaseOperator,
    chain,
    chain_linear,
    cross_downstream,
)
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.trigger import TriggerFailureReason
from airflow.providers.common.compat.lineage.entities import File
from airflow.providers.common.sql.operators import sql
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunType

from tests.models import DEFAULT_DATE
from tests_common.test_utils.mock_operators import MockOperator


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{ClassWithCustomAttributes.__name__}({str(self.__dict__)})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


class TestBaseOperator:
    def test_trigger_rule_validation(self):
        from airflow.models.abstractoperator import DEFAULT_TRIGGER_RULE

        fail_fast_dag = DAG(
            dag_id="test_dag_trigger_rule_validation",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_fast=True,
        )
        non_fail_fast_dag = DAG(
            dag_id="test_dag_trigger_rule_validation",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_fast=False,
        )

        # An operator with default trigger rule and a fail-stop dag should be allowed
        BaseOperator(task_id="test_valid_trigger_rule", dag=fail_fast_dag, trigger_rule=DEFAULT_TRIGGER_RULE)
        # An operator with non default trigger rule and a non fail-stop dag should be allowed
        BaseOperator(
            task_id="test_valid_trigger_rule", dag=non_fail_fast_dag, trigger_rule=TriggerRule.ALWAYS
        )

    def test_cross_downstream(self):
        """Test if all dependencies between tasks are all set correctly."""
        dag = DAG(dag_id="test_dag", schedule=None, start_date=datetime.now())
        start_tasks = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 4)]
        end_tasks = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(4, 7)]
        cross_downstream(from_tasks=start_tasks, to_tasks=end_tasks)

        for start_task in start_tasks:
            assert set(start_task.get_direct_relatives(upstream=False)) == set(end_tasks)

        # Begin test for `XComArgs`
        xstart_tasks = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 4)
        ]
        xend_tasks = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(4, 7)
        ]
        cross_downstream(from_tasks=xstart_tasks, to_tasks=xend_tasks)

        for xstart_task in xstart_tasks:
            assert set(xstart_task.operator.get_direct_relatives(upstream=False)) == {
                xend_task.operator for xend_task in xend_tasks
            }

    def test_chain(self):
        dag = DAG(dag_id="test_chain", schedule=None, start_date=datetime.now())

        # Begin test for classic operators with `EdgeModifiers`
        [label1, label2] = [Label(label=f"label{i}") for i in range(1, 3)]
        [op1, op2, op3, op4, op5, op6] = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 7)]
        chain(op1, [label1, label2], [op2, op3], [op4, op5], op6)

        assert {op2, op3} == set(op1.get_direct_relatives(upstream=False))
        assert [op4] == op2.get_direct_relatives(upstream=False)
        assert [op5] == op3.get_direct_relatives(upstream=False)
        assert {op4, op5} == set(op6.get_direct_relatives(upstream=True))

        assert dag.get_edge_info(upstream_task_id=op1.task_id, downstream_task_id=op2.task_id) == {
            "label": "label1"
        }
        assert dag.get_edge_info(upstream_task_id=op1.task_id, downstream_task_id=op3.task_id) == {
            "label": "label2"
        }

        # Begin test for `XComArgs` with `EdgeModifiers`
        [xlabel1, xlabel2] = [Label(label=f"xcomarg_label{i}") for i in range(1, 3)]
        [xop1, xop2, xop3, xop4, xop5, xop6] = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 7)
        ]
        chain(xop1, [xlabel1, xlabel2], [xop2, xop3], [xop4, xop5], xop6)

        assert {xop2.operator, xop3.operator} == set(xop1.operator.get_direct_relatives(upstream=False))
        assert [xop4.operator] == xop2.operator.get_direct_relatives(upstream=False)
        assert [xop5.operator] == xop3.operator.get_direct_relatives(upstream=False)
        assert {xop4.operator, xop5.operator} == set(xop6.operator.get_direct_relatives(upstream=True))

        assert dag.get_edge_info(
            upstream_task_id=xop1.operator.task_id, downstream_task_id=xop2.operator.task_id
        ) == {"label": "xcomarg_label1"}
        assert dag.get_edge_info(
            upstream_task_id=xop1.operator.task_id, downstream_task_id=xop3.operator.task_id
        ) == {"label": "xcomarg_label2"}

        # Begin test for `TaskGroups`
        [tg1, tg2] = [TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 3)]
        [op1, op2] = [BaseOperator(task_id=f"task{i}", dag=dag) for i in range(1, 3)]
        [tgop1, tgop2] = [
            BaseOperator(task_id=f"task_group_task{i}", task_group=tg1, dag=dag) for i in range(1, 3)
        ]
        [tgop3, tgop4] = [
            BaseOperator(task_id=f"task_group_task{i}", task_group=tg2, dag=dag) for i in range(1, 3)
        ]
        chain(op1, tg1, tg2, op2)

        assert {tgop1, tgop2} == set(op1.get_direct_relatives(upstream=False))
        assert {tgop3, tgop4} == set(tgop1.get_direct_relatives(upstream=False))
        assert {tgop3, tgop4} == set(tgop2.get_direct_relatives(upstream=False))
        assert [op2] == tgop3.get_direct_relatives(upstream=False)
        assert [op2] == tgop4.get_direct_relatives(upstream=False)

    def test_baseoperator_raises_exception_when_task_id_plus_taskgroup_id_exceeds_250_chars(self):
        """Test exception is raised when operator task id + taskgroup id > 250 chars."""
        dag = DAG(dag_id="foo", schedule=None, start_date=datetime.now())

        tg1 = TaskGroup("A" * 20, dag=dag)
        with pytest.raises(ValueError, match="The key has to be less than 250 characters"):
            BaseOperator(task_id="1" * 250, task_group=tg1, dag=dag)

    def test_baseoperator_with_task_id_and_taskgroup_id_less_than_250_chars(self):
        """Test exception is not raised when operator task id + taskgroup id < 250 chars."""
        dag = DAG(dag_id="foo", schedule=None, start_date=datetime.now())

        tg1 = TaskGroup("A" * 10, dag=dag)
        try:
            BaseOperator(task_id="1" * 239, task_group=tg1, dag=dag)
        except Exception as e:
            pytest.fail(f"Exception raised: {e}")

    def test_baseoperator_with_task_id_less_than_250_chars(self):
        """Test exception is not raised when operator task id  < 250 chars."""
        dag = DAG(dag_id="foo", schedule=None, start_date=datetime.now())

        try:
            BaseOperator(task_id="1" * 249, dag=dag)
        except Exception as e:
            pytest.fail(f"Exception raised: {e}")

    def test_chain_linear(self):
        dag = DAG(dag_id="test_chain_linear", schedule=None, start_date=datetime.now())

        t1, t2, t3, t4, t5, t6, t7 = (BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 8))
        chain_linear(t1, [t2, t3, t4], [t5, t6], t7)

        assert set(t1.get_direct_relatives(upstream=False)) == {t2, t3, t4}
        assert set(t2.get_direct_relatives(upstream=False)) == {t5, t6}
        assert set(t3.get_direct_relatives(upstream=False)) == {t5, t6}
        assert set(t7.get_direct_relatives(upstream=True)) == {t5, t6}

        t1, t2, t3, t4, t5, t6 = (
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 7)
        )
        chain_linear(t1, [t2, t3], [t4, t5], t6)

        assert set(t1.operator.get_direct_relatives(upstream=False)) == {t2.operator, t3.operator}
        assert set(t2.operator.get_direct_relatives(upstream=False)) == {t4.operator, t5.operator}
        assert set(t3.operator.get_direct_relatives(upstream=False)) == {t4.operator, t5.operator}
        assert set(t6.operator.get_direct_relatives(upstream=True)) == {t4.operator, t5.operator}

        # Begin test for `TaskGroups`
        tg1, tg2 = (TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 3))
        op1, op2 = (BaseOperator(task_id=f"task{i}", dag=dag) for i in range(1, 3))
        tgop1, tgop2 = (
            BaseOperator(task_id=f"task_group_task{i}", task_group=tg1, dag=dag) for i in range(1, 3)
        )
        tgop3, tgop4 = (
            BaseOperator(task_id=f"task_group_task{i}", task_group=tg2, dag=dag) for i in range(1, 3)
        )
        chain_linear(op1, tg1, tg2, op2)

        assert set(op1.get_direct_relatives(upstream=False)) == {tgop1, tgop2}
        assert set(tgop1.get_direct_relatives(upstream=False)) == {tgop3, tgop4}
        assert set(tgop2.get_direct_relatives(upstream=False)) == {tgop3, tgop4}
        assert set(tgop3.get_direct_relatives(upstream=False)) == {op2}
        assert set(tgop4.get_direct_relatives(upstream=False)) == {op2}

        t1, t2 = (BaseOperator(task_id=f"t-{i}", dag=dag) for i in range(1, 3))
        with pytest.raises(ValueError, match="Labels are not supported"):
            chain_linear(t1, Label("hi"), t2)

        with pytest.raises(ValueError, match="nothing to do"):
            chain_linear()

        with pytest.raises(ValueError, match="Did you forget to expand"):
            chain_linear(t1)

    def test_chain_not_support_type(self):
        dag = DAG(dag_id="test_chain", schedule=None, start_date=datetime.now())
        [op1, op2] = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 3)]
        with pytest.raises(TypeError):
            chain([op1, op2], 1)

        # Begin test for `XComArgs`
        [xop1, xop2] = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 3)
        ]

        with pytest.raises(TypeError):
            chain([xop1, xop2], 1)

        # Begin test for `EdgeModifiers`
        with pytest.raises(TypeError):
            chain([Label("labe1"), Label("label2")], 1)

        # Begin test for `TaskGroups`
        [tg1, tg2] = [TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 3)]

        with pytest.raises(TypeError):
            chain([tg1, tg2], 1)

    def test_chain_different_length_iterable(self):
        dag = DAG(dag_id="test_chain", schedule=None, start_date=datetime.now())
        [label1, label2] = [Label(label=f"label{i}") for i in range(1, 3)]
        [op1, op2, op3, op4, op5] = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 6)]

        with pytest.raises(AirflowException):
            chain([op1, op2], [op3, op4, op5])

        with pytest.raises(AirflowException):
            chain([op1, op2, op3], [label1, label2])

        # Begin test for `XComArgs` with `EdgeModifiers`
        [label3, label4] = [Label(label=f"xcomarg_label{i}") for i in range(1, 3)]
        [xop1, xop2, xop3, xop4, xop5] = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 6)
        ]

        with pytest.raises(AirflowException):
            chain([xop1, xop2], [xop3, xop4, xop5])

        with pytest.raises(AirflowException):
            chain([xop1, xop2, xop3], [label1, label2])

        # Begin test for `TaskGroups`
        [tg1, tg2, tg3, tg4, tg5] = [TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 6)]

        with pytest.raises(AirflowException):
            chain([tg1, tg2], [tg3, tg4, tg5])

    def test_lineage_composition(self):
        """
        Test composition with lineage
        """
        inlet = File(url="in")
        outlet = File(url="out")
        dag = DAG("test-dag", schedule=None, start_date=DEFAULT_DATE)
        task1 = BaseOperator(task_id="op1", dag=dag)
        task2 = BaseOperator(task_id="op2", dag=dag)

        # mock
        task1.supports_lineage = True

        # note: operator precedence still applies
        inlet > task1 | (task2 > outlet)

        assert task1.get_inlet_defs() == [inlet]
        assert task2.get_inlet_defs() == [task1.task_id]
        assert task2.get_outlet_defs() == [outlet]

        fail = ClassWithCustomAttributes()
        with pytest.raises(TypeError):
            fail > task1
        with pytest.raises(TypeError):
            task1 > fail
        with pytest.raises(TypeError):
            fail | task1
        with pytest.raises(TypeError):
            task1 | fail

        task3 = BaseOperator(task_id="op3", dag=dag)
        extra = File(url="extra")
        [inlet, extra] > task3

        assert task3.get_inlet_defs() == [inlet, extra]

        task1.supports_lineage = False
        with pytest.raises(ValueError):
            task1 | task3

        assert task2.supports_lineage is False
        task2 | task3
        assert len(task3.get_inlet_defs()) == 3

        task4 = BaseOperator(task_id="op4", dag=dag)
        task4 > [inlet, outlet, extra]
        assert task4.get_outlet_defs() == [inlet, outlet, extra]

    def test_pre_execute_hook(self):
        hook = mock.MagicMock()

        op = BaseOperator(task_id="test_task", pre_execute=hook)
        op_copy = op.prepare_for_execution()
        op_copy.pre_execute({})
        assert hook.called

    def test_post_execute_hook(self):
        hook = mock.MagicMock()

        op = BaseOperator(task_id="test_task", post_execute=hook)
        op_copy = op.prepare_for_execution()
        op_copy.post_execute({})
        assert hook.called

    def test_task_naive_datetime(self):
        naive_datetime = DEFAULT_DATE.replace(tzinfo=None)

        op_no_dag = BaseOperator(
            task_id="test_task_naive_datetime", start_date=naive_datetime, end_date=naive_datetime
        )

        assert op_no_dag.start_date.tzinfo
        assert op_no_dag.end_date.tzinfo

    # ensure the default logging config is used for this test, no matter what ran before
    @pytest.mark.usefixtures("reset_logging_config")
    def test_logging_propogated_by_default(self, caplog):
        """Test that when set_context hasn't been called that log records are emitted"""
        BaseOperator(task_id="test").log.warning("test")
        # This looks like "how could it fail" but this actually checks that the handler called `emit`. Testing
        # the other case (that when we have set_context it goes to the file is harder to achieve without
        # leaking a lot of state)
        assert caplog.messages == ["test"]

    def test_resume_execution(self):
        op = BaseOperator(task_id="hi")
        with pytest.raises(TaskDeferralTimeout):
            op.resume_execution(
                next_method="__fail__",
                next_kwargs={"error": TriggerFailureReason.TRIGGER_TIMEOUT},
                context={},
            )


def test_deepcopy():
    # Test bug when copying an operator attached to a DAG
    with DAG("dag0", schedule=None, start_date=DEFAULT_DATE) as dag:

        @dag.task
        def task0():
            pass

        MockOperator(task_id="task1", arg1=task0())
    copy.deepcopy(dag)


def get_states(dr):
    """
    For a given dag run, get a dict of states.

    Example::
        {
            "my_setup": "success",
            "my_teardown": {0: "success", 1: "success", 2: "success"},
            "my_work": "failed",
        }
    """
    ti_dict = defaultdict(dict)
    for ti in dr.get_task_instances():
        if ti.map_index == -1:
            ti_dict[ti.task_id] = ti.state
        else:
            ti_dict[ti.task_id][ti.map_index] = ti.state
    return dict(ti_dict)


@pytest.mark.db_test
def test_teardown_and_fail_fast(dag_maker):
    """
    when fail_fast enabled, teardowns should run according to their setups.
    in this case, the second teardown skips because its setup skips.
    """

    with dag_maker(fail_fast=True) as dag:
        for num in (1, 2):
            with TaskGroup(f"tg_{num}"):

                @task_decorator
                def my_setup():
                    print("setting up multiple things")
                    return [1, 2, 3]

                @task_decorator
                def my_work(val):
                    print(f"doing work with multiple things: {val}")
                    raise ValueError("this fails")
                    return val

                @task_decorator
                def my_teardown():
                    print("teardown")

                s = my_setup()
                t = my_teardown().as_teardown(setups=s)
                with t:
                    my_work(s)
    tg1, tg2 = dag.task_group.children.values()
    tg1 >> tg2
    dr = dag.test()
    states = get_states(dr)
    expected = {
        "tg_1.my_setup": "success",
        "tg_1.my_teardown": "success",
        "tg_1.my_work": "failed",
        "tg_2.my_setup": "skipped",
        "tg_2.my_teardown": "skipped",
        "tg_2.my_work": "skipped",
    }
    assert states == expected


@pytest.mark.db_test
def test_get_task_instances(session):
    import pendulum

    first_logical_date = pendulum.datetime(2023, 1, 1)
    second_logical_date = pendulum.datetime(2023, 1, 2)
    third_logical_date = pendulum.datetime(2023, 1, 3)

    test_dag = DAG(dag_id="test_dag", schedule=None, start_date=first_logical_date)
    task = BaseOperator(task_id="test_task", dag=test_dag)

    common_dr_kwargs = {
        "dag_id": test_dag.dag_id,
        "run_type": DagRunType.MANUAL,
    }
    dr1 = DagRun(logical_date=first_logical_date, run_id="test_run_id_1", **common_dr_kwargs)
    ti_1 = TaskInstance(run_id=dr1.run_id, task=task)
    dr2 = DagRun(logical_date=second_logical_date, run_id="test_run_id_2", **common_dr_kwargs)
    ti_2 = TaskInstance(run_id=dr2.run_id, task=task)
    dr3 = DagRun(logical_date=third_logical_date, run_id="test_run_id_3", **common_dr_kwargs)
    ti_3 = TaskInstance(run_id=dr3.run_id, task=task)
    session.add_all([dr1, dr2, dr3, ti_1, ti_2, ti_3])
    session.commit()

    # get all task instances
    assert task.get_task_instances(session=session) == [ti_1, ti_2, ti_3]
    # get task instances with start_date
    assert task.get_task_instances(session=session, start_date=second_logical_date) == [ti_2, ti_3]
    # get task instances with end_date
    assert task.get_task_instances(session=session, end_date=second_logical_date) == [ti_1, ti_2]
    # get task instances with start_date and end_date
    assert task.get_task_instances(
        session=session, start_date=second_logical_date, end_date=second_logical_date
    ) == [ti_2]


def test_mro():
    class Mixin(sql.BaseSQLOperator):
        pass

    class Branch(Mixin, sql.BranchSQLOperator):
        pass

    # The following throws an exception if metaclass breaks MRO:
    #   airflow.exceptions.AirflowException: Invalid arguments were passed to Branch (task_id: test). Invalid arguments were:
    #   **kwargs: {'sql': 'sql', 'follow_task_ids_if_true': ['x'], 'follow_task_ids_if_false': ['y']}
    op = Branch(
        task_id="test",
        conn_id="abc",
        sql="sql",
        follow_task_ids_if_true=["x"],
        follow_task_ids_if_false=["y"],
    )
    assert isinstance(op, Branch)
