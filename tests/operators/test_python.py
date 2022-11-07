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
import logging
import os
import sys
import unittest.mock
import warnings
from collections import namedtuple
from datetime import date, datetime, timedelta
from subprocess import CalledProcessError

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import clear_task_instances, set_current_context
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    ShortCircuitOperator,
    get_current_context,
)
from airflow.utils import timezone
from airflow.utils.context import AirflowContextDeprecationWarning, Context
from airflow.utils.python_virtualenv import prepare_virtualenv
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunType
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.db import clear_db_runs

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = [
    "AIRFLOW_CTX_DAG_ID",
    "AIRFLOW_CTX_TASK_ID",
    "AIRFLOW_CTX_EXECUTION_DATE",
    "AIRFLOW_CTX_DAG_RUN_ID",
]

TEMPLATE_SEARCHPATH = os.path.join(AIRFLOW_MAIN_FOLDER, "tests", "config_templates")


class Call:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def build_recording_function(calls_collection):
    """
    We can not use a Mock instance as a PythonOperator callable function or some tests fail with a
    TypeError: Object of type Mock is not JSON serializable
    Then using this custom function recording custom Call objects for further testing
    (replacing Mock.assert_called_with assertion method)
    """

    def recording_function(*args, **kwargs):
        calls_collection.append(Call(*args, **kwargs))

    return recording_function


def assert_calls_equal(first: Call, second: Call) -> None:
    assert isinstance(first, Call)
    assert isinstance(second, Call)
    assert first.args == second.args
    # eliminate context (conf, dag_run, task_instance, etc.)
    test_args = ["an_int", "a_date", "a_templated_string"]
    first.kwargs = {key: value for (key, value) in first.kwargs.items() if key in test_args}
    second.kwargs = {key: value for (key, value) in second.kwargs.items() if key in test_args}
    assert first.kwargs == second.kwargs


class TestPythonBase(unittest.TestCase):
    """Base test class for TestPythonOperator and TestPythonSensor classes"""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        super().setUp()
        self.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def clear_run(self):
        self.run = False


class TestPythonOperator(TestPythonBase):
    def do_run(self):
        self.run = True

    def is_run(self):
        return self.run

    def test_python_operator_run(self):
        """Tests that the python callable is invoked on task run."""
        task = PythonOperator(python_callable=self.do_run, task_id="python_operator", dag=self.dag)
        assert not self.is_run()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert self.is_run()

    def test_python_operator_python_callable_is_callable(self):
        """Tests that PythonOperator will only instantiate if
        the python_callable argument is callable."""
        not_callable = {}
        with pytest.raises(AirflowException):
            PythonOperator(python_callable=not_callable, task_id="python_operator", dag=self.dag)
        not_callable = None
        with pytest.raises(AirflowException):
            PythonOperator(python_callable=not_callable, task_id="python_operator", dag=self.dag)

    def test_python_callable_arguments_are_templatized(self):
        """Test PythonOperator op_args are templatized"""
        recorded_calls = []

        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple("Named", ["var1", "var2"])
        named_tuple = Named("{{ ds }}", "unchanged")

        task = PythonOperator(
            task_id="python_operator",
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            python_callable=build_recording_function(recorded_calls),
            op_args=[4, date(2019, 1, 1), "dag {{dag.dag_id}} ran on {{ds}}.", named_tuple],
            dag=self.dag,
        )

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ds_templated = DEFAULT_DATE.date().isoformat()
        assert 1 == len(recorded_calls)
        assert_calls_equal(
            recorded_calls[0],
            Call(
                4,
                date(2019, 1, 1),
                f"dag {self.dag.dag_id} ran on {ds_templated}.",
                Named(ds_templated, "unchanged"),
            ),
        )

    def test_python_callable_keyword_arguments_are_templatized(self):
        """Test PythonOperator op_kwargs are templatized"""
        recorded_calls = []

        task = PythonOperator(
            task_id="python_operator",
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            python_callable=build_recording_function(recorded_calls),
            op_kwargs={
                "an_int": 4,
                "a_date": date(2019, 1, 1),
                "a_templated_string": "dag {{dag.dag_id}} ran on {{ds}}.",
            },
            dag=self.dag,
        )

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        assert 1 == len(recorded_calls)
        assert_calls_equal(
            recorded_calls[0],
            Call(
                an_int=4,
                a_date=date(2019, 1, 1),
                a_templated_string=f"dag {self.dag.dag_id} ran on {DEFAULT_DATE.date().isoformat()}.",
            ),
        )

    def test_python_operator_shallow_copy_attr(self):
        def not_callable(x):
            return x

        original_task = PythonOperator(
            python_callable=not_callable,
            task_id="python_operator",
            op_kwargs={"certain_attrs": ""},
            dag=self.dag,
        )
        new_task = copy.deepcopy(original_task)
        # shallow copy op_kwargs
        assert id(original_task.op_kwargs["certain_attrs"]) == id(new_task.op_kwargs["certain_attrs"])
        # shallow copy python_callable
        assert id(original_task.python_callable) == id(new_task.python_callable)

    def test_conflicting_kwargs(self):
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        # dag is not allowed since it is a reserved keyword
        def func(dag):
            # An ValueError should be triggered since we're using dag as a
            # reserved keyword
            raise RuntimeError(f"Should not be triggered, dag: {dag}")

        python_operator = PythonOperator(
            task_id="python_operator", op_args=[1], python_callable=func, dag=self.dag
        )

        with pytest.raises(ValueError) as ctx:
            python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert "dag" in str(ctx.value), "'dag' not found in the exception"

    def test_provide_context_does_not_fail(self):
        """
        ensures that provide_context doesn't break dags in 2.0
        """
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def func(custom, dag):
            assert 1 == custom, "custom should be 1"
            assert dag is not None, "dag should be set"

        python_operator = PythonOperator(
            task_id="python_operator",
            op_kwargs={"custom": 1},
            python_callable=func,
            provide_context=True,
            dag=self.dag,
        )
        python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_context_with_conflicting_op_args(self):
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def func(custom, dag):
            assert 1 == custom, "custom should be 1"
            assert dag is not None, "dag should be set"

        python_operator = PythonOperator(
            task_id="python_operator", op_kwargs={"custom": 1}, python_callable=func, dag=self.dag
        )
        python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_context_with_kwargs(self):
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def func(**context):
            # check if context is being set
            assert len(context) > 0, "Context has not been injected"

        python_operator = PythonOperator(
            task_id="python_operator", op_kwargs={"custom": 1}, python_callable=func, dag=self.dag
        )
        python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_return_value_log_with_show_return_value_in_logs_default(self):
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def func():
            return "test_return_value"

        python_operator = PythonOperator(task_id="python_operator", python_callable=func, dag=self.dag)

        with self.assertLogs("airflow.task.operators", level=logging.INFO) as cm:
            python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        assert (
            "INFO:airflow.task.operators:Done. Returned value was: test_return_value" in cm.output
        ), "Return value should be shown"

    def test_return_value_log_with_show_return_value_in_logs_false(self):
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def func():
            return "test_return_value"

        python_operator = PythonOperator(
            task_id="python_operator",
            python_callable=func,
            dag=self.dag,
            show_return_value_in_logs=False,
        )

        with self.assertLogs("airflow.task.operators", level=logging.INFO) as cm:
            python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        assert (
            "INFO:airflow.task.operators:Done. Returned value was: test_return_value" not in cm.output
        ), "Return value should not be shown"
        assert (
            "INFO:airflow.task.operators:Done. Returned value not shown" in cm.output
        ), "Log message that the option is turned off should be shown"


class TestBranchOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        self.dag = DAG(
            "branch_operator_test",
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
        )

        self.branch_1 = EmptyOperator(task_id="branch_1", dag=self.dag)
        self.branch_2 = EmptyOperator(task_id="branch_2", dag=self.dag)
        self.branch_3 = None

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_with_dag_run(self):
        branch_op = BranchPythonOperator(
            task_id="make_choice", dag=self.dag, python_callable=lambda: "branch_1"
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_1":
                assert ti.state == State.NONE
            elif ti.task_id == "branch_2":
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

    def test_with_skip_in_branch_downstream_dependencies(self):
        branch_op = BranchPythonOperator(
            task_id="make_choice", dag=self.dag, python_callable=lambda: "branch_1"
        )

        branch_op >> self.branch_1 >> self.branch_2
        branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_1":
                assert ti.state == State.NONE
            elif ti.task_id == "branch_2":
                assert ti.state == State.NONE
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

    def test_with_skip_in_branch_downstream_dependencies2(self):
        branch_op = BranchPythonOperator(
            task_id="make_choice", dag=self.dag, python_callable=lambda: "branch_2"
        )

        branch_op >> self.branch_1 >> self.branch_2
        branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_1":
                assert ti.state == State.SKIPPED
            elif ti.task_id == "branch_2":
                assert ti.state == State.NONE
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

    def test_xcom_push(self):
        branch_op = BranchPythonOperator(
            task_id="make_choice", dag=self.dag, python_callable=lambda: "branch_1"
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                assert ti.xcom_pull(task_ids="make_choice") == "branch_1"

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by BranchPythonOperator, clearing the skipped task
        should not cause it to be executed.
        """
        branch_op = BranchPythonOperator(
            task_id="make_choice", dag=self.dag, python_callable=lambda: "branch_1"
        )
        branches = [self.branch_1, self.branch_2]
        branch_op >> branches
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        for task in branches:
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_1":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_2":
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

        children_tis = [ti for ti in tis if ti.task_id in branch_op.get_direct_relative_ids()]

        # Clear the children tasks.
        with create_session() as session:
            clear_task_instances(children_tis, session=session, dag=self.dag)

        # Run the cleared tasks again.
        for task in branches:
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        # Check if the states are correct after children tasks are cleared.
        for ti in dr.get_task_instances():
            if ti.task_id == "make_choice":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_1":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_2":
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

    def test_raise_exception_on_no_accepted_type_return(self):
        branch_op = BranchPythonOperator(task_id="make_choice", dag=self.dag, python_callable=lambda: 5)
        self.dag.clear()
        with pytest.raises(AirflowException) as ctx:
            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert "Branch callable must return either None, a task ID, or a list of IDs" == str(ctx.value)

    def test_raise_exception_on_invalid_task_id(self):
        branch_op = BranchPythonOperator(
            task_id="make_choice", dag=self.dag, python_callable=lambda: "some_task_id"
        )
        self.dag.clear()
        with pytest.raises(AirflowException) as ctx:
            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert """Branch callable must return valid task_ids. Invalid tasks found: {'some_task_id'}""" == str(
            ctx.value
        )


class TestShortCircuitOperator:
    def setup(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

        self.dag = DAG(
            "short_circuit_op_test",
            start_date=DEFAULT_DATE,
            schedule=INTERVAL,
        )

        with self.dag:
            self.op1 = EmptyOperator(task_id="op1")
            self.op2 = EmptyOperator(task_id="op2")
            self.op1.set_downstream(self.op2)

    def teardown(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def _assert_expected_task_states(self, dagrun, expected_states):
        """Helper function that asserts `TaskInstances` of a given `task_id` are in a given state."""

        tis = dagrun.get_task_instances()
        for ti in tis:
            try:
                expected_state = expected_states[ti.task_id]
            except KeyError:
                raise ValueError(f"Invalid task id {ti.task_id} found!")
            else:
                assert ti.state == expected_state

    all_downstream_skipped_states = {
        "short_circuit": State.SUCCESS,
        "op1": State.SKIPPED,
        "op2": State.SKIPPED,
    }
    all_success_states = {"short_circuit": State.SUCCESS, "op1": State.SUCCESS, "op2": State.SUCCESS}

    @pytest.mark.parametrize(
        argnames=(
            "callable_return, test_ignore_downstream_trigger_rules, test_trigger_rule, expected_task_states"
        ),
        argvalues=[
            # Skip downstream tasks, do not respect trigger rules, default trigger rule on all downstream
            # tasks
            (False, True, TriggerRule.ALL_SUCCESS, all_downstream_skipped_states),
            # Skip downstream tasks via a falsy value, do not respect trigger rules, default trigger rule on
            # all downstream tasks
            ([], True, TriggerRule.ALL_SUCCESS, all_downstream_skipped_states),
            # Skip downstream tasks, do not respect trigger rules, non-default trigger rule on a downstream
            # task
            (False, True, TriggerRule.ALL_DONE, all_downstream_skipped_states),
            # Skip downstream tasks via a falsy value, do not respect trigger rules, non-default trigger rule
            # on a downstream task
            ([], True, TriggerRule.ALL_DONE, all_downstream_skipped_states),
            # Skip downstream tasks, respect trigger rules, default trigger rule on all downstream tasks
            (
                False,
                False,
                TriggerRule.ALL_SUCCESS,
                {"short_circuit": State.SUCCESS, "op1": State.SKIPPED, "op2": State.NONE},
            ),
            # Skip downstream tasks via a falsy value, respect trigger rules, default trigger rule on all
            # downstream tasks
            (
                [],
                False,
                TriggerRule.ALL_SUCCESS,
                {"short_circuit": State.SUCCESS, "op1": State.SKIPPED, "op2": State.NONE},
            ),
            # Skip downstream tasks, respect trigger rules, non-default trigger rule on a downstream task
            (
                False,
                False,
                TriggerRule.ALL_DONE,
                {"short_circuit": State.SUCCESS, "op1": State.SKIPPED, "op2": State.SUCCESS},
            ),
            # Skip downstream tasks via a falsy value, respect trigger rules, non-default trigger rule on a
            # downstream task
            (
                [],
                False,
                TriggerRule.ALL_DONE,
                {"short_circuit": State.SUCCESS, "op1": State.SKIPPED, "op2": State.SUCCESS},
            ),
            # Do not skip downstream tasks, do not respect trigger rules, default trigger rule on all
            # downstream tasks
            (True, True, TriggerRule.ALL_SUCCESS, all_success_states),
            # Do not skip downstream tasks via a truthy value, do not respect trigger rules, default trigger
            # rule on all downstream tasks
            (["a", "b", "c"], True, TriggerRule.ALL_SUCCESS, all_success_states),
            # Do not skip downstream tasks, do not respect trigger rules, non-default trigger rule on a
            # downstream task
            (True, True, TriggerRule.ALL_DONE, all_success_states),
            # Do not skip downstream tasks via a truthy value, do not respect trigger rules, non-default
            # trigger rule on a downstream task
            (["a", "b", "c"], True, TriggerRule.ALL_DONE, all_success_states),
            # Do not skip downstream tasks, respect trigger rules, default trigger rule on all downstream
            # tasks
            (True, False, TriggerRule.ALL_SUCCESS, all_success_states),
            # Do not skip downstream tasks via a truthy value, respect trigger rules, default trigger rule on
            # all downstream tasks
            (["a", "b", "c"], False, TriggerRule.ALL_SUCCESS, all_success_states),
            # Do not skip downstream tasks, respect trigger rules, non-default trigger rule on a downstream
            # task
            (True, False, TriggerRule.ALL_DONE, all_success_states),
            # Do not skip downstream tasks via a truthy value, respect trigger rules, non-default trigger rule
            # on a downstream  task
            (["a", "b", "c"], False, TriggerRule.ALL_DONE, all_success_states),
        ],
        ids=[
            "skip_ignore_with_default_trigger_rule_on_all_tasks",
            "skip_falsy_result_ignore_with_default_trigger_rule_on_all_tasks",
            "skip_ignore_respect_with_non-default_trigger_rule_on_single_task",
            "skip_falsy_result_ignore_respect_with_non-default_trigger_rule_on_single_task",
            "skip_respect_with_default_trigger_rule_all_tasks",
            "skip_falsy_result_respect_with_default_trigger_rule_all_tasks",
            "skip_respect_with_non-default_trigger_rule_on_single_task",
            "skip_falsy_result_respect_respect_with_non-default_trigger_rule_on_single_task",
            "no_skip_ignore_with_default_trigger_rule_on_all_tasks",
            "no_skip_truthy_result_ignore_with_default_trigger_rule_all_tasks",
            "no_skip_no_respect_with_non-default_trigger_rule_on_single_task",
            "no_skip_truthy_result_ignore_with_non-default_trigger_rule_on_single_task",
            "no_skip_respect_with_default_trigger_rule_all_tasks",
            "no_skip_truthy_result_respect_with_default_trigger_rule_all_tasks",
            "no_skip_respect_with_non-default_trigger_rule_on_single_task",
            "no_skip_truthy_result_respect_with_non-default_trigger_rule_on_single_task",
        ],
    )
    def test_short_circuiting(
        self, callable_return, test_ignore_downstream_trigger_rules, test_trigger_rule, expected_task_states
    ):
        """
        Checking the behavior of the ShortCircuitOperator in several scenarios enabling/disabling the skipping
        of downstream tasks, both short-circuiting modes, and various trigger rules of downstream tasks.
        """

        self.short_circuit = ShortCircuitOperator(
            task_id="short_circuit",
            python_callable=lambda: callable_return,
            ignore_downstream_trigger_rules=test_ignore_downstream_trigger_rules,
            dag=self.dag,
        )
        self.short_circuit.set_downstream(self.op1)
        self.op2.trigger_rule = test_trigger_rule
        self.dag.clear()

        dagrun = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        self.short_circuit.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        assert self.short_circuit.ignore_downstream_trigger_rules == test_ignore_downstream_trigger_rules
        assert self.short_circuit.trigger_rule == TriggerRule.ALL_SUCCESS
        assert self.op1.trigger_rule == TriggerRule.ALL_SUCCESS
        assert self.op2.trigger_rule == test_trigger_rule

        self._assert_expected_task_states(dagrun, expected_task_states)

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by ShortCircuitOperator, clearing the skipped task
        should not cause it to be executed.
        """

        self.short_circuit = ShortCircuitOperator(
            task_id="short_circuit",
            python_callable=lambda: False,
            dag=self.dag,
        )
        self.short_circuit.set_downstream(self.op1)
        self.dag.clear()

        dagrun = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        self.short_circuit.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        assert self.short_circuit.ignore_downstream_trigger_rules
        assert self.short_circuit.trigger_rule == TriggerRule.ALL_SUCCESS
        assert self.op1.trigger_rule == TriggerRule.ALL_SUCCESS
        assert self.op2.trigger_rule == TriggerRule.ALL_SUCCESS

        expected_states = {
            "short_circuit": State.SUCCESS,
            "op1": State.SKIPPED,
            "op2": State.SKIPPED,
        }
        self._assert_expected_task_states(dagrun, expected_states)

        # Clear downstream task "op1" that was previously executed.
        tis = dagrun.get_task_instances()

        with create_session() as session:
            clear_task_instances([ti for ti in tis if ti.task_id == "op1"], session=session, dag=self.dag)

        self.op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self._assert_expected_task_states(dagrun, expected_states)

    def test_xcom_push(self):
        short_op_push_xcom = ShortCircuitOperator(
            task_id="push_xcom_from_shortcircuit", dag=self.dag, python_callable=lambda: "signature"
        )

        short_op_no_push_xcom = ShortCircuitOperator(
            task_id="do_not_push_xcom_from_shortcircuit", dag=self.dag, python_callable=lambda: False
        )

        self.dag.clear()
        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        short_op_push_xcom.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        short_op_no_push_xcom.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        xcom_value_short_op_push_xcom = tis[0].xcom_pull(
            task_ids="push_xcom_from_shortcircuit", key="return_value"
        )
        assert xcom_value_short_op_push_xcom == "signature"

        xcom_value_short_op_no_push_xcom = tis[0].xcom_pull(
            task_ids="do_not_push_xcom_from_shortcircuit", key="return_value"
        )
        assert xcom_value_short_op_no_push_xcom is None


virtualenv_string_args: list[str] = []


class TestPythonVirtualenvOperator(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.dag = DAG(
            "test_dag",
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            template_searchpath=TEMPLATE_SEARCHPATH,
            schedule=INTERVAL,
        )
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        self.addCleanup(self.dag.clear)

    def tearDown(self):
        super().tearDown()
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def _run_as_operator(self, fn, python_version=sys.version_info[0], **kwargs):

        task = PythonVirtualenvOperator(
            python_callable=fn, python_version=python_version, task_id="task", dag=self.dag, **kwargs
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        return task

    def test_template_fields(self):
        assert set(PythonOperator.template_fields).issubset(PythonVirtualenvOperator.template_fields)

    def test_add_dill(self):
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        self._run_as_operator(f, use_dill=True, system_site_packages=False)

    def test_no_requirements(self):
        """Tests that the python callable is invoked on task run."""

        def f():
            pass

        self._run_as_operator(f)

    def test_no_system_site_packages(self):
        def f():
            try:
                import funcsigs  # noqa: F401
            except ImportError:
                return True
            raise Exception

        self._run_as_operator(f, system_site_packages=False, requirements=["dill"])

    def test_system_site_packages(self):
        def f():
            import funcsigs  # noqa: F401

        self._run_as_operator(f, requirements=["funcsigs"], system_site_packages=True)

    def test_with_requirements_pinned(self):
        def f():
            import funcsigs

            if funcsigs.__version__ != "0.4":
                raise Exception

        self._run_as_operator(f, requirements=["funcsigs==0.4"])

    def test_unpinned_requirements(self):
        def f():
            import funcsigs  # noqa: F401

        self._run_as_operator(f, requirements=["funcsigs", "dill"], system_site_packages=False)

    def test_range_requirements(self):
        def f():
            import funcsigs  # noqa: F401

        self._run_as_operator(f, requirements=["funcsigs>1.0", "dill"], system_site_packages=False)

    def test_requirements_file(self):
        def f():
            import funcsigs  # noqa: F401

        self._run_as_operator(f, requirements="requirements.txt", system_site_packages=False)

    @unittest.mock.patch("airflow.operators.python.prepare_virtualenv")
    def test_pip_install_options(self, mocked_prepare_virtualenv):
        def f():
            import funcsigs  # noqa: F401

        mocked_prepare_virtualenv.side_effect = prepare_virtualenv

        self._run_as_operator(
            f,
            requirements=["funcsigs==0.4"],
            system_site_packages=False,
            pip_install_options=["--no-deps"],
        )
        mocked_prepare_virtualenv.assert_called_with(
            venv_directory=unittest.mock.ANY,
            python_bin=unittest.mock.ANY,
            system_site_packages=False,
            requirements_file_path=unittest.mock.ANY,
            pip_install_options=["--no-deps"],
        )

    def test_templated_requirements_file(self):
        def f():
            import funcsigs

            assert funcsigs.__version__ == "1.0.2"

        self._run_as_operator(
            f,
            requirements="requirements.txt",
            use_dill=True,
            params={"environ": "templated_unit_test"},
            system_site_packages=False,
        )

    def test_fail(self):
        def f():
            raise Exception

        with pytest.raises(CalledProcessError):
            self._run_as_operator(f)

    def test_python_3(self):
        def f():
            import sys

            print(sys.version)
            try:
                {}.iteritems()
            except AttributeError:
                return
            raise Exception

        self._run_as_operator(f, python_version=3, use_dill=False, requirements=["dill"])

    def test_without_dill(self):
        def f(a):
            return a

        self._run_as_operator(f, system_site_packages=False, use_dill=False, op_args=[4])

    def test_string_args(self):
        def f():
            global virtualenv_string_args
            print(virtualenv_string_args)
            if virtualenv_string_args[0] != virtualenv_string_args[2]:
                raise Exception

        self._run_as_operator(f, string_args=[1, 2, 1])

    def test_with_args(self):
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception

        self._run_as_operator(f, op_args=[0, 1], op_kwargs={"c": True})

    def test_return_none(self):
        def f():
            return None

        task = self._run_as_operator(f)
        assert task.execute_callable() is None

    def test_return_false(self):
        def f():
            return False

        task = self._run_as_operator(f)
        assert task.execute_callable() is False

    def test_lambda(self):
        with pytest.raises(AirflowException):
            PythonVirtualenvOperator(python_callable=lambda x: 4, task_id="task", dag=self.dag)

    def test_nonimported_as_arg(self):
        def f(_):
            return None

        self._run_as_operator(f, op_args=[datetime.utcnow()])

    def test_context(self):
        def f(templates_dict):
            return templates_dict["ds"]

        self._run_as_operator(f, templates_dict={"ds": "{{ ds }}"})

    # This tests might take longer than default 60 seconds as it is serializing a lot of
    # context using dill (which is slow apparently).
    @pytest.mark.execution_timeout(120)
    @pytest.mark.filterwarnings("ignore::airflow.utils.context.AirflowContextDeprecationWarning")
    def test_airflow_context(self):
        def f(
            # basic
            ds_nodash,
            inlets,
            next_ds,
            next_ds_nodash,
            outlets,
            params,
            prev_ds,
            prev_ds_nodash,
            run_id,
            task_instance_key_str,
            test_mode,
            tomorrow_ds,
            tomorrow_ds_nodash,
            ts,
            ts_nodash,
            ts_nodash_with_tz,
            yesterday_ds,
            yesterday_ds_nodash,
            # pendulum-specific
            execution_date,
            next_execution_date,
            prev_execution_date,
            prev_execution_date_success,
            prev_start_date_success,
            # airflow-specific
            macros,
            conf,
            dag,
            dag_run,
            task,
            # other
            **context,
        ):
            pass

        self._run_as_operator(f, use_dill=True, system_site_packages=True, requirements=None)

    @pytest.mark.filterwarnings("ignore::airflow.utils.context.AirflowContextDeprecationWarning")
    def test_pendulum_context(self):
        def f(
            # basic
            ds_nodash,
            inlets,
            next_ds,
            next_ds_nodash,
            outlets,
            prev_ds,
            prev_ds_nodash,
            run_id,
            task_instance_key_str,
            test_mode,
            tomorrow_ds,
            tomorrow_ds_nodash,
            ts,
            ts_nodash,
            ts_nodash_with_tz,
            yesterday_ds,
            yesterday_ds_nodash,
            # pendulum-specific
            execution_date,
            next_execution_date,
            prev_execution_date,
            prev_execution_date_success,
            prev_start_date_success,
            # other
            **context,
        ):
            pass

        self._run_as_operator(f, use_dill=True, system_site_packages=False, requirements=["pendulum"])

    @pytest.mark.filterwarnings("ignore::airflow.utils.context.AirflowContextDeprecationWarning")
    def test_base_context(self):
        def f(
            # basic
            ds_nodash,
            inlets,
            next_ds,
            next_ds_nodash,
            outlets,
            prev_ds,
            prev_ds_nodash,
            run_id,
            task_instance_key_str,
            test_mode,
            tomorrow_ds,
            tomorrow_ds_nodash,
            ts,
            ts_nodash,
            ts_nodash_with_tz,
            yesterday_ds,
            yesterday_ds_nodash,
            # other
            **context,
        ):
            pass

        self._run_as_operator(f, use_dill=True, system_site_packages=False, requirements=None)

    def test_deepcopy(self):
        """Test that PythonVirtualenvOperator are deep-copyable."""

        def f():
            return 1

        task = PythonVirtualenvOperator(
            python_callable=f,
            task_id="task",
            dag=self.dag,
        )
        copy.deepcopy(task)


DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": timezone.datetime(2022, 1, 1),
    "end_date": datetime.today(),
    "schedule_interval": "@once",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


class TestCurrentContext:
    def test_current_context_no_context_raise(self):
        with pytest.raises(AirflowException):
            get_current_context()

    def test_current_context_roundtrip(self):
        example_context = {"Hello": "World"}

        with set_current_context(example_context):
            assert get_current_context() == example_context

    def test_context_removed_after_exit(self):
        example_context = {"Hello": "World"}

        with set_current_context(example_context):
            pass
        with pytest.raises(AirflowException):
            get_current_context()

    def test_nested_context(self):
        """
        Nested execution context should be supported in case the user uses multiple context managers.
        Each time the execute method of an operator is called, we set a new 'current' context.
        This test verifies that no matter how many contexts are entered - order is preserved
        """
        max_stack_depth = 15
        ctx_list = []
        for i in range(max_stack_depth):
            # Create all contexts in ascending order
            new_context = {"ContextId": i}
            # Like 15 nested with statements
            ctx_obj = set_current_context(new_context)
            ctx_obj.__enter__()
            ctx_list.append(ctx_obj)
        for i in reversed(range(max_stack_depth)):
            # Iterate over contexts in reverse order - stack is LIFO
            ctx = get_current_context()
            assert ctx["ContextId"] == i
            # End of with statement
            ctx_list[i].__exit__(None, None, None)


class MyContextAssertOperator(BaseOperator):
    def execute(self, context: Context):
        assert context == get_current_context()


def get_all_the_context(**context):
    current_context = get_current_context()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", AirflowContextDeprecationWarning)
        assert context == current_context._context


@pytest.fixture()
def clear_db():
    clear_db_runs()
    yield
    clear_db_runs()


@pytest.mark.usefixtures("clear_db")
class TestCurrentContextRuntime:
    def test_context_in_task(self):
        with DAG(dag_id="assert_context_dag", default_args=DEFAULT_ARGS):
            op = MyContextAssertOperator(task_id="assert_context")
            op.run(ignore_first_depends_on_past=True, ignore_ti_state=True)

    def test_get_context_in_old_style_context_task(self):
        with DAG(dag_id="edge_case_context_dag", default_args=DEFAULT_ARGS):
            op = PythonOperator(python_callable=get_all_the_context, task_id="get_all_the_context")
            op.run(ignore_first_depends_on_past=True, ignore_ti_state=True)


@pytest.mark.parametrize(
    "choice,expected_states",
    [
        ("task1", [State.SUCCESS, State.SUCCESS, State.SUCCESS]),
        ("join", [State.SUCCESS, State.SKIPPED, State.SUCCESS]),
    ],
)
def test_empty_branch(dag_maker, choice, expected_states):
    """
    Tests that BranchPythonOperator handles empty branches properly.
    """
    with dag_maker(
        "test_empty_branch",
        start_date=DEFAULT_DATE,
    ) as dag:
        branch = BranchPythonOperator(task_id="branch", python_callable=lambda: choice)
        task1 = EmptyOperator(task_id="task1")
        join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

        branch >> [task1, join]
        task1 >> join

    dag.clear(start_date=DEFAULT_DATE)
    dag_run = dag_maker.create_dagrun()

    task_ids = ["branch", "task1", "join"]
    tis = {ti.task_id: ti for ti in dag_run.task_instances}

    for task_id in task_ids:  # Mimic the specific order the scheduling would run the tests.
        task_instance = tis[task_id]
        task_instance.refresh_from_task(dag.get_task(task_id))
        task_instance.run()

    def get_state(ti):
        ti.refresh_from_db()
        return ti.state

    assert [get_state(tis[task_id]) for task_id in task_ids] == expected_states


def test_virtualenv_serializable_context_fields(create_task_instance):
    """Ensure all template context fields are listed in the operator.

    This exists mainly so when a field is added to the context, we remember to
    also add it to PythonVirtualenvOperator.
    """
    # These are intentionally NOT serialized into the virtual environment:
    # * Variables pointing to the task instance itself.
    # * Variables that are accessor instances.
    intentionally_excluded_context_keys = [
        "task_instance",
        "ti",
        "var",  # Accessor for Variable; var->json and var->value.
        "conn",  # Accessor for Connection.
    ]

    ti = create_task_instance(
        dag_id="test_virtualenv_serializable_context_fields",
        task_id="test_virtualenv_serializable_context_fields_task",
        schedule=None,
    )
    context = ti.get_template_context()

    declared_keys = {
        *PythonVirtualenvOperator.BASE_SERIALIZABLE_CONTEXT_KEYS,
        *PythonVirtualenvOperator.PENDULUM_SERIALIZABLE_CONTEXT_KEYS,
        *PythonVirtualenvOperator.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS,
        *intentionally_excluded_context_keys,
    }

    assert set(context) == declared_keys
