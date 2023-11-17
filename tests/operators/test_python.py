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
import pickle
import re
import sys
import tempfile
import warnings
from collections import namedtuple
from datetime import date, datetime, timedelta
from functools import partial
from subprocess import CalledProcessError
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Generator
from unittest import mock
from unittest.mock import MagicMock

import pytest
from slugify import slugify

from airflow import PY311
from airflow.decorators import task_group
from airflow.exceptions import AirflowException, DeserializingResultError, RemovedInAirflow3Warning
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance, clear_task_instances, set_current_context
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchExternalPythonOperator,
    BranchPythonOperator,
    BranchPythonVirtualenvOperator,
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    ShortCircuitOperator,
    get_current_context,
)
from airflow.utils import timezone
from airflow.utils.context import AirflowContextDeprecationWarning, Context
from airflow.utils.python_virtualenv import prepare_virtualenv
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import NOTSET, DagRunType
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun

TI = TaskInstance
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
TEMPLATE_SEARCHPATH = os.path.join(AIRFLOW_MAIN_FOLDER, "tests", "config_templates")
LOGGER_NAME = "airflow.task.operators"
DEFAULT_PYTHON_VERSION = f"{sys.version_info[0]}.{sys.version_info[1]}"


class BasePythonTest:
    """Base test class for TestPythonOperator and TestPythonSensor classes"""

    opcls: type[BaseOperator]
    dag_id: str
    task_id: str
    run_id: str
    dag: DAG
    ds_templated: str
    default_date: datetime = DEFAULT_DATE

    @pytest.fixture(autouse=True)
    def base_tests_setup(self, request, create_task_instance_of_operator, dag_maker):
        self.dag_id = f"dag_{slugify(request.cls.__name__)}"
        self.task_id = f"task_{slugify(request.node.name, max_length=40)}"
        self.run_id = f"run_{slugify(request.node.name, max_length=40)}"
        self.ds_templated = self.default_date.date().isoformat()
        self.ti_maker = create_task_instance_of_operator
        self.dag_maker = dag_maker
        self.dag = self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH).dag
        clear_db_runs()
        yield
        clear_db_runs()

    @staticmethod
    def assert_expected_task_states(dag_run: DagRun, expected_states: dict):
        """Helper function that asserts `TaskInstances` of a given `task_id` are in a given state."""
        asserts = []
        for ti in dag_run.get_task_instances():
            try:
                expected = expected_states[ti.task_id]
            except KeyError:
                asserts.append(f"Unexpected task id {ti.task_id!r} found, expected {expected_states.keys()}")
                continue

            if ti.state != expected:
                asserts.append(f"Task {ti.task_id!r} has state {ti.state!r} instead of expected {expected!r}")
        if asserts:
            pytest.fail("\n".join(asserts))

    @staticmethod
    def default_kwargs(**kwargs):
        """Default arguments for specific Operator."""
        return kwargs

    def create_dag_run(self) -> DagRun:
        return self.dag.create_dagrun(
            state=DagRunState.RUNNING,
            start_date=self.dag_maker.start_date,
            session=self.dag_maker.session,
            execution_date=self.default_date,
            run_type=DagRunType.MANUAL,
        )

    def create_ti(self, fn, **kwargs) -> TI:
        """Create TaskInstance for class defined Operator."""
        return self.ti_maker(
            self.opcls,
            python_callable=fn,
            **self.default_kwargs(**kwargs),
            dag_id=self.dag_id,
            task_id=self.task_id,
            execution_date=self.default_date,
        )

    def run_as_operator(self, fn, **kwargs):
        """Run task by direct call ``run`` method."""
        with self.dag:
            task = self.opcls(task_id=self.task_id, python_callable=fn, **self.default_kwargs(**kwargs))

        task.run(start_date=self.default_date, end_date=self.default_date)
        return task

    def run_as_task(self, fn, return_ti=False, **kwargs):
        """Create TaskInstance and run it."""
        ti = self.create_ti(fn, **kwargs)
        ti.run()
        if return_ti:
            return ti
        return ti.task

    def render_templates(self, fn, **kwargs):
        """Create TaskInstance and render templates without actual run."""
        return self.create_ti(fn, **kwargs).render_templates()


class TestPythonOperator(BasePythonTest):
    opcls = PythonOperator

    @pytest.fixture(autouse=True)
    def setup_tests(self):
        self.run = False

    def do_run(self):
        self.run = True

    def is_run(self):
        return self.run

    def test_python_operator_run(self):
        """Tests that the python callable is invoked on task run."""
        ti = self.create_ti(self.do_run)
        assert not self.is_run()
        ti.run()
        assert self.is_run()

    @pytest.mark.parametrize("not_callable", [{}, None])
    def test_python_operator_python_callable_is_callable(self, not_callable):
        """Tests that PythonOperator will only instantiate if the python_callable argument is callable."""
        with pytest.raises(AirflowException, match="`python_callable` param must be callable"):
            PythonOperator(python_callable=not_callable, task_id="python_operator")

    def test_python_callable_arguments_are_templatized(self):
        """Test PythonOperator op_args are templatized"""
        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple("Named", ["var1", "var2"])
        named_tuple = Named("{{ ds }}", "unchanged")

        task = self.render_templates(
            lambda: 0,
            op_args=[4, date(2019, 1, 1), "dag {{dag.dag_id}} ran on {{ds}}.", named_tuple],
        )
        rendered_op_args = task.op_args
        assert len(rendered_op_args) == 4
        assert rendered_op_args[0] == 4
        assert rendered_op_args[1] == date(2019, 1, 1)
        assert rendered_op_args[2] == f"dag {self.dag_id} ran on {self.ds_templated}."
        assert rendered_op_args[3] == Named(self.ds_templated, "unchanged")

    def test_python_callable_keyword_arguments_are_templatized(self):
        """Test PythonOperator op_kwargs are templatized"""
        task = self.render_templates(
            lambda: 0,
            op_kwargs={
                "an_int": 4,
                "a_date": date(2019, 1, 1),
                "a_templated_string": "dag {{dag.dag_id}} ran on {{ds}}.",
            },
        )
        rendered_op_kwargs = task.op_kwargs
        assert rendered_op_kwargs["an_int"] == 4
        assert rendered_op_kwargs["a_date"] == date(2019, 1, 1)
        assert rendered_op_kwargs["a_templated_string"] == f"dag {self.dag_id} ran on {self.ds_templated}."

    def test_python_operator_shallow_copy_attr(self):
        def not_callable(x):
            assert False, "Should not be triggered"

        original_task = PythonOperator(
            python_callable=not_callable,
            op_kwargs={"certain_attrs": ""},
            task_id=self.task_id,
        )
        new_task = copy.deepcopy(original_task)
        # shallow copy op_kwargs
        assert id(original_task.op_kwargs["certain_attrs"]) == id(new_task.op_kwargs["certain_attrs"])
        # shallow copy python_callable
        assert id(original_task.python_callable) == id(new_task.python_callable)

    def test_conflicting_kwargs(self):
        # dag is not allowed since it is a reserved keyword
        def func(dag):
            # An ValueError should be triggered since we're using dag as a reserved keyword
            raise RuntimeError(f"Should not be triggered, dag: {dag}")

        ti = self.create_ti(func, op_args=[1])
        error_message = re.escape("The key 'dag' in args is a part of kwargs and therefore reserved.")
        with pytest.raises(ValueError, match=error_message):
            ti.run()

    def test_provide_context_does_not_fail(self):
        """Ensures that provide_context doesn't break dags in 2.0."""

        def func(custom, dag):
            assert 1 == custom, "custom should be 1"
            assert dag is not None, "dag should be set"

        with pytest.warns(RemovedInAirflow3Warning):
            self.run_as_task(func, op_kwargs={"custom": 1}, provide_context=True)

    def test_context_with_conflicting_op_args(self):
        def func(custom, dag):
            assert 1 == custom, "custom should be 1"
            assert dag is not None, "dag should be set"

        self.run_as_task(func, op_kwargs={"custom": 1})

    def test_context_with_kwargs(self):
        def func(**context):
            # check if context is being set
            assert len(context) > 0, "Context has not been injected"

        self.run_as_task(func, op_kwargs={"custom": 1})

    @pytest.mark.parametrize(
        "show_return_value_in_logs, should_shown",
        [
            pytest.param(NOTSET, True, id="default"),
            pytest.param(True, True, id="show"),
            pytest.param(False, False, id="hide"),
        ],
    )
    def test_return_value_log(self, show_return_value_in_logs, should_shown, caplog):
        caplog.set_level(logging.INFO, logger=LOGGER_NAME)

        def func():
            return "test_return_value"

        if show_return_value_in_logs is NOTSET:
            self.run_as_task(func)
        else:
            self.run_as_task(func, show_return_value_in_logs=show_return_value_in_logs)

        if should_shown:
            assert "Done. Returned value was: test_return_value" in caplog.messages
            assert "Done. Returned value not shown" not in caplog.messages
        else:
            assert "Done. Returned value was: test_return_value" not in caplog.messages
            assert "Done. Returned value not shown" in caplog.messages

    def test_python_operator_templates_exts(self):
        def func():
            return "test_return_value"

        python_operator = PythonOperator(
            task_id="python_operator",
            python_callable=func,
            dag=self.dag,
            show_return_value_in_logs=False,
            templates_exts=["test_ext"],
        )

        assert python_operator.template_ext == ["test_ext"]

    def test_python_operator_has_default_logger_name(self):
        python_operator = PythonOperator(task_id="task", python_callable=partial(int, 2))

        logger_name: str = "airflow.task.operators.airflow.operators.python.PythonOperator"
        assert python_operator.log.name == logger_name

    def test_custom_logger_name_is_correctly_set(self):
        """
        Ensure the custom logger name is correctly set when the Operator is created,
        and when its state is resumed via __setstate__.
        """
        logger_name: str = "airflow.task.operators.custom.logger"

        python_operator = PythonOperator(
            task_id="task", python_callable=partial(int, 2), logger_name="custom.logger"
        )
        assert python_operator.log.name == logger_name

        setstate_operator = pickle.loads(pickle.dumps(python_operator))
        assert setstate_operator.log.name == logger_name

    def test_custom_logger_name_can_be_empty_string(self):
        python_operator = PythonOperator(task_id="task", python_callable=partial(int, 2), logger_name="")
        assert python_operator.log.name == "airflow.task.operators"


class TestBranchOperator(BasePythonTest):
    opcls = BranchPythonOperator

    @pytest.fixture(autouse=True)
    def setup_tests(self):
        self.branch_1 = EmptyOperator(task_id="branch_1")
        self.branch_2 = EmptyOperator(task_id="branch_2")

    def test_with_dag_run(self):
        with self.dag:

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> [self.branch_1, self.branch_2]

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        self.assert_expected_task_states(
            dr, {self.task_id: State.SUCCESS, "branch_1": State.NONE, "branch_2": State.SKIPPED}
        )

    def test_with_skip_in_branch_downstream_dependencies(self):
        with self.dag:

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> self.branch_1 >> self.branch_2
            branch_op >> self.branch_2

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        self.assert_expected_task_states(
            dr, {self.task_id: State.SUCCESS, "branch_1": State.NONE, "branch_2": State.NONE}
        )

    def test_with_skip_in_branch_downstream_dependencies2(self):
        with self.dag:

            def f():
                return "branch_2"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> self.branch_1 >> self.branch_2
            branch_op >> self.branch_2

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        self.assert_expected_task_states(
            dr, {self.task_id: State.SUCCESS, "branch_1": State.SKIPPED, "branch_2": State.NONE}
        )

    def test_xcom_push(self):
        with self.dag:

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> [self.branch_1, self.branch_2]

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        for ti in dr.get_task_instances():
            if ti.task_id == self.task_id:
                assert ti.xcom_pull(task_ids=self.task_id) == "branch_1"
                break
        else:
            pytest.fail(f"{self.task_id!r} not found.")

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by BranchPythonOperator, clearing the skipped task
        should not cause it to be executed.
        """
        with self.dag:

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branches = [self.branch_1, self.branch_2]
            branch_op >> branches

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        for task in branches:
            task.run(start_date=self.default_date, end_date=self.default_date)

        expected_states = {
            self.task_id: State.SUCCESS,
            "branch_1": State.SUCCESS,
            "branch_2": State.SKIPPED,
        }

        self.assert_expected_task_states(dr, expected_states)

        # Clear the children tasks.
        tis = dr.get_task_instances()
        children_tis = [ti for ti in tis if ti.task_id in branch_op.get_direct_relative_ids()]
        with create_session() as session:
            clear_task_instances(children_tis, session=session, dag=branch_op.dag)

        # Run the cleared tasks again.
        for task in branches:
            task.run(start_date=self.default_date, end_date=self.default_date)

        # Check if the states are correct after children tasks are cleared.
        self.assert_expected_task_states(dr, expected_states)

    def test_raise_exception_on_no_accepted_type_return(self):
        def f():
            return 5

        ti = self.create_ti(f)
        with pytest.raises(AirflowException, match="must be either None, a task ID, or an Iterable of IDs"):
            ti.run()

    def test_raise_exception_on_invalid_task_id(self):
        def f():
            return "some_task_id"

        ti = self.create_ti(f)
        with pytest.raises(AirflowException, match="Invalid tasks found: {'some_task_id'}"):
            ti.run()

    @pytest.mark.parametrize(
        "choice,expected_states",
        [
            ("task1", [State.SUCCESS, State.SUCCESS, State.SUCCESS]),
            ("join", [State.SUCCESS, State.SKIPPED, State.SUCCESS]),
        ],
    )
    def test_empty_branch(self, choice, expected_states):
        """
        Tests that BranchPythonOperator handles empty branches properly.
        """
        with self.dag:

            def f():
                return choice

            branch = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            task1 = EmptyOperator(task_id="task1")
            join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

            branch >> [task1, join]
            task1 >> join

        dr = self.create_dag_run()
        task_ids = [self.task_id, "task1", "join"]
        tis = {ti.task_id: ti for ti in dr.task_instances}

        for task_id in task_ids:  # Mimic the specific order the scheduling would run the tests.
            task_instance = tis[task_id]
            task_instance.refresh_from_task(self.dag.get_task(task_id))
            task_instance.run()

        def get_state(ti):
            ti.refresh_from_db()
            return ti.state

        assert [get_state(tis[task_id]) for task_id in task_ids] == expected_states


class TestShortCircuitOperator(BasePythonTest):
    opcls = ShortCircuitOperator

    @pytest.fixture(autouse=True)
    def setup_tests(self):
        self.task_id = "short_circuit"
        self.op1 = EmptyOperator(task_id="op1")
        self.op2 = EmptyOperator(task_id="op2")

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
        with self.dag:
            short_circuit = ShortCircuitOperator(
                task_id="short_circuit",
                python_callable=lambda: callable_return,
                ignore_downstream_trigger_rules=test_ignore_downstream_trigger_rules,
            )
            short_circuit >> self.op1 >> self.op2
            self.op2.trigger_rule = test_trigger_rule

        dr = self.create_dag_run()
        short_circuit.run(start_date=self.default_date, end_date=self.default_date)
        self.op1.run(start_date=self.default_date, end_date=self.default_date)
        self.op2.run(start_date=self.default_date, end_date=self.default_date)

        assert short_circuit.ignore_downstream_trigger_rules == test_ignore_downstream_trigger_rules
        assert short_circuit.trigger_rule == TriggerRule.ALL_SUCCESS
        assert self.op1.trigger_rule == TriggerRule.ALL_SUCCESS
        assert self.op2.trigger_rule == test_trigger_rule
        self.assert_expected_task_states(dr, expected_task_states)

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by ShortCircuitOperator, clearing the skipped task
        should not cause it to be executed.
        """
        with self.dag:
            short_circuit = ShortCircuitOperator(task_id="short_circuit", python_callable=lambda: False)
            short_circuit >> self.op1 >> self.op2
        dr = self.create_dag_run()

        short_circuit.run(start_date=self.default_date, end_date=self.default_date)
        self.op1.run(start_date=self.default_date, end_date=self.default_date)
        self.op2.run(start_date=self.default_date, end_date=self.default_date)
        assert short_circuit.ignore_downstream_trigger_rules
        assert short_circuit.trigger_rule == TriggerRule.ALL_SUCCESS
        assert self.op1.trigger_rule == TriggerRule.ALL_SUCCESS
        assert self.op2.trigger_rule == TriggerRule.ALL_SUCCESS

        expected_states = {
            "short_circuit": State.SUCCESS,
            "op1": State.SKIPPED,
            "op2": State.SKIPPED,
        }
        self.assert_expected_task_states(dr, expected_states)

        # Clear downstream task "op1" that was previously executed.
        tis = dr.get_task_instances()
        with create_session() as session:
            clear_task_instances(
                [ti for ti in tis if ti.task_id == "op1"], session=session, dag=short_circuit.dag
            )
        self.op1.run(start_date=self.default_date, end_date=self.default_date)
        self.assert_expected_task_states(dr, expected_states)

    def test_xcom_push(self):
        with self.dag:
            short_op_push_xcom = ShortCircuitOperator(
                task_id="push_xcom_from_shortcircuit", python_callable=lambda: "signature"
            )
            short_op_no_push_xcom = ShortCircuitOperator(
                task_id="do_not_push_xcom_from_shortcircuit", python_callable=lambda: False
            )

        dr = self.create_dag_run()
        short_op_push_xcom.run(start_date=self.default_date, end_date=self.default_date)
        short_op_no_push_xcom.run(start_date=self.default_date, end_date=self.default_date)

        tis = dr.get_task_instances()
        assert tis[0].xcom_pull(task_ids=short_op_push_xcom.task_id, key="return_value") == "signature"
        assert tis[0].xcom_pull(task_ids=short_op_no_push_xcom.task_id, key="return_value") is False

    def test_xcom_push_skipped_tasks(self):
        with self.dag:
            short_op_push_xcom = ShortCircuitOperator(
                task_id="push_xcom_from_shortcircuit", python_callable=lambda: False
            )
            empty_task = EmptyOperator(task_id="empty_task")
            short_op_push_xcom >> empty_task
        dr = self.create_dag_run()
        short_op_push_xcom.run(start_date=self.default_date, end_date=self.default_date)
        tis = dr.get_task_instances()
        assert tis[0].xcom_pull(task_ids=short_op_push_xcom.task_id, key="skipmixin_key") == {
            "skipped": ["empty_task"]
        }

    def test_mapped_xcom_push_skipped_tasks(self, session):
        with self.dag:

            @task_group
            def group(x):
                short_op_push_xcom = ShortCircuitOperator(
                    task_id="push_xcom_from_shortcircuit",
                    python_callable=lambda arg: arg % 2 == 0,
                    op_kwargs={"arg": x},
                )
                empty_task = EmptyOperator(task_id="empty_task")
                short_op_push_xcom >> empty_task

            group.expand(x=[0, 1])
        dr = self.create_dag_run()
        decision = dr.task_instance_scheduling_decisions(session=session)
        for ti in decision.schedulable_tis:
            ti.run()
        # dr.run(start_date=self.default_date, end_date=self.default_date)
        tis = dr.get_task_instances()

        assert (
            tis[0].xcom_pull(task_ids="group.push_xcom_from_shortcircuit", key="return_value", map_indexes=0)
            is True
        )
        assert (
            tis[0].xcom_pull(task_ids="group.push_xcom_from_shortcircuit", key="skipmixin_key", map_indexes=0)
            is None
        )
        assert tis[0].xcom_pull(
            task_ids="group.push_xcom_from_shortcircuit", key="skipmixin_key", map_indexes=1
        ) == {"skipped": ["group.empty_task"]}


virtualenv_string_args: list[str] = []


class BaseTestPythonVirtualenvOperator(BasePythonTest):
    def test_template_fields(self):
        assert set(PythonOperator.template_fields).issubset(PythonVirtualenvOperator.template_fields)

    def test_fail(self):
        def f():
            raise Exception

        with pytest.raises(CalledProcessError):
            self.run_as_task(f)

    def test_fail_with_message(self):
        def f():
            raise Exception("Custom error message")

        with pytest.raises(AirflowException) as e:
            self.run_as_task(f)
            assert "Custom error message" in str(e)

    def test_string_args(self):
        def f():
            global virtualenv_string_args
            print(virtualenv_string_args)
            if virtualenv_string_args[0] != virtualenv_string_args[2]:
                raise Exception

        self.run_as_task(f, string_args=[1, 2, 1])

    def test_with_args(self):
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception

        self.run_as_task(f, op_args=[0, 1], op_kwargs={"c": True})

    def test_return_none(self):
        def f():
            return None

        task = self.run_as_task(f)
        assert task.execute_callable() is None

    def test_return_false(self):
        def f():
            return False

        task = self.run_as_task(f)
        assert task.execute_callable() is False

    def test_lambda(self):
        with pytest.raises(AirflowException):
            PythonVirtualenvOperator(python_callable=lambda x: 4, task_id=self.task_id)

    def test_nonimported_as_arg(self):
        def f(_):
            return None

        self.run_as_task(f, op_args=[datetime.utcnow()])

    def test_context(self):
        def f(templates_dict):
            return templates_dict["ds"]

        task = self.run_as_task(f, templates_dict={"ds": "{{ ds }}"})
        assert task.templates_dict == {"ds": self.ds_templated}

    def test_deepcopy(self):
        """Test that PythonVirtualenvOperator are deep-copyable."""

        def f():
            return 1

        task = PythonVirtualenvOperator(python_callable=f, task_id="task")
        copy.deepcopy(task)

    def test_virtualenv_serializable_context_fields(self, create_task_instance):
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

        ti = create_task_instance(dag_id=self.dag_id, task_id=self.task_id, schedule=None)
        context = ti.get_template_context()

        declared_keys = {
            *PythonVirtualenvOperator.BASE_SERIALIZABLE_CONTEXT_KEYS,
            *PythonVirtualenvOperator.PENDULUM_SERIALIZABLE_CONTEXT_KEYS,
            *PythonVirtualenvOperator.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS,
            *intentionally_excluded_context_keys,
        }
        assert set(context) == declared_keys

    @pytest.mark.parametrize(
        "extra_kwargs, actual_exit_code, expected_state",
        [
            (None, 99, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": 100}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": [100]}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": (100, 101)}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": 100}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": [100, 102]}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": None}, 0, TaskInstanceState.SUCCESS),
        ],
    )
    def test_on_skip_exit_code(self, extra_kwargs, actual_exit_code, expected_state):
        def f(exit_code):
            if exit_code != 0:
                raise SystemExit(exit_code)

        if expected_state == TaskInstanceState.FAILED:
            with pytest.raises(CalledProcessError):
                self.run_as_task(f, op_kwargs={"exit_code": actual_exit_code}, **(extra_kwargs or {}))
        else:
            ti = self.run_as_task(
                f,
                return_ti=True,
                op_kwargs={"exit_code": actual_exit_code},
                **(extra_kwargs or {}),
            )
            assert ti.state == expected_state


venv_cache_path = tempfile.mkdtemp(prefix="venv_cache_path")


# when venv tests are run in parallel to other test they create new processes and this might take
# quite some time in shared docker environment and get some contention even between different containers
# therefore we have to extend timeouts for those tests
@pytest.mark.execution_timeout(120)
@pytest.mark.virtualenv_operator
class TestPythonVirtualenvOperator(BaseTestPythonVirtualenvOperator):
    opcls = PythonVirtualenvOperator

    @staticmethod
    def default_kwargs(*, python_version=DEFAULT_PYTHON_VERSION, **kwargs):
        kwargs["python_version"] = python_version
        if "do_not_use_caching" in kwargs:
            kwargs.pop("do_not_use_caching")
        else:
            # Caching by default makes the tests run faster except few cases we want to test with regular venv
            if "venv_cache_path" not in kwargs:
                kwargs["venv_cache_path"] = venv_cache_path
        return kwargs

    @mock.patch("shutil.which")
    @mock.patch("airflow.operators.python.importlib")
    def test_virtualenv_not_installed(self, importlib_mock, which_mock):
        which_mock.return_value = None
        importlib_mock.util.find_spec.return_value = None
        with pytest.raises(AirflowException, match="requires virtualenv"):

            def f():
                pass

            self.run_as_task(f)

    def test_add_dill(self):
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        self.run_as_task(f, use_dill=True, system_site_packages=False)

    def test_no_requirements(self):
        """Tests that the python callable is invoked on task run."""

        def f():
            pass

        self.run_as_task(f)

    def test_no_system_site_packages(self):
        def f():
            try:
                import funcsigs  # noqa: F401
            except ImportError:
                return True
            raise Exception

        self.run_as_task(f, system_site_packages=False, requirements=["dill"])

    def test_system_site_packages(self):
        def f():
            import funcsigs  # noqa: F401

        self.run_as_task(f, requirements=["funcsigs"], system_site_packages=True)

    def test_with_requirements_pinned(self):
        def f():
            import funcsigs

            if funcsigs.__version__ != "0.4":
                raise Exception

        self.run_as_task(f, requirements=["funcsigs==0.4"])

    def test_with_no_caching(self):
        """
        Most of venv tests use caching to speed up the tests. This test ensures that
        we have test without caching as well.

        :return:
        """

        def f():
            import funcsigs

            if funcsigs.__version__ != "0.4":
                raise Exception

        self.run_as_task(f, requirements=["funcsigs==0.4"], do_not_use_caching=True)

    def test_unpinned_requirements(self):
        def f():
            import funcsigs  # noqa: F401

        self.run_as_task(f, requirements=["funcsigs", "dill"], system_site_packages=False)

    def test_range_requirements(self):
        def f():
            import funcsigs  # noqa: F401

        self.run_as_task(f, requirements=["funcsigs>1.0", "dill"], system_site_packages=False)

    def test_requirements_file(self):
        def f():
            import funcsigs  # noqa: F401

        self.run_as_operator(f, requirements="requirements.txt", system_site_packages=False)

    @mock.patch("airflow.operators.python.prepare_virtualenv")
    def test_pip_install_options(self, mocked_prepare_virtualenv):
        def f():
            import funcsigs  # noqa: F401

        mocked_prepare_virtualenv.side_effect = prepare_virtualenv

        self.run_as_task(
            f,
            requirements=["funcsigs==0.4"],
            system_site_packages=False,
            pip_install_options=["--no-deps"],
        )
        mocked_prepare_virtualenv.assert_called_with(
            index_urls=None,
            venv_directory=mock.ANY,
            python_bin=mock.ANY,
            system_site_packages=False,
            requirements_file_path=mock.ANY,
            pip_install_options=["--no-deps"],
        )

    def test_templated_requirements_file(self):
        def f():
            import funcsigs

            assert funcsigs.__version__ == "1.0.2"

        self.run_as_operator(
            f,
            requirements="requirements.txt",
            use_dill=True,
            params={"environ": "templated_unit_test"},
            system_site_packages=False,
        )

    def test_python_3(self):
        def f():
            import sys

            print(sys.version)
            try:
                {}.iteritems()
            except AttributeError:
                return
            raise Exception

        self.run_as_task(f, python_version="3", use_dill=False, requirements=["dill"])

    def test_without_dill(self):
        def f(a):
            return a

        self.run_as_task(f, system_site_packages=False, use_dill=False, op_args=[4])

    def test_with_index_urls(self):
        def f(a):
            import sys
            from pathlib import Path

            pip_conf = (Path(sys.executable).parent.parent / "pip.conf").read_text()
            assert "abc.def.de" in pip_conf
            assert "xyz.abc.de" in pip_conf
            return a

        self.run_as_task(f, index_urls=["https://abc.def.de", "http://xyz.abc.de"], op_args=[4])

    def test_caching(self):
        def f(a):
            import sys

            assert "pytest_venv_1234" in sys.executable
            return a

        with TemporaryDirectory(prefix="pytest_venv_1234") as tmp_dir:
            self.run_as_task(f, venv_cache_path=tmp_dir, op_args=[4])

    # This tests might take longer than default 60 seconds as it is serializing a lot of
    # context using dill (which is slow apparently).
    @pytest.mark.execution_timeout(120)
    @pytest.mark.filterwarnings("ignore::airflow.utils.context.AirflowContextDeprecationWarning")
    @pytest.mark.skipif(
        os.environ.get("PYTEST_PLAIN_ASSERTS") != "true" or PY311,
        reason="assertion rewriting breaks this test because dill will try to serialize "
        "AssertRewritingHook including captured stdout and we need to run "
        "it with `--assert=plain`pytest option and PYTEST_PLAIN_ASSERTS=true ."
        "Also this test is skipped on Python 3.11 because of impact of regression in Python 3.11 "
        "connected likely with CodeType behaviour https://github.com/python/cpython/issues/100316 "
        "That likely causes that dill is not able to serialize the `conf` correctly "
        "Issue about fixing it is captured in https://github.com/apache/airflow/issues/35307",
    )
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
            prev_end_date_success,
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

        self.run_as_operator(f, use_dill=True, system_site_packages=True, requirements=None)

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
            prev_end_date_success,
            # other
            **context,
        ):
            pass

        self.run_as_task(f, use_dill=True, system_site_packages=False, requirements=["pendulum"])

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

        self.run_as_task(f, use_dill=True, system_site_packages=False, requirements=None)


# when venv tests are run in parallel to other test they create new processes and this might take
# quite some time in shared docker environment and get some contention even between different containers
# therefore we have to extend timeouts for those tests
@pytest.mark.execution_timeout(120)
@pytest.mark.external_python_operator
class TestExternalPythonOperator(BaseTestPythonVirtualenvOperator):
    opcls = ExternalPythonOperator

    @staticmethod
    def default_kwargs(*, python_version=DEFAULT_PYTHON_VERSION, **kwargs):
        kwargs["python"] = sys.executable
        return kwargs

    @mock.patch("pickle.loads")
    def test_except_value_error(self, loads_mock):
        def f():
            return 1

        task = PythonVirtualenvOperator(
            python_callable=f,
            task_id="task",
            dag=self.dag,
        )

        loads_mock.side_effect = DeserializingResultError
        with pytest.raises(DeserializingResultError):
            task._read_result(path=mock.Mock())


class BaseTestBranchPythonVirtualenvOperator(BaseTestPythonVirtualenvOperator):
    @pytest.fixture(autouse=True)
    def setup_tests(self):
        self.branch_1 = EmptyOperator(task_id="branch_1")
        self.branch_2 = EmptyOperator(task_id="branch_2")

    def test_with_args(self):
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception

        with pytest.raises(AirflowException, match="but got 'bool'"):
            self.run_as_task(f, op_args=[0, 1], op_kwargs={"c": True})

    def test_return_false(self):
        def f():
            return False

        with pytest.raises(AirflowException, match="but got 'bool'"):
            self.run_as_task(f)

    def test_context(self):
        def f(templates_dict):
            return templates_dict["ds"]

        with pytest.raises(AirflowException, match="Invalid tasks found:"):
            self.run_as_task(f, templates_dict={"ds": "{{ ds }}"})

    def test_with_no_caching(self):
        """
        Most of venv tests use caching to speed up the tests. This test ensures that
        we have test without caching as well.

        :return:
        """

        def f():
            return False

        with pytest.raises(AirflowException, match="but got 'bool'"):
            self.run_as_task(f, do_not_use_caching=True)

    def test_with_dag_run(self):
        with self.dag:

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> [self.branch_1, self.branch_2]

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        self.assert_expected_task_states(
            dr, {self.task_id: State.SUCCESS, "branch_1": State.NONE, "branch_2": State.SKIPPED}
        )

    def test_with_skip_in_branch_downstream_dependencies(self):
        with self.dag:

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> self.branch_1 >> self.branch_2
            branch_op >> self.branch_2

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        self.assert_expected_task_states(
            dr, {self.task_id: State.SUCCESS, "branch_1": State.NONE, "branch_2": State.NONE}
        )

    def test_with_skip_in_branch_downstream_dependencies2(self):
        with self.dag:

            def f():
                return "branch_2"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> self.branch_1 >> self.branch_2
            branch_op >> self.branch_2

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        self.assert_expected_task_states(
            dr, {self.task_id: State.SUCCESS, "branch_1": State.SKIPPED, "branch_2": State.NONE}
        )

    def test_xcom_push(self):
        with self.dag:

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> [self.branch_1, self.branch_2]

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        for ti in dr.get_task_instances():
            if ti.task_id == self.task_id:
                assert ti.xcom_pull(task_ids=self.task_id) == "branch_1"
                break
        else:
            pytest.fail(f"{self.task_id!r} not found.")

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by BranchPythonOperator, clearing the skipped task
        should not cause it to be executed.
        """
        with self.dag:

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branches = [self.branch_1, self.branch_2]
            branch_op >> branches

        dr = self.create_dag_run()
        branch_op.run(start_date=self.default_date, end_date=self.default_date)
        for task in branches:
            task.run(start_date=self.default_date, end_date=self.default_date)

        expected_states = {
            self.task_id: State.SUCCESS,
            "branch_1": State.SUCCESS,
            "branch_2": State.SKIPPED,
        }

        self.assert_expected_task_states(dr, expected_states)

        # Clear the children tasks.
        tis = dr.get_task_instances()
        children_tis = [ti for ti in tis if ti.task_id in branch_op.get_direct_relative_ids()]
        with create_session() as session:
            clear_task_instances(children_tis, session=session, dag=branch_op.dag)

        # Run the cleared tasks again.
        for task in branches:
            task.run(start_date=self.default_date, end_date=self.default_date)

        # Check if the states are correct after children tasks are cleared.
        self.assert_expected_task_states(dr, expected_states)

    def test_raise_exception_on_no_accepted_type_return(self):
        def f():
            return 5

        ti = self.create_ti(f)
        with pytest.raises(AirflowException, match="must be either None, a task ID, or an Iterable of IDs"):
            ti.run()

    def test_raise_exception_on_invalid_task_id(self):
        def f():
            return "some_task_id"

        ti = self.create_ti(f)
        with pytest.raises(AirflowException, match="Invalid tasks found: {'some_task_id'}"):
            ti.run()


# when venv tests are run in parallel to other test they create new processes and this might take
# quite some time in shared docker environment and get some contention even between different containers
# therefore we have to extend timeouts for those tests


@pytest.mark.execution_timeout(120)
@pytest.mark.virtualenv_operator
class TestBranchPythonVirtualenvOperator(BaseTestBranchPythonVirtualenvOperator):
    opcls = BranchPythonVirtualenvOperator

    @staticmethod
    def default_kwargs(*, python_version=DEFAULT_PYTHON_VERSION, **kwargs):
        if "do_not_use_caching" in kwargs:
            kwargs.pop("do_not_use_caching")
        else:
            # Caching by default makes the tests run faster except few cases we want to test with regular venv
            if "venv_cache_path" not in kwargs:
                kwargs["venv_cache_path"] = venv_cache_path
        return kwargs


# when venv tests are run in parallel to other test they create new processes and this might take
# quite some time in shared docker environment and get some contention even between different containers
# therefore we have to extend timeouts for those tests
@pytest.mark.external_python_operator
class TestBranchExternalPythonOperator(BaseTestBranchPythonVirtualenvOperator):
    opcls = BranchExternalPythonOperator

    @staticmethod
    def default_kwargs(*, python_version=DEFAULT_PYTHON_VERSION, **kwargs):
        # Remove do not use caching that might come from one of the tests in the base class
        if "do_not_use_caching" in kwargs:
            kwargs.pop("do_not_use_caching")
        kwargs["python"] = sys.executable
        return kwargs


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


@pytest.fixture
def clear_db():
    clear_db_runs()
    yield
    clear_db_runs()


DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": datetime(2022, 1, 1),
    "end_date": datetime.today(),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@pytest.mark.usefixtures("clear_db")
class TestCurrentContextRuntime:
    def test_context_in_task(self):
        with DAG(dag_id="assert_context_dag", default_args=DEFAULT_ARGS, schedule="@once"):
            op = MyContextAssertOperator(task_id="assert_context")
            op.run(ignore_first_depends_on_past=True, ignore_ti_state=True)

    def test_get_context_in_old_style_context_task(self):
        with DAG(dag_id="edge_case_context_dag", default_args=DEFAULT_ARGS, schedule="@once"):
            op = PythonOperator(python_callable=get_all_the_context, task_id="get_all_the_context")
            op.run(ignore_first_depends_on_past=True, ignore_ti_state=True)


class TestShortCircuitWithTeardown:
    @pytest.mark.parametrize(
        "ignore_downstream_trigger_rules, with_teardown, should_skip, expected",
        [
            (False, True, True, ["op2"]),
            (False, True, False, []),
            (False, False, True, ["op2"]),
            (False, False, False, []),
            (True, True, True, ["op2", "op3"]),
            (True, True, False, []),
            (True, False, True, ["op2", "op3", "op4"]),
            (True, False, False, []),
        ],
    )
    def test_short_circuit_with_teardowns(
        self, dag_maker, ignore_downstream_trigger_rules, should_skip, with_teardown, expected
    ):
        with dag_maker() as dag:
            op1 = ShortCircuitOperator(
                task_id="op1",
                python_callable=lambda: not should_skip,
                ignore_downstream_trigger_rules=ignore_downstream_trigger_rules,
            )
            op2 = PythonOperator(task_id="op2", python_callable=print)
            op3 = PythonOperator(task_id="op3", python_callable=print)
            op4 = PythonOperator(task_id="op4", python_callable=print)
            if with_teardown:
                op4.as_teardown()
            op1 >> op2 >> op3 >> op4
            op1.skip = MagicMock()
            dagrun = dag_maker.create_dagrun()
            tis = dagrun.get_task_instances()
            ti: TaskInstance = next(x for x in tis if x.task_id == "op1")
            ti._run_raw_task()
            expected_tasks = {dag.task_dict[x] for x in expected}
        if should_skip:
            # we can't use assert_called_with because it's a set and therefore not ordered
            actual_skipped = set(op1.skip.call_args.kwargs["tasks"])
            assert actual_skipped == expected_tasks
        else:
            op1.skip.assert_not_called()

    @pytest.mark.parametrize("config", ["sequence", "parallel"])
    def test_short_circuit_with_teardowns_complicated(self, dag_maker, config):
        with dag_maker():
            s1 = PythonOperator(task_id="s1", python_callable=print).as_setup()
            s2 = PythonOperator(task_id="s2", python_callable=print).as_setup()
            op1 = ShortCircuitOperator(
                task_id="op1",
                python_callable=lambda: False,
            )
            op2 = PythonOperator(task_id="op2", python_callable=print)
            t1 = PythonOperator(task_id="t1", python_callable=print).as_teardown(setups=s1)
            t2 = PythonOperator(task_id="t2", python_callable=print).as_teardown(setups=s2)
            if config == "sequence":
                s1 >> op1 >> s2 >> op2 >> [t1, t2]
            elif config == "parallel":
                s1 >> op1 >> s2 >> op2 >> t2 >> t1
            else:
                raise ValueError("unexpected")
            op1.skip = MagicMock()
            dagrun = dag_maker.create_dagrun()
            tis = dagrun.get_task_instances()
            ti: TaskInstance = next(x for x in tis if x.task_id == "op1")
            ti._run_raw_task()
            # we can't use assert_called_with because it's a set and therefore not ordered
            actual_skipped = set(op1.skip.call_args.kwargs["tasks"])
            assert actual_skipped == {s2, op2}

    def test_short_circuit_with_teardowns_complicated_2(self, dag_maker):
        with dag_maker():
            s1 = PythonOperator(task_id="s1", python_callable=print).as_setup()
            s2 = PythonOperator(task_id="s2", python_callable=print).as_setup()
            op1 = ShortCircuitOperator(
                task_id="op1",
                python_callable=lambda: False,
            )
            op2 = PythonOperator(task_id="op2", python_callable=print)
            op3 = PythonOperator(task_id="op3", python_callable=print)
            t1 = PythonOperator(task_id="t1", python_callable=print).as_teardown(setups=s1)
            t2 = PythonOperator(task_id="t2", python_callable=print).as_teardown(setups=s2)
            s1 >> op1 >> op3 >> t1
            s2 >> op2 >> t2

            # this is the weird, maybe nonsensical part
            # in this case we don't want to skip t2 since it should run
            op1 >> t2
            op1.skip = MagicMock()
            dagrun = dag_maker.create_dagrun()
            tis = dagrun.get_task_instances()
            ti: TaskInstance = next(x for x in tis if x.task_id == "op1")
            ti._run_raw_task()
            # we can't use assert_called_with because it's a set and therefore not ordered
            actual_kwargs = op1.skip.call_args.kwargs
            actual_skipped = set(actual_kwargs["tasks"])
            assert actual_kwargs["execution_date"] == dagrun.logical_date
            assert actual_skipped == {op3}

    @pytest.mark.parametrize("level", [logging.DEBUG, logging.INFO])
    def test_short_circuit_with_teardowns_debug_level(self, dag_maker, level, clear_db):
        """
        When logging is debug we convert to a list to log the tasks skipped
        before passing them to the skip method.
        """
        with dag_maker():
            s1 = PythonOperator(task_id="s1", python_callable=print).as_setup()
            s2 = PythonOperator(task_id="s2", python_callable=print).as_setup()
            op1 = ShortCircuitOperator(
                task_id="op1",
                python_callable=lambda: False,
            )
            op1.log.setLevel(level)
            op2 = PythonOperator(task_id="op2", python_callable=print)
            op3 = PythonOperator(task_id="op3", python_callable=print)
            t1 = PythonOperator(task_id="t1", python_callable=print).as_teardown(setups=s1)
            t2 = PythonOperator(task_id="t2", python_callable=print).as_teardown(setups=s2)
            s1 >> op1 >> op3 >> t1
            s2 >> op2 >> t2

            # this is the weird, maybe nonsensical part
            # in this case we don't want to skip t2 since it should run
            op1 >> t2
            op1.skip = MagicMock()
            dagrun = dag_maker.create_dagrun()
            tis = dagrun.get_task_instances()
            ti: TaskInstance = next(x for x in tis if x.task_id == "op1")
            ti._run_raw_task()
            # we can't use assert_called_with because it's a set and therefore not ordered
            actual_kwargs = op1.skip.call_args.kwargs
            actual_skipped = actual_kwargs["tasks"]
            if level <= logging.DEBUG:
                assert isinstance(actual_skipped, list)
            else:
                assert isinstance(actual_skipped, Generator)
            assert set(actual_skipped) == {op3}
            assert actual_kwargs["execution_date"] == dagrun.logical_date
