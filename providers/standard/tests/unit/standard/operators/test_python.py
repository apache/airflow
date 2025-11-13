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
from collections.abc import Generator
from datetime import date, datetime, timezone as _timezone
from functools import partial
from importlib.util import find_spec
from pathlib import Path
from subprocess import CalledProcessError
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest
from slugify import slugify

from airflow.exceptions import AirflowProviderDeprecationWarning, DeserializingResultError
from airflow.models.connection import Connection
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchExternalPythonOperator,
    BranchPythonOperator,
    BranchPythonVirtualenvOperator,
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    ShortCircuitOperator,
    _parse_version_info,
    _PythonVersionInfo,
    get_current_context,
)
from airflow.providers.standard.utils.python_virtualenv import _execute_in_subprocess, prepare_virtualenv
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.version_compat import (
    AIRFLOW_V_3_0_1,
    AIRFLOW_V_3_0_PLUS,
    AIRFLOW_V_3_1_PLUS,
    NOTSET,
)

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import BaseOperator
    from airflow.sdk.execution_time.context import set_current_context
    from airflow.serialization.serialized_objects import LazyDeserializedDAG
else:
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]
    from airflow.models.taskinstance import set_current_context  # type: ignore[attr-defined,no-redef]

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]
try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

if TYPE_CHECKING:
    from airflow.models.dag import DAG
    from airflow.models.dagrun import DagRun
    from airflow.utils.context import Context

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

AIRFLOW_ROOT_PATH = Path(__file__).parents[6]

TI = TaskInstance
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
TEMPLATE_SEARCHPATH = (AIRFLOW_ROOT_PATH / "airflow-core" / "tests" / "unit" / "config_templates").as_posix()
LOGGER_NAME = "airflow.task.operators"
DEFAULT_PYTHON_VERSION = f"{sys.version_info[0]}.{sys.version_info[1]}"
DILL_INSTALLED = find_spec("dill") is not None
DILL_MARKER = pytest.mark.skipif(not DILL_INSTALLED, reason="`dill` is not installed")
CLOUDPICKLE_INSTALLED = find_spec("cloudpickle") is not None
CLOUDPICKLE_MARKER = pytest.mark.skipif(not CLOUDPICKLE_INSTALLED, reason="`cloudpickle` is not installed")

if AIRFLOW_V_3_0_1:
    from airflow.providers.common.compat.sdk import DownstreamTasksSkipped


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
    def base_tests_setup(self, request, create_serialized_task_instance_of_operator, dag_maker):
        self.dag_id = f"dag_{slugify(request.cls.__name__)}"
        self.task_id = f"task_{slugify(request.node.name, max_length=40)}"
        self.run_id = f"run_{slugify(request.node.name, max_length=40)}"
        self.ds_templated = self.default_date.date().isoformat()
        self.ti_maker = create_serialized_task_instance_of_operator

        self.dag_maker = dag_maker
        self.dag_non_serialized = self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH).dag
        # We need to entre the context in order to the factory to create things
        with self.dag_maker:
            ...
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
        from airflow.models.serialized_dag import SerializedDagModel

        # Update the serialized DAG with any tasks added after initial dag was created
        if AIRFLOW_V_3_1_PLUS:
            self.dag_maker.serialized_model = SerializedDagModel(
                LazyDeserializedDAG.from_dag(self.dag_non_serialized)
            )
        else:
            self.dag_maker.serialized_model = SerializedDagModel(self.dag_non_serialized)
        return self.dag_maker.create_dagrun(
            state=DagRunState.RUNNING,
            start_date=self.dag_maker.start_date,
            session=self.dag_maker.session,
            logical_date=self.default_date,
            run_type=DagRunType.MANUAL,
            data_interval=(self.default_date, self.default_date),
        )

    def create_ti(self, fn, **kwargs) -> TI:
        """Create TaskInstance for class defined Operator."""
        return self.ti_maker(
            self.opcls,
            python_callable=fn,
            **self.default_kwargs(**kwargs),
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=self.default_date,
        )

    def run_as_operator(self, fn, **kwargs):
        """Run task by direct call ``run`` method."""
        clear_db_runs()
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):
            task = self.opcls(task_id=self.task_id, python_callable=fn, **self.default_kwargs(**kwargs))
        dr = self.dag_maker.create_dagrun()
        self.dag_maker.run_ti(self.task_id, dr)
        clear_db_runs()
        return task

    def run_as_task(self, fn, return_ti=False, **kwargs):
        """Create TaskInstance and run it."""
        ti = self.create_ti(fn, **kwargs)
        assert ti.task is not None
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

    def test_python_callable_keyword_arguments_callable_not_templatized(self):
        """Test PythonOperator op_kwargs are not templatized if it's a callable"""

        def a_fn():
            return 4

        task = self.render_templates(
            lambda: 0,
            op_kwargs={
                "a_callable": a_fn,
            },
        )
        rendered_op_kwargs = task.op_kwargs
        assert rendered_op_kwargs["a_callable"] == a_fn

    def test_python_operator_shallow_copy_attr(self):
        def not_callable(x):
            raise RuntimeError("Should not be triggered")

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
            assert custom == 1, "custom should be 1"
            assert dag is not None, "dag should be set"

        error_message = "Invalid arguments were passed to PythonOperator \\(task_id: task_test-provide-context-does-not-fail\\). Invalid arguments were:\n\\*\\*kwargs: {'provide_context': True}"

        with pytest.raises((TypeError, AirflowException), match=error_message):
            self.run_as_task(func, op_kwargs={"custom": 1}, provide_context=True)

    def test_context_with_conflicting_op_args(self):
        def func(custom, dag):
            assert custom == 1, "custom should be 1"
            assert dag is not None, "dag should be set"

        self.run_as_task(func, op_kwargs={"custom": 1})

    def test_context_with_kwargs(self):
        def func(**context):
            # check if context is being set
            assert len(context) > 0, "Context has not been injected"

        self.run_as_task(func, op_kwargs={"custom": 1})

    @pytest.mark.parametrize(
        ("show_return_value_in_logs", "should_shown"),
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

        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):
            python_operator = PythonOperator(
                task_id="python_operator",
                python_callable=func,
                show_return_value_in_logs=False,
                templates_exts=["test_ext"],
            )

        assert python_operator.template_ext == ["test_ext"]

    def test_python_operator_has_default_logger_name(self):
        python_operator = PythonOperator(task_id="task", python_callable=partial(int, 2))

        logger_name: str = "airflow.task.operators.airflow.providers.standard.operators.python.PythonOperator"
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
        clear_db_runs()
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> [self.branch_1, self.branch_2]

        dr = self.dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as dts:
                self.dag_maker.run_ti(self.task_id, dr)
            assert dts.value.tasks == [("branch_2", -1)]
        else:
            self.dag_maker.run_ti(self.task_id, dr)
            self.assert_expected_task_states(
                dr, {self.task_id: State.SUCCESS, "branch_1": State.NONE, "branch_2": State.SKIPPED}
            )

    def test_with_skip_in_branch_downstream_dependencies(self):
        clear_db_runs()
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> self.branch_1 >> self.branch_2
            branch_op >> self.branch_2

        dr = self.dag_maker.create_dagrun()
        self.dag_maker.run_ti(self.task_id, dr)
        self.assert_expected_task_states(
            dr, {self.task_id: State.SUCCESS, "branch_1": State.NONE, "branch_2": State.NONE}
        )

    def test_with_skip_in_branch_downstream_dependencies2(self):
        clear_db_runs()
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):

            def f():
                return "branch_2"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> self.branch_1 >> self.branch_2
            branch_op >> self.branch_2

        dr = self.dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as dts:
                branch_op.run(start_date=self.default_date, end_date=self.default_date)
            assert dts.value.tasks == [("branch_1", -1)]
        else:
            self.dag_maker.run_ti(branch_op.task_id, dr)
            self.assert_expected_task_states(
                dr, {self.task_id: State.SUCCESS, "branch_1": State.SKIPPED, "branch_2": State.NONE}
            )

    def test_clear_skipped_downstream_task(self, dag_maker):
        """
        After a downstream task is skipped by BranchPythonOperator, clearing the skipped task
        should not cause it to be executed.
        """
        with dag_maker(serialized=False):

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branches = [self.branch_1, self.branch_2]
            branch_op >> branches

        dr = dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with create_session() as session:
                branch_ti = dr.get_task_instance(task_id=self.task_id, session=session)
                with pytest.raises(DownstreamTasksSkipped) as exc_info:
                    branch_ti.run()
                assert exc_info.value.tasks == [("branch_2", -1)]
                branch_ti.set_state(TaskInstanceState.SUCCESS, session=session)
                dr.task_instance_scheduling_decisions(session=session)
                branch_2_ti = dr.get_task_instance(task_id="branch_2", session=session)
                branch_2_ti.task = self.branch_2
                assert branch_2_ti.state == TaskInstanceState.SKIPPED
                branch_2_ti.set_state(None)
                branch_2_ti.run()
                assert branch_2_ti.state == TaskInstanceState.SKIPPED
        else:
            dag_maker.run_ti(branch_op.task_id, dr)
            for task in branches:
                dag_maker.run_ti(task.task_id, dr)

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
                if AIRFLOW_V_3_0_PLUS:
                    clear_task_instances(children_tis, session=session)
                else:
                    clear_task_instances(children_tis, session=session, dag=branch_op.dag)

            # Run the cleared tasks again.
            for task in branches:
                dag_maker.run_ti(task.task_id, dr)

            # Check if the states are correct after children tasks are cleared.
            self.assert_expected_task_states(dr, expected_states)

    def test_raise_exception_on_no_accepted_type_return(self):
        def f():
            return 5

        ti = self.create_ti(f)
        with pytest.raises(
            AirflowException,
            match=r"'branch_task_ids'.*task.*",
        ):
            ti.run()

    def test_raise_exception_on_invalid_task_id(self):
        def f():
            return "some_task_id"

        ti = self.create_ti(f)
        with pytest.raises(
            AirflowException, match=r"Invalid tasks found: {\(False, 'bool'\)}.|'branch_task_ids'.*task.*"
        ):
            ti.run()

    def test_none_return_value_should_skip_all_downstream(self):
        """Test that returning None from callable should skip all downstream tasks."""
        clear_db_runs()
        with self.dag_maker(self.dag_id, serialized=True):

            def return_none():
                return None

            branch_op = self.opcls(task_id=self.task_id, python_callable=return_none, **self.default_kwargs())
            branch_op >> [self.branch_1, self.branch_2]

        dr = self.dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as dts:
                self.dag_maker.run_ti(self.task_id, dr)

            # When None is returned, all downstream tasks should be skipped
            expected_skipped = {("branch_1", -1), ("branch_2", -1)}
            assert set(dts.value.tasks) == expected_skipped
        else:
            self.dag_maker.run_ti(self.task_id, dr)
            self.assert_expected_task_states(
                dr, {self.task_id: State.SUCCESS, "branch_1": State.SKIPPED, "branch_2": State.SKIPPED}
            )

    @pytest.mark.parametrize(
        ("choice", "expected_states"),
        [
            ("task1", [State.SUCCESS, State.SUCCESS, State.SUCCESS]),
            ("join", [State.SUCCESS, State.SKIPPED, State.SUCCESS]),
        ],
    )
    def test_empty_branch(self, choice, expected_states, session):
        """
        Tests that BranchPythonOperator handles empty branches properly.
        """
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):

            def f():
                return choice

            branch = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            task1 = EmptyOperator(task_id="task1")
            join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

            branch >> [task1, join]
            task1 >> join

        dr = self.dag_maker.create_dagrun()
        task_ids = [self.task_id, "task1", "join"]
        tis = {ti.task_id: ti for ti in dr.task_instances}

        for task_id in task_ids:  # Mimic the specific order the scheduling would run the tests.
            task_instance = tis[task_id]
            task_instance.refresh_from_task(self.dag_maker.dag.get_task(task_id))
            if AIRFLOW_V_3_0_1:
                from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

                try:
                    task_instance.run()
                except DownstreamTasksSkipped:
                    task_instance.set_state(State.SUCCESS)
            else:
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
    all_downstream_skipped_tasks = {"op1", "op2"}
    all_success_states = {"short_circuit": State.SUCCESS, "op1": State.SUCCESS, "op2": State.SUCCESS}
    all_success_skipped_tasks: set[str] = set()

    @pytest.mark.parametrize(
        (
            "callable_return",
            "test_ignore_downstream_trigger_rules",
            "test_trigger_rule",
            "expected_skipped_tasks",
            "expected_task_states",
        ),
        [
            # Skip downstream tasks, do not respect trigger rules, default trigger rule on all downstream
            # tasks
            (
                False,
                True,
                TriggerRule.ALL_SUCCESS,
                all_downstream_skipped_tasks,
                all_downstream_skipped_states,
            ),
            # Skip downstream tasks via a falsy value, do not respect trigger rules, default trigger rule on
            # all downstream tasks
            ([], True, TriggerRule.ALL_SUCCESS, all_downstream_skipped_tasks, all_downstream_skipped_states),
            # Skip downstream tasks, do not respect trigger rules, non-default trigger rule on a downstream
            # task
            (False, True, TriggerRule.ALL_DONE, all_downstream_skipped_tasks, all_downstream_skipped_states),
            # Skip downstream tasks via a falsy value, do not respect trigger rules, non-default trigger rule
            # on a downstream task
            ([], True, TriggerRule.ALL_DONE, all_downstream_skipped_tasks, all_downstream_skipped_states),
            # Skip downstream tasks, respect trigger rules, default trigger rule on all downstream tasks
            (
                False,
                False,
                TriggerRule.ALL_SUCCESS,
                {"op1"},
                {"short_circuit": State.SUCCESS, "op1": State.SKIPPED, "op2": State.NONE},
            ),
            # Skip downstream tasks via a falsy value, respect trigger rules, default trigger rule on all
            # downstream tasks
            (
                [],
                False,
                TriggerRule.ALL_SUCCESS,
                {"op1"},
                {"short_circuit": State.SUCCESS, "op1": State.SKIPPED, "op2": State.NONE},
            ),
            # Skip downstream tasks, respect trigger rules, non-default trigger rule on a downstream task
            (
                False,
                False,
                TriggerRule.ALL_DONE,
                {"op1"},
                {"short_circuit": State.SUCCESS, "op1": State.SKIPPED, "op2": State.SUCCESS},
            ),
            # Skip downstream tasks via a falsy value, respect trigger rules, non-default trigger rule on a
            # downstream task
            (
                [],
                False,
                TriggerRule.ALL_DONE,
                {"op1"},
                {"short_circuit": State.SUCCESS, "op1": State.SKIPPED, "op2": State.SUCCESS},
            ),
            # Do not skip downstream tasks, do not respect trigger rules, default trigger rule on all
            # downstream tasks
            (True, True, TriggerRule.ALL_SUCCESS, all_success_skipped_tasks, all_success_states),
            # Do not skip downstream tasks via a truthy value, do not respect trigger rules, default trigger
            # rule on all downstream tasks
            (["a", "b", "c"], True, TriggerRule.ALL_SUCCESS, all_success_skipped_tasks, all_success_states),
            # Do not skip downstream tasks, do not respect trigger rules, non-default trigger rule on a
            # downstream task
            (True, True, TriggerRule.ALL_DONE, all_success_skipped_tasks, all_success_states),
            # Do not skip downstream tasks via a truthy value, do not respect trigger rules, non-default
            # trigger rule on a downstream task
            (["a", "b", "c"], True, TriggerRule.ALL_DONE, all_success_skipped_tasks, all_success_states),
            # Do not skip downstream tasks, respect trigger rules, default trigger rule on all downstream
            # tasks
            (True, False, TriggerRule.ALL_SUCCESS, all_success_skipped_tasks, all_success_states),
            # Do not skip downstream tasks via a truthy value, respect trigger rules, default trigger rule on
            # all downstream tasks
            (["a", "b", "c"], False, TriggerRule.ALL_SUCCESS, all_success_skipped_tasks, all_success_states),
            # Do not skip downstream tasks, respect trigger rules, non-default trigger rule on a downstream
            # task
            (True, False, TriggerRule.ALL_DONE, all_success_skipped_tasks, all_success_states),
            # Do not skip downstream tasks via a truthy value, respect trigger rules, non-default trigger rule
            # on a downstream  task
            (["a", "b", "c"], False, TriggerRule.ALL_DONE, all_success_skipped_tasks, all_success_states),
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
        self,
        callable_return,
        test_ignore_downstream_trigger_rules,
        test_trigger_rule,
        expected_skipped_tasks,
        expected_task_states,
    ):
        """
        Checking the behavior of the ShortCircuitOperator in several scenarios enabling/disabling the skipping
        of downstream tasks, both short-circuiting modes, and various trigger rules of downstream tasks.
        """
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):
            short_circuit = ShortCircuitOperator(
                task_id="short_circuit",
                python_callable=lambda: callable_return,
                ignore_downstream_trigger_rules=test_ignore_downstream_trigger_rules,
            )
            short_circuit >> self.op1 >> self.op2
            self.op2.trigger_rule = test_trigger_rule

        dr = self.dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            if expected_skipped_tasks:
                with pytest.raises(DownstreamTasksSkipped) as exc_info:
                    self.dag_maker.run_ti("short_circuit", dr)
                assert set(exc_info.value.tasks) == set(expected_skipped_tasks)
            else:
                assert self.dag_maker.run_ti("short_circuit", dr) is None

        else:
            self.dag_maker.run_ti("short_circuit", dr)
            self.dag_maker.run_ti("op1", dr)
            self.dag_maker.run_ti("op2", dr)

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
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):
            short_circuit = ShortCircuitOperator(task_id=self.task_id, python_callable=lambda: False)
            short_circuit >> self.op1 >> self.op2

        dr = self.dag_maker.create_dagrun()

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with create_session() as session:
                sc_ti = dr.get_task_instance(task_id=self.task_id, session=session)
                with pytest.raises(DownstreamTasksSkipped) as exc_info:
                    sc_ti.run()
                assert set(exc_info.value.tasks) == {"op1", "op2"}
                sc_ti.set_state(TaskInstanceState.SUCCESS, session=session)
                dr.task_instance_scheduling_decisions(session=session)
                op1_ti = dr.get_task_instance(task_id="op1", session=session)
                op1_ti.task = self.op1
                assert op1_ti.state == TaskInstanceState.SKIPPED
                op1_ti.set_state(None)
                op1_ti.run()
                assert op1_ti.state == TaskInstanceState.SKIPPED
        else:
            self.dag_maker.run_ti(self.task_id, dr)
            self.dag_maker.run_ti(self.op1.task_id, dr)
            self.dag_maker.run_ti(self.op2.task_id, dr)
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
                if AIRFLOW_V_3_0_PLUS:
                    clear_task_instances([ti for ti in tis if ti.task_id == "op1"], session=session)
                else:
                    clear_task_instances(
                        [ti for ti in tis if ti.task_id == "op1"], session=session, dag=short_circuit.dag
                    )
            self.dag_maker.run_ti("op1", dr)
            self.assert_expected_task_states(dr, expected_states)

    def test_xcom_push(self):
        clear_db_runs()
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):
            short_op_push_xcom = ShortCircuitOperator(
                task_id="push_xcom_from_shortcircuit", python_callable=lambda: "signature"
            )
            short_op_no_push_xcom = ShortCircuitOperator(
                task_id="do_not_push_xcom_from_shortcircuit", python_callable=lambda: False
            )

        dr = self.dag_maker.create_dagrun()
        self.dag_maker.run_ti("push_xcom_from_shortcircuit", dr)
        self.dag_maker.run_ti("do_not_push_xcom_from_shortcircuit", dr)

        tis = dr.get_task_instances()
        assert tis[0].xcom_pull(task_ids=short_op_push_xcom.task_id, key="return_value") == "signature"
        assert tis[0].xcom_pull(task_ids=short_op_no_push_xcom.task_id, key="return_value") is False

    def test_xcom_push_skipped_tasks(self):
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):
            short_op_push_xcom = ShortCircuitOperator(
                task_id="push_xcom_from_shortcircuit", python_callable=lambda: False
            )
            empty_task = EmptyOperator(task_id="empty_task")
            short_op_push_xcom >> empty_task
        dr = self.dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped):
                short_op_push_xcom.run(start_date=self.default_date, end_date=self.default_date)
        else:
            self.dag_maker.run_ti("push_xcom_from_shortcircuit", dr)
        tis = dr.get_task_instances()
        assert tis[0].xcom_pull(task_ids=short_op_push_xcom.task_id, key="skipmixin_key") == {
            "skipped": ["empty_task"]
        }

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 2 implementation is different")
    def test_short_circuit_operator_skips_sensors(self):
        """Test that ShortCircuitOperator properly skips sensors in Airflow 3.x."""
        from airflow.sdk.bases.sensor import BaseSensorOperator

        # Create a sensor similar to S3FileSensor to reproduce the issue
        class CustomS3Sensor(BaseSensorOperator):
            def __init__(self, bucket_name: str, object_key: str, **kwargs):
                super().__init__(**kwargs)
                self.bucket_name = bucket_name
                self.object_key = object_key
                self.timeout = 0
                self.poke_interval = 0

            def poke(self, context):
                # Simulate sensor logic
                return True

        with self.dag_maker(self.dag_id):
            # ShortCircuit that evaluates to False (should skip all downstream)
            short_circuit = ShortCircuitOperator(
                task_id="check_dis_is_mon_to_fri_not_holiday",
                python_callable=lambda: False,  # This causes skipping
            )

            sensor_task = CustomS3Sensor(
                task_id="wait_for_ticker_to_secid_lookup_s3_file",
                bucket_name="test-bucket",
                object_key="ticker_to_secid_lookup.csv",
            )

            short_circuit >> sensor_task

        dr = self.dag_maker.create_dagrun()

        self.dag_maker.run_ti("check_dis_is_mon_to_fri_not_holiday", dr)

        # Verify the sensor is included in the skip list by checking XCom
        # (this was the bug - sensors were not being included in skip list)
        tis = dr.get_task_instances()
        xcom_data = tis[0].xcom_pull(task_ids="check_dis_is_mon_to_fri_not_holiday", key="skipmixin_key")

        assert xcom_data is not None, "XCom data should exist"
        skipped_task_ids = set(xcom_data.get("skipped", []))
        assert "wait_for_ticker_to_secid_lookup_s3_file" in skipped_task_ids, (
            "Sensor should be skipped by ShortCircuitOperator"
        )


virtualenv_string_args: list[str] = []


def _callable_that_imports_from_bundle():
    """
    A callable function for testing DAG bundle imports.
    This import will fail if PYTHONPATH is not set correctly by the operator.
    The module 'bug_test_dag_repro' is created dynamically in the test.
    """
    try:
        from bug_test_dag_repro.lib.helper import get_message

        return get_message()
    except ImportError as e:
        # This helps debug if the import fails during the test
        import sys

        return f"Failed to import: {e}. PYTHONPATH: {sys.path}"


@pytest.mark.execution_timeout(120)
@pytest.mark.parametrize(
    ("opcls", "pytest_marks", "test_class_ref"),
    [
        pytest.param(
            PythonVirtualenvOperator,
            [pytest.mark.virtualenv_operator],
            lambda: TestPythonVirtualenvOperator,
            id="PythonVirtualenvOperator",
        ),
        pytest.param(
            ExternalPythonOperator,
            [pytest.mark.external_python_operator],
            lambda: TestExternalPythonOperator,
            id="ExternalPythonOperator",
        ),
    ],
)
class TestDagBundleImportInSubprocess(BasePythonTest):
    """
    Test DAG bundle imports for subprocess-based Python operators.

    This test ensures that callables running in subprocesses can import modules
    from their DAG bundle by verifying PYTHONPATH is correctly set (Airflow 3.x+).
    """

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="DAG Bundle import fix is for Airflow 3.x+")
    def test_dag_bundle_import_in_subprocess(self, dag_maker, opcls, pytest_marks, test_class_ref):
        """
        Tests that a callable in a subprocess can import modules from its
        own DAG bundle (fix for Airflow 3.x).
        """
        with TemporaryDirectory() as tmp_dir:
            bundle_root = Path(tmp_dir)

            # Dummy module structure inside the temp dir
            module_dir = bundle_root / "bug_test_dag_repro"
            lib_dir = module_dir / "lib"
            lib_dir.mkdir(parents=True)

            (module_dir / "__init__.py").touch()
            (lib_dir / "__init__.py").touch()
            (lib_dir / "helper.py").write_text("def get_message():\n    return 'it works from bundle'")

            # We need a real DAG to create a real TI context
            with dag_maker(self.dag_id, serialized=True):
                op = opcls(
                    task_id=self.task_id,
                    python_callable=_callable_that_imports_from_bundle,
                    **test_class_ref().default_kwargs(),
                )

            dr = dag_maker.create_dagrun()
            ti = dr.get_task_instance(self.task_id)

            mock_bundle_instance = mock.Mock()
            mock_bundle_instance.path = str(bundle_root)
            ti.bundle_instance = mock_bundle_instance

            context = ti.get_template_context()
            result = op.execute(context)

            assert result == "it works from bundle"


@pytest.mark.execution_timeout(120)
class BaseTestPythonVirtualenvOperator(BasePythonTest):
    def test_template_fields(self):
        assert set(PythonOperator.template_fields).issubset(PythonVirtualenvOperator.template_fields)

    def test_fail(self):
        def f():
            raise RuntimeError

        with pytest.raises(CalledProcessError):
            self.run_as_task(f)

    def test_fail_with_message(self):
        def f():
            raise RuntimeError("Custom error message")

        with pytest.raises(AirflowException, match="Custom error message"):
            self.run_as_task(f)

    def test_string_args(self):
        def f():
            print(virtualenv_string_args)
            if virtualenv_string_args[0] != virtualenv_string_args[2]:
                raise RuntimeError

        self.run_as_task(f, string_args=[1, 2, 1])

    def test_with_args(self):
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            raise RuntimeError

        self.run_as_task(f, op_args=[0, 1], op_kwargs={"c": True})

    def test_return_none(self):
        def f():
            return None

        ti = self.run_as_task(f, return_ti=True)
        assert ti.xcom_pull() is None

    def test_return_false(self):
        def f():
            return False

        ti = self.run_as_task(f, return_ti=True)

        assert ti.xcom_pull() is False

    def test_lambda(self):
        with pytest.raises(
            ValueError, match="PythonVirtualenvOperator only supports functions for python_callable arg"
        ):
            PythonVirtualenvOperator(python_callable=lambda x: 4, task_id=self.task_id)

    def test_nonimported_as_arg(self):
        def f(_):
            return None

        self.run_as_task(f, op_args=[datetime.now(tz=_timezone.utc)])

    def test_context(self):
        def f(templates_dict):
            return templates_dict["ds"]

        task = self.run_as_task(f, templates_dict={"ds": "{{ ds }}"})
        assert task.templates_dict == {"ds": self.ds_templated}

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_deepcopy(self, serializer):
        """Test that operator are deep-copyable."""

        def f():
            return 1

        op = self.opcls(task_id="task", python_callable=f, **self.default_kwargs())
        copy.deepcopy(op)

    def test_virtualenv_serializable_context_fields(self, create_task_instance):
        """Ensure all template context fields are listed in the operator.

        This exists mainly so when a field is added to the context, we remember to
        also add it to PythonVirtualenvOperator.
        """
        # These are intentionally NOT serialized into the virtual environment:
        # * Variables pointing to the task instance itself.
        # * Variables that are accessor instances.
        intentionally_excluded_context_keys = {
            "task_instance",
            "ti",
            "var",  # Accessor for Variable; var->json and var->value.
            "conn",  # Accessor for Connection.
            "map_index_template",
        }
        intentionally_excluded_context_keys |= {
            "inlet_events",
            "outlet_events",
        }

        ti = create_task_instance(dag_id=self.dag_id, task_id=self.task_id, schedule=None)
        context = ti.get_template_context()

        declared_keys = {
            *PythonVirtualenvOperator.BASE_SERIALIZABLE_CONTEXT_KEYS,
            *PythonVirtualenvOperator.PENDULUM_SERIALIZABLE_CONTEXT_KEYS,
            *PythonVirtualenvOperator.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS,
            *intentionally_excluded_context_keys,
        }
        if AIRFLOW_V_3_0_PLUS:
            declared_keys -= {
                "execution_date",
                "next_ds",
                "next_ds_nodash",
                "prev_ds",
                "prev_ds_nodash",
                "tomorrow_ds",
                "tomorrow_ds_nodash",
                "triggering_dataset_events",
                "yesterday_ds",
                "yesterday_ds_nodash",
                "next_execution_date",
                "prev_execution_date",
                "prev_execution_date_success",
                "conf",
                "expanded_ti_count",
            }
        else:
            declared_keys.remove("triggering_asset_events")

        assert set(context) == declared_keys

    @pytest.mark.parametrize(
        ("kwargs", "actual_exit_code", "expected_state"),
        [
            ({}, 0, TaskInstanceState.SUCCESS),
            ({}, 100, TaskInstanceState.FAILED),
            ({}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": None}, 0, TaskInstanceState.SUCCESS),
            ({"skip_on_exit_code": None}, 100, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": None}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": 100}, 0, TaskInstanceState.SUCCESS),
            ({"skip_on_exit_code": 100}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": 100}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": 0}, 0, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": [100]}, 0, TaskInstanceState.SUCCESS),
            ({"skip_on_exit_code": [100]}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": [100]}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": [100, 102]}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": (100,)}, 0, TaskInstanceState.SUCCESS),
            ({"skip_on_exit_code": (100,)}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": (100,)}, 101, TaskInstanceState.FAILED),
        ],
    )
    def test_on_skip_exit_code(self, kwargs, actual_exit_code, expected_state):
        def f(exit_code):
            if exit_code != 0:
                raise SystemExit(exit_code)

        if expected_state == TaskInstanceState.FAILED:
            with pytest.raises(CalledProcessError):
                self.run_as_task(f, op_kwargs={"exit_code": actual_exit_code}, **kwargs)
        else:
            ti = self.run_as_task(
                f,
                return_ti=True,
                op_kwargs={"exit_code": actual_exit_code},
                **kwargs,
            )
            assert ti.state == expected_state

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param(
                "dill",
                marks=pytest.mark.skipif(
                    DILL_INSTALLED, reason="For this test case `dill` shouldn't be installed"
                ),
                id="dill",
            ),
            pytest.param(
                "cloudpickle",
                marks=pytest.mark.skipif(
                    CLOUDPICKLE_INSTALLED, reason="For this test case `cloudpickle` shouldn't be installed"
                ),
                id="cloudpickle",
            ),
        ],
    )
    def test_advanced_serializer_not_installed(self, serializer, caplog):
        """Test case for check raising an error if dill/cloudpickle is not installed."""

        def f(a): ...

        with pytest.raises(ModuleNotFoundError):
            self.run_as_task(f, op_args=[42], serializer=serializer)
        assert f"Unable to import `{serializer}` module." in caplog.text

    def test_environment_variables(self):
        def f():
            import os

            return os.environ["MY_ENV_VAR"]

        ti = self.run_as_task(f, env_vars={"MY_ENV_VAR": "ABCDE"}, return_ti=True)
        assert ti.xcom_pull() == "ABCDE"

    def test_environment_variables_with_inherit_env_true(self, monkeypatch):
        monkeypatch.setenv("MY_ENV_VAR", "QWERT")

        def f():
            import os

            return os.environ["MY_ENV_VAR"]

        ti = self.run_as_task(f, inherit_env=True, return_ti=True)
        assert ti.xcom_pull() == "QWERT"

    def test_environment_variables_with_inherit_env_false(self, monkeypatch):
        monkeypatch.setenv("MY_ENV_VAR", "TYUIO")

        def f():
            import os

            return os.environ["MY_ENV_VAR"]

        with pytest.raises(AirflowException):
            self.run_as_task(f, inherit_env=False)

    def test_environment_variables_overriding(self, monkeypatch):
        monkeypatch.setenv("MY_ENV_VAR", "ABCDE")

        def f():
            import os

            return os.environ["MY_ENV_VAR"]

        ti = self.run_as_task(f, env_vars={"MY_ENV_VAR": "EFGHI"}, inherit_env=True, return_ti=True)
        assert ti.xcom_pull() == "EFGHI"


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

    @CLOUDPICKLE_MARKER
    def test_add_cloudpickle(self):
        def f():
            """Ensure cloudpickle is correctly installed."""
            import cloudpickle  # noqa: F401

        self.run_as_task(f, serializer="cloudpickle", system_site_packages=False)

    @DILL_MARKER
    def test_add_dill(self):
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        self.run_as_task(f, serializer="dill", system_site_packages=False)

    def test_invalid_serializer(self):
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        with pytest.raises(AirflowException, match="Unsupported serializer 'airflow'"):
            self.run_as_task(f, serializer="airflow")

    def test_no_requirements(self):
        """Tests that the python callable is invoked on task run."""

        def f():
            pass

        self.run_as_task(f)

    @pytest.mark.parametrize(
        ("serializer", "extra_requirements"),
        [
            pytest.param("pickle", [], id="pickle"),
            pytest.param("dill", ["dill"], marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", ["cloudpickle"], marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, [], id="default"),
        ],
    )
    def test_no_system_site_packages(self, serializer, extra_requirements):
        def f():
            try:
                import funcsigs  # noqa: F401
            except ImportError:
                return True
            raise RuntimeError

        self.run_as_task(
            f, system_site_packages=False, requirements=extra_requirements, serializer=serializer
        )

    def test_system_site_packages(self):
        def f():
            import funcsigs  # noqa: F401

        self.run_as_task(f, requirements=["funcsigs"], system_site_packages=True)

    def test_with_requirements_pinned(self):
        def f():
            import funcsigs

            if funcsigs.__version__ != "0.4":
                raise RuntimeError

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
                raise RuntimeError

        self.run_as_task(f, requirements=["funcsigs==0.4"], do_not_use_caching=True)

    @pytest.mark.parametrize(
        ("serializer", "extra_requirements"),
        [
            pytest.param("pickle", [], id="pickle"),
            pytest.param("dill", ["dill"], marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", ["cloudpickle"], marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, [], id="default"),
        ],
    )
    def test_unpinned_requirements(self, serializer, extra_requirements):
        def f():
            import funcsigs  # noqa: F401

        self.run_as_task(
            f,
            requirements=["funcsigs", *extra_requirements],
            system_site_packages=False,
            serializer=serializer,
        )

    @pytest.mark.parametrize(
        ("serializer", "extra_requirements"),
        [
            pytest.param("pickle", [], id="pickle"),
            pytest.param("dill", ["dill"], marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", ["cloudpickle"], marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, [], id="default"),
        ],
    )
    def test_range_requirements(self, serializer, extra_requirements):
        def f():
            import funcsigs  # noqa: F401

        self.run_as_task(
            f,
            requirements=["funcsigs>1.0", *extra_requirements],
            system_site_packages=False,
            serializer=serializer,
        )

    def test_requirements_file(self):
        def f():
            import funcsigs  # noqa: F401

        self.run_as_operator(f, requirements="requirements.txt", system_site_packages=False)

    @mock.patch("airflow.providers.standard.operators.python.prepare_virtualenv")
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

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_templated_requirements_file(self, serializer):
        def f():
            import funcsigs

            assert funcsigs.__version__ == "1.0.2"

        self.run_as_operator(
            f,
            requirements="requirements.txt",
            serializer=serializer,
            params={"environ": "templated_unit_test"},
            system_site_packages=False,
        )

    @pytest.mark.parametrize(
        ("serializer", "extra_requirements"),
        [
            pytest.param("pickle", [], id="pickle"),
            pytest.param("dill", ["dill"], marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", ["cloudpickle"], marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, [], id="default"),
        ],
    )
    def test_python_3_serializers(self, serializer, extra_requirements):
        def f():
            import sys

            print(sys.version)
            try:
                {}.iteritems()
            except AttributeError:
                return
            raise RuntimeError

        self.run_as_task(f, python_version="3", serializer=serializer, requirements=extra_requirements)

    def test_with_default(self):
        def f(a):
            return a

        self.run_as_task(f, system_site_packages=False, op_args=[4])

    @mock.patch(
        "airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess",
    )
    def test_with_index_urls(self, mock_execute_in_subprocess):
        def safe_execute_in_subprocess(cmd, **kwargs):
            """Wrapper that removes unreachable URLs from env before executing."""
            env = kwargs.get("env", {}).copy()
            # Remove fake URLs to allow venv creation to succeed
            env.pop("UV_DEFAULT_INDEX", None)
            env.pop("UV_INDEX", None)
            return _execute_in_subprocess(cmd, **{**kwargs, "env": env if env else None})

        mock_execute_in_subprocess.side_effect = safe_execute_in_subprocess

        def f(a):
            import sys
            from pathlib import Path

            pip_conf = (Path(sys.executable).parents[1] / "pip.conf").read_text()
            assert "first.package.index" in pip_conf
            assert "second.package.index" in pip_conf
            assert "third.package.index" in pip_conf
            return a

        self.run_as_task(
            f,
            index_urls=[
                "https://first.package.index",
                "http://second.package.index",
                "http://third.package.index",
            ],
            op_args=[4],
        )

        # Verify that env was passed with the correct index URLs to pip install
        # The first call is venv creation (with cleaned env), second call is pip install (with full env)
        assert len(mock_execute_in_subprocess.call_args_list) >= 2
        package_install_call_args = mock_execute_in_subprocess.call_args_list[1]
        assert package_install_call_args[1]["env"]["UV_DEFAULT_INDEX"] == "https://first.package.index"
        assert (
            package_install_call_args[1]["env"]["UV_INDEX"]
            == "http://second.package.index http://third.package.index"
        )

    @mock.patch(
        "airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess",
    )
    def test_with_index_url_from_connection(self, mock_execute_in_subprocess, monkeypatch):
        def safe_execute_in_subprocess(cmd, **kwargs):
            """Wrapper that removes unreachable URLs from env before executing."""
            env = kwargs.get("env", {}).copy()
            # Remove fake URLs to allow venv creation to succeed
            env.pop("UV_DEFAULT_INDEX", None)
            env.pop("UV_INDEX", None)
            return _execute_in_subprocess(cmd, **{**kwargs, "env": env if env else None})

        mock_execute_in_subprocess.side_effect = safe_execute_in_subprocess

        class MockConnection(Connection):
            """Mock for the Connection class."""

            def __init__(self, host: str | None, login: str | None, password: str | None):
                super().__init__()
                self.host = host
                self.login = login
                self.password = password

        monkeypatch.setattr(
            "airflow.providers.standard.hooks.package_index.PackageIndexHook.get_connection",
            lambda *_: MockConnection("https://my.package.index", "my_username", "my_password"),
        )

        def f(a):
            import sys
            from pathlib import Path

            pip_conf = (Path(sys.executable).parents[1] / "pip.conf").read_text()
            assert "abc.def.de" in pip_conf
            assert "https://my_username:my_password@my.package.index" in pip_conf
            return a

        self.run_as_task(
            f,
            index_urls=["https://abc.def.de"],
            index_urls_from_connection_ids=["my_connection"],
            op_args=[4],
        )

    def test_caching(self):
        def f(a):
            import sys

            assert "pytest_venv_1234" in sys.executable
            return a

        with TemporaryDirectory(prefix="pytest_venv_1234") as tmp_dir:
            self.run_as_task(f, venv_cache_path=tmp_dir, op_args=[4])

    # This tests might take longer than default 60 seconds as it is serializing a lot of
    # context using dill/cloudpickle (which is slow apparently).
    @pytest.mark.execution_timeout(120)
    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param(
                "dill",
                marks=[
                    DILL_MARKER,
                    pytest.mark.xfail(
                        sys.version_info[:2] == (3, 11),
                        reason=(
                            "Also this test is failed on Python 3.11 because of impact of "
                            "regression in Python 3.11 connected likely with CodeType behaviour "
                            "https://github.com/python/cpython/issues/100316. "
                            "That likely causes that dill is not able to serialize the `conf` correctly. "
                            "Issue about fixing it is captured in https://github.com/apache/airflow/issues/35307"
                        ),
                    ),
                ],
                id="dill",
            ),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
        ],
    )
    @pytest.mark.skipif(
        os.environ.get("PYTEST_PLAIN_ASSERTS") != "true",
        reason="assertion rewriting breaks this test because serializer will try to serialize "
        "AssertRewritingHook including captured stdout and we need to run "
        "it with `--assert=plain` pytest option and PYTEST_PLAIN_ASSERTS=true .",
    )
    def test_airflow_context(self, serializer):
        def f(
            # basic
            ds_nodash,
            inlets,
            outlets,
            params,
            run_id,
            task_instance_key_str,
            ts,
            ts_nodash,
            ts_nodash_with_tz,
            # pendulum-specific
            logical_date,
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

        self.run_as_operator(f, serializer=serializer, system_site_packages=True, requirements=None)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
        ],
    )
    def test_pendulum_context(self, serializer):
        def f(
            # basic
            ds_nodash,
            inlets,
            outlets,
            run_id,
            task_instance_key_str,
            ts,
            ts_nodash,
            ts_nodash_with_tz,
            # pendulum-specific
            logical_date,
            prev_start_date_success,
            prev_end_date_success,
            # other
            **context,
        ):
            pass

        self.run_as_task(f, serializer=serializer, system_site_packages=False, requirements=["pendulum"])

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_base_context(self, serializer):
        def f(
            # basic
            ds_nodash,
            inlets,
            outlets,
            run_id,
            task_instance_key_str,
            ts,
            ts_nodash,
            ts_nodash_with_tz,
            # other
            **context,
        ):
            pass

        self.run_as_task(f, serializer=serializer, system_site_packages=False, requirements=None)

    @pytest.mark.parametrize(
        ("requirements", "system_site", "want_airflow", "want_pendulum"),
        [
            # nothing → just base keys
            ([], False, False, False),
            # site-packages → base keys + pendulum keys
            ([], True, True, True),
            # apache-airflow / no version constraint
            (["apache-airflow"], False, True, True),
            # specific version
            (["apache-airflow==2.10.2"], False, True, True),
            # minimum version
            (["apache-airflow>=2.10"], False, True, True),
            # pendulum / no version constraint
            (["pendulum"], False, False, True),
            # compatible release
            (["pendulum~=2.1.0"], False, False, True),
            # other package
            (["foo==1.0.0"], False, False, False),
            # with other package
            (["apache-airflow", "foo"], False, True, True),
            # full-line comment only
            (["# comment"], False, False, False),
            # inline comment after requirement
            (["apache-airflow==2.10.2  # comment"], False, True, True),
            # blank line + requirement
            (["", "pendulum"], False, False, True),
            # indented comment + requirement
            (["  # comment", "pendulum~=2.1.0"], False, False, True),
            # requirements passed as multi-line strings
            ("funcsigs==0.4\nattrs==23.1.0", False, False, False),
            (["funcsigs==0.4\nattrs==23.1.0"], False, False, False),
            ("pendulum==2.1.2  # pinned version\nattrs==23.1.0  # optional", False, False, True),
        ],
    )
    def test_iter_serializable_context_keys(self, requirements, system_site, want_airflow, want_pendulum):
        def func():
            return "test_return_value"

        op = PythonVirtualenvOperator(
            task_id="task",
            python_callable=func,
            requirements=requirements,
            system_site_packages=system_site,
        )
        keys = set(op._iter_serializable_context_keys())

        base_keys = set(op.BASE_SERIALIZABLE_CONTEXT_KEYS)
        airflow_keys = set(op.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS)
        pendulum_keys = set(op.PENDULUM_SERIALIZABLE_CONTEXT_KEYS)

        # BASE keys always present
        assert base_keys <= keys

        # AIRFLOW keys only when expected
        if want_airflow:
            assert airflow_keys <= keys, f"expected AIRFLOW keys for requirements: {requirements}"
        else:
            assert not (airflow_keys & keys), f"unexpected AIRFLOW keys for requirements: {requirements}"

        # PENDULUM keys only when expected
        if want_pendulum:
            assert pendulum_keys <= keys, f"expected PENDULUM keys for requirements: {requirements}"
        else:
            assert not (pendulum_keys & keys), f"unexpected PENDULUM keys for requirements: {requirements}"

    @pytest.mark.parametrize(
        "invalid_requirement",
        [
            # invalid version format
            "pendulum==3..0",
            # invalid operator (=< instead of <=)
            "apache-airflow=<2.0",
            # same invalid operator on pendulum
            "pendulum=<3.0",
            # totally malformed
            "invalid requirement",
        ],
    )
    def test_iter_serializable_context_keys_invalid_requirement(self, invalid_requirement):
        def func():
            return "test_return_value"

        op = PythonVirtualenvOperator(
            task_id="task",
            python_callable=func,
            requirements=[invalid_requirement],
            system_site_packages=False,
        )

        with pytest.raises(ValueError, match=rf"Invalid requirement '{invalid_requirement}'"):
            # Consume the generator to trigger parsing
            list(op._iter_serializable_context_keys())

    @mock.patch("airflow.providers.standard.operators.python.PythonVirtualenvOperator._prepare_venv")
    @mock.patch(
        "airflow.providers.standard.operators.python.PythonVirtualenvOperator._execute_python_callable_in_subprocess"
    )
    @mock.patch(
        "airflow.providers.standard.operators.python.PythonVirtualenvOperator._cleanup_python_pycache_dir"
    )
    def test_execute_callable_pycache_cleanup(
        self, pycache_cleanup_mock, execute_in_subprocess_mock, prepare_venv_mock
    ):
        custom_pycache_prefix = "custom/__pycache__"
        tempdir_name = "tmp"
        venv_dir_temp_name = "dummy12345/venvrandom"
        venv_path_tmp = f"/{tempdir_name}/{venv_dir_temp_name}"
        expected_cleanup_path = Path.cwd() / custom_pycache_prefix / tempdir_name / venv_dir_temp_name

        def f():
            return 1

        op = PythonVirtualenvOperator(
            task_id="task",
            python_callable=f,
            system_site_packages=False,
        )
        with mock.patch.object(sys, "pycache_prefix", new=custom_pycache_prefix):
            with mock.patch(
                "airflow.providers.standard.operators.python.TemporaryDirectory"
            ) as mock_temp_dir:
                mock_context = mock_temp_dir.return_value.__enter__
                mock_context.return_value = venv_path_tmp
                op.execute_callable()

        execute_in_subprocess_mock.assert_called_once()
        prepare_venv_mock.assert_called_once_with(Path(venv_path_tmp))
        pycache_cleanup_mock.assert_called_once_with(expected_cleanup_path)


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

        task = ExternalPythonOperator(
            python_callable=f,
            task_id="task",
            python=sys.executable,
            dag=self.dag_non_serialized,
        )

        loads_mock.side_effect = DeserializingResultError
        with pytest.raises(DeserializingResultError):
            task._read_result(path=mock.Mock())

    def test_airflow_version(self):
        def f():
            return 42

        op = ExternalPythonOperator(
            python_callable=f, task_id="task", python=sys.executable, expect_airflow=True
        )
        assert op._get_airflow_version_from_target_env()

    def test_airflow_version_doesnt_match(self, caplog):
        def f():
            return 42

        op = ExternalPythonOperator(
            python_callable=f, task_id="task", python=sys.executable, expect_airflow=True
        )

        with mock.patch.object(
            ExternalPythonOperator, "_external_airflow_version_script", new_callable=mock.PropertyMock
        ) as mock_script:
            mock_script.return_value = "print('1.10.4')"
            caplog.set_level("WARNING")
            caplog.clear()
            assert op._get_airflow_version_from_target_env() is None
            assert "(1.10.4) is different than the runtime Airflow version" in caplog.text

    def test_airflow_version_script_error_handle(self, caplog):
        def f():
            return 42

        op = ExternalPythonOperator(
            python_callable=f, task_id="task", python=sys.executable, expect_airflow=True
        )

        with mock.patch.object(
            ExternalPythonOperator, "_external_airflow_version_script", new_callable=mock.PropertyMock
        ) as mock_script:
            mock_script.return_value = "raise SystemExit('Something went wrong')"
            caplog.set_level("WARNING")
            caplog.clear()
            assert op._get_airflow_version_from_target_env() is None
            assert "Something went wrong" in caplog.text
            assert "returned non-zero exit status" in caplog.text

    @mock.patch.object(ExternalPythonOperator, "_get_airflow_version_from_target_env")
    def test_iter_serializable_context_keys_respects_expect_airflow_false(self, mock_get_airflow_version):
        """Test that when expect_airflow=False, _get_airflow_version_from_target_env is not called."""

        def f():
            return 42

        op = ExternalPythonOperator(
            python_callable=f, task_id="task", python=sys.executable, expect_airflow=False
        )

        keys = set(op._iter_serializable_context_keys())

        mock_get_airflow_version.assert_not_called()

        # BASE keys should always be present
        base_keys = set(op.BASE_SERIALIZABLE_CONTEXT_KEYS)
        airflow_keys = set(op.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS)

        assert base_keys <= keys
        assert not (airflow_keys & keys), "Airflow keys should not be present when expect_airflow=False"


class BaseTestBranchPythonVirtualenvOperator(BaseTestPythonVirtualenvOperator):
    @pytest.fixture(autouse=True)
    def setup_tests(self):
        self.branch_1 = EmptyOperator(task_id="branch_1")
        self.branch_2 = EmptyOperator(task_id="branch_2")

    # Skip some tests from base class that are not applicable for branching operators
    # as the branching condition is mandatory but not given by base class
    @pytest.mark.skip(reason="Test is not working for branching operators")
    def test_string_args(self):
        pass

    @pytest.mark.skip(reason="Test is not working for branching operators")
    def test_return_none(self):
        pass

    @pytest.mark.skip(reason="Test is not working for branching operators")
    def test_nonimported_as_arg(self):
        pass

    @pytest.mark.skip(reason="Test is not working for branching operators")
    def test_on_skip_exit_code(self):
        pass

    def test_with_args(self):
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            raise RuntimeError

        with pytest.raises(
            AirflowException, match=r"Invalid tasks found: {\(False, 'bool'\)}.|'branch_task_ids'.*task.*"
        ):
            self.run_as_task(f, op_args=[0, 1], op_kwargs={"c": True})

    def test_return_false(self):
        def f():
            return False

        with pytest.raises(
            AirflowException, match=r"Invalid tasks found: {\(False, 'bool'\)}.|'branch_task_ids'.*task.*"
        ):
            self.run_as_task(f)

    def test_context(self):
        def f(templates_dict):
            return templates_dict["ds"]

        with pytest.raises(AirflowException, match="Invalid tasks found:|'branch_task_ids'.*task.*"):
            self.run_as_task(f, templates_dict={"ds": "{{ ds }}"})

    def test_environment_variables(self):
        def f():
            import os

            return os.environ["MY_ENV_VAR"]

        with pytest.raises(
            AirflowException,
            match=r"'branch_task_ids'.*task.*",
        ):
            self.run_as_task(f, env_vars={"MY_ENV_VAR": "ABCDE"})

    def test_environment_variables_with_inherit_env_true(self, monkeypatch):
        monkeypatch.setenv("MY_ENV_VAR", "QWERT")

        def f():
            import os

            return os.environ["MY_ENV_VAR"]

        with pytest.raises(
            AirflowException,
            match=r"'branch_task_ids'.*task.*",
        ):
            self.run_as_task(f, inherit_env=True)

    def test_environment_variables_with_inherit_env_false(self, monkeypatch):
        monkeypatch.setenv("MY_ENV_VAR", "TYUIO")

        def f():
            import os

            return os.environ["MY_ENV_VAR"]

        with pytest.raises(AirflowException):
            self.run_as_task(f, inherit_env=False)

    def test_environment_variables_overriding(self, monkeypatch):
        monkeypatch.setenv("MY_ENV_VAR", "ABCDE")

        def f():
            import os

            return os.environ["MY_ENV_VAR"]

        with pytest.raises(
            AirflowException,
            match=r"'branch_task_ids'.*task.*",
        ):
            self.run_as_task(f, env_vars={"MY_ENV_VAR": "EFGHI"}, inherit_env=True)

    def test_with_no_caching(self):
        """
        Most of venv tests use caching to speed up the tests. This test ensures that
        we have test without caching as well.

        :return:
        """

        def f():
            return False

        with pytest.raises(
            AirflowException, match=r"Invalid tasks found: {\(False, 'bool'\)}.|'branch_task_ids'.*task.*"
        ):
            self.run_as_task(f, do_not_use_caching=True)

    def test_with_dag_run(self):
        clear_db_runs()
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> [self.branch_1, self.branch_2]

        dr = self.dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as dts:
                self.dag_maker.run_ti(self.task_id, dr)
            assert dts.value.tasks == [("branch_2", -1)]
        else:
            self.dag_maker.run_ti(self.task_id, dr)
            self.assert_expected_task_states(
                dr, {self.task_id: State.SUCCESS, "branch_1": State.NONE, "branch_2": State.SKIPPED}
            )

    def test_with_skip_in_branch_downstream_dependencies(self):
        clear_db_runs()
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> self.branch_1 >> self.branch_2
            branch_op >> self.branch_2

        dr = self.dag_maker.create_dagrun()
        self.dag_maker.run_ti(branch_op.task_id, dr)
        self.assert_expected_task_states(
            dr, {self.task_id: State.SUCCESS, "branch_1": State.NONE, "branch_2": State.NONE}
        )

    def test_with_skip_in_branch_downstream_dependencies2(self):
        clear_db_runs()
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=True):

            def f():
                return "branch_2"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branch_op >> self.branch_1 >> self.branch_2
            branch_op >> self.branch_2

        dr = self.dag_maker.create_dagrun()

        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as dts:
                self.dag_maker.run_ti(branch_op.task_id, dr)

            assert dts.value.tasks == [("branch_1", -1)]
        else:
            self.dag_maker.run_ti(branch_op.task_id, dr)
            self.assert_expected_task_states(
                dr, {self.task_id: State.SUCCESS, "branch_1": State.SKIPPED, "branch_2": State.NONE}
            )

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by BranchPythonOperator, clearing the skipped task
        should not cause it to be executed.
        """
        with self.dag_maker(self.dag_id, template_searchpath=TEMPLATE_SEARCHPATH, serialized=False):

            def f():
                return "branch_1"

            branch_op = self.opcls(task_id=self.task_id, python_callable=f, **self.default_kwargs())
            branches = [self.branch_1, self.branch_2]
            branch_op >> branches

        dr = self.dag_maker.create_dagrun()

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with create_session() as session:
                branch_ti = dr.get_task_instance(task_id=self.task_id, session=session)
                with pytest.raises(DownstreamTasksSkipped) as exc_info:
                    branch_ti.run()
                assert exc_info.value.tasks == [("branch_2", -1)]
                branch_ti.set_state(TaskInstanceState.SUCCESS, session=session)
                dr.task_instance_scheduling_decisions(session=session)
                branch_2_ti = dr.get_task_instance(task_id="branch_2", session=session)
                # FIXME
                if self.opcls != BranchExternalPythonOperator:
                    branch_2_ti.task = self.branch_2
                    assert branch_2_ti.state == TaskInstanceState.SKIPPED
                    branch_2_ti.set_state(None, session=session)
                    branch_2_ti.run()
                    assert branch_2_ti.state == TaskInstanceState.SKIPPED
        else:
            self.dag_maker.run_ti(branch_op.task_id, dr)
            for task in branches:
                self.dag_maker.run_ti(task.task_id, dr)

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
                if AIRFLOW_V_3_0_PLUS:
                    clear_task_instances(children_tis, session=session)
                else:
                    clear_task_instances(children_tis, session=session, dag=branch_op.dag)

            # Run the cleared tasks again.
            for task in branches:
                self.dag_maker.run_ti(task.task_id, dr)

            # Check if the states are correct after children tasks are cleared.
            self.assert_expected_task_states(dr, expected_states)

    def test_raise_exception_on_no_accepted_type_return(self):
        def f():
            return 5

        ti = self.create_ti(f)
        with pytest.raises(
            AirflowException,
            match=r"'branch_task_ids'.*task.*",
        ):
            ti.run()

    def test_raise_exception_on_invalid_task_id(self):
        def f():
            return "some_task_id"

        ti = self.create_ti(f)
        with pytest.raises(
            AirflowException, match=r"Invalid tasks found: {\(False, 'bool'\)}.|'branch_task_ids'.*task.*"
        ):
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
        if AIRFLOW_V_3_0_PLUS:
            with pytest.warns(AirflowProviderDeprecationWarning):
                with pytest.raises(RuntimeError):
                    get_current_context()
        else:
            with pytest.raises(RuntimeError):
                get_current_context()

    def test_current_context_roundtrip(self):
        example_context = {"Hello": "World"}
        with set_current_context(example_context):
            if AIRFLOW_V_3_0_PLUS:
                with pytest.warns(AirflowProviderDeprecationWarning):
                    assert get_current_context() == example_context
            else:
                assert get_current_context() == example_context

    def test_context_removed_after_exit(self):
        example_context = {"Hello": "World"}

        with set_current_context(example_context):
            pass
        if AIRFLOW_V_3_0_PLUS:
            with pytest.raises(RuntimeError):
                with pytest.warns(AirflowProviderDeprecationWarning):
                    get_current_context()
        else:
            with pytest.raises(RuntimeError):
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
        if AIRFLOW_V_3_0_PLUS:
            for i in reversed(range(max_stack_depth)):
                # Iterate over contexts in reverse order - stack is LIFO
                with pytest.warns(AirflowProviderDeprecationWarning):
                    ctx = get_current_context()
                assert ctx["ContextId"] == i
                # End of with statement
                ctx_list[i].__exit__(None, None, None)
        else:
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
        if AIRFLOW_V_3_0_PLUS:
            assert context == current_context
        else:
            from airflow.utils.context import AirflowContextDeprecationWarning

            warnings.simplefilter("ignore", AirflowContextDeprecationWarning)
            assert current_context._context


@pytest.fixture
def clear_db():
    clear_db_runs()
    yield
    clear_db_runs()


@pytest.mark.usefixtures("clear_db")
class TestCurrentContextRuntime:
    def test_context_in_task(self, dag_maker):
        with dag_maker(dag_id="assert_context_dag", serialized=True):
            op = MyContextAssertOperator(task_id="assert_context")
        dr = dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_PLUS:
            with pytest.warns(AirflowProviderDeprecationWarning):
                dag_maker.run_ti(op.task_id, dr)
        else:
            dag_maker.run_ti(op.task_id, dr)

    def test_get_context_in_old_style_context_task(self, dag_maker):
        with dag_maker(dag_id="assert_context_dag", serialized=True):
            op = PythonOperator(python_callable=get_all_the_context, task_id="get_all_the_context")
        dr = dag_maker.create_dagrun()
        if AIRFLOW_V_3_0_PLUS:
            with pytest.warns(AirflowProviderDeprecationWarning):
                dag_maker.run_ti(op.task_id, dr)
        else:
            dag_maker.run_ti(op.task_id, dr)


@pytest.mark.need_serialized_dag(False)
class TestShortCircuitWithTeardown:
    @pytest.mark.parametrize(
        ("ignore_downstream_trigger_rules", "with_teardown", "should_skip", "expected"),
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
        with dag_maker(serialized=True):
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
        if should_skip:
            # we can't use assert_called_with because it's a set and therefore not ordered
            actual_skipped = set(x.task_id for x in op1.skip.call_args.kwargs["tasks"])
            assert actual_skipped == set(expected)
        else:
            op1.skip.assert_not_called()

    @pytest.mark.parametrize("config", ["sequence", "parallel"])
    def test_short_circuit_with_teardowns_complicated(self, dag_maker, config):
        with dag_maker(serialized=True):
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
        with dag_maker(serialized=True):
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
        assert actual_skipped == {op3}

    @pytest.mark.parametrize("level", [logging.DEBUG, logging.INFO])
    def test_short_circuit_with_teardowns_debug_level(self, dag_maker, level, clear_db, caplog):
        """
        When logging is debug we convert to a list to log the tasks skipped
        before passing them to the skip method.
        """
        with dag_maker(serialized=True):
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

        with caplog.at_level(level):
            if hasattr(ti.task.log, "setLevel"):
                # Compat with Pre Airflow 3.1
                ti.task.log.setLevel(level)
            ti._run_raw_task()
        # we can't use assert_called_with because it's a set and therefore not ordered
        actual_kwargs = op1.skip.call_args.kwargs
        actual_skipped = actual_kwargs["tasks"]
        if level <= logging.DEBUG:
            assert isinstance(actual_skipped, list)
        else:
            assert isinstance(actual_skipped, Generator)
        assert set(actual_skipped) == {op3}


@pytest.mark.parametrize(
    ("text_input", "expected_tuple"),
    [
        pytest.param("   2.7.18.final.0  ", (2, 7, 18, "final", 0), id="py27"),
        pytest.param("3.10.13.final.0\n", (3, 10, 13, "final", 0), id="py310"),
        pytest.param("\n3.13.0.alpha.3", (3, 13, 0, "alpha", 3), id="py313-alpha"),
    ],
)
def test_parse_version_info(text_input, expected_tuple):
    assert _parse_version_info(text_input) == expected_tuple


@pytest.mark.parametrize(
    "text_input",
    [
        pytest.param("   2.7.18.final.0.3  ", id="more-than-5-parts"),
        pytest.param("3.10.13\n", id="less-than-5-parts"),
        pytest.param("Apache Airflow 3.0.0", id="garbage-input"),
    ],
)
def test_parse_version_invalid_parts(text_input):
    with pytest.raises(ValueError, match=r"expected 5 components separated by '\.'"):
        _parse_version_info(text_input)


@pytest.mark.parametrize(
    "text_input",
    [
        pytest.param("2EOL.7.18.final.0", id="major-non-int"),
        pytest.param("3.XXX.13.final.3", id="minor-non-int"),
        pytest.param("3.13.0a.alpha.3", id="micro-non-int"),
        pytest.param("3.8.18.alpha.beta", id="serial-non-int"),
    ],
)
def test_parse_version_invalid_parts_types(text_input):
    with pytest.raises(ValueError, match="Unable to convert parts.*parsed from.*to"):
        _parse_version_info(text_input)


def test_python_version_info_fail_subprocess(mocker):
    mocked_subprocess = mocker.patch("subprocess.check_output")
    mocked_subprocess.side_effect = RuntimeError("some error")

    with pytest.raises(ValueError, match="Error while executing command.*some error"):
        _PythonVersionInfo.from_executable("/dev/null")
    mocked_subprocess.assert_called_once()


def test_python_version_info(mocker):
    result = _PythonVersionInfo.from_executable(sys.executable)
    assert result.major == sys.version_info.major
    assert result.minor == sys.version_info.minor
    assert result.micro == sys.version_info.micro
    assert result.releaselevel == sys.version_info.releaselevel
    assert result.serial == sys.version_info.serial
    assert list(result) == list(sys.version_info)
