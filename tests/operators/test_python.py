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
import copy
import logging
import sys
import unittest.mock
from collections import namedtuple
from datetime import date, datetime, timedelta
from subprocess import CalledProcessError
from typing import Dict, List, Tuple

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import clear_task_instances, set_current_context
from airflow.models.xcom_arg import XComArg
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    ShortCircuitOperator,
    get_current_context,
    task as task_decorator,
)
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = [
    'AIRFLOW_CTX_DAG_ID',
    'AIRFLOW_CTX_TASK_ID',
    'AIRFLOW_CTX_EXECUTION_DATE',
    'AIRFLOW_CTX_DAG_RUN_ID',
]


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
        self.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
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

    def _assert_calls_equal(self, first, second):
        assert isinstance(first, Call)
        assert isinstance(second, Call)
        assert first.args == second.args
        # eliminate context (conf, dag_run, task_instance, etc.)
        test_args = ["an_int", "a_date", "a_templated_string"]
        first.kwargs = {key: value for (key, value) in first.kwargs.items() if key in test_args}
        second.kwargs = {key: value for (key, value) in second.kwargs.items() if key in test_args}
        assert first.kwargs == second.kwargs


class TestPythonOperator(TestPythonBase):
    def do_run(self):
        self.run = True

    def is_run(self):
        return self.run

    def test_python_operator_run(self):
        """Tests that the python callable is invoked on task run."""
        task = PythonOperator(python_callable=self.do_run, task_id='python_operator', dag=self.dag)
        assert not self.is_run()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert self.is_run()

    def test_python_operator_python_callable_is_callable(self):
        """Tests that PythonOperator will only instantiate if
        the python_callable argument is callable."""
        not_callable = {}
        with pytest.raises(AirflowException):
            PythonOperator(python_callable=not_callable, task_id='python_operator', dag=self.dag)
        not_callable = None
        with pytest.raises(AirflowException):
            PythonOperator(python_callable=not_callable, task_id='python_operator', dag=self.dag)

    def test_python_callable_arguments_are_templatized(self):
        """Test PythonOperator op_args are templatized"""
        recorded_calls = []

        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple('Named', ['var1', 'var2'])
        named_tuple = Named('{{ ds }}', 'unchanged')

        task = PythonOperator(
            task_id='python_operator',
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            python_callable=build_recording_function(recorded_calls),
            op_args=[4, date(2019, 1, 1), "dag {{dag.dag_id}} ran on {{ds}}.", named_tuple],
            dag=self.dag,
        )

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ds_templated = DEFAULT_DATE.date().isoformat()
        assert 1 == len(recorded_calls)
        self._assert_calls_equal(
            recorded_calls[0],
            Call(
                4,
                date(2019, 1, 1),
                f"dag {self.dag.dag_id} ran on {ds_templated}.",
                Named(ds_templated, 'unchanged'),
            ),
        )

    def test_python_callable_keyword_arguments_are_templatized(self):
        """Test PythonOperator op_kwargs are templatized"""
        recorded_calls = []

        task = PythonOperator(
            task_id='python_operator',
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            python_callable=build_recording_function(recorded_calls),
            op_kwargs={
                'an_int': 4,
                'a_date': date(2019, 1, 1),
                'a_templated_string': "dag {{dag.dag_id}} ran on {{ds}}.",
            },
            dag=self.dag,
        )

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        assert 1 == len(recorded_calls)
        self._assert_calls_equal(
            recorded_calls[0],
            Call(
                an_int=4,
                a_date=date(2019, 1, 1),
                a_templated_string="dag {} ran on {}.".format(
                    self.dag.dag_id, DEFAULT_DATE.date().isoformat()
                ),
            ),
        )

    def test_python_operator_shallow_copy_attr(self):
        not_callable = lambda x: x
        original_task = PythonOperator(
            python_callable=not_callable,
            task_id='python_operator',
            op_kwargs={'certain_attrs': ''},
            dag=self.dag,
        )
        new_task = copy.deepcopy(original_task)
        # shallow copy op_kwargs
        assert id(original_task.op_kwargs['certain_attrs']) == id(new_task.op_kwargs['certain_attrs'])
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
            task_id='python_operator', op_args=[1], python_callable=func, dag=self.dag
        )

        with pytest.raises(ValueError) as ctx:
            python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert 'dag' in str(ctx.value), "'dag' not found in the exception"

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
            task_id='python_operator',
            op_kwargs={'custom': 1},
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
            task_id='python_operator', op_kwargs={'custom': 1}, python_callable=func, dag=self.dag
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
            task_id='python_operator', op_kwargs={'custom': 1}, python_callable=func, dag=self.dag
        )
        python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)


class TestAirflowTaskDecorator(TestPythonBase):
    def test_python_operator_python_callable_is_callable(self):
        """Tests that @task will only instantiate if
        the python_callable argument is callable."""
        not_callable = {}
        with pytest.raises(AirflowException):
            task_decorator(not_callable, dag=self.dag)

    def test_infer_multiple_outputs_using_typing(self):
        @task_decorator
        def identity_dict(x: int, y: int) -> Dict[str, int]:
            return {"x": x, "y": y}

        assert identity_dict(5, 5).operator.multiple_outputs is True  # pylint: disable=maybe-no-member

        @task_decorator
        def identity_tuple(x: int, y: int) -> Tuple[int, int]:
            return x, y

        assert identity_tuple(5, 5).operator.multiple_outputs is False  # pylint: disable=maybe-no-member

        @task_decorator
        def identity_int(x: int) -> int:
            return x

        assert identity_int(5).operator.multiple_outputs is False  # pylint: disable=maybe-no-member

        @task_decorator
        def identity_notyping(x: int):
            return x

        assert identity_notyping(5).operator.multiple_outputs is False  # pylint: disable=maybe-no-member

    def test_manual_multiple_outputs_false_with_typings(self):
        @task_decorator(multiple_outputs=False)
        def identity2(x: int, y: int) -> Dict[int, int]:
            return (x, y)

        with self.dag:
            res = identity2(8, 4)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        res.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ti = dr.get_task_instances()[0]

        assert res.operator.multiple_outputs is False  # pylint: disable=maybe-no-member
        assert ti.xcom_pull() == [8, 4]  # pylint: disable=maybe-no-member
        assert ti.xcom_pull(key="return_value_0") is None
        assert ti.xcom_pull(key="return_value_1") is None

    def test_multiple_outputs_ignore_typing(self):
        @task_decorator
        def identity_tuple(x: int, y: int) -> Tuple[int, int]:
            return x, y

        with self.dag:
            ident = identity_tuple(35, 36)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        ident.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ti = dr.get_task_instances()[0]

        assert not ident.operator.multiple_outputs  # pylint: disable=maybe-no-member
        assert ti.xcom_pull() == [35, 36]
        assert ti.xcom_pull(key="return_value_0") is None
        assert ti.xcom_pull(key="return_value_1") is None

    def test_fails_bad_signature(self):
        """Tests that @task will fail if signature is not binding."""

        @task_decorator
        def add_number(num: int) -> int:
            return num + 2

        with pytest.raises(TypeError):
            add_number(2, 3)  # pylint: disable=too-many-function-args
        with pytest.raises(TypeError):
            add_number()  # pylint: disable=no-value-for-parameter
        add_number('test')  # pylint: disable=no-value-for-parameter

    def test_fail_method(self):
        """Tests that @task will fail if signature is not binding."""

        with pytest.raises(AirflowException):

            class Test:
                num = 2

                @task_decorator
                def add_number(self, num: int) -> int:
                    return self.num + num

            Test().add_number(2)

    def test_fail_multiple_outputs_key_type(self):
        @task_decorator(multiple_outputs=True)
        def add_number(num: int):
            return {2: num}

        with self.dag:
            ret = add_number(2)
        self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        with pytest.raises(AirflowException):
            # pylint: disable=maybe-no-member
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_fail_multiple_outputs_no_dict(self):
        @task_decorator(multiple_outputs=True)
        def add_number(num: int):
            return num

        with self.dag:
            ret = add_number(2)
        self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        with pytest.raises(AirflowException):
            # pylint: disable=maybe-no-member
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_python_callable_arguments_are_templatized(self):
        """Test @task op_args are templatized"""
        recorded_calls = []

        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple('Named', ['var1', 'var2'])
        named_tuple = Named('{{ ds }}', 'unchanged')

        task = task_decorator(
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            build_recording_function(recorded_calls),
            dag=self.dag,
        )
        ret = task(4, date(2019, 1, 1), "dag {{dag.dag_id}} ran on {{ds}}.", named_tuple)

        self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ds_templated = DEFAULT_DATE.date().isoformat()
        assert len(recorded_calls) == 1
        self._assert_calls_equal(
            recorded_calls[0],
            Call(
                4,
                date(2019, 1, 1),
                f"dag {self.dag.dag_id} ran on {ds_templated}.",
                Named(ds_templated, 'unchanged'),
            ),
        )

    def test_python_callable_keyword_arguments_are_templatized(self):
        """Test PythonOperator op_kwargs are templatized"""
        recorded_calls = []

        task = task_decorator(
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            build_recording_function(recorded_calls),
            dag=self.dag,
        )
        ret = task(an_int=4, a_date=date(2019, 1, 1), a_templated_string="dag {{dag.dag_id}} ran on {{ds}}.")
        self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        assert len(recorded_calls) == 1
        self._assert_calls_equal(
            recorded_calls[0],
            Call(
                an_int=4,
                a_date=date(2019, 1, 1),
                a_templated_string="dag {} ran on {}.".format(
                    self.dag.dag_id, DEFAULT_DATE.date().isoformat()
                ),
            ),
        )

    def test_manual_task_id(self):
        """Test manually setting task_id"""

        @task_decorator(task_id='some_name')
        def do_run():
            return 4

        with self.dag:
            do_run()
            assert ['some_name'] == self.dag.task_ids

    def test_multiple_calls(self):
        """Test calling task multiple times in a DAG"""

        @task_decorator
        def do_run():
            return 4

        with self.dag:
            do_run()
            assert ['do_run'] == self.dag.task_ids
            do_run_1 = do_run()
            do_run_2 = do_run()
            assert ['do_run', 'do_run__1', 'do_run__2'] == self.dag.task_ids

        assert do_run_1.operator.task_id == 'do_run__1'  # pylint: disable=maybe-no-member
        assert do_run_2.operator.task_id == 'do_run__2'  # pylint: disable=maybe-no-member

    def test_multiple_calls_in_task_group(self):
        """Test calling task multiple times in a TaskGroup"""

        @task_decorator
        def do_run():
            return 4

        group_id = "KnightsOfNii"
        with self.dag:
            with TaskGroup(group_id=group_id):
                do_run()
                assert [f"{group_id}.do_run"] == self.dag.task_ids
                do_run()
                assert [f"{group_id}.do_run", f"{group_id}.do_run__1"] == self.dag.task_ids

        assert len(self.dag.task_ids) == 2

    def test_call_20(self):
        """Test calling decorated function 21 times in a DAG"""

        @task_decorator
        def __do_run():
            return 4

        with self.dag:
            __do_run()
            for _ in range(20):
                __do_run()

        assert self.dag.task_ids[-1] == '__do_run__20'

    def test_multiple_outputs(self):
        """Tests pushing multiple outputs as a dictionary"""

        @task_decorator(multiple_outputs=True)
        def return_dict(number: int):
            return {'number': number + 1, '43': 43}

        test_number = 10
        with self.dag:
            ret = return_dict(test_number)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull(key='number') == test_number + 1
        assert ti.xcom_pull(key='43') == 43
        assert ti.xcom_pull() == {'number': test_number + 1, '43': 43}

    def test_default_args(self):
        """Test that default_args are captured when calling the function correctly"""

        @task_decorator
        def do_run():
            return 4

        with self.dag:
            ret = do_run()
        assert ret.operator.owner == 'airflow'  # pylint: disable=maybe-no-member

    def test_xcom_arg(self):
        """Tests that returned key in XComArg is returned correctly"""

        @task_decorator
        def add_2(number: int):
            return number + 2

        @task_decorator
        def add_num(number: int, num2: int = 2):
            return number + num2

        test_number = 10

        with self.dag:
            bigger_number = add_2(test_number)
            ret = add_num(bigger_number, XComArg(bigger_number.operator))  # pylint: disable=maybe-no-member

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        bigger_number.operator.run(  # pylint: disable=maybe-no-member
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE
        )
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member
        ti_add_num = [ti for ti in dr.get_task_instances() if ti.task_id == 'add_num'][0]
        assert ti_add_num.xcom_pull(key=ret.key) == (test_number + 2) * 2  # pylint: disable=maybe-no-member

    def test_dag_task(self):
        """Tests dag.task property to generate task"""

        @self.dag.task
        def add_2(number: int):
            return number + 2

        test_number = 10
        res = add_2(test_number)
        add_2(res)

        assert 'add_2' in self.dag.task_ids

    def test_dag_task_multiple_outputs(self):
        """Tests dag.task property to generate task with multiple outputs"""

        @self.dag.task(multiple_outputs=True)
        def add_2(number: int):
            return {'1': number + 2, '2': 42}

        test_number = 10
        add_2(test_number)
        add_2(test_number)

        assert 'add_2' in self.dag.task_ids

    def test_airflow_task(self):
        """Tests airflow.task decorator to generate task"""
        from airflow.decorators import task

        @task
        def add_2(number: int):
            return number + 2

        test_number = 10
        with self.dag:
            add_2(test_number)

        assert 'add_2' in self.dag.task_ids

    def test_task_documentation(self):
        """Tests that task_decorator loads doc_md from function doc"""

        @task_decorator
        def add_2(number: int):
            """
            Adds 2 to number.
            """
            return number + 2

        test_number = 10
        with self.dag:
            ret = add_2(test_number)

        assert ret.operator.doc_md.strip(), "Adds 2 to number."  # pylint: disable=maybe-no-member


class TestBranchOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        self.dag = DAG(
            'branch_operator_test',
            default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )

        self.branch_1 = DummyOperator(task_id='branch_1', dag=self.dag)
        self.branch_2 = DummyOperator(task_id='branch_2', dag=self.dag)
        self.branch_3 = None

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_without_dag_run(self):
        """This checks the defensive against non existent tasks in a dag run"""
        branch_op = BranchPythonOperator(
            task_id='make_choice', dag=self.dag, python_callable=lambda: 'branch_1'
        )
        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(TI.dag_id == self.dag.dag_id, TI.execution_date == DEFAULT_DATE)

            for ti in tis:
                if ti.task_id == 'make_choice':
                    assert ti.state == State.SUCCESS
                elif ti.task_id == 'branch_1':
                    # should exist with state None
                    assert ti.state == State.NONE
                elif ti.task_id == 'branch_2':
                    assert ti.state == State.SKIPPED
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_branch_list_without_dag_run(self):
        """This checks if the BranchPythonOperator supports branching off to a list of tasks."""
        branch_op = BranchPythonOperator(
            task_id='make_choice', dag=self.dag, python_callable=lambda: ['branch_1', 'branch_2']
        )
        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.branch_3 = DummyOperator(task_id='branch_3', dag=self.dag)
        self.branch_3.set_upstream(branch_op)
        self.dag.clear()

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(TI.dag_id == self.dag.dag_id, TI.execution_date == DEFAULT_DATE)

            expected = {
                "make_choice": State.SUCCESS,
                "branch_1": State.NONE,
                "branch_2": State.NONE,
                "branch_3": State.SKIPPED,
            }

            for ti in tis:
                if ti.task_id in expected:
                    assert ti.state == expected[ti.task_id]
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_with_dag_run(self):
        branch_op = BranchPythonOperator(
            task_id='make_choice', dag=self.dag, python_callable=lambda: 'branch_1'
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
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_1':
                assert ti.state == State.NONE
            elif ti.task_id == 'branch_2':
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_with_skip_in_branch_downstream_dependencies(self):
        branch_op = BranchPythonOperator(
            task_id='make_choice', dag=self.dag, python_callable=lambda: 'branch_1'
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
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_1':
                assert ti.state == State.NONE
            elif ti.task_id == 'branch_2':
                assert ti.state == State.NONE
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_with_skip_in_branch_downstream_dependencies2(self):
        branch_op = BranchPythonOperator(
            task_id='make_choice', dag=self.dag, python_callable=lambda: 'branch_2'
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
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_1':
                assert ti.state == State.SKIPPED
            elif ti.task_id == 'branch_2':
                assert ti.state == State.NONE
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_xcom_push(self):
        branch_op = BranchPythonOperator(
            task_id='make_choice', dag=self.dag, python_callable=lambda: 'branch_1'
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
            if ti.task_id == 'make_choice':
                assert ti.xcom_pull(task_ids='make_choice') == 'branch_1'

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by BranchPythonOperator, clearing the skipped task
        should not cause it to be executed.
        """
        branch_op = BranchPythonOperator(
            task_id='make_choice', dag=self.dag, python_callable=lambda: 'branch_1'
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
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_1':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_2':
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

        children_tis = [ti for ti in tis if ti.task_id in branch_op.get_direct_relative_ids()]

        # Clear the children tasks.
        with create_session() as session:
            clear_task_instances(children_tis, session=session, dag=self.dag)

        # Run the cleared tasks again.
        for task in branches:
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        # Check if the states are correct after children tasks are cleared.
        for ti in dr.get_task_instances():
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_1':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_2':
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')


class TestShortCircuitOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_without_dag_run(self):
        """This checks the defensive against non existent tasks in a dag run"""
        value = False
        dag = DAG(
            'shortcircuit_operator_test_without_dag_run',
            default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )
        short_op = ShortCircuitOperator(task_id='make_choice', dag=dag, python_callable=lambda: value)
        branch_1 = DummyOperator(task_id='branch_1', dag=dag)
        branch_1.set_upstream(short_op)
        branch_2 = DummyOperator(task_id='branch_2', dag=dag)
        branch_2.set_upstream(branch_1)
        upstream = DummyOperator(task_id='upstream', dag=dag)
        upstream.set_downstream(short_op)
        dag.clear()

        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(TI.dag_id == dag.dag_id, TI.execution_date == DEFAULT_DATE)

            for ti in tis:
                if ti.task_id == 'make_choice':
                    assert ti.state == State.SUCCESS
                elif ti.task_id == 'upstream':
                    # should not exist
                    raise ValueError(f'Invalid task id {ti.task_id} found!')
                elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                    assert ti.state == State.SKIPPED
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

            value = True
            dag.clear()

            short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            for ti in tis:
                if ti.task_id == 'make_choice':
                    assert ti.state == State.SUCCESS
                elif ti.task_id == 'upstream':
                    # should not exist
                    raise ValueError(f'Invalid task id {ti.task_id} found!')
                elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                    assert ti.state == State.NONE
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_with_dag_run(self):
        value = False
        dag = DAG(
            'shortcircuit_operator_test_with_dag_run',
            default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )
        short_op = ShortCircuitOperator(task_id='make_choice', dag=dag, python_callable=lambda: value)
        branch_1 = DummyOperator(task_id='branch_1', dag=dag)
        branch_1.set_upstream(short_op)
        branch_2 = DummyOperator(task_id='branch_2', dag=dag)
        branch_2.set_upstream(branch_1)
        upstream = DummyOperator(task_id='upstream', dag=dag)
        upstream.set_downstream(short_op)
        dag.clear()

        logging.error("Tasks %s", dag.tasks)
        dr = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        upstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        assert len(tis) == 4
        for ti in tis:
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'upstream':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

        value = True
        dag.clear()
        dr.verify_integrity()
        upstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        assert len(tis) == 4
        for ti in tis:
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'upstream':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                assert ti.state == State.NONE
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by ShortCircuitOperator, clearing the skipped task
        should not cause it to be executed.
        """
        dag = DAG(
            'shortcircuit_clear_skipped_downstream_task',
            default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )
        short_op = ShortCircuitOperator(task_id='make_choice', dag=dag, python_callable=lambda: False)
        downstream = DummyOperator(task_id='downstream', dag=dag)

        short_op >> downstream

        dag.clear()

        dr = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        downstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()

        for ti in tis:
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'downstream':
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

        # Clear downstream
        with create_session() as session:
            clear_task_instances([t for t in tis if t.task_id == "downstream"], session=session, dag=dag)

        # Run downstream again
        downstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        # Check if the states are correct.
        for ti in dr.get_task_instances():
            if ti.task_id == 'make_choice':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'downstream':
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')


virtualenv_string_args: List[str] = []


class TestPythonVirtualenvOperator(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.dag = DAG(
            'test_dag',
            default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL,
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
            python_callable=fn, python_version=python_version, task_id='task', dag=self.dag, **kwargs
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        return task

    def test_add_dill(self):
        def f():
            pass

        task = self._run_as_operator(f, use_dill=True, system_site_packages=False)
        assert 'dill' in task.requirements

    def test_no_requirements(self):
        """Tests that the python callable is invoked on task run."""

        def f():
            pass

        self._run_as_operator(f)

    def test_no_system_site_packages(self):
        def f():
            try:
                import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import
            except ImportError:
                return True
            raise Exception

        self._run_as_operator(f, system_site_packages=False, requirements=['dill'])

    def test_system_site_packages(self):
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import

        self._run_as_operator(f, requirements=['funcsigs'], system_site_packages=True)

    def test_with_requirements_pinned(self):
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported

            if funcsigs.__version__ != '0.4':
                raise Exception

        self._run_as_operator(f, requirements=['funcsigs==0.4'])

    def test_unpinned_requirements(self):
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import

        self._run_as_operator(f, requirements=['funcsigs', 'dill'], system_site_packages=False)

    def test_range_requirements(self):
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import

        self._run_as_operator(f, requirements=['funcsigs>1.0', 'dill'], system_site_packages=False)

    def test_fail(self):
        def f():
            raise Exception

        with pytest.raises(CalledProcessError):
            self._run_as_operator(f)

    def test_python_2(self):
        def f():
            {}.iteritems()  # pylint: disable=no-member

        self._run_as_operator(f, python_version=2, requirements=['dill'])

    def test_python_2_7(self):
        def f():
            {}.iteritems()  # pylint: disable=no-member
            return True

        self._run_as_operator(f, python_version='2.7', requirements=['dill'])

    def test_python_3(self):
        def f():
            import sys  # pylint: disable=reimported,unused-import,redefined-outer-name

            print(sys.version)
            try:
                {}.iteritems()  # pylint: disable=no-member
            except AttributeError:
                return
            raise Exception

        self._run_as_operator(f, python_version=3, use_dill=False, requirements=['dill'])

    @staticmethod
    def _invert_python_major_version():
        if sys.version_info[0] == 2:
            return 3
        else:
            return 2

    def test_wrong_python_op_args(self):
        if sys.version_info[0] == 2:
            version = 3
        else:
            version = 2

        def f():
            pass

        with pytest.raises(AirflowException):
            self._run_as_operator(f, python_version=version, op_args=[1])

    def test_without_dill(self):
        def f(a):
            return a

        self._run_as_operator(f, system_site_packages=False, use_dill=False, op_args=[4])

    def test_string_args(self):
        def f():
            global virtualenv_string_args  # pylint: disable=global-statement
            print(virtualenv_string_args)
            if virtualenv_string_args[0] != virtualenv_string_args[2]:
                raise Exception

        self._run_as_operator(f, python_version=self._invert_python_major_version(), string_args=[1, 2, 1])

    def test_with_args(self):
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception

        self._run_as_operator(f, op_args=[0, 1], op_kwargs={'c': True})

    def test_return_none(self):
        def f():
            return None

        self._run_as_operator(f)

    def test_lambda(self):
        with pytest.raises(AirflowException):
            PythonVirtualenvOperator(python_callable=lambda x: 4, task_id='task', dag=self.dag)

    def test_nonimported_as_arg(self):
        def f(_):
            return None

        self._run_as_operator(f, op_args=[datetime.utcnow()])

    def test_context(self):
        def f(templates_dict):
            return templates_dict['ds']

        self._run_as_operator(f, templates_dict={'ds': '{{ ds }}'})

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
        ):  # pylint: disable=unused-argument,too-many-arguments,too-many-locals
            pass

        self._run_as_operator(f, use_dill=True, system_site_packages=True, requirements=None)

    def test_pendulum_context(self):
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
            # other
            **context,
        ):  # pylint: disable=unused-argument,too-many-arguments,too-many-locals
            pass

        self._run_as_operator(
            f, use_dill=True, system_site_packages=False, requirements=['pendulum', 'lazy_object_proxy']
        )

    def test_base_context(self):
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
            # other
            **context,
        ):  # pylint: disable=unused-argument,too-many-arguments,too-many-locals
            pass

        self._run_as_operator(f, use_dill=True, system_site_packages=False, requirements=None)


DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": days_ago(1),
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
        with pytest.raises(
            AirflowException,
        ):
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
            ctx_obj.__enter__()  # pylint: disable=E1101
            ctx_list.append(ctx_obj)
        for i in reversed(range(max_stack_depth)):
            # Iterate over contexts in reverse order - stack is LIFO
            ctx = get_current_context()
            assert ctx["ContextId"] == i
            # End of with statement
            ctx_list[i].__exit__(None, None, None)


class MyContextAssertOperator(BaseOperator):
    def execute(self, context):
        assert context == get_current_context()


def get_all_the_context(**context):
    current_context = get_current_context()
    assert context == current_context


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
def test_empty_branch(choice, expected_states):
    """
    Tests that BranchPythonOperator handles empty branches properly.
    """
    with DAG(
        'test_empty_branch',
        start_date=DEFAULT_DATE,
    ) as dag:
        branch = BranchPythonOperator(task_id='branch', python_callable=lambda: choice)
        task1 = DummyOperator(task_id='task1')
        join = DummyOperator(task_id='join', trigger_rule="none_failed_or_skipped")

        branch >> [task1, join]
        task1 >> join

    dag.clear(start_date=DEFAULT_DATE)

    task_ids = ["branch", "task1", "join"]

    tis = {}
    for task_id in task_ids:
        task_instance = TI(dag.get_task(task_id), execution_date=DEFAULT_DATE)
        tis[task_id] = task_instance
        task_instance.run()

    def get_state(ti):
        ti.refresh_from_db()
        return ti.state

    assert [get_state(tis[task_id]) for task_id in task_ids] == expected_states
