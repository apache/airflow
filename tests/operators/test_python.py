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
import datetime
import logging
import sys
import unittest
import unittest.mock
from collections import namedtuple
from datetime import date, timedelta
from subprocess import CalledProcessError
from typing import List

import funcsigs

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.models.taskinstance import clear_task_instances
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import (
    BranchPythonOperator, PythonOperator, PythonVirtualenvOperator, ShortCircuitOperator,
)
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = ['AIRFLOW_CTX_DAG_ID',
                       'AIRFLOW_CTX_TASK_ID',
                       'AIRFLOW_CTX_EXECUTION_DATE',
                       'AIRFLOW_CTX_DAG_RUN_ID']


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
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE})
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
        self.assertIsInstance(first, Call)
        self.assertIsInstance(second, Call)
        self.assertTupleEqual(first.args, second.args)
        # eliminate context (conf, dag_run, task_instance, etc.)
        test_args = ["an_int", "a_date", "a_templated_string"]
        first.kwargs = {
            key: value
            for (key, value) in first.kwargs.items()
            if key in test_args
        }
        second.kwargs = {
            key: value
            for (key, value) in second.kwargs.items()
            if key in test_args
        }
        self.assertDictEqual(first.kwargs, second.kwargs)


class TestPythonOperator(TestPythonBase):

    def do_run(self):
        self.run = True

    def is_run(self):
        return self.run

    def test_python_operator_run(self):
        """Tests that the python callable is invoked on task run."""
        task = PythonOperator(
            python_callable=self.do_run,
            task_id='python_operator',
            dag=self.dag)
        self.assertFalse(self.is_run())
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.assertTrue(self.is_run())

    def test_python_operator_python_callable_is_callable(self):
        """Tests that PythonOperator will only instantiate if
        the python_callable argument is callable."""
        not_callable = {}
        with self.assertRaises(AirflowException):
            PythonOperator(
                python_callable=not_callable,
                task_id='python_operator',
                dag=self.dag)
        not_callable = None
        with self.assertRaises(AirflowException):
            PythonOperator(
                python_callable=not_callable,
                task_id='python_operator',
                dag=self.dag)

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
            op_args=[
                4,
                date(2019, 1, 1),
                "dag {{dag.dag_id}} ran on {{ds}}.",
                named_tuple
            ],
            dag=self.dag)

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ds_templated = DEFAULT_DATE.date().isoformat()
        self.assertEqual(1, len(recorded_calls))
        self._assert_calls_equal(
            recorded_calls[0],
            Call(4,
                 date(2019, 1, 1),
                 "dag {} ran on {}.".format(self.dag.dag_id, ds_templated),
                 Named(ds_templated, 'unchanged'))
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
                'a_templated_string': "dag {{dag.dag_id}} ran on {{ds}}."
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self._assert_calls_equal(
            recorded_calls[0],
            Call(an_int=4,
                 a_date=date(2019, 1, 1),
                 a_templated_string="dag {} ran on {}.".format(
                     self.dag.dag_id, DEFAULT_DATE.date().isoformat()))
        )

    def test_python_operator_shallow_copy_attr(self):
        not_callable = lambda x: x
        original_task = PythonOperator(
            python_callable=not_callable,
            task_id='python_operator',
            op_kwargs={'certain_attrs': ''},
            dag=self.dag
        )
        new_task = copy.deepcopy(original_task)
        # shallow copy op_kwargs
        self.assertEqual(id(original_task.op_kwargs['certain_attrs']),
                         id(new_task.op_kwargs['certain_attrs']))
        # shallow copy python_callable
        self.assertEqual(id(original_task.python_callable),
                         id(new_task.python_callable))

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
            raise RuntimeError("Should not be triggered, dag: {}".format(dag))

        python_operator = PythonOperator(
            task_id='python_operator',
            op_args=[1],
            python_callable=func,
            dag=self.dag
        )

        with self.assertRaises(ValueError) as context:
            python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            self.assertTrue('dag' in context.exception, "'dag' not found in the exception")

    def test_context_with_conflicting_op_args(self):
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def func(custom, dag):
            self.assertEqual(1, custom, "custom should be 1")
            self.assertIsNotNone(dag, "dag should be set")

        python_operator = PythonOperator(
            task_id='python_operator',
            op_kwargs={'custom': 1},
            python_callable=func,
            dag=self.dag
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
            self.assertGreater(len(context), 0, "Context has not been injected")

        python_operator = PythonOperator(
            task_id='python_operator',
            op_kwargs={'custom': 1},
            python_callable=func,
            dag=self.dag
        )
        python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)


class TestBranchOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        self.dag = DAG('branch_operator_test',
                       default_args={
                           'owner': 'airflow',
                           'start_date': DEFAULT_DATE},
                       schedule_interval=INTERVAL)

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
        branch_op = BranchPythonOperator(task_id='make_choice',
                                         dag=self.dag,
                                         python_callable=lambda: 'branch_1')
        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(
                TI.dag_id == self.dag.dag_id,
                TI.execution_date == DEFAULT_DATE
            )

            for ti in tis:
                if ti.task_id == 'make_choice':
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == 'branch_1':
                    # should exist with state None
                    self.assertEqual(ti.state, State.NONE)
                elif ti.task_id == 'branch_2':
                    self.assertEqual(ti.state, State.SKIPPED)
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_branch_list_without_dag_run(self):
        """This checks if the BranchPythonOperator supports branching off to a list of tasks."""
        branch_op = BranchPythonOperator(task_id='make_choice',
                                         dag=self.dag,
                                         python_callable=lambda: ['branch_1', 'branch_2'])
        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.branch_3 = DummyOperator(task_id='branch_3', dag=self.dag)
        self.branch_3.set_upstream(branch_op)
        self.dag.clear()

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(
                TI.dag_id == self.dag.dag_id,
                TI.execution_date == DEFAULT_DATE
            )

            expected = {
                "make_choice": State.SUCCESS,
                "branch_1": State.NONE,
                "branch_2": State.NONE,
                "branch_3": State.SKIPPED,
            }

            for ti in tis:
                if ti.task_id in expected:
                    self.assertEqual(ti.state, expected[ti.task_id])
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_with_dag_run(self):
        branch_op = BranchPythonOperator(task_id='make_choice',
                                         dag=self.dag,
                                         python_callable=lambda: 'branch_1')

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_with_skip_in_branch_downstream_dependencies(self):
        branch_op = BranchPythonOperator(task_id='make_choice',
                                         dag=self.dag,
                                         python_callable=lambda: 'branch_1')

        branch_op >> self.branch_1 >> self.branch_2
        branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_with_skip_in_branch_downstream_dependencies2(self):
        branch_op = BranchPythonOperator(task_id='make_choice',
                                         dag=self.dag,
                                         python_callable=lambda: 'branch_2')

        branch_op >> self.branch_1 >> self.branch_2
        branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.SKIPPED)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_xcom_push(self):
        branch_op = BranchPythonOperator(task_id='make_choice',
                                         dag=self.dag,
                                         python_callable=lambda: 'branch_1')

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(
                    ti.xcom_pull(task_ids='make_choice'), 'branch_1')

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by BranchPythonOperator, clearing the skipped task
        should not cause it to be executed.
        """
        branch_op = BranchPythonOperator(task_id='make_choice',
                                         dag=self.dag,
                                         python_callable=lambda: 'branch_1')
        branches = [self.branch_1, self.branch_2]
        branch_op >> branches
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        for task in branches:
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
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
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
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
        dag = DAG('shortcircuit_operator_test_without_dag_run',
                  default_args={
                      'owner': 'airflow',
                      'start_date': DEFAULT_DATE
                  },
                  schedule_interval=INTERVAL)
        short_op = ShortCircuitOperator(task_id='make_choice',
                                        dag=dag,
                                        python_callable=lambda: value)
        branch_1 = DummyOperator(task_id='branch_1', dag=dag)
        branch_1.set_upstream(short_op)
        branch_2 = DummyOperator(task_id='branch_2', dag=dag)
        branch_2.set_upstream(branch_1)
        upstream = DummyOperator(task_id='upstream', dag=dag)
        upstream.set_downstream(short_op)
        dag.clear()

        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(
                TI.dag_id == dag.dag_id,
                TI.execution_date == DEFAULT_DATE
            )

            for ti in tis:
                if ti.task_id == 'make_choice':
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == 'upstream':
                    # should not exist
                    raise ValueError(f'Invalid task id {ti.task_id} found!')
                elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                    self.assertEqual(ti.state, State.SKIPPED)
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

            value = True
            dag.clear()

            short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            for ti in tis:
                if ti.task_id == 'make_choice':
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == 'upstream':
                    # should not exist
                    raise ValueError(f'Invalid task id {ti.task_id} found!')
                elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                    self.assertEqual(ti.state, State.NONE)
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_with_dag_run(self):
        value = False
        dag = DAG('shortcircuit_operator_test_with_dag_run',
                  default_args={
                      'owner': 'airflow',
                      'start_date': DEFAULT_DATE
                  },
                  schedule_interval=INTERVAL)
        short_op = ShortCircuitOperator(task_id='make_choice',
                                        dag=dag,
                                        python_callable=lambda: value)
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
            state=State.RUNNING
        )

        upstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 4)
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

        value = True
        dag.clear()
        dr.verify_integrity()
        upstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 4)
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    def test_clear_skipped_downstream_task(self):
        """
        After a downstream task is skipped by ShortCircuitOperator, clearing the skipped task
        should not cause it to be executed.
        """
        dag = DAG('shortcircuit_clear_skipped_downstream_task',
                  default_args={
                      'owner': 'airflow',
                      'start_date': DEFAULT_DATE
                  },
                  schedule_interval=INTERVAL)
        short_op = ShortCircuitOperator(task_id='make_choice',
                                        dag=dag,
                                        python_callable=lambda: False)
        downstream = DummyOperator(task_id='downstream', dag=dag)

        short_op >> downstream

        dag.clear()

        dr = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        downstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()

        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'downstream':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

        # Clear downstream
        with create_session() as session:
            clear_task_instances([t for t in tis if t.task_id == "downstream"],
                                 session=session,
                                 dag=dag)

        # Run downstream again
        downstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        # Check if the states are correct.
        for ti in dr.get_task_instances():
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'downstream':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')


virtualenv_string_args: List[str] = []


class TestPythonVirtualenvOperator(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL)
        self.addCleanup(self.dag.clear)

    def _run_as_operator(self, fn, python_version=sys.version_info[0], **kwargs):
        task = PythonVirtualenvOperator(
            python_callable=fn,
            python_version=python_version,
            task_id='task',
            dag=self.dag,
            **kwargs)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_dill_warning(self):
        def f():
            pass
        with self.assertRaises(AirflowException):
            PythonVirtualenvOperator(
                python_callable=f,
                task_id='task',
                dag=self.dag,
                use_dill=True,
                system_site_packages=False)

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
        self.assertNotEqual(
            '0.4', funcsigs.__version__, 'Please update this string if this fails')

        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported
            if funcsigs.__version__ != '0.4':
                raise Exception

        self._run_as_operator(f, requirements=['funcsigs==0.4'])

    def test_unpinned_requirements(self):
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import
        self._run_as_operator(
            f, requirements=['funcsigs', 'dill'], system_site_packages=False)

    def test_range_requirements(self):
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import
        self._run_as_operator(
            f, requirements=['funcsigs>1.0', 'dill'], system_site_packages=False)

    def test_fail(self):
        def f():
            raise Exception
        with self.assertRaises(CalledProcessError):
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

        with self.assertRaises(AirflowException):
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
        self._run_as_operator(
            f, python_version=self._invert_python_major_version(), string_args=[1, 2, 1])

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
        with self.assertRaises(AirflowException):
            PythonVirtualenvOperator(
                python_callable=lambda x: 4,
                task_id='task',
                dag=self.dag)

    def test_nonimported_as_arg(self):
        def f(_):
            return None
        self._run_as_operator(f, op_args=[datetime.datetime.utcnow()])

    def test_context(self):
        def f(templates_dict):
            return templates_dict['ds']
        self._run_as_operator(f, templates_dict={'ds': '{{ ds }}'})
