# -*- coding: utf-8 -*-
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

from __future__ import print_function, unicode_literals

import copy
import logging
import os
import pytest

import unittest

from tests.compat import mock

from collections import namedtuple
from datetime import timedelta, date

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance as TI, DAG, DagRun
from airflow.models.taskinstance import clear_task_instances
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.settings import Session
from airflow.utils import timezone
from tests.test_utils.db import clear_db_runs, clear_db_dags
from airflow.utils.db import create_session
from airflow.utils.state import State

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
        super(TestPythonBase, cls).setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        super(TestPythonBase, self).setUp()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)

    def tearDown(self):
        super(TestPythonBase, self).tearDown()

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


@mock.patch.dict('os.environ', {})
class TestPythonOperator(TestPythonBase):

    @classmethod
    def setUpClass(cls):
        super(TestPythonOperator, cls).setUpClass()

        session = Session()

        session.query(DagRun).delete()
        session.query(TI).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestPythonOperator, self).setUp()

        def del_env(key):
            try:
                del os.environ[key]
            except KeyError:
                pass

        del_env('AIRFLOW_CTX_DAG_ID')
        del_env('AIRFLOW_CTX_TASK_ID')
        del_env('AIRFLOW_CTX_EXECUTION_DATE')
        del_env('AIRFLOW_CTX_DAG_RUN_ID')
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)

    def tearDown(self):
        super(TestPythonOperator, self).tearDown()

        session = Session()

        session.query(DagRun).delete()
        session.query(TI).delete()
        print(len(session.query(DagRun).all()))
        session.commit()
        session.close()

        for var in TI_CONTEXT_ENV_VARS:
            if var in os.environ:
                del os.environ[var]

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
            python_callable=(build_recording_function(recorded_calls)),
            op_args=[
                4,
                date(2019, 1, 1),
                "dag {{dag.dag_id}} ran on {{ds}}.",
                named_tuple
            ],
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
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
            python_callable=(build_recording_function(recorded_calls)),
            op_kwargs={
                'an_int': 4,
                'a_date': date(2019, 1, 1),
                'a_templated_string': "dag {{dag.dag_id}} ran on {{ds}}."
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
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

    def _env_var_check_callback(self):
        self.assertEqual('test_dag', os.environ['AIRFLOW_CTX_DAG_ID'])
        self.assertEqual('hive_in_python_op', os.environ['AIRFLOW_CTX_TASK_ID'])
        self.assertEqual(DEFAULT_DATE.isoformat(),
                         os.environ['AIRFLOW_CTX_EXECUTION_DATE'])
        self.assertEqual('manual__' + DEFAULT_DATE.isoformat(),
                         os.environ['AIRFLOW_CTX_DAG_RUN_ID'])

    def test_echo_env_variables(self):
        """
        Test that env variables are exported correctly to the
        python callback in the task.
        """
        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        t = PythonOperator(task_id='hive_in_python_op',
                           dag=self.dag,
                           python_callable=self._env_var_check_callback
                           )
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)


class TestPythonVirtualenvOperator(TestPythonBase):
    @classmethod
    def setUpClass(cls):
        super(TestPythonVirtualenvOperator, cls).setUpClass()
        clear_db_runs()

    def setUp(self):
        super(TestPythonVirtualenvOperator, self).setUp()

        def del_env(key):
            try:
                del os.environ[key]
            except KeyError:
                pass

        del_env('AIRFLOW_CTX_DAG_ID')
        del_env('AIRFLOW_CTX_TASK_ID')
        del_env('AIRFLOW_CTX_EXECUTION_DATE')
        del_env('AIRFLOW_CTX_DAG_RUN_ID')
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)

    def tearDown(self):
        super(TestPythonVirtualenvOperator, self).tearDown()
        clear_db_runs()
        clear_db_dags()

        for var in TI_CONTEXT_ENV_VARS:
            if var in os.environ:
                del os.environ[var]

    def clear_run(self):
        self.run = False

    def do_run(self):
        self.run = True

    def is_run(self):
        return self.run

    def test_config_context(self):
        """
        This test ensures we can use dag_run from the context
        to access the configuration at run time that's being
        passed from the UI, CLI, and REST API.
        """
        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def pass_function(**kwargs):
            kwargs['dag_run'].conf

        t = PythonVirtualenvOperator(task_id='config_dag_run', dag=self.dag,
                                     provide_context=True,
                                     python_callable=pass_function)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)


class BranchOperatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(BranchOperatorTest, cls).setUpClass()

        session = Session()

        session.query(DagRun).delete()
        session.query(TI).delete()
        session.commit()
        session.close()

    def setUp(self):
        self.dag = DAG('branch_operator_test',
                       default_args={
                           'owner': 'airflow',
                           'start_date': DEFAULT_DATE},
                       schedule_interval=INTERVAL)

        self.branch_1 = DummyOperator(task_id='branch_1', dag=self.dag)
        self.branch_2 = DummyOperator(task_id='branch_2', dag=self.dag)

    def tearDown(self):
        super(BranchOperatorTest, self).tearDown()

        session = Session()

        session.query(DagRun).delete()
        session.query(TI).delete()
        print(len(session.query(DagRun).all()))
        session.commit()
        session.close()

    def test_without_dag_run(self):
        """This checks the defensive against non existent tasks in a dag run"""
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_1')
        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.dag.clear()

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        session = Session()
        tis = session.query(TI).filter(
            TI.dag_id == self.dag.dag_id,
            TI.execution_date == DEFAULT_DATE
        )
        session.close()

        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                # should exist with state None
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise

    def test_branch_list_without_dag_run(self):
        """This checks if the BranchPythonOperator supports branching off to a list of tasks."""
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: ['branch_1', 'branch_2'])
        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.branch_3 = DummyOperator(task_id='branch_3', dag=self.dag)
        self.branch_3.set_upstream(self.branch_op)
        self.dag.clear()

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        session = Session()
        tis = session.query(TI).filter(
            TI.dag_id == self.dag.dag_id,
            TI.execution_date == DEFAULT_DATE
        )
        session.close()

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
                raise

    def test_with_dag_run(self):
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_1')

        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise

    def test_with_skip_in_branch_downstream_dependencies(self):
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_1')

        self.branch_op >> self.branch_1 >> self.branch_2
        self.branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise

    def test_with_skip_in_branch_downstream_dependencies2(self):
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_2')

        self.branch_op >> self.branch_1 >> self.branch_2
        self.branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.SKIPPED)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise

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
            run_id="manual__",
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
                raise

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
                raise


class ShortCircuitOperatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(ShortCircuitOperatorTest, cls).setUpClass()

        session = Session()

        session.query(DagRun).delete()
        session.query(TI).delete()
        session.commit()
        session.close()

    def tearDown(self):
        super(ShortCircuitOperatorTest, self).tearDown()

        session = Session()

        session.query(DagRun).delete()
        session.query(TI).delete()
        session.commit()
        session.close()

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

        session = Session()
        tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.execution_date == DEFAULT_DATE
        )

        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                # should not exist
                raise
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise

        value = True
        dag.clear()

        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                # should not exist
                raise
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise

        session.close()

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

        logging.error("Tasks {}".format(dag.tasks))
        dr = dag.create_dagrun(
            run_id="manual__",
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
                raise

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
                raise

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
            run_id="manual__",
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
                raise

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
                raise


@pytest.mark.parametrize(
    "choice,expected_states",
    [
        ("task1", [State.SUCCESS, State.SUCCESS, State.SUCCESS]),
        ("join", [State.SUCCESS, State.SKIPPED, State.SUCCESS]),
    ]
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
