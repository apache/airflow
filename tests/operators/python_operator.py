# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, unicode_literals

import datetime
import unittest

from airflow import configuration, DAG
from airflow.models import TaskInstance as TI
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.settings import Session
from airflow.utils.state import State

from airflow.exceptions import AirflowException
import logging

DEFAULT_DATE = datetime.datetime(2016, 1, 1)
END_DATE = datetime.datetime(2016, 1, 2)
INTERVAL = datetime.timedelta(hours=12)
FROZEN_NOW = datetime.datetime(2016, 1, 2, 12, 1, 1)


class PythonOperatorTest(unittest.TestCase):

    def setUp(self):
        super(PythonOperatorTest, self).setUp()
        configuration.load_test_config()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL)
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)

    def do_run(self):
        self.run = True

    def clear_run(self):
        self.run = False

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


class BranchOperatorTest(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('branch_operator_test',
                       default_args={
                           'owner': 'airflow',
                           'start_date': DEFAULT_DATE},
                       schedule_interval=INTERVAL)
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_1')

        self.branch_1 = DummyOperator(task_id='branch_1', dag=self.dag)
        self.branch_1.set_upstream(self.branch_op)
        self.branch_2 = DummyOperator(task_id='branch_2', dag=self.dag)
        self.branch_2.set_upstream(self.branch_op)
        self.dag.clear()

    def test_without_dag_run(self):
        """This checks the defensive against non existent tasks in a dag run"""
        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        session = Session()
        tis = session.query(TI).filter(
            TI.dag_id == self.dag.dag_id,
            TI.execution_date == DEFAULT_DATE
        )
        session.close()

        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEquals(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                # should not exist
                raise
            elif ti.task_id == 'branch_2':
                self.assertEquals(ti.state, State.SKIPPED)
            else:
                raise

    def test_with_dag_run(self):
        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=datetime.datetime.now(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEquals(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEquals(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEquals(ti.state, State.SKIPPED)
            else:
                raise


class ShortCircuitOperatorTest(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('shortcircuit_operator_test',
                       default_args={
                           'owner': 'airflow',
                           'start_date': DEFAULT_DATE},
                       schedule_interval=INTERVAL)
        self.short_op = ShortCircuitOperator(task_id='make_choice',
                                             dag=self.dag,
                                             python_callable=lambda: self.value)

        self.branch_1 = DummyOperator(task_id='branch_1', dag=self.dag)
        self.branch_1.set_upstream(self.short_op)
        self.branch_2 = DummyOperator(task_id='branch_2', dag=self.dag)
        self.branch_2.set_upstream(self.branch_1)
        self.upstream = DummyOperator(task_id='upstream', dag=self.dag)
        self.upstream.set_downstream(self.short_op)
        self.dag.clear()

        self.value = True

    def test_without_dag_run(self):
        """This checks the defensive against non existent tasks in a dag run"""
        self.value = False
        self.short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        session = Session()
        tis = session.query(TI).filter(
            TI.dag_id == self.dag.dag_id,
            TI.execution_date == DEFAULT_DATE
        )

        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEquals(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                # should not exist
                raise
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEquals(ti.state, State.SKIPPED)
            else:
                raise

        self.value = True
        self.dag.clear()

        self.short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEquals(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                # should not exist
                raise
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEquals(ti.state, State.NONE)
            else:
                raise

        session.close()

    def test_with_dag_run(self):
        self.value = False
        logging.error("Tasks {}".format(self.dag.tasks))
        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=datetime.datetime.now(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        self.upstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 4)
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEquals(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                self.assertEquals(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEquals(ti.state, State.SKIPPED)
            else:
                raise

        self.value = True
        self.dag.clear()
        dr.verify_integrity()
        self.upstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 4)
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEquals(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                self.assertEquals(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEquals(ti.state, State.NONE)
            else:
                raise
