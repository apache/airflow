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

from datetime import datetime
from io import StringIO
from logging import getLogger, StreamHandler
from mock import patch
from unittest import TestCase

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from tests.helpers import mock_dag_bag


DEFAULT_DATE = datetime(2015, 1, 1)
EXECUTION_DATE_OVERRIDE = datetime(2015, 2, 1, 14, 20, 5)
TEST_DAG_ID = 'unit_test_dag'
TRIGGER_DAG_ID = 'unit_test_trigger_dag'


class TriggerDagRunOperatorTestCase(TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
        }
        self.dag = DAG(TEST_DAG_ID, default_args=args)
        self.trigger_dag = DAG(TRIGGER_DAG_ID, default_args=args)
        self.dag_bag = mock_dag_bag(self.trigger_dag)

        # Set up logging intercept.
        self.root_logger = getLogger()
        self.log_stream = StringIO()
        self.root_logger.addHandler(StreamHandler(stream=self.log_stream))

        self.callback_called = False

    def test_callable_ignore_status(self):
        # Make sure python_callable is correctly called and ignore output
        # (None) is respected.
        def my_callable(context, dro):
            self.assertEqual(dro.run_id, 'trig__2015-01-01T00:00:00')
            self.assertEqual(dro.execution_date, DEFAULT_DATE)
            self.callback_called = True

        task = TriggerDagRunOperator(
            task_id='test_trigger_dag_run_ignore_status',
            trigger_dag_id=self.trigger_dag.dag_id,
            python_callable=my_callable,
            dag=self.dag,
            # No execution_date set -> should be passed from TaskInstance in
            # context.
        )
        ti = TaskInstance(task, DEFAULT_DATE)
        ti.run()

        self.assertTrue(self.callback_called)
        self.assertIn('Criteria not met, moving on',
                      self.log_stream.getvalue())

    @patch('airflow.operators.dagrun_operator.DagBag')
    def test_callable_run_status(self, mock_DagBag):
        mock_DagBag.return_value = self.dag_bag

        def my_callable(context, dro):
            self.assertEqual(dro.run_id, 'trig__2015-02-01T14:20:05')
            self.assertEqual(dro.execution_date, EXECUTION_DATE_OVERRIDE)
            self.callback_called = True
            return dro

        task = TriggerDagRunOperator(
            task_id='test_trigger_dag_run_run_status',
            trigger_dag_id=self.trigger_dag.dag_id,
            python_callable=my_callable,
            dag=self.dag,
            execution_date=EXECUTION_DATE_OVERRIDE,
        )

        # Run task.
        ti = TaskInstance(task, DEFAULT_DATE)
        ti.run()

        self.assertTrue(self.callback_called)
        self.assertIn('Creating DagRun', self.log_stream.getvalue())

    @patch('airflow.operators.dagrun_operator.DagBag')
    def test_callable_can_modify_dro(self, mock_DagBag):
        mock_DagBag.return_value = self.dag_bag
        RUN_ID = 'test_trigger_dag_run_hotswap'

        def my_callable(context, dro):
            self.assertEqual(dro.execution_date, DEFAULT_DATE)
            dro.run_id = RUN_ID
            dro.execution_date = EXECUTION_DATE_OVERRIDE
            self.callback_called = True
            return dro

        task = TriggerDagRunOperator(
            task_id='test_trigger_dag_run_modify_dro',
            trigger_dag_id=self.trigger_dag.dag_id,
            python_callable=my_callable,
            dag=self.dag,
        )

        # Run task.
        ti = TaskInstance(task, DEFAULT_DATE)
        ti.run()

        self.assertTrue(self.callback_called)
        self.assertIn('Creating DagRun', self.log_stream.getvalue())

        # Make sure we kicked off a DAG run with the right params.
        dr = DagRun.find(dag_id=self.trigger_dag.dag_id,
                         run_id=RUN_ID,
                         execution_date=EXECUTION_DATE_OVERRIDE)
        self.assertIsNotNone(dr)
