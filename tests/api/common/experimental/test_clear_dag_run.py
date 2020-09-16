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
from datetime import datetime
import mock
import unittest
import json

from airflow.api.common.experimental.clear_dag_run import clear_dag_run
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.state import State

from tests.models import DEFAULT_DATE

class ClearDagRunTests(unittest.TestCase):
    def setUp(self):
        self.dag_id = 'test_dag'
        self.dag_run_id = 'test_dagrun'

    def _assert_raises_with_message(self, msg, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            self.assertEqual(e.message, msg)

    def _setup_dag_and_tasks(self, dag_bag_mock, dag_run_mock, state):
        self.dag = DAG(self.dag_id)
        self.dag_run = DagRun(_state=state)
        task0 = DummyOperator(task_id='0', owner='test', dag=self.dag, start_date=DEFAULT_DATE)
        self.dag.add_task(task0)

        dag_bag_mock.return_value.dags = [self.dag_id]
        dag_bag_mock.return_value.get_dag.return_value = self.dag
        dag_run_mock.find.return_value = [self.dag_run]
        self.dag_run.get_task_instances = lambda *args, **kwargs: [TI(task0, DEFAULT_DATE)]

        session_mock = mock.Mock()
        session_query_mock = mock.Mock()
        session_query_mock.filter.return_value.all.return_value = []
        session_mock.query.side_effect = [mock.Mock(), session_query_mock]

        return session_mock

    @mock.patch('airflow.api.common.experimental.clear_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_clear_dagrun_dag_not_found(self, dag_bag_mock, dag_run_mock):
        dag_bag_mock.return_value.dags = []
        self._assert_raises_with_message(
            'Dag id {} not found'.format(self.dag_id),
            AirflowException,
            clear_dag_run,
            self.dag_id,
            self.dag_run_id
        )

    @mock.patch('airflow.api.common.experimental.clear_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_clear_dagrun_dagrun_not_found(self, dag_bag_mock, dag_run_mock):
        self._setup_dag_and_tasks(dag_bag_mock, dag_run_mock, State.RUNNING)
        dag_run_mock.find.return_value = []
        self._assert_raises_with_message(
            'No matching dagrun with run_id {}'.format(self.dag_run_id),
            AirflowException,
            clear_dag_run,
            self.dag_id,
            self.dag_run_id
        )

    @mock.patch('airflow.api.common.experimental.clear_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_clear_dagrun_dagrun_running_state(self, dag_bag_mock, dag_run_mock):
        session_mock = self._setup_dag_and_tasks(dag_bag_mock, dag_run_mock, State.RUNNING)

        result = clear_dag_run(self.dag_id, self.dag_run_id, session_mock)
        self.assertEqual(self.dag_run, result)

    @mock.patch('airflow.api.common.experimental.clear_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_clear_dagrun_dagrun_success_state(self, dag_bag_mock, dag_run_mock):
        session_mock = self._setup_dag_and_tasks(dag_bag_mock, dag_run_mock, State.SUCCESS)

        result = clear_dag_run(self.dag_id, self.dag_run_id, session_mock)
        self.assertEqual(self.dag_run, result)

    @mock.patch('airflow.api.common.experimental.clear_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_clear_dagrun_dagrun_failed_state(self, dag_bag_mock, dag_run_mock):
        session_mock = self._setup_dag_and_tasks(dag_bag_mock, dag_run_mock, State.FAILED)

        result = clear_dag_run(self.dag_id, self.dag_run_id, session_mock)
        self.assertEqual(self.dag_run, result)

    @mock.patch('airflow.api.common.experimental.clear_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_clear_dagrun_dagrun_no_tis(self, dag_bag_mock, dag_run_mock):
        session_mock = self._setup_dag_and_tasks(dag_bag_mock, dag_run_mock, State.SUCCESS)
        self.dag_run.get_task_instances = lambda *args, **kwargs: []

        self._assert_raises_with_message(
            'dagrun with run_id {} has no existing task instances to clear.'.format(
                self.dag_run_id
            ),
            AirflowException,
            clear_dag_run,
            self.dag_id,
            self.dag_run_id,
            session_mock
        )


if __name__ == '__main__':
    unittest.main()
