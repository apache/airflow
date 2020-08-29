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

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.api.common.experimental.cancel_dag_run import cancel_dag_run
from airflow.utils import timezone
from airflow.utils.state import State


class CancelDagRunTests(unittest.TestCase):

    @mock.patch('airflow.api.common.experimental.cancel_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_cancel_dagrun_dag_not_found(self, dag_bag_mock, dag_run_mock):
        dag_bag_mock.return_value.dags = []
        self.assertRaises(
            AirflowException,
            cancel_dag_run,
            'dag_not_found',
            'whatever'
        )

    @mock.patch('airflow.api.common.experimental.cancel_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_cancel_dagrun_dagrun_not_found(self, dag_bag_mock, dag_run_mock):
        dag_id = "dag_run_exist"
        dag = DAG(dag_id)
        dag_bag_mock.return_value.dags = [dag_id]
        dag_bag_mock.return_value.get_dag.return_value = dag
        dag_run_mock.find.return_value = []
        self.assertRaises(
            AirflowException,
            cancel_dag_run,
            dag_id,
            'nochance'
        )

    @mock.patch('airflow.api.common.experimental.cancel_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_cancel_dagrun_dagrun_success_state(self, dag_bag_mock, dag_run_mock):
        dag_id = "dag_run_exist"
        dag = DAG(dag_id)
        dag_bag_mock.return_value.dags = [dag_id]
        dag_bag_mock.return_value.get_dag.return_value = dag
        dag_run = DagRun(_state=State.SUCCESS)
        dag_run_mock.find.return_value = [dag_run]
        self.assertRaises(
            AirflowException,
            cancel_dag_run,
            dag_id,
            'ok'
        )

    @mock.patch('airflow.api.common.experimental.cancel_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_cancel_dagrun_dagrun_failed_state(self, dag_bag_mock, dag_run_mock):
        dag_id = "dag_run_exist"
        dag = DAG(dag_id)
        dag_bag_mock.return_value.dags = [dag_id]
        dag_bag_mock.return_value.get_dag.return_value = dag
        dag_run = DagRun(_state=State.FAILED)
        dag_run_mock.find.return_value = [dag_run]
        self.assertRaises(
            AirflowException,
            cancel_dag_run,
            dag_id,
            'ok'
        )

    @mock.patch('airflow.api.common.experimental.cancel_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_cancel_dagrun_dagrun_no_tis(self, dag_bag_mock, dag_run_mock):
        dag_id = "dag_run_exist"
        dag = DAG(dag_id)
        dag_bag_mock.return_value.dags = [dag_id]
        dag_bag_mock.return_value.get_dag.return_value = dag
        dag_run = DagRun()
        dag_run_mock.find.return_value = [dag_run]
        dag_run.get_task_instances = lambda *args, **kwargs: []
        self.assertRaises(
            AirflowException,
            cancel_dag_run,
            dag_id,
            'ok'
        )

    @mock.patch('airflow.api.common.experimental.cancel_dag_run.cancel_task_instances', autospec=True)
    @mock.patch('airflow.api.common.experimental.cancel_dag_run.DagRun')
    @mock.patch('airflow.api.common.experimental.DagBag')
    def test_cancel_dagrun_OK(self, dag_bag_mock, dag_run_mock, mock_cancel_task_instances):
        dag_id = "dag_run_exist"
        dag = DAG(dag_id)
        dag_bag_mock.return_value.dags = [dag_id]
        dag_bag_mock.return_value.get_dag.return_value = dag
        dag_run = DagRun()
        dag_run_mock.find.return_value = [dag_run]
        dag_run.get_task_instances = lambda *args, **kwargs: [TaskInstance(mock.Mock(), datetime(2020, 1, 1))]
        result = cancel_dag_run(dag_id, 'ok', mock.Mock())
        self.assertEqual(dag_run, result)


if __name__ == '__main__':
    unittest.main()
