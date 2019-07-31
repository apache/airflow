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

import mock
import unittest
import json

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun
from airflow.api.common.experimental.trigger_dag import _trigger_dag


class TriggerDagTests(unittest.TestCase):

    @mock.patch('airflow.models.DagRun')
    @mock.patch('airflow.models.DagBag')
    def test_trigger_dag_dag_not_found(self, dag_bag_mock, dag_run_mock):
        dag_bag_mock.dags = []
        self.assertRaises(
            AirflowException,
            _trigger_dag,
            'dag_not_found',
            dag_bag_mock,
            dag_run_mock,
            run_id=None,
            conf=None,
            execution_date=None,
            replace_microseconds=True,
        )

    @mock.patch('airflow.models.DagRun')
    @mock.patch('airflow.models.DagBag')
    def test_trigger_dag_dag_run_exist(self, dag_bag_mock, dag_run_mock):
        dag_id = "dag_run_exist"
        dag = DAG(dag_id)
        dag_bag_mock.dags = [dag_id]
        dag_bag_mock.get_dag.return_value = dag
        dag_run_mock.find.return_value = DagRun()
        self.assertRaises(
            AirflowException,
            _trigger_dag,
            dag_id,
            dag_bag_mock,
            dag_run_mock,
            run_id=None,
            conf=None,
            execution_date=None,
            replace_microseconds=True,
        )

    @mock.patch('airflow.models.DAG')
    @mock.patch('airflow.models.DagRun')
    @mock.patch('airflow.models.DagBag')
    def test_trigger_dag_include_subdags(self, dag_bag_mock, dag_run_mock, dag_mock):
        dag_id = "trigger_dag"
        dag_bag_mock.dags = [dag_id]
        dag_bag_mock.get_dag.return_value = dag_mock
        dag_run_mock.find.return_value = None
        dag1 = mock.MagicMock()
        dag1.subdags = []
        dag2 = mock.MagicMock()
        dag2.subdags = []
        dag_mock.subdags = [dag1, dag2]

        triggers = _trigger_dag(
            dag_id,
            dag_bag_mock,
            dag_run_mock,
            run_id=None,
            conf=None,
            execution_date=None,
            replace_microseconds=True)

        self.assertEqual(3, len(triggers))

    @mock.patch('airflow.models.DagBag')
    def test_trigger_dag_with_str_conf(self, dag_bag_mock):
        dag_id = "trigger_dag_with_str_conf"
        dag = DAG(dag_id)
        dag_bag_mock.dags = [dag_id]
        dag_bag_mock.get_dag.return_value = dag
        conf = "{\"foo\": \"bar\"}"
        dag_run = DagRun()
        triggers = _trigger_dag(
            dag_id,
            dag_bag_mock,
            dag_run,
            run_id=None,
            conf=conf,
            execution_date=None,
            replace_microseconds=True)

        self.assertEqual(triggers[0].conf, json.loads(conf))

    @mock.patch('airflow.models.DagBag')
    def test_trigger_dag_with_dict_conf(self, dag_bag_mock):
        dag_id = "trigger_dag_with_dict_conf"
        dag = DAG(dag_id)
        dag_bag_mock.dags = [dag_id]
        dag_bag_mock.get_dag.return_value = dag
        conf = dict(foo="bar")
        dag_run = DagRun()
        triggers = _trigger_dag(
            dag_id,
            dag_bag_mock,
            dag_run,
            run_id=None,
            conf=conf,
            execution_date=None,
            replace_microseconds=True)

        self.assertEqual(triggers[0].conf, conf)


if __name__ == '__main__':
    unittest.main()
