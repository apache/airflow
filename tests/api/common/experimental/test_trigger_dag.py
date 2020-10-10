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

import unittest
from unittest import mock

from parameterized import parameterized

from airflow.api.common.experimental.trigger_dag import _trigger_dag
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun
from airflow.utils import timezone
from tests.test_utils import db


class TestTriggerDag(unittest.TestCase):

    def setUp(self) -> None:
        db.clear_db_runs()

    def tearDown(self) -> None:
        db.clear_db_runs()

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

    @mock.patch('airflow.models.DAG')
    @mock.patch('airflow.models.DagRun')
    @mock.patch('airflow.models.DagBag')
    def test_trigger_dag_include_nested_subdags(self, dag_bag_mock, dag_run_mock, dag_mock):
        dag_id = "trigger_dag"
        dag_bag_mock.dags = [dag_id]
        dag_bag_mock.get_dag.return_value = dag_mock
        dag_run_mock.find.return_value = None
        dag1 = mock.MagicMock()
        dag1.subdags = []
        dag2 = mock.MagicMock()
        dag2.subdags = [dag1]
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
    def test_trigger_dag_with_too_early_start_date(self, dag_bag_mock):
        dag_id = "trigger_dag_with_too_early_start_date"
        dag = DAG(dag_id, default_args={'start_date': timezone.datetime(2016, 9, 5, 10, 10, 0)})
        dag_bag_mock.dags = [dag_id]
        dag_bag_mock.get_dag.return_value = dag
        dag_run = DagRun()

        self.assertRaises(
            ValueError,
            _trigger_dag,
            dag_id,
            dag_bag_mock,
            dag_run,
            run_id=None,
            conf=None,
            execution_date=timezone.datetime(2015, 7, 5, 10, 10, 0),
            replace_microseconds=True,
        )

    @mock.patch('airflow.models.DagBag')
    def test_trigger_dag_with_valid_start_date(self, dag_bag_mock):
        dag_id = "trigger_dag_with_valid_start_date"
        dag = DAG(dag_id, default_args={'start_date': timezone.datetime(2016, 9, 5, 10, 10, 0)})
        dag_bag_mock.dags = [dag_id]
        dag_bag_mock.get_dag.return_value = dag
        dag_bag_mock.dags_hash = {}
        dag_run = DagRun()

        triggers = _trigger_dag(
            dag_id,
            dag_bag_mock,
            dag_run,
            run_id=None,
            conf=None,
            execution_date=timezone.datetime(2018, 7, 5, 10, 10, 0),
            replace_microseconds=True,
        )

        assert len(triggers) == 1

    @parameterized.expand([
        (None, {}),
        ({"foo": "bar"}, {"foo": "bar"}),
        ('{"foo": "bar"}', {"foo": "bar"}),
    ])
    @mock.patch('airflow.models.DagBag')
    def test_trigger_dag_with_conf(self, conf, expected_conf, dag_bag_mock):
        dag_id = "trigger_dag_with_conf"
        dag = DAG(dag_id)
        dag_bag_mock.dags = [dag_id]
        dag_bag_mock.get_dag.return_value = dag
        dag_run = DagRun()

        dag_bag_mock.dags_hash = {}

        triggers = _trigger_dag(
            dag_id,
            dag_bag_mock,
            dag_run,
            run_id=None,
            conf=conf,
            execution_date=None,
            replace_microseconds=True)

        self.assertEqual(triggers[0].conf, expected_conf)
