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
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
#     specific language governing permissions and limitations
# under the License.

import unittest
from unittest import mock

from datetime import datetime, timedelta

from airflow.models import TaskInstance, DAG, DagRun
from airflow.operators.singleton_operator import SingletonOperator
from airflow.utils import timezone
from airflow.utils.state import State

DEFAULT_DATE = datetime(2019, 1, 1, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class SingletonOperatorTest(unittest.TestCase):
    """
    Test cases for :py:class:SingletonOperator.
    """

    def setUp(self):
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'unittest',
                'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL)
        self.singleton = SingletonOperator(task_id='singleton', dag=self.dag)
        self.singleton_ext_trigger = SingletonOperator(task_id='singleton',
                                                       allow_external_trigger=True,
                                                       dag=self.dag)
        self.ti = TaskInstance(task=self.singleton, execution_date=DEFAULT_DATE)
        self.dag_run = DagRun()
        self.dag_run.state = State.RUNNING
        self.dag_run.run_id = "manual"
        self.dag_run.dag_id = "test_dag"
        self.dag_run.external_trigger = True
        self.context = {"ti": self.ti, "dag_run": self.dag_run}

    def tearDown(self):
        self.dag_run = None
        self.dag = None
        self.context = None
        self.singleton = None
        self.singleton_ext_trigger = None
        self.ti = None

    def test_task_count(self):
        self.assertEqual(len(self.dag.tasks), 1)

    @mock.patch.object(SingletonOperator, 'get_other_active_dag_runs')
    def test_get_other_active_dag_runs(self, get_other_active_dag_runs_mock):
        get_other_active_dag_runs_mock.return_value = [self.dag_run]
        active_dag_run = self.singleton.get_other_active_dag_runs()[0]
        self.assertEqual(active_dag_run.dag_id, 'test_dag')
        self.assertEqual(active_dag_run.external_trigger, True)
        self.assertEqual(active_dag_run.run_id, "manual")

    @mock.patch.object(SingletonOperator, 'unfinished_tasks')
    def test_unfinished_tasks(self, unfinished_tasks_mock):
        unfinished_tasks_mock.return_value = ['downstream', 1]
        task, skip = self.singleton.unfinished_tasks(self.dag_run)
        self.assertTrue(self.dag_run.state == State.RUNNING)
        self.assertEqual(task, 'downstream')
        self.assertTrue(skip)

    @mock.patch.object(SingletonOperator, 'skip_or_schedule')
    def test_skip_or_schedule(self, _skip_or_schedule_mock):
        _skip_or_schedule_mock.return_value = 'SKIPPED'
        result = self.singleton.skip_or_schedule(self.dag_run, [])
        self.assertEqual(result, 'SKIPPED')
        _skip_or_schedule_mock.return_value = 'SCHEDULED'
        result = self.singleton.skip_or_schedule(self.dag_run, [])
        self.assertEqual(result, 'SCHEDULED')

    @mock.patch.object(SingletonOperator, 'get_other_active_dag_runs')
    @mock.patch.object(SingletonOperator, 'skip_or_schedule')
    def test_execute_skip(self,
                          _skip_or_schedule_mock,
                          get_other_active_dag_runs_mock):
        get_other_active_dag_runs_mock.return_value = [self.dag_run]
        _skip_or_schedule_mock.return_value = 'SKIPPED'
        self.assertTrue(self.singleton.execute(self.context) is None)
        self.singleton.skip_or_schedule(self.dag_run,
                                        [])  # pylint: disable=W0212

    @mock.patch.object(SingletonOperator, 'get_other_active_dag_runs')
    @mock.patch.object(SingletonOperator, 'skip_or_schedule')
    def test_execute_schedule(self, _skip_or_schedule_mock,
                              get_other_active_dag_runs_mock):
        self.assertTrue(self.singleton.execute(self.context) is None)
        get_other_active_dag_runs_mock.return_value = [self.dag_run]
        _skip_or_schedule_mock.return_value = 'SCHEDULED'
        self.assertTrue(self.singleton.execute(self.context) is None)
