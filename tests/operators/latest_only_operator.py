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

import datetime
import logging
import unittest

from airflow import configuration, DAG, settings
from airflow.jobs import BackfillJob
from airflow.models import TaskInstance
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.state import State
from freezegun import freeze_time

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = datetime.timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)


def get_task_instances(task_id):
    session = settings.Session()
    return session \
        .query(TaskInstance) \
        .filter(TaskInstance.task_id == task_id) \
        .order_by(TaskInstance.execution_date) \
        .all()


class LatestOnlyOperatorTest(unittest.TestCase):

    def setUp(self):
        super(LatestOnlyOperatorTest, self).setUp()
        configuration.load_test_config()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL)
        self.addCleanup(self.dag.clear)
        freezer = freeze_time(FROZEN_NOW)
        freezer.start()
        self.addCleanup(freezer.stop)

    def test_run(self):
        task = LatestOnlyOperator(
            task_id='latest',
            dag=self.dag)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_skipping(self):
        latest_task = LatestOnlyOperator(
            task_id='latest',
            dag=self.dag)
        downstream_task = DummyOperator(
            task_id='downstream',
            dag=self.dag)
        downstream_task2 = DummyOperator(
            task_id='downstream_2',
            dag=self.dag)

        downstream_task.set_upstream(latest_task)
        downstream_task2.set_upstream(downstream_task)

        latest_task.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task2.run(start_date=DEFAULT_DATE, end_date=END_DATE)

        latest_instances = get_task_instances('latest')
        exec_date_to_latest_state = {
            ti.execution_date: ti.state for ti in latest_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'success',
            timezone.datetime(2016, 1, 1, 12): 'success',
            timezone.datetime(2016, 1, 2): 'success', },
            exec_date_to_latest_state)

        downstream_instances = get_task_instances('downstream')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'skipped',
            timezone.datetime(2016, 1, 1, 12): 'skipped',
            timezone.datetime(2016, 1, 2): 'success',},
            exec_date_to_downstream_state)

        downstream_instances = get_task_instances('downstream_2')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'skipped',
            timezone.datetime(2016, 1, 1, 12): 'skipped',
            timezone.datetime(2016, 1, 2): 'success',},
            exec_date_to_downstream_state)

    def test_skipping_dagrun(self):
        latest_task = LatestOnlyOperator(
            task_id='latest',
            dag=self.dag)
        downstream_task = DummyOperator(
            task_id='downstream',
            dag=self.dag)
        downstream_task2 = DummyOperator(
            task_id='downstream_2',
            dag=self.dag)

        downstream_task.set_upstream(latest_task)
        downstream_task2.set_upstream(downstream_task)

        dr1 = self.dag.create_dagrun(
            run_id="manual__1",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        dr2 = self.dag.create_dagrun(
            run_id="manual__2",
            start_date=timezone.utcnow(),
            execution_date=timezone.datetime(2016, 1, 1, 12),
            state=State.RUNNING
        )

        dr2 = self.dag.create_dagrun(
            run_id="manual__3",
            start_date=timezone.utcnow(),
            execution_date=END_DATE,
            state=State.RUNNING
        )

        latest_task.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task2.run(start_date=DEFAULT_DATE, end_date=END_DATE)

        latest_instances = get_task_instances('latest')
        exec_date_to_latest_state = {
            ti.execution_date: ti.state for ti in latest_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'success',
            timezone.datetime(2016, 1, 1, 12): 'success',
            timezone.datetime(2016, 1, 2): 'success', },
            exec_date_to_latest_state)

        downstream_instances = get_task_instances('downstream')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'skipped',
            timezone.datetime(2016, 1, 1, 12): 'skipped',
            timezone.datetime(2016, 1, 2): 'success',},
            exec_date_to_downstream_state)

        downstream_instances = get_task_instances('downstream_2')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'skipped',
            timezone.datetime(2016, 1, 1, 12): 'skipped',
            timezone.datetime(2016, 1, 2): 'success',},
            exec_date_to_downstream_state)
