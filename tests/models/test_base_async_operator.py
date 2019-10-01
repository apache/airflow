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

import unittest
from unittest.mock import call, Mock
import uuid
import random

from airflow import DAG, settings
from airflow.exceptions import (AirflowSensorTimeout, AirflowException,
                                AirflowRescheduleException)
from airflow.models import (BaseAsyncOperator, DagRun, TaskInstance,
                            TaskReschedule)
from airflow.models.xcom import XCOM_EXTERNAL_RESOURCE_ID_KEY
from airflow.operators.dummy_operator import DummyOperator
from airflow.async_ops.base_sensor_operator import BaseSensorOperator
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.timezone import datetime

from datetime import timedelta
from time import sleep
from freezegun import freeze_time
from parameterized import parameterized

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
DUMMY_OP = 'dummy_op'
ASYNC_OP = 'async_op'

def _rand_job_id():
    yield 'job_id-{}'.format(uuid.uuid4())

ALL_ID_TYPES = [
    _job_id(),
    random.randint(),
    [_job_id(), _job_id()],
    {'job1:' _job_id()},
    None
]


class DummyAsyncOperator(BaseAsyncOperator):
    def __init__(self, return_value=False, submit_fail=False,
                 **kwargs):
        super().__init__(**kwargs)
        self.return_value = return_value

    def poke(self, context):
        return self.return_value

    def submit_request(self, context):
        return _job_id()

    def process_result(self, context):
        return self.get_external_resource_id()


class TestBaseAsyncOperator(unittest.TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(TEST_DAG_ID, default_args=args)

        session = settings.Session()
        session.query(TaskReschedule).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        session.commit()

    def _make_dag_run(self):
        return self.dag.create_dagrun(
            run_id='manual__',
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

    def _make_async_op(self, return_value, resource_id, **kwargs):
        poke_interval = 'poke_interval'
        timeout = 'timeout'
        if poke_interval not in kwargs:
            kwargs[poke_interval] = 0
        if timeout not in kwargs:
            kwargs[timeout] = 0

        async_op = DummyAsyncOperator(
            task_id=ASYNC_OP,
            return_value=return_value,
            resource_id=resource_id,
            dag=self.dag,
            **kwargs
        )

        dummy_op = DummyOperator(
            task_id=DUMMY_OP,
            dag=self.dag
        )
        dummy_op.set_upstream(async_op)
        return async_op

    @classmethod
    def _run(cls, task):
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_ok(self):
        async_op = self._make_async_op(True)
        dr = self._make_dag_run()

        self._run(async_op)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)
        for ti in tis:
            if ti.task_id == ASYNC_OP:
                self.assertEqual(ti.state, State.SUCCESS)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)

    def test_fail(self):
        async_op = self._make_async_op(False)
        dr = self._make_dag_run()

        with self.assertRaises(AirflowSensorTimeout):
            self._run(async_op)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)
        for ti in tis:
            if ti.task_id == ASYNC_OP:
                self.assertEqual(ti.state, State.FAILED)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)

    @parameterized.expand(ALL_ID_TYPES):
    def test_set_get_external_resource_id(self, resource_id):
        async_op = self._make_async_op(
            return_value=None,
            poke_interval=10,
            timeout=25,
            mode='reschedule')

        dr = self._make_dag_run()

        self._run(async_op)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)
        for ti in tis:
            if ti.task_id == ASYNC_OP:
                context = ti.get_template_context()
                async_op.set_external_resource_id(context, resource_id)
                self.assertEqual(resource_id, async_op.get_external_resource_id())

    def test_xcom_stores_resource_id(self):
        async_op = self._make_async_op(
            return_value=None,
            poke_interval=10,
            timeout=25,
            mode='reschedule')
        async_op.set_external_resource_id = Mock()
        async_op.get_external_resource_id = Mock()

        dr = self._make_dag_run()

        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(async_op)
        tis = dr.get_task_instances()

        self.assertEqual(async_op.set_external_resource_id.call_count, 2)
        async_op.get_external_resource_id.assert_called_once()

        #Check that XCom was set to None.
        for ti in tis:
            if ti.task_id == ASYNC_OP:
                resource_id = ti.xcom_pull(taks_ids=ASYNC_OP,
                                           key=XCOM_EXTERNAL_RESOURCE_ID_KEY)
                self.assertIsNone(resource_id)

    def test_ok_with_reschedule(self):
        async_op = self._make_async_op(
            return_value=None,
            poke_interval=10,
            timeout=25,
            mode='reschedule')
        async_op.poke = Mock(side_effect=[False, False, True])
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(async_op)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)

        # second poke returns False and task is re-scheduled
        date2 = date1 + timedelta(seconds=async_op.poke_interval)
        with freeze_time(date2):
            self._run(async_op)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)

        # third poke returns True and task succeeds
        date3 = date2 + timedelta(seconds=async_op.poke_interval)
        with freeze_time(date3):
            self._run(async_op)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)
        for ti in tis:
            if ti.task_id == ASYNC_OP:
                self.assertEqual(ti.state, State.SUCCESS)
                # verify task start date is the initial one
                self.assertEqual(ti.start_date, date1)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)

        async_op.submit_request.assert_called_once()
        async_op.process_result.assert_called_once()
        self.assertEqual(async_op.poke.call_count, 3)

    def test_ok_with_reschedule_and_retry(self):
        async_op = self._make_async_op(
            return_value=None,
            poke_interval=10,
            timeout=5,
            retries=1,
            retry_delay=timedelta(seconds=10),
            mode='reschedule')
        async_op.poke = Mock(side_effect=[False, False, False, True])
        async_op.submit_request = Mock(side_effect=[_job_id(), _job_id()])
        async_op.process_result = Mock()
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(async_op)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)

        # second poke fails and task instance is marked up to retry
        date2 = date1 + timedelta(seconds=async_op.poke_interval)
        with freeze_time(date2):
            with self.assertRaises(AirflowSensorTimeout):
                self._run(async_op)

        # process_result should not be called on retry.
        self.assertEqual(async_op.process_result.call_count, 0)

        # third poke returns False and task is rescheduled again
        date3 = date2 + timedelta(seconds=async_op.poke_interval) + sensor.retry_delay
        with freeze_time(date3):
            self._run(async_op)

        # submit request should be retried.
        self.assertEqual(async_op.submit_request.call_count, 2)

        # fourth poke return True and task succeeds
        date4 = date3 + timedelta(seconds=async_op.poke_interval)
        with freeze_time(date4):
            self._run(async_op)

        # process_results has been called once, only after the successful poke.
        self.assertEqual(async_op.process_result.call_count, 1)
