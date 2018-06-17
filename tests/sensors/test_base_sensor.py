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

from airflow import DAG, configuration, settings
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import DagRun, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from datetime import timedelta
from time import sleep

configuration.load_test_config()

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
DUMMY_OP = 'dummy_op'
SENSOR_OP = 'sensor_op'


class DummySensor(BaseSensorOperator):
    def __init__(self, return_value=False, **kwargs):
        super(DummySensor, self).__init__(**kwargs)
        self.return_value = return_value

    def poke(self, context):
        return self.return_value


class BaseSensorTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(TEST_DAG_ID, default_args=args)

        session = settings.Session()
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

    def _make_sensor(self, return_value, **kwargs):
        poke_interval = 'poke_interval'
        timeout = 'timeout'
        if poke_interval not in kwargs:
            kwargs[poke_interval] = 0
        if timeout not in kwargs:
            kwargs[timeout] = 0

        sensor = DummySensor(
            task_id=SENSOR_OP,
            return_value=return_value,
            dag=self.dag,
            **kwargs
        )

        dummy_op = DummyOperator(
            task_id=DUMMY_OP,
            dag=self.dag
        )
        dummy_op.set_upstream(sensor)
        return sensor

    @classmethod
    def _run(cls, task):
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_ok(self):
        sensor = self._make_sensor(True)
        dr = self._make_dag_run()

        self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.SUCCESS)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

    def test_fail(self):
        sensor = self._make_sensor(False)
        dr = self._make_dag_run()

        with self.assertRaises(AirflowSensorTimeout):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.FAILED)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

    def test_soft_fail(self):
        sensor = self._make_sensor(False, soft_fail=True)
        dr = self._make_dag_run()

        self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            self.assertEquals(ti.state, State.SKIPPED)

    def test_soft_fail_with_retries(self):
        sensor = self._make_sensor(
            return_value=False,
            soft_fail=True,
            retries=1,
            retry_delay=timedelta(milliseconds=1))
        dr = self._make_dag_run()

        # first run fails and task instance is marked up to retry
        with self.assertRaises(AirflowSensorTimeout):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.UP_FOR_RETRY)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        sleep(0.001)
        # after retry DAG run is skipped
        self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            self.assertEquals(ti.state, State.SKIPPED)
