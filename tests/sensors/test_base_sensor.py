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
from mock import Mock

from airflow import DAG, configuration, settings
from airflow.exceptions import (AirflowSensorTimeout, AirflowException,
                                AirflowRescheduleException)
from airflow.models import DagRun, TaskInstance, TaskReschedule
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from datetime import timedelta
from time import sleep
from freezegun import freeze_time

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

    def test_ok_with_reschedule(self):
        sensor = self._make_sensor(
            return_value=None,
            poke_interval=10,
            timeout=25,
            mode='reschedule')
        sensor.poke = Mock(side_effect=[False, False, True])
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                self.assertEquals(ti.state, State.NONE)
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEquals(len(task_reschedules), 1)
                self.assertEquals(task_reschedules[0].start_date, date1)
                self.assertEquals(task_reschedules[0].reschedule_date,
                                  date1 + timedelta(seconds=sensor.poke_interval))
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # second poke returns False and task is re-scheduled
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                self.assertEquals(ti.state, State.NONE)
                # verify two rows in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEquals(len(task_reschedules), 2)
                self.assertEquals(task_reschedules[1].start_date, date2)
                self.assertEquals(task_reschedules[1].reschedule_date,
                                  date2 + timedelta(seconds=sensor.poke_interval))
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # third poke returns True and task succeeds
        date3 = date2 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.SUCCESS)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

    def test_fail_with_reschedule(self):
        sensor = self._make_sensor(
            return_value=False,
            poke_interval=10,
            timeout=5,
            mode='reschedule')
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.NONE)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # second poke returns False, timeout occurs
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            with self.assertRaises(AirflowSensorTimeout):
                self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.FAILED)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

    def test_soft_fail_with_reschedule(self):
        sensor = self._make_sensor(
            return_value=False,
            poke_interval=10,
            timeout=5,
            soft_fail=True,
            mode='reschedule')
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.NONE)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # second poke returns False, timeout occurs
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            self.assertEquals(ti.state, State.SKIPPED)

    def test_ok_with_reschedule_and_retry(self):
        sensor = self._make_sensor(
            return_value=None,
            poke_interval=10,
            timeout=5,
            retries=1,
            retry_delay=timedelta(seconds=10),
            mode='reschedule')
        sensor.poke = Mock(side_effect=[False, False, False, True])
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.NONE)
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEquals(len(task_reschedules), 1)
                self.assertEquals(task_reschedules[0].start_date, date1)
                self.assertEquals(task_reschedules[0].reschedule_date,
                                  date1 + timedelta(seconds=sensor.poke_interval))
                self.assertEqual(task_reschedules[0].try_number, 1)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # second poke fails and task instance is marked up to retry
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            with self.assertRaises(AirflowSensorTimeout):
                self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.UP_FOR_RETRY)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # third poke returns False and task is rescheduled again
        date3 = date2 + timedelta(seconds=sensor.poke_interval) + sensor.retry_delay
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.NONE)
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEquals(len(task_reschedules), 1)
                self.assertEquals(task_reschedules[0].start_date, date3)
                self.assertEquals(task_reschedules[0].reschedule_date,
                                  date3 + timedelta(seconds=sensor.poke_interval))
                self.assertEqual(task_reschedules[0].try_number, 2)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # fourth poke return True and task succeeds
        date4 = date3 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date4):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.SUCCESS)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

    def test_should_include_ready_to_reschedule_dep(self):
        sensor = self._make_sensor(True)
        deps = sensor.deps
        self.assertTrue(ReadyToRescheduleDep() in deps)

    def test_invalid_mode(self):
        with self.assertRaises(AirflowException):
            self._make_sensor(
                return_value=True,
                mode='foo')

    def test_ok_with_custom_reschedule_exception(self):
        sensor = self._make_sensor(
            return_value=None,
            mode='reschedule')
        date1 = timezone.utcnow()
        date2 = date1 + timedelta(seconds=60)
        date3 = date1 + timedelta(seconds=120)
        sensor.poke = Mock(side_effect=[
            AirflowRescheduleException(date2),
            AirflowRescheduleException(date3),
            True,
        ])
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                self.assertEquals(ti.state, State.NONE)
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEquals(len(task_reschedules), 1)
                self.assertEquals(task_reschedules[0].start_date, date1)
                self.assertEquals(task_reschedules[0].reschedule_date, date2)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # second poke returns False and task is re-scheduled
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                self.assertEquals(ti.state, State.NONE)
                # verify two rows in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEquals(len(task_reschedules), 2)
                self.assertEquals(task_reschedules[1].start_date, date2)
                self.assertEquals(task_reschedules[1].reschedule_date, date3)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

        # third poke returns True and task succeeds
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEquals(ti.state, State.SUCCESS)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)

    def test_reschedule_with_test_mode(self):
        sensor = self._make_sensor(
            return_value=None,
            poke_interval=10,
            timeout=25,
            mode='reschedule')
        sensor.poke = Mock(side_effect=[False])
        dr = self._make_dag_run()

        # poke returns False and AirflowRescheduleException is raised
        date1 = timezone.utcnow()
        with freeze_time(date1):
            for dt in self.dag.date_range(DEFAULT_DATE, end_date=DEFAULT_DATE):
                TaskInstance(sensor, dt).run(
                    ignore_ti_state=True,
                    test_mode=True)
        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # in test mode state is not modified
                self.assertEquals(ti.state, State.NONE)
                # in test mode no reschedule request is recorded
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEquals(len(task_reschedules), 0)
            if ti.task_id == DUMMY_OP:
                self.assertEquals(ti.state, State.NONE)
