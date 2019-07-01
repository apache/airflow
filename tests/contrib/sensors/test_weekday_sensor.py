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
#

import unittest
from airflow import DAG, models
from airflow.contrib.sensors.weekday_sensor import DayOfWeekSensor
from airflow.contrib.utils.weekday import WeekDay
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import DagBag, TaskFail
from airflow.settings import Session
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2018, 12, 10)
WEEKDAY_DATE = datetime(2018, 12, 20)
WEEKEND_DATE = datetime(2018, 12, 22)
TEST_DAG_ID = 'weekday_sensor_dag'
DEV_NULL = '/dev/null'


class DayOfWeekSensorTests(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(
            dag_folder=DEV_NULL,
            include_examples=True
        )
        self.args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        dag = DAG(TEST_DAG_ID, default_args=self.args)
        self.dag = dag

    def tearDown(self):
        session = Session()
        session.query(models.TaskInstance).filter_by(
            dag_id=TEST_DAG_ID).delete()
        session.query(TaskFail).filter_by(
            dag_id=TEST_DAG_ID).delete()
        session.commit()
        session.close()

    def test_weekday_sensor_true(self):
        t = DayOfWeekSensor(
            task_id='weekday_sensor_check_true',
            week_day='Thursday',
            use_task_execution_day=True,
            dag=self.dag)
        t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)

    def test_weekday_sensor_false(self):
        t = DayOfWeekSensor(
            task_id='weekday_sensor_check_false',
            poke_interval=1,
            timeout=2,
            week_day='Tuesday',
            use_task_execution_day=True,
            dag=self.dag)
        with self.assertRaises(AirflowSensorTimeout):
            t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)

    def test_invalid_weekday_number(self):
        invalid_week_day = 'Thsday'
        with self.assertRaisesRegexp(AttributeError,
                                     'Invalid Week Day passed: "{}"'.format(
                                         invalid_week_day)):
            DayOfWeekSensor(
                task_id='weekday_sensor_invalid_weekday_num',
                week_day=invalid_week_day,
                use_task_execution_day=True,
                dag=self.dag)

    def test_weekday_sensor_with_enum(self):
        week_day = WeekDay.THURSDAY
        t = DayOfWeekSensor(
            task_id='weekday_sensor_check_true',
            week_day=WeekDay.THURSDAY,
            use_task_execution_day=True,
            dag=self.dag)
        t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
        self.assertEqual(t.week_day, week_day)

    def test_weekday_sensor_with_enum_set(self):
        week_day = {WeekDay.THURSDAY}
        t = DayOfWeekSensor(
            task_id='weekday_sensor_check_true',
            week_day=week_day,
            use_task_execution_day=True,
            dag=self.dag)
        t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
        self.assertEqual(t.week_day, week_day)

    def test_weekday_sensor_with_enum_set_2_items(self):
        week_day = {WeekDay.THURSDAY, WeekDay.FRIDAY}
        t = DayOfWeekSensor(
            task_id='weekday_sensor_check_true',
            week_day=week_day,
            use_task_execution_day=True,
            dag=self.dag)
        t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
        self.assertEqual(t.week_day, week_day)

    def test_weekday_sensor_with_string_set(self):
        week_day = {'Thursday'}
        t = DayOfWeekSensor(
            task_id='weekday_sensor_check_true',
            week_day=week_day,
            use_task_execution_day=True,
            dag=self.dag)
        t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
        self.assertEqual(t.week_day, week_day)

    def test_weekday_sensor_with_string_set_2_items(self):
        week_day = {'Thursday', 'Friday'}
        t = DayOfWeekSensor(
            task_id='weekday_sensor_check_true',
            week_day=week_day,
            use_task_execution_day=True,
            dag=self.dag)
        t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
        self.assertEqual(t.week_day, week_day)

    def test_weekday_sensor_with_invalid_type(self):
        invalid_week_day = ['Thsday']
        with self.assertRaisesRegexp(TypeError,
                                     'Unsupported Type for week_day parameter:'
                                     ' {}. It should be one of str, set or '
                                     'Weekday enum type'.format(type(invalid_week_day))
                                     ):
            DayOfWeekSensor(
                task_id='weekday_sensor_check_true',
                week_day=invalid_week_day,
                use_task_execution_day=True,
                dag=self.dag)

    def test_weekday_sensor_timeout_with_set(self):
        t = DayOfWeekSensor(
            task_id='weekday_sensor_check_false',
            poke_interval=1,
            timeout=2,
            week_day={WeekDay.MONDAY, WeekDay.TUESDAY},
            use_task_execution_day=True,
            dag=self.dag)
        with self.assertRaises(AirflowSensorTimeout):
            t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
