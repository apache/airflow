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

from airflow import DAG, configuration
from airflow.contrib.sensors.weekday_sensor import DayOfWeekSensor, WeekEndSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import DagBag
from airflow.utils.timezone import datetime


DEFAULT_DATE = datetime(2018, 12, 10)
WEEKDAY_DATE = datetime(2018, 12, 20)
WEEKEND_DATE = datetime(2018, 12, 22)
TEST_DAG_ID = 'weekday_sensor_dag'
DEV_NULL = '/dev/null'


class DayOfWeekSensorTests(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
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


class WeekEndSensorTests(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.dagbag = DagBag(
            dag_folder=DEV_NULL,
            include_examples=False
        )
        self.args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        dag = DAG(TEST_DAG_ID, default_args=self.args)
        self.dag = dag

    def test_weekend_sensor_true(self):
        t = WeekEndSensor(
            task_id='weekend_sensor_check_true',
            use_task_execution_day=True,
            dag=self.dag)
        t.run(start_date=WEEKEND_DATE, end_date=WEEKEND_DATE, ignore_ti_state=True)

    def test_weekend_sensor_false(self):
        t = WeekEndSensor(
            task_id='weekend_sensor_check_false',
            poke_interval=1,
            timeout=2,
            use_task_execution_day=True,
            dag=self.dag)
        with self.assertRaises(AirflowSensorTimeout):
            t.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
