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

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowSensorTimeout
from airflow.models import DagBag
from airflow.models.dag import DAG
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.utils.timezone import datetime
from airflow.utils.weekday import WeekDay
from tests.test_utils import db

DEFAULT_DATE = datetime(2018, 12, 10)
WEEKDAY_DATE = datetime(2018, 12, 20)
WEEKEND_DATE = datetime(2018, 12, 22)
TEST_DAG_ID = 'weekday_sensor_dag'
DEV_NULL = '/dev/null'


class TestDayOfWeekSensor(unittest.TestCase):
    @staticmethod
    def clean_db():
        db.clear_db_runs()
        db.clear_db_task_fail()

    def setUp(self):
        self.clean_db()
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=self.args)
        self.dag = dag

    def tearDown(self):
        self.clean_db()

    @parameterized.expand(
        [
            ("with-string", 'Thursday'),
            ("with-enum", WeekDay.THURSDAY),
            ("with-enum-set", {WeekDay.THURSDAY}),
            ("with-enum-set-2-items", {WeekDay.THURSDAY, WeekDay.FRIDAY}),
            ("with-string-set", {'Thursday'}),
            ("with-string-set-2-items", {'Thursday', 'Friday'}),
        ]
    )
    def test_weekday_sensor_true(self, _, week_day):
        op = DayOfWeekSensor(
            task_id='weekday_sensor_check_true', week_day=week_day, use_task_execution_day=True, dag=self.dag
        )
        op.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
        assert op.week_day == week_day

    def test_weekday_sensor_false(self):
        op = DayOfWeekSensor(
            task_id='weekday_sensor_check_false',
            poke_interval=1,
            timeout=2,
            week_day='Tuesday',
            use_task_execution_day=True,
            dag=self.dag,
        )
        with pytest.raises(AirflowSensorTimeout):
            op.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)

    def test_invalid_weekday_number(self):
        invalid_week_day = 'Thsday'
        with pytest.raises(AttributeError, match=f'Invalid Week Day passed: "{invalid_week_day}"'):
            DayOfWeekSensor(
                task_id='weekday_sensor_invalid_weekday_num',
                week_day=invalid_week_day,
                use_task_execution_day=True,
                dag=self.dag,
            )

    def test_weekday_sensor_with_invalid_type(self):
        invalid_week_day = ['Thsday']
        with pytest.raises(
            TypeError,
            match='Unsupported Type for week_day parameter:'
            ' {}. It should be one of str, set or '
            'Weekday enum type'.format(type(invalid_week_day)),
        ):
            DayOfWeekSensor(
                task_id='weekday_sensor_check_true',
                week_day=invalid_week_day,
                use_task_execution_day=True,
                dag=self.dag,
            )

    def test_weekday_sensor_timeout_with_set(self):
        op = DayOfWeekSensor(
            task_id='weekday_sensor_check_false',
            poke_interval=1,
            timeout=2,
            week_day={WeekDay.MONDAY, WeekDay.TUESDAY},
            use_task_execution_day=True,
            dag=self.dag,
        )
        with pytest.raises(AirflowSensorTimeout):
            op.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
