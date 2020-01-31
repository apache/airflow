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

import pytest

from airflow import DAG, models
from airflow.contrib.utils.weekday import WeekDay
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import TaskFail
from airflow.sensors.weekday_sensor import DayOfWeekSensor
from airflow.settings import Session
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2018, 12, 10)
WEEKDAY_DATE = datetime(2018, 12, 20)
WEEKEND_DATE = datetime(2018, 12, 22)
TEST_DAG_ID = 'weekday_sensor_dag'
DEV_NULL = '/dev/null'
TRUE_TESTS_CONFIG = {
    "string": "Thursday",
    "enum": WeekDay.THURSDAY,
    "enum-set": {WeekDay.THURSDAY},
    "enum-set-2-items": {WeekDay.THURSDAY, WeekDay.FRIDAY},
    "string-set": {'Thursday'},
    "string-set-2-items": {'Thursday', 'Friday'},
}


@pytest.fixture
def dag():
    args = {"owner": "airflow", "start_date": DEFAULT_DATE}
    return DAG(TEST_DAG_ID, default_args=args)


@pytest.fixture
def tear_down():
    yield

    session = Session()
    session.query(models.TaskInstance).filter_by(
        dag_id=TEST_DAG_ID).delete()
    session.query(TaskFail).filter_by(
        dag_id=TEST_DAG_ID).delete()
    session.commit()
    session.close()


@pytest.mark.parametrize("weekday", TRUE_TESTS_CONFIG.values(), ids=list(TRUE_TESTS_CONFIG.keys()))
def test_weekday_sensor_true(dag, weekday, tear_down):
    op = DayOfWeekSensor(
        task_id="weekday_sensor_check_true",
        week_day=weekday,
        use_task_execution_day=True,
        dag=dag)
    op.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)


def test_weekday_sensor_false(dag, tear_down):
    op = DayOfWeekSensor(
        task_id='weekday_sensor_check_false',
        poke_interval=0,
        timeout=2,
        week_day='Tuesday',
        use_task_execution_day=True,
        dag=dag)
    with pytest.raises(AirflowSensorTimeout):
        op.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)


def test_invalid_weekday_number(dag, tear_down):
    invalid_week_day = 'Thsday'
    with pytest.raises(
        AttributeError,
        match=r'Invalid Week Day passed: "{}"'.format(invalid_week_day)
    ):
        DayOfWeekSensor(
            task_id='weekday_sensor_invalid_weekday_num',
            week_day=invalid_week_day,
            use_task_execution_day=True,
            dag=dag)


def test_weekday_sensor_with_invalid_type(dag, tear_down):
    invalid_week_day = ['Thsday']
    with pytest.raises(
        TypeError,
        match=r"Unsupported Type for week_day parameter: {}. It should be one "
              "of str, set or Weekday enum type".format(type(invalid_week_day))
    ):
        DayOfWeekSensor(
            task_id='weekday_sensor_invalid_type',
            week_day=invalid_week_day,
            use_task_execution_day=True,
            dag=dag)


def test_weekday_sensor_timeout_with_set(dag, tear_down):
    op = DayOfWeekSensor(
        task_id='weekday_sensor_check_false',
        poke_interval=1,
        timeout=2,
        week_day={WeekDay.MONDAY, WeekDay.TUESDAY},
        use_task_execution_day=True,
        dag=dag)
    with pytest.raises(AirflowSensorTimeout):
        op.run(start_date=WEEKDAY_DATE, end_date=WEEKDAY_DATE, ignore_ti_state=True)
