# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest

from datetime import timedelta

from airflow import configuration
from airflow import models, DAG
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.utils.timezone import datetime

configuration.load_test_config()

DEFAULT_DATE = datetime(2015, 1, 1)
DEV_NULL = '/dev/null'
TEST_DAG_ID = 'unit_tests'


class TimedeltaSensorTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)

    def test_timedelta_sensor(self):
        t = TimeDeltaSensor(
            task_id='timedelta_sensor_check',
            delta=timedelta(seconds=2),
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
