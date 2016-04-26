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


import logging
import os
import time
import unittest

from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.operators.sensors import HttpSensor, BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import (AirflowException,
                                AirflowSensorTimeout,
                                AirflowSkipException)
configuration.load_test_config()

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'


class TimeoutTestSensor(BaseSensorOperator):
    """
    Sensor that always returns the return_value provided

    :param return_value: Set to true to mark the task as SKIPPED on failure
    :type return_value: any
    """

    @apply_defaults
    def __init__(
            self,
            return_value=False,
            *args,
            **kwargs):
        self.return_value = return_value
        super(TimeoutTestSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        return self.return_value

    def execute(self, context):
        started_at = datetime.now()
        time_jump = self.params.get('time_jump')
        while not self.poke(context):
            if time_jump:
                started_at -= time_jump
            if (datetime.now() - started_at).total_seconds() > self.timeout:
                if self.soft_fail:
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
            time.sleep(self.poke_interval)
        logging.info("Success criteria met. Exiting.")


class SensorTimeoutTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_timeout(self):
        t = TimeoutTestSensor(
            task_id='test_timeout',
            execution_timeout=timedelta(days=2),
            return_value=False,
            poke_interval=5,
            params={'time_jump': timedelta(days=2, seconds=1)},
            dag=self.dag
            )
        self.assertRaises(
            AirflowSensorTimeout,
            t.run,
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


class HttpSensorTests(unittest.TestCase):

    def test_poke_exception(self):
        """
        Exception occurs in poke function should not be ignored.
        """
        def resp_check(resp):
            raise AirflowException('AirflowException raised here!')

        task = HttpSensor(
            task_id='http_sensor_poke_exception',
            http_conn_id='http_default',
            endpoint='',
            params={},
            response_check=resp_check,
            poke_interval=5)
        with self.assertRaisesRegexp(AirflowException, 'AirflowException raised here!'):
            task.execute(None)
