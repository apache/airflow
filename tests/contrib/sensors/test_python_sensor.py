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

from airflow import DAG, configuration
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import DagBag
from airflow.utils.timezone import datetime


DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'python_sensor_dag'
DEV_NULL = '/dev/null'


class PythonSensorTests(unittest.TestCase):

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

    def test_python_sensor_true(self):
        t = PythonSensor(
            task_id='python_sensor_check_true',
            python_callable=lambda: True,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_python_sensor_false(self):
        t = PythonSensor(
            task_id='python_sensor_check_false',
            timeout=1,
            python_callable=lambda: False,
            dag=self.dag)
        with self.assertRaises(AirflowSensorTimeout):
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_python_sensor_raise(self):
        t = PythonSensor(
            task_id='python_sensor_check_raise',
            python_callable=lambda: 1 / 0,
            dag=self.dag)
        with self.assertRaises(ZeroDivisionError):
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
