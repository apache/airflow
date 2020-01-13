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
from datetime import datetime

from airflow import DAG
from airflow.models import DagBag
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.sensors.decorators.poke_mode_only import poke_mode_only

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
TEST_TASK_ID = 'time_sensor_check'
DEV_NULL = '/dev/null'


@poke_mode_only
class DummySensor(BaseSensorOperator):
    def __init__(self, poke_changes_mode=False, **kwargs):
        self.mode = kwargs['mode']
        super().__init__(**kwargs)
        self.poke_changes_mode = poke_changes_mode
        self.return_value = True

    def poke(self, context):
        if self.poke_changes_mode:
            self.change_mode('reschedule')
        return self.return_value

    def change_mode(self, mode):
        self.mode = mode


class TestPokeModeOnly(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(
            dag_folder=DEV_NULL,
            include_examples=True
        )
        self.args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)

    def test_poke_mode_only_allows_poke_mode(self):
        try:
            sensor = DummySensor(task_id='foo', mode='poke', poke_changes_mode=False,
                                 dag=self.dag)
        except ValueError:
            self.fail("__init__ failed with mode='poke'.")
        try:
            sensor.poke({})
        except ValueError:
            self.fail("poke failed without changing mode from 'poke'.")
        try:
            sensor.change_mode('poke')
        except ValueError:
            self.fail("class method failed without changing mode from 'poke'.")

    def test_poke_mode_only_bad_class_method(self):
        sensor = DummySensor(task_id='foo', mode='poke', poke_changes_mode=False,
                             dag=self.dag)
        with self.assertRaises(ValueError):
            sensor.change_mode('reschedule')

    def test_poke_mode_only_bad_init(self):
        with self.assertRaises(ValueError):
            DummySensor(task_id='foo', mode='reschedule',
                        poke_changes_mode=False, dag=self.dag)

    def test_poke_mode_only_bad_poke(self):
        sensor = DummySensor(task_id='foo', mode='poke', poke_changes_mode=True,
                             dag=self.dag)
        with self.assertRaises(ValueError):
            sensor.poke({})


if __name__ == "__main__":
    unittest.main()
