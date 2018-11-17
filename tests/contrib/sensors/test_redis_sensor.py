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

from mock import patch

from airflow import DAG
from airflow import configuration
from airflow.contrib.sensors.redis_key_sensor import RedisKeySensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestRedisSensor(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.dag = DAG('test_dag_id', default_args=args)
        self.sensor = RedisKeySensor(
            task_id='test_task',
            redis_conn_id='redis_default',
            dag=self.dag,
            key='test_key'
        )

    @patch("airflow.contrib.hooks.redis_hook.RedisHook.key_exists")
    def test_poke(self, key_exists):
        key_exists.return_value = True
        self.assertTrue(self.sensor.poke(None))

        key_exists.return_value = False
        self.assertFalse(self.sensor.poke(None))

    @patch("airflow.contrib.hooks.redis_hook.Redis.exists")
    def test_existing_key_called(self, redis_client_exists):
        self.sensor.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE, ignore_ti_state=True
        )

        self.assertTrue(redis_client_exists.called_with('test_key'))


if __name__ == '__main__':
    unittest.main()
