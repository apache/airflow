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
import logging
from airflow import DAG
from airflow import configuration
from airflow.contrib.sensors.redis_pub_sub_sensor import RedisPubSubSensor
from airflow.utils import timezone
from mock import patch
from mock import call

from unittest.mock import MagicMock

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestRedisPubSubSensor(unittest.TestCase):

    @patch('airflow.contrib.hooks.redis_hook.RedisHook')
    def setUp(self, mock_redis_hook):
        configuration.load_test_config()

        self.log = logging.getLogger()
        self.log.setLevel(logging.DEBUG)

        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.mock_redis_hook = mock_redis_hook
        self.mock_context = MagicMock()

        self.dag = DAG('test_dag_id', default_args=args)

        self.sensor = RedisPubSubSensor(
            task_id='test_task',
            dag=self.dag,
            channels='test',
            hook=self.mock_redis_hook
        )

    def test_poke_success(self):
        self.mock_redis_hook.get_conn().pubsub().get_message.return_value = \
            {'type': 'message', 'channel': b'test', 'data': b'd1'}

        result = self.sensor.poke(self.mock_context)
        self.assertTrue(result)

        hook_calls = [call.get_conn(), call.get_conn()]
        context_calls = [call.xcom_push(key='message',
                                        value={'type': 'message', 'channel': b'test', 'data': b'd1'})]

        self.assertTrue(self.mock_redis_hook.method_calls == hook_calls, "calls should be same")
        self.assertTrue(self.mock_context['ti'].method_calls == context_calls, "calls should be same")

    def test_poke_failed(self):
        self.mock_redis_hook.get_conn().pubsub().get_message.return_value = \
            {'type': 'subscribe', 'channel': b'test', 'data': b'd1'}

        result = self.sensor.poke(self.mock_context)
        self.assertFalse(result)

        hook_calls = [call.get_conn(), call.get_conn()]
        context_calls = []

        self.assertTrue(self.mock_redis_hook.method_calls == hook_calls, "calls should be same")
        self.assertTrue(self.mock_context['ti'].method_calls == context_calls, "calls should be same")


if __name__ == '__main__':
    unittest.main()
