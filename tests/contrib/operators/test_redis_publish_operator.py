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
from airflow.contrib.operators.redis_publish_operator import RedisPublishOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.utils import timezone
from mock import MagicMock

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestRedisPublishOperator(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.dag = DAG('test_redis_dag_id', default_args=args)

        self.mock_context = MagicMock()
        self.channel = 'test'

    def test_execute_hello(self):
        operator = RedisPublishOperator(
            task_id='test_task',
            dag=self.dag,
            message='hello',
            channel=self.channel,
            redis_conn_id='redis_default'
        )

        hook = RedisHook(redis_conn_id='redis_default')
        pubsub = hook.get_conn().pubsub()
        pubsub.subscribe(self.channel)

        operator.execute(self.mock_context)
        context_calls = []
        self.assertTrue(self.mock_context['ti'].method_calls == context_calls, "context calls should be same")

        message = pubsub.get_message()
        self.assertEquals(message['type'], 'subscribe')

        message = pubsub.get_message()
        self.assertEquals(message['type'], 'message')
        self.assertEquals(message['data'], b'hello')

        pubsub.unsubscribe(self.channel)
