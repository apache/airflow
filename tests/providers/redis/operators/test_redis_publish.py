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
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

import pytest

from airflow.models.dag import DAG
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.redis.operators.redis_publish import RedisPublishOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


@pytest.mark.integration("redis")
class TestRedisPublishOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.dag = DAG("test_redis_dag_id", default_args=args)

        self.mock_context = MagicMock()
        self.channel = "test"

    def test_execute_hello(self):
        operator = RedisPublishOperator(
            task_id="test_task",
            dag=self.dag,
            message="hello",
            channel=self.channel,
            redis_conn_id="redis_default",
        )

        hook = RedisHook(redis_conn_id="redis_default")
        pubsub = hook.get_conn().pubsub()
        pubsub.subscribe(self.channel)

        operator.execute(self.mock_context)
        context_calls = []
        assert self.mock_context["ti"].method_calls == context_calls, "context calls should be same"

        message = pubsub.get_message()
        assert message["type"] == "subscribe"

        message = pubsub.get_message()
        assert message["type"] == "message"
        assert message["data"] == b"hello"

        pubsub.unsubscribe(self.channel)
