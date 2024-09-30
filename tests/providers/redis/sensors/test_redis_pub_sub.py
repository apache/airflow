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

from unittest.mock import MagicMock, call, patch

from airflow.models.dag import DAG
from airflow.providers.redis.sensors.redis_pub_sub import RedisPubSubSensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestRedisPubSubSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

        self.mock_context = MagicMock()

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_conn")
    def test_poke_mock_true(self, mock_redis_conn):
        sensor = RedisPubSubSensor(
            task_id="test_task", dag=self.dag, channels="test", redis_conn_id="redis_default"
        )

        mock_redis_conn().pubsub().get_message.return_value = {
            "type": "message",
            "channel": b"test",
            "data": b"d1",
        }

        result = sensor.poke(self.mock_context)
        assert result

        context_calls = [
            call.xcom_push(key="message", value={"type": "message", "channel": b"test", "data": b"d1"})
        ]

        assert self.mock_context["ti"].method_calls == context_calls, "context call  should be same"

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_conn")
    def test_poke_mock_false(self, mock_redis_conn):
        sensor = RedisPubSubSensor(
            task_id="test_task", dag=self.dag, channels="test", redis_conn_id="redis_default"
        )

        mock_redis_conn().pubsub().get_message.return_value = {
            "type": "subscribe",
            "channel": b"test",
            "data": b"d1",
        }

        result = sensor.poke(self.mock_context)
        assert not result

        context_calls = []
        assert self.mock_context["ti"].method_calls == context_calls, "context calls should be same"

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_conn")
    def test_poke_subscribe_called_only_once(self, mock_redis_conn):
        sensor = RedisPubSubSensor(
            task_id="test_task", dag=self.dag, channels="test", redis_conn_id="redis_default"
        )

        mock_redis_conn().pubsub().get_message.return_value = {
            "type": "subscribe",
            "channel": b"test",
            "data": b"d1",
        }

        result = sensor.poke(self.mock_context)
        assert not result

        context_calls = []
        assert self.mock_context["ti"].method_calls == context_calls, "context calls should be same"
        result = sensor.poke(self.mock_context)

        assert mock_redis_conn().pubsub().subscribe.call_count == 1
