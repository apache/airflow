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

import time

import pytest

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.redis.queues.redis import RedisPubSubMessageQueueProvider


@pytest.mark.integration("redis")
class TestRedisPubSubMessageQueueProviderIntegration:
    def setup_method(self):
        self.redis_hook = RedisHook(redis_conn_id="redis_default")
        self.redis = self.redis_hook.get_conn()
        self.provider = RedisPubSubMessageQueueProvider()
        self.channel = "test_pubsub_channel"

    def test_pubsub_send_and_receive(self):
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self.channel)

        test_message = "airflow-pubsub-integration-message"
        self.redis.publish(self.channel, test_message)

        received = None
        for _ in range(10):
            message = pubsub.get_message()
            if message and message["type"] == "message":
                received = message["data"]
                break
            time.sleep(0.1)

        assert received == test_message.encode(), f"Expected {test_message!r}, got {received!r}"

        pubsub.unsubscribe(self.channel)

    def test_queue_matches(self):
        assert self.provider.scheme_matches("redis+pubsub")
