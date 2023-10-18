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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedisPubSubSensor(BaseSensorOperator):
    """
    Redis sensor for reading a message from pub sub channels.

    :param channels: The channels to be subscribed to (templated)
    :param redis_conn_id: the redis connection id
    """

    template_fields: Sequence[str] = ("channels",)
    ui_color = "#f0eee4"

    def __init__(self, *, channels: list[str] | str, redis_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.channels = channels
        self.redis_conn_id = redis_conn_id

    @cached_property
    def pubsub(self):
        hook = RedisHook(redis_conn_id=self.redis_conn_id).get_conn().pubsub()
        hook.subscribe(self.channels)
        return hook

    def poke(self, context: Context) -> bool:
        """
        Check for message on subscribed channels and write to xcom the message with key ``message``.

        An example of message ``{'type': 'message', 'pattern': None, 'channel': b'test', 'data': b'hello'}``

        :param context: the context object
        :return: ``True`` if message (with type 'message') is available or ``False`` if not
        """
        self.log.info("RedisPubSubSensor checking for message on channels: %s", self.channels)
        message = self.pubsub.get_message()
        self.log.info("Message %s from channel %s", message, self.channels)

        # Process only message types
        if message and message["type"] == "message":
            context["ti"].xcom_push(key="message", value=message)
            self.pubsub.unsubscribe(self.channels)

            return True

        return False
