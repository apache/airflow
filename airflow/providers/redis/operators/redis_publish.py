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

from typing import Dict

from airflow.models import BaseOperator
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.decorators import apply_defaults


class RedisPublishOperator(BaseOperator):
    """
    Publish a message to Redis.

    :param channel: redis channel to which the message is published (templated)
    :type channel: str
    :param message: the message to publish (templated)
    :type message: str
    :param redis_conn_id: redis connection to use
    :type redis_conn_id: str
    """

    template_fields = ('channel', 'message')

    @apply_defaults
    def __init__(self, *, channel: str, message: str, redis_conn_id: str = 'redis_default', **kwargs) -> None:

        super().__init__(**kwargs)
        self.redis_conn_id = redis_conn_id
        self.channel = channel
        self.message = message

    def execute(self, context: Dict) -> None:
        """
        Publish the message to Redis channel

        :param context: the context object
        :type context: dict
        """
        redis_hook = RedisHook(redis_conn_id=self.redis_conn_id)

        self.log.info('Sending message %s to Redis on channel %s', self.message, self.channel)

        result = redis_hook.get_conn().publish(channel=self.channel, message=self.message)

        self.log.info('Result of publishing %s', result)
