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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.redis.hooks.redis import RedisHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class RedisLPushOperator(BaseOperator):
    """
    Push a message to a Redis list using LPUSH.

    This operator pushes a message to the head (left) of a Redis list.
    Combined with BRPOP on the consumer side, this provides a FIFO queue pattern
    with durable, exactly-once delivery semantics.

    :param list_name: redis list to push the message to (templated)
    :param message: the message to push (templated)
    :param redis_conn_id: redis connection to use
    """

    template_fields: Sequence[str] = ("list_name", "message")

    def __init__(
        self, *, list_name: str, message: str, redis_conn_id: str = "redis_default", **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.redis_conn_id = redis_conn_id
        self.list_name = list_name
        self.message = message

    def execute(self, context: Context) -> int:
        """
        Push the message to Redis list.

        :param context: the context object
        :return: the length of the list after the push
        """
        redis_hook = RedisHook(redis_conn_id=self.redis_conn_id)

        self.log.info("Pushing message to Redis list %s", self.list_name)

        result = redis_hook.get_conn().lpush(self.list_name, self.message)

        self.log.info("List %s now has %s elements", self.list_name, result)
        return result
