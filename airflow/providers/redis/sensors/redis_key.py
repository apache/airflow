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

from typing import TYPE_CHECKING, Sequence

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedisKeySensor(BaseSensorOperator):
    """Checks for the existence of a key in a Redis"""

    template_fields: Sequence[str] = ('key',)
    ui_color = '#f0eee4'

    def __init__(self, *, key: str, redis_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.redis_conn_id = redis_conn_id
        self.key = key

    def poke(self, context: Context) -> bool:
        self.log.info('Sensor checks for existence of key: %s', self.key)
        return RedisHook(self.redis_conn_id).get_conn().exists(self.key)
