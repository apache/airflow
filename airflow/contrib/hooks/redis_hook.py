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

"""
RedisHook module
"""
from typing import Any, Optional

from redis import Redis
from redis.client import PubSub

from airflow.hooks.base_hook import BaseHook


# noinspection PyAbstractClass
class RedisHook(BaseHook):
    """
    Wrapper for connection to interact with Redis in-memory data structure store
    """
    def __init__(self, redis_conn_id='redis_default'):
        """
        Prepares hook to connect to a Redis database.

        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to Redis.
        """
        self.redis_conn_id = redis_conn_id
        self.redis = None
        self.host = None
        self.port = None
        self.password = None
        self.db = None

    def get_conn(self) -> Redis:
        """
        Returns a Redis connection.
        """
        conn = self.get_connection(self.redis_conn_id)
        self.host = conn.host
        self.port = conn.port
        self.password = None if str(conn.password).lower() in ['none', 'false', ''] else conn.password
        self.db = conn.extra_dejson.get('db', None)

        if not self.redis:
            self.log.debug(
                'Initializing redis object for conn_id "%s" on %s:%s:%s',
                self.redis_conn_id, self.host, self.port, self.db
            )
            self.redis = Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db)

        return self.redis

    def set_key_value(self, key: str, value: Any) -> Optional[bool]:
        """
        Create redis key ${k} with value ${v}
        """
        client = self.get_conn()

        return client.set(key, value)

    def get_key(self, key: str) -> Optional[Any]:
        """ Get value of key ${key} stored in Redis """
        client = self.get_conn()

        return client.get(key)

    def delete_key(self, key: str) -> int:
        """ Delete key ${k} from redis """
        client = self.get_conn()

        return client.delete(key)

    def publish_message(self, channel: str, message: str) -> int:
        """ Publish message ${message} to channel ${channel}"""
        client = self.get_conn()

        return client.publish(channel=channel, message=message)

    def check_if_key_exists(self, key: str) -> bool:
        """ Verify if key ${k} exists in Redis """
        client = self.get_conn()

        return client.exists(key)

    def create_pubsub(self) -> PubSub:
        """ Create pubsub object which can subscribe to Redis channel """
        client = self.get_conn()

        return client.pubsub()

    def ping_redis(self) -> Optional[bool]:
        """ Ping Redis to verify if connection is fine """
        client = self.get_conn()

        return client.ping()
