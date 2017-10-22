# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
RedisHook module
"""
from redis import StrictRedis

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class RedisHook(BaseHook, LoggingMixin):
    """
    Hook to interact with Redis database
    """
    def __init__(self, redis_conn_id='redis_default'):
        """
        Prepares hook to connect to a Redis database.

        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to Redis.
        """
        self.redis_conn_id = redis_conn_id
        self.client = None
        conn = self.get_connection(self.redis_conn_id)
        self.host = conn.host
        self.port = int(conn.port)
        self.password = conn.password
        self.db = int(conn.extra_dejson.get('db', 0))

        self.log.debug(
            '''Connection "{conn}":
            \thost: {host}
            \tport: {port}
            \textra: {extra}
            '''.format(
                conn=self.redis_conn_id,
                host=self.host,
                port=self.port,
                extra=conn.extra_dejson
            )
        )

    def get_conn(self):
        """
        Returns a Redis connection.
        """
        if not self.client:
            self.log.debug(
                'generating Redis client for conn_id "%s" on %s:%s:%s',
                self.redis_conn_id, self.host, self.port, self.db
            )
            try:
                self.client = StrictRedis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db)
            except Exception as general_error:
                raise AirflowException(
                    'Failed to create Redis client, error: {error}'.format(
                        error=str(general_error)
                    )
                )

        return self.client

    def key_exists(self, key):
        """
        Checks if a key exists in Redis database

        :param key: The key to check the existence.
        :type key: string
        """
        return self.get_conn().exists(key)
