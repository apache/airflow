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
from redis import Redis
from airflow.hooks.base_hook import BaseHook
from rediscluster import RedisCluster
import logging

logging.basicConfig()
logger = logging.getLogger('rediscluster')
logger.setLevel(logging.DEBUG)
logger.propagate = True


class RedisHook(BaseHook):
    """
    Wrapper for connection to interact with Redis in-memory data structure store
    """

    def __init__(self, redis_conn_id='redis_default', health_check_interval=0):
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
        self.health_check_interval = health_check_interval
        self.use_cluster = False
        self.startup_nodes = []

    def get_conn(self) -> Redis:
        """
        Returns a Redis connection.
        """
        conn = self.get_connection(self.redis_conn_id)
        self.host = conn.host
        self.port = conn.port
        self.password = None if str(conn.password).lower() in ['none', 'false', ''] else conn.password
        self.db = conn.extra_dejson.get('db', None)
        self.use_cluster = conn.extra_dejson.get('use_cluster', False)
        self.startup_nodes = conn.extra_dejson.get('startup_nodes', [])

        if not self.redis:
            self.log.debug(
                'Initializing redis object (cluster: {}) for conn_id "{}" on {}:{}:{}'
                    .format(self.use_cluster, self.redis_conn_id, self.host, self.port, self.db)
            )
            if not self.use_cluster or self.use_cluster == 'False':
                self.redis = Redis(
                    health_check_interval=self.health_check_interval,
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db)
            else:
                self.redis = RedisCluster(
                    host=self.host,
                    port=self.port,
                    startup_nodes=self.startup_nodes,
                    health_check_interval=self.health_check_interval,
                    password=self.password
                )

        return self.redis
