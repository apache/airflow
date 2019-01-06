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

from rediscluster import StrictRedisCluster
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class RedisClusterHook(BaseHook, LoggingMixin):
    """
    Hook to interact with Redis Cluster

    Note: If the Redis Cluster is launched in AWS Elasticache,
    the hook must be initialized with skip_full_coverage_check=True option.
    This is because AWS disallowed the CONFIG command in the Redis Cluster.
    """
    def __init__(self, redis_cluster_conn_id='redis_cluster_default',
                 skip_full_coverage_check=False, **kwargs):
        """
        Prepares hook to connect to a Redis Cluster.

        :param redis_cluster_conn_id: the name of the connection that has the
            parameters we need to connect to Redis Cluster.
        :type redis_cluster_conn_id: str
        :param skip_full_coverage_check: If True, the hook will skip the check
            of 'cluster-require-full-coverage' config, useful for clusters
            without the CONFIG command.
        :type skip_full_coverage_check: bool
        :param kwargs: optional arguments to pass to `StrictRedisCluster()`.
        :type kwargs: object
        """
        self.redis_cluster_conn_id = redis_cluster_conn_id
        self.client = None
        conn = self.get_connection(self.redis_cluster_conn_id)
        self.startup_nodes = conn.extra_dejson.get('startup_nodes', [])
        if conn.host and conn.port:
            self.startup_nodes.append({
                "host": conn.host,
                "port": int(conn.port)
            })
        self.skip_full_coverage_check = skip_full_coverage_check
        self.kwargs = kwargs

        self.log.info(
            '''Connection "{conn}":
            \tstartup_nodes: {startup_nodes}
            '''.format(
                conn=self.redis_cluster_conn_id,
                startup_nodes=self.startup_nodes
            )
        )

    def get_conn(self):
        """
        Returns a StrictRedisCluster connection.
        """
        if not self.client:
            self.log.info(
                'generating StrictRedisCluster client for conn_id "%s" with startup_nodes = %s',
                self.redis_cluster_conn_id, self.startup_nodes
            )
            try:
                self.client = StrictRedisCluster(
                    startup_nodes=self.startup_nodes,
                    skip_full_coverage_check=self.skip_full_coverage_check,
                    **self.kwargs
                )
            except Exception as general_error:
                raise AirflowException(
                    'Failed to create StrictRedisCluster client, error: {error}'.format(
                        error=str(general_error)
                    )
                )

        return self.client
