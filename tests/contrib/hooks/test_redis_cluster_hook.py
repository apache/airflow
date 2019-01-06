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


import unittest
from airflow import configuration
from airflow.contrib.hooks.redis_cluster_hook import RedisClusterHook


class TestRedisClusterHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

    def test_get_conn(self):
        hook = RedisClusterHook(redis_cluster_conn_id='redis_cluster_default')
        self.assertEqual(hook.client, None)
        self.assertEqual(hook.skip_full_coverage_check, False)

        self.assertEqual(
            hook.startup_nodes,
            [{'host': 'redis-cluster', 'port': 7000}],
            'startup_nodes is correctly initialized.'
        )
        self.assertIs(hook.get_conn(), hook.get_conn(), 'Connection initialized only if None.')

    def test_real_ping(self):
        hook = RedisClusterHook(redis_cluster_conn_id='redis_cluster_default')
        redis_cluster = hook.get_conn()

        pong = redis_cluster.ping()
        self.assertTrue(all(pong.values()), 'Connection to Redis Cluster with PING works.')

    def test_real_get_and_set(self):
        hook = RedisClusterHook(
            redis_cluster_conn_id='redis_cluster_default',
            skip_full_coverage_check=True
        )
        redis_cluster = hook.get_conn()

        self.assertTrue(
            redis_cluster.set('test_key', 'test_value'),
            'Connection to Redis Cluster with SET works.'
        )
        self.assertEqual(
            redis_cluster.get('test_key'),
            b'test_value',
            'Connection to Redis Cluster with GET works.'
        )
        self.assertEqual(
            redis_cluster.delete('test_key'),
            1,
            'Connection to Redis Cluster with DELETE works.'
        )


if __name__ == '__main__':
    unittest.main()
