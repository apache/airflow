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
import mock

from airflow import configuration
from airflow.contrib.hooks.cassandra_hook import CassandraHook
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy
from airflow import models
from airflow.utils import db


class CassandraHookTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='cassandra_test', conn_type='cassandra',
                host='host-1,host-2', port='9042', schema='test_keyspace',
                extra='{"load_balancing_policy":"TokenAwarePolicy"'))

    def test_get_conn(self):
        with mock.patch.object(Cluster, "connect") as mock_connect, \
                mock.patch("socket.getaddrinfo", return_value=[]) as mock_getaddrinfo:
            mock_connect.return_value = 'session'
            hook = CassandraHook(cassandra_conn_id='cassandra_test')
            hook.get_conn()
            mock_getaddrinfo.assert_called()
            mock_connect.assert_called_once_with('test_keyspace')

            cluster = hook.get_cluster()
            self.assertEqual(cluster.contact_points, ['host-1', 'host-2'])
            self.assertEqual(cluster.port, 9042)
            self.assertTrue(isinstance(cluster.load_balancing_policy, TokenAwarePolicy))


if __name__ == '__main__':
    unittest.main()
