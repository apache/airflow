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

import json
import unittest
from unittest import mock

from airflow.models import Connection
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook

try:
    import snakebite
    snakebite_loaded = True
except ImportError:
    snakebite_loaded = False

if not snakebite_loaded:
    raise unittest.SkipTest(
        "Skipping test because HDFSHook is not installed"
    )


class TestHDFSHook(unittest.TestCase):
    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_HDFS_DEFAULT': 'hdfs://localhost:8020',
    })
    def test_get_client(self):
        client = HDFSHook(proxy_user='foo').get_conn()
        self.assertIsInstance(client, snakebite.client.Client)
        self.assertEqual('localhost', client.host)
        self.assertEqual(8020, client.port)
        self.assertEqual('foo', client.service.channel.effective_user)

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_HDFS_DEFAULT': 'hdfs://localhost:8020',
    })
    @mock.patch('airflow.providers.apache.hdfs.hooks.hdfs.AutoConfigClient')
    @mock.patch('airflow.providers.apache.hdfs.hooks.hdfs.HDFSHook.get_connections')
    def test_get_autoconfig_client(self, mock_get_connections, mock_client):
        conn = Connection(
            conn_id='hdfs',
            conn_type='hdfs',
            host='localhost',
            port=8020,
            login='foo',
            extra=json.dumps({'autoconfig': True})
        )
        mock_get_connections.return_value = [conn]
        HDFSHook(hdfs_conn_id='hdfs').get_conn()
        mock_client.assert_called_once_with(effective_user='foo', use_sasl=False)

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_HDFS_DEFAULT': 'hdfs://localhost:8020',
    })
    @mock.patch('airflow.providers.apache.hdfs.hooks.hdfs.AutoConfigClient')
    def test_get_autoconfig_client_no_conn(self, mock_client):
        HDFSHook(hdfs_conn_id='hdfs_missing', autoconfig=True).get_conn()
        mock_client.assert_called_once_with(effective_user=None, use_sasl=False)

    @mock.patch('airflow.providers.apache.hdfs.hooks.hdfs.HDFSHook.get_connections')
    def test_get_ha_client(self, mock_get_connections):
        conn_1 = Connection(
            conn_id='hdfs_default',
            conn_type='hdfs',
            host='localhost',
            port=8020
        )
        conn_2 = Connection(
            conn_id='hdfs_default',
            conn_type='hdfs',
            host='localhost2',
            port=8020
        )
        mock_get_connections.return_value = [conn_1, conn_2]
        client = HDFSHook().get_conn()
        self.assertIsInstance(client, snakebite.client.HAClient)
