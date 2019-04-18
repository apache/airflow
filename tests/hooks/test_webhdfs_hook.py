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

from hdfs import HdfsError
from mock import patch, call

from airflow.hooks.webhdfs_hook import WebHDFSHook, AirflowWebHDFSHookException
from airflow.models.connection import Connection


class TestWebHDFSHook(unittest.TestCase):

    def setUp(self):
        self.webhdfs_hook = WebHDFSHook()

    @patch('airflow.hooks.webhdfs_hook.InsecureClient')
    @patch('airflow.hooks.webhdfs_hook.WebHDFSHook.get_connections', return_value=[
        Connection(host='host_1', port=123),
        Connection(host='host_2', port=321, login='user')
    ])
    def test_get_conn(self, mock_get_connections, mock_insecure_client):
        mock_insecure_client.side_effect = [HdfsError('Error'), mock_insecure_client.return_value]
        conn = self.webhdfs_hook.get_conn()

        mock_insecure_client.assert_has_calls([
            call('http://{host}:{port}'.format(host=connection.host, port=connection.port),
                 user=connection.login)
            for connection in mock_get_connections.return_value
        ])
        mock_insecure_client.return_value.status.assert_called_once_with('/')
        self.assertEqual(conn, mock_insecure_client.return_value)

    @patch('airflow.hooks.webhdfs_hook.KerberosClient', create=True)
    @patch('airflow.hooks.webhdfs_hook.WebHDFSHook.get_connections', return_value=[
        Connection(host='host_1', port=123)
    ])
    @patch('airflow.hooks.webhdfs_hook._kerberos_security_mode', return_value=True)
    def test_get_conn_kerberos_security_mode(self,
                                             mock_kerberos_security_mode,
                                             mock_get_connections,
                                             mock_kerberos_client):
        conn = self.webhdfs_hook.get_conn()

        connection = mock_get_connections.return_value[0]
        mock_kerberos_client.assert_called_once_with(
            'http://{host}:{port}'.format(host=connection.host, port=connection.port))
        self.assertEqual(conn, mock_kerberos_client.return_value)

    @patch('airflow.hooks.webhdfs_hook.WebHDFSHook.get_connections', return_value=[])
    def test_get_conn_no_connection_found(self, mock_get_connection):
        with self.assertRaises(AirflowWebHDFSHookException):
            self.webhdfs_hook.get_conn()

    @patch('airflow.hooks.webhdfs_hook.WebHDFSHook.get_conn')
    def test_check_for_path(self, mock_get_conn):
        hdfs_path = 'path'

        exists_path = self.webhdfs_hook.check_for_path(hdfs_path)

        mock_get_conn.assert_called_once_with()
        mock_status = mock_get_conn.return_value.status
        mock_status.assert_called_once_with(hdfs_path, strict=False)
        self.assertEqual(exists_path, bool(mock_status.return_value))

    @patch('airflow.hooks.webhdfs_hook.WebHDFSHook.get_conn')
    def test_load_file(self, mock_get_conn):
        source = 'source'
        destination = 'destination'

        self.webhdfs_hook.load_file(source, destination)

        mock_get_conn.assert_called_once_with()
        mock_upload = mock_get_conn.return_value.upload
        mock_upload.assert_called_once_with(
            hdfs_path=destination,
            local_path=source,
            overwrite=True,
            n_threads=1
        )
