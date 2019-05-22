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
#

import unittest

from unittest.mock import patch

from airflow import AirflowException
from airflow.contrib.hooks.winrm_hook import WinRMHook
from airflow.models import Connection


class TestWinRMHook(unittest.TestCase):

    @patch('airflow.contrib.hooks.winrm_hook.Protocol')
    def test_get_conn_exists(self, mock_protocol):
        winrm_hook = WinRMHook()
        winrm_hook.client = mock_protocol.return_value.open_shell.return_value

        conn = winrm_hook.get_conn()

        self.assertEqual(conn, winrm_hook.client)

    def test_get_conn_missing_remote_host(self):
        with self.assertRaises(AirflowException):
            WinRMHook().get_conn()

    @patch('airflow.contrib.hooks.winrm_hook.Protocol')
    def test_get_conn_error(self, mock_protocol):
        mock_protocol.side_effect = Exception('Error')

        with self.assertRaises(AirflowException):
            WinRMHook(remote_host='host').get_conn()

    @patch('airflow.contrib.hooks.winrm_hook.Protocol')
    @patch('airflow.contrib.hooks.winrm_hook.WinRMHook.get_connection',
           return_value=Connection(
               login='username',
               password='password',
               host='remote_host',
               extra="""{
                   "endpoint": "endpoint",
                   "remote_port": 123,
                   "transport": "transport",
                   "service": "service",
                   "keytab": "keytab",
                   "ca_trust_path": "ca_trust_path",
                   "cert_pem": "cert_pem",
                   "cert_key_pem": "cert_key_pem",
                   "server_cert_validation": "server_cert_validation",
                   "kerberos_delegation": "true",
                   "read_timeout_sec": 123,
                   "operation_timeout_sec": 123,
                   "kerberos_hostname_override": "kerberos_hostname_override",
                   "message_encryption": "message_encryption",
                   "credssp_disable_tlsv1_2": "true",
                   "send_cbt": "false"
               }"""
           ))
    def test_get_conn_from_connection(self, mock_get_connection, mock_protocol):
        connection = mock_get_connection.return_value
        winrm_hook = WinRMHook(ssh_conn_id='conn_id')

        winrm_hook.get_conn()

        mock_get_connection.assert_called_once_with(winrm_hook.ssh_conn_id)
        mock_protocol.assert_called_once_with(
            endpoint=str(connection.extra_dejson['endpoint']),
            transport=str(connection.extra_dejson['transport']),
            username=connection.login,
            password=connection.password,
            service=str(connection.extra_dejson['service']),
            keytab=str(connection.extra_dejson['keytab']),
            ca_trust_path=str(connection.extra_dejson['ca_trust_path']),
            cert_pem=str(connection.extra_dejson['cert_pem']),
            cert_key_pem=str(connection.extra_dejson['cert_key_pem']),
            server_cert_validation=str(connection.extra_dejson['server_cert_validation']),
            kerberos_delegation=str(connection.extra_dejson['kerberos_delegation']).lower() == 'true',
            read_timeout_sec=int(connection.extra_dejson['read_timeout_sec']),
            operation_timeout_sec=int(connection.extra_dejson['operation_timeout_sec']),
            kerberos_hostname_override=str(connection.extra_dejson['kerberos_hostname_override']),
            message_encryption=str(connection.extra_dejson['message_encryption']),
            credssp_disable_tlsv1_2=str(connection.extra_dejson['credssp_disable_tlsv1_2']).lower() == 'true',
            send_cbt=str(connection.extra_dejson['send_cbt']).lower() == 'true'
        )

    @patch('airflow.contrib.hooks.winrm_hook.getpass.getuser', return_value='user')
    @patch('airflow.contrib.hooks.winrm_hook.Protocol')
    def test_get_conn_no_username(self, mock_protocol, mock_getuser):
        winrm_hook = WinRMHook(remote_host='host', password='password')

        winrm_hook.get_conn()

        self.assertEqual(mock_getuser.return_value, winrm_hook.username)

    @patch('airflow.contrib.hooks.winrm_hook.Protocol')
    def test_get_conn_no_endpoint(self, mock_protocol):
        winrm_hook = WinRMHook(remote_host='host', password='password')

        winrm_hook.get_conn()

        self.assertEqual('http://{0}:{1}/wsman'.format(winrm_hook.remote_host, winrm_hook.remote_port),
                         winrm_hook.endpoint)
