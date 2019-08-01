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

from unittest.mock import patch, Mock
from simple_salesforce import Salesforce

from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.models.connection import Connection


class TestSalesforceHook(unittest.TestCase):

    def setUp(self):
        self.salesforce_hook = SalesforceHook(conn_id='conn_id')

    def test_get_conn_exists(self):
        self.salesforce_hook.conn = Mock(spec=Salesforce)

        self.salesforce_hook.get_conn()

        self.assertIsNotNone(self.salesforce_hook.conn.return_value)

    @patch('airflow.contrib.hooks.salesforce_hook.SalesforceHook.get_connection',
           return_value=Connection(
               login='username',
               password='password',
               extra='{"security_token": "token", "sandbox": "true"}'
           ))
    @patch('airflow.contrib.hooks.salesforce_hook.Salesforce')
    def test_get_conn(self, mock_salesforce, mock_get_connection):
        self.salesforce_hook.get_conn()

        self.assertEqual(self.salesforce_hook.conn, mock_salesforce.return_value)
        mock_salesforce.assert_called_once_with(
            username=mock_get_connection.return_value.login,
            password=mock_get_connection.return_value.password,
            security_token=mock_get_connection.return_value.extra_dejson['security_token'],
            instance_url=mock_get_connection.return_value.host,
            sandbox=mock_get_connection.return_value.extra_dejson.get('sandbox', False)
        )

    @patch('airflow.contrib.hooks.salesforce_hook.Salesforce')
    def test_make_query(self, mock_salesforce):
        mock_salesforce.return_value.query_all.return_value = dict(totalSize=123, done=True)
        self.salesforce_hook.conn = mock_salesforce.return_value
        query = 'SELECT * FROM table'

        query_results = self.salesforce_hook.make_query(query)

        mock_salesforce.return_value.query_all.assert_called_once_with(query)
        self.assertEqual(query_results, mock_salesforce.return_value.query_all.return_value)

        query_results = self.salesforce_hook.make_query(query, include_deleted=True)

        mock_salesforce.return_value.query_all.assert_called_once_with(query)
        self.assertEqual(query_results, mock_salesforce.return_value.query_all.return_value)
