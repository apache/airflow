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
from unittest.mock import patch

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.cloudant.hooks.cloudant import CloudantHook


class TestCloudantHook(unittest.TestCase):
    def setUp(self):
        self.cloudant_hook = CloudantHook()

    @patch(
        'airflow.providers.cloudant.hooks.cloudant.CloudantHook.get_connection',
        return_value=Connection(login='user', password='password', host='account'),
    )
    @patch('airflow.providers.cloudant.hooks.cloudant.cloudant')
    def test_get_conn(self, mock_cloudant, mock_get_connection):
        cloudant_session = self.cloudant_hook.get_conn()

        conn = mock_get_connection.return_value
        mock_cloudant.assert_called_once_with(user=conn.login, passwd=conn.password, account=conn.host)
        self.assertEqual(cloudant_session, mock_cloudant.return_value)

    @patch(
        'airflow.providers.cloudant.hooks.cloudant.CloudantHook.get_connection',
        return_value=Connection(login='user'),
    )
    def test_get_conn_invalid_connection(self, mock_get_connection):
        with self.assertRaises(AirflowException):
            self.cloudant_hook.get_conn()
