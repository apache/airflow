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
import json
import unittest
from unittest import mock

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.redshift_statement import RedshiftStatementHook


class TestRedshiftStatementHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(
            login='login', password='password', host='host', port=5439, extra=json.dumps({"database": "dev"})
        )

        class UnitTestRedshiftStatementHook(RedshiftStatementHook):
            conn_name_attr = "redshift_conn_id"

        self.db_hook = UnitTestRedshiftStatementHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    def test_get_uri(self):
        uri_shouldbe = 'redshift://login:password@host:5439/dev'
        x = self.db_hook.get_uri()
        assert uri_shouldbe == x

    @mock.patch('airflow.providers.amazon.aws.hooks.redshift_statement.redshift_connector.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user='login', password='password', host='host', port=5439, database='dev'
        )

    @mock.patch('airflow.providers.amazon.aws.hooks.redshift_statement.redshift_connector.connect')
    def test_get_conn_extra(self, mock_connect):
        self.connection.extra = json.dumps(
            {
                "iam": True,
                "cluster_identifier": "my-test-cluster",
                "profile": "default",
                "database": "different",
            }
        )
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user='login',
            password='password',
            host='host',
            port=5439,
            cluster_identifier="my-test-cluster",
            profile="default",
            database='different',
            iam=True,
        )
