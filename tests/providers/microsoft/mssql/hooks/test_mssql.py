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

from unittest import mock

from airflow import PY38
from airflow.models import Connection

if not PY38:
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

PYMSSQL_CONN = Connection(host='ip', schema='share', login='username', password='password', port=8081)


class TestMsSqlHook(unittest.TestCase):
    @unittest.skipIf(PY38, "Mssql package not available when Python >= 3.8.")
    @mock.patch('airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn')
    @mock.patch('airflow.hooks.dbapi_hook.DbApiHook.get_connection')
    def test_get_conn_should_return_connection(self, get_connection, mssql_get_conn):
        get_connection.return_value = PYMSSQL_CONN
        mssql_get_conn.return_value = mock.Mock()

        hook = MsSqlHook()
        conn = hook.get_conn()

        self.assertEqual(mssql_get_conn.return_value, conn)
        mssql_get_conn.assert_called_once()

    @unittest.skipIf(PY38, "Mssql package not available when Python >= 3.8.")
    @mock.patch('airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn')
    @mock.patch('airflow.hooks.dbapi_hook.DbApiHook.get_connection')
    def test_set_autocommit_should_invoke_autocommit(self, get_connection, mssql_get_conn):
        get_connection.return_value = PYMSSQL_CONN
        mssql_get_conn.return_value = mock.Mock()
        autocommit_value = mock.Mock()

        hook = MsSqlHook()
        conn = hook.get_conn()

        hook.set_autocommit(conn, autocommit_value)
        mssql_get_conn.assert_called_once()
        mssql_get_conn.return_value.autocommit.assert_called_once_with(autocommit_value)

    @unittest.skipIf(PY38, "Mssql package not available when Python >= 3.8.")
    @mock.patch('airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn')
    @mock.patch('airflow.hooks.dbapi_hook.DbApiHook.get_connection')
    def test_get_autocommit_should_return_autocommit_state(self, get_connection, mssql_get_conn):
        get_connection.return_value = PYMSSQL_CONN
        mssql_get_conn.return_value = mock.Mock()
        mssql_get_conn.return_value.autocommit_state = 'autocommit_state'

        hook = MsSqlHook()
        conn = hook.get_conn()

        mssql_get_conn.assert_called_once()
        self.assertEqual(hook.get_autocommit(conn), 'autocommit_state')
