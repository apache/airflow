# pylint: disable=c-extension-no-member
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
from unittest import mock
from urllib.parse import quote_plus, urlparse

import pyodbc
from parameterized import parameterized

from airflow.models import Connection
from airflow.providers.odbc.hooks.odbc import OdbcHook


class TestOdbcHook(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.conn = conn = mock.MagicMock()
        self.conn.login = 'login'
        self.conn.password = 'password'
        self.conn.host = 'host'
        self.conn.schema = 'schema'
        self.conn.port = 1234

        class UnitTestOdbcHook(OdbcHook):
            conn_name_attr = 'test_conn_id'

            @property
            def connection(self) -> Connection:
                return conn

        self.db_hook = UnitTestOdbcHook

    def test_driver_in_extra(self):
        self.conn.extra_dejson = dict(Driver='Fake Driver', Fake_Param='Fake Param')
        hook = self.db_hook()
        expected = (
            'DRIVER={Fake Driver};'
            'SERVER=host;'
            'DATABASE=schema;'
            'UID=login;'
            'PWD=password;'
            'Fake_Param=Fake Param;'
        )
        assert hook.odbc_connection_string == expected

    def test_driver_in_both(self):
        self.conn.extra_dejson = dict(Driver='Fake Driver', Fake_Param='Fake Param')
        hook_params = {'driver': 'ParamDriver'}
        hook = self.db_hook(**hook_params)
        expected = (
            'DRIVER={ParamDriver};'
            'SERVER=host;'
            'DATABASE=schema;'
            'UID=login;'
            'PWD=password;'
            'Fake_Param=Fake Param;'
        )
        assert hook.odbc_connection_string == expected

    def test_dsn_in_extra(self):
        self.conn.extra_dejson = dict(DSN='MyDSN', Fake_Param='Fake Param')
        hook = self.db_hook()
        expected = 'DSN=MyDSN;SERVER=host;DATABASE=schema;UID=login;PWD=password;Fake_Param=Fake Param;'
        assert hook.odbc_connection_string == expected

    def test_dsn_in_both(self):
        self.conn.extra_dejson = {'DSN': 'MyDSN', 'Fake_Param': 'Fake Param'}
        hook_params = {'driver': 'ParamDriver', 'dsn': 'ParamDSN'}
        hook = self.db_hook(**hook_params)
        expected = (
            'DRIVER={ParamDriver};'
            'DSN=ParamDSN;'
            'SERVER=host;'
            'DATABASE=schema;'
            'UID=login;'
            'PWD=password;'
            'Fake_Param=Fake Param;'
        )
        assert hook.odbc_connection_string == expected

    def test_get_uri(self):
        self.conn.extra_dejson = {'DSN': 'MyDSN', 'Fake_Param': 'Fake Param'}
        hook_params = {'dsn': 'ParamDSN'}
        hook = self.db_hook(**hook_params)
        uri_param = quote_plus(
            'DSN=ParamDSN;SERVER=host;DATABASE=schema;UID=login;PWD=password;Fake_Param=Fake Param;'
        )
        expected = 'mssql+pyodbc:///?odbc_connect=' + uri_param
        assert hook.get_uri() == expected

    def test_connect_kwargs_from_hook(self):
        connect_kwargs = {
            'attrs_before': {
                1: 2,
                pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED
            },
            'readonly': True,
            'autocommit': False,
        }
        hook_params = {'connect_kwargs': connect_kwargs}
        hook = self.db_hook(**hook_params)
        assert hook.connect_kwargs == connect_kwargs

    def test_connect_kwargs_from_conn(self):
        connect_kwargs = {
            'attrs_before': {
                1: 2,
                pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED
            },
            'readonly': True,
            'autocommit': True,
        }
        self.conn.extra_dejson = {'connect_kwargs': connect_kwargs}
        hook = self.db_hook()
        assert hook.connect_kwargs == connect_kwargs

    def test_connect_kwargs_from_conn_and_hook(self):
        """
        When connect_kwargs in both hook and conn, should be merged properly.
        Hook beats conn.
        """
        conn_extra = json.dumps(dict(connect_kwargs={'attrs_before': {1: 2, 3: 4}, 'readonly': False}))
        hook_params = dict(
            connect_kwargs={'attrs_before': {3: 5, pyodbc.SQL_TXN_ISOLATION: 0}, 'readonly': True}
        )

        hook = self.get_hook(conn_params=dict(extra=conn_extra), hook_params=hook_params)
        assert hook.connect_kwargs == {
            'attrs_before': {1: 2, 3: 5, pyodbc.SQL_TXN_ISOLATION: 0},
            'readonly': True,
        }
        self.conn.extra_dejson = {'connect_kwargs': connect_kwargs}
        hook = self.db_hook(**hook_params)
        assert hook.connect_kwargs == expect

    def test_connect_kwargs_bool_from_uri(self):
        """
        Bools will be parsed from uri as strings
        """
        connect_kwargs = {'ansi': True}
        self.conn.extra_dejson = {'connect_kwargs': connect_kwargs}
        hook = self.db_hook()
        assert hook.connect_kwargs == connect_kwargs

    @parameterized.expand([
        ({'driver': 'Blah driver'}, 'Blah driver'),
        ({'driver': '{Blah driver}'}, 'Blah driver'),
    ])
    def test_driver(self, params, driver):
        hook = self.db_hook(**params)
        assert hook.driver == driver
        self.conn.extra_dejson = params
        hook = self.db_hook()
        assert hook.driver == driver

    @parameterized.expand([
        ({'database': 'abc'}, 'abc'),
        ({}, 'schema'),
    ])
    def test_database(self, hook_params, db):
        hook = self.db_hook(**hook_params)
        assert hook.database == db

    def test_sqlalchemy_scheme_default(self):
        hook = self.db_hook()
        uri = hook.get_uri()
        assert urlparse(uri).scheme == 'mssql+pyodbc'

    def test_sqlalchemy_scheme_param(self):
        hook_params = {'sqlalchemy_scheme': 'my-scheme'}
        hook = self.db_hook(**hook_params)
        uri = hook.get_uri()
        assert urlparse(uri).scheme == 'my-scheme'

    def test_sqlalchemy_scheme_extra(self):
        self.conn.extra_dejson = {'sqlalchemy_scheme': 'my-scheme'}
        hook = self.db_hook()
        uri = hook.get_uri()
        assert urlparse(uri).scheme == 'my-scheme'
