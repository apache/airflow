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
import json
from unittest import mock
from urllib.parse import quote_plus, urlparse

import pyodbc

from airflow.models import Connection
from airflow.providers.odbc.hooks.odbc import OdbcHook


class TestOdbcHook:
    def get_hook(self=None, hook_params=None, conn_params=None):
        hook_params = hook_params or {}
        conn_params = conn_params or {}
        connection = Connection(
            **{
                **dict(login='login', password='password', host='host', schema='schema', port=1234),
                **conn_params,
            }
        )

        hook = OdbcHook(**hook_params)
        hook.get_connection = mock.Mock()
        hook.get_connection.return_value = connection
        return hook

    def test_driver_in_extra(self):
        conn_params = dict(extra=json.dumps(dict(Driver='Fake Driver', Fake_Param='Fake Param')))
        hook = self.get_hook(conn_params=conn_params)
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
        conn_params = dict(extra=json.dumps(dict(Driver='Fake Driver', Fake_Param='Fake Param')))
        hook_params = dict(driver='ParamDriver')
        hook = self.get_hook(hook_params=hook_params, conn_params=conn_params)
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
        conn_params = dict(extra=json.dumps(dict(DSN='MyDSN', Fake_Param='Fake Param')))
        hook = self.get_hook(conn_params=conn_params)
        expected = (
            'DSN=MyDSN;SERVER=host;DATABASE=schema;UID=login;PWD=password;Fake_Param=Fake Param;'
        )
        assert hook.odbc_connection_string == expected

    def test_dsn_in_both(self):
        conn_params = dict(extra=json.dumps(dict(DSN='MyDSN', Fake_Param='Fake Param')))
        hook_params = dict(driver='ParamDriver', dsn='ParamDSN')
        hook = self.get_hook(hook_params=hook_params, conn_params=conn_params)
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
        conn_params = dict(extra=json.dumps(dict(DSN='MyDSN', Fake_Param='Fake Param')))
        hook_params = dict(dsn='ParamDSN')
        hook = self.get_hook(hook_params=hook_params, conn_params=conn_params)
        uri_param = quote_plus(
            'DSN=ParamDSN;SERVER=host;DATABASE=schema;UID=login;PWD=password;Fake_Param=Fake Param;'
        )
        expected = 'mssql+pyodbc:///?odbc_connect=' + uri_param
        assert hook.get_uri() == expected

    def test_connect_kwargs_from_hook(self):
        hook = self.get_hook(
            hook_params=dict(
                connect_kwargs={
                    'attrs_before': {
                        1: 2,
                        pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED,
                    },
                    'readonly': True,
                    'autocommit': False,
                }
            ),
        )
        assert hook.connect_kwargs == {
            'attrs_before': {1: 2, pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED},
            'readonly': True,
            'autocommit': False,
        }

    def test_connect_kwargs_from_conn(self):
        extra = json.dumps(
            dict(
                connect_kwargs={
                    'attrs_before': {
                        1: 2,
                        pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED,
                    },
                    'readonly': True,
                    'autocommit': True,
                }
            )
        )

        hook = self.get_hook(conn_params=dict(extra=extra))
        assert hook.connect_kwargs == {
            'attrs_before': {1: 2, pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED},
            'readonly': True,
            'autocommit': True,
        }

    def test_connect_kwargs_from_conn_and_hook(self):
        """
        When connect_kwargs in both hook and conn, should be merged properly.
        Hook beats conn.
        """
        conn_extra = json.dumps(
            dict(connect_kwargs={'attrs_before': {1: 2, 3: 4}, 'readonly': False})
        )
        hook_params = dict(
            connect_kwargs={'attrs_before': {3: 5, pyodbc.SQL_TXN_ISOLATION: 0}, 'readonly': True}
        )

        hook = self.get_hook(conn_params=dict(extra=conn_extra), hook_params=hook_params)
        assert hook.connect_kwargs == {
            'attrs_before': {1: 2, 3: 5, pyodbc.SQL_TXN_ISOLATION: 0},
            'readonly': True,
        }

    def test_connect_kwargs_bool_from_uri(self):
        """
        Bools will be parsed from uri as strings
        """
        conn_extra = json.dumps(dict(connect_kwargs={'ansi': 'true'}))
        hook = self.get_hook(conn_params=dict(extra=conn_extra))
        assert hook.connect_kwargs == {
            'ansi': True,
        }

    def test_driver(self):
        hook = self.get_hook(hook_params=dict(driver='Blah driver'))
        assert hook.driver == 'Blah driver'
        hook = self.get_hook(hook_params=dict(driver='{Blah driver}'))
        assert hook.driver == 'Blah driver'
        hook = self.get_hook(conn_params=dict(extra='{"driver": "Blah driver"}'))
        assert hook.driver == 'Blah driver'
        hook = self.get_hook(conn_params=dict(extra='{"driver": "{Blah driver}"}'))
        assert hook.driver == 'Blah driver'

    def test_database(self):
        hook = self.get_hook(hook_params=dict(database='abc'))
        assert hook.database == 'abc'
        hook = self.get_hook()
        assert hook.database == 'schema'

    def test_sqlalchemy_scheme_default(self):
        hook = self.get_hook()
        uri = hook.get_uri()
        assert urlparse(uri).scheme == 'mssql+pyodbc'

    def test_sqlalchemy_scheme_param(self):
        hook = self.get_hook(hook_params=dict(sqlalchemy_scheme='my-scheme'))
        uri = hook.get_uri()
        assert urlparse(uri).scheme == 'my-scheme'

    def test_sqlalchemy_scheme_extra(self):
        hook = self.get_hook(conn_params=dict(extra=json.dumps(dict(sqlalchemy_scheme='my-scheme'))))
        uri = hook.get_uri()
        assert urlparse(uri).scheme == 'my-scheme'
