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
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from unittest import mock
from unittest.mock import patch
from urllib.parse import quote_plus, urlsplit

import pyodbc
import pytest

from airflow.models import Connection
from airflow.providers.odbc.hooks.odbc import OdbcHook


@pytest.fixture
def mock_row():
    """
    Mock a pyodbc.Row object - This is a C object that can only be created from C API of pyodbc.
    This mock implements the two features used by the hook:
        - cursor_description: which return column names and type
        - __iter__: which allows exploding a row instance (*row)
    """

    @dataclass
    class Row:
        key: int
        column: str

        def __iter__(self):
            yield self.key
            yield self.column

        @property
        def cursor_description(self):
            return [
                ("key", int, None, 11, 11, 0, None),
                ("column", str, None, 256, 256, 0, None),
            ]

    return Row


class TestOdbcHook:
    def get_hook(self=None, hook_params=None, conn_params=None):
        hook_params = hook_params or {}
        conn_params = conn_params or {}
        connection = Connection(
            **{
                **dict(login="login", password="password", host="host", schema="schema", port=1234),
                **conn_params,
            }
        )

        cursor = mock.MagicMock(
            rowcount=0, spec=["description", "rowcount", "execute", "fetchall", "fetchone", "close"]
        )
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor

        class UnitTestOdbcHook(OdbcHook):
            conn_name_attr = "test_conn_id"

            @classmethod
            def get_connection(cls, conn_id: str):
                return connection

            def get_conn(self):
                return conn

        return UnitTestOdbcHook(**hook_params)

    def test_driver_in_extra_not_used(self):
        conn_params = dict(extra=json.dumps(dict(Driver="Fake Driver", Fake_Param="Fake Param")))
        hook_params = {"driver": "ParamDriver"}
        hook = self.get_hook(conn_params=conn_params, hook_params=hook_params)
        expected = (
            "DRIVER={ParamDriver};"
            "SERVER=host;"
            "DATABASE=schema;"
            "UID=login;"
            "PWD=password;"
            "PORT=1234;"
            "Fake_Param=Fake Param;"
        )
        assert hook.odbc_connection_string == expected

    def test_driver_in_both(self):
        conn_params = dict(extra=json.dumps(dict(Driver="Fake Driver", Fake_Param="Fake Param")))
        hook_params = dict(driver="ParamDriver")
        hook = self.get_hook(hook_params=hook_params, conn_params=conn_params)
        expected = (
            "DRIVER={ParamDriver};"
            "SERVER=host;"
            "DATABASE=schema;"
            "UID=login;"
            "PWD=password;"
            "PORT=1234;"
            "Fake_Param=Fake Param;"
        )
        assert hook.odbc_connection_string == expected

    def test_dsn_in_extra(self):
        conn_params = dict(extra=json.dumps(dict(DSN="MyDSN", Fake_Param="Fake Param")))
        hook = self.get_hook(conn_params=conn_params)
        expected = (
            "DSN=MyDSN;SERVER=host;DATABASE=schema;UID=login;PWD=password;PORT=1234;Fake_Param=Fake Param;"
        )
        assert hook.odbc_connection_string == expected

    def test_dsn_in_both(self):
        conn_params = dict(extra=json.dumps(dict(DSN="MyDSN", Fake_Param="Fake Param")))
        hook_params = dict(driver="ParamDriver", dsn="ParamDSN")
        hook = self.get_hook(hook_params=hook_params, conn_params=conn_params)
        expected = (
            "DRIVER={ParamDriver};"
            "DSN=ParamDSN;"
            "SERVER=host;"
            "DATABASE=schema;"
            "UID=login;"
            "PWD=password;"
            "PORT=1234;"
            "Fake_Param=Fake Param;"
        )
        assert hook.odbc_connection_string == expected

    def test_get_uri(self):
        conn_params = dict(extra=json.dumps(dict(DSN="MyDSN", Fake_Param="Fake Param")))
        hook_params = dict(dsn="ParamDSN")
        hook = self.get_hook(hook_params=hook_params, conn_params=conn_params)
        uri_param = quote_plus(
            "DSN=ParamDSN;SERVER=host;DATABASE=schema;UID=login;PWD=password;PORT=1234;Fake_Param=Fake Param;"
        )
        expected = "mssql+pyodbc:///?odbc_connect=" + uri_param
        assert hook.get_uri() == expected

    def test_connect_kwargs_from_hook(self):
        hook = self.get_hook(
            hook_params=dict(
                connect_kwargs={
                    "attrs_before": {
                        1: 2,
                        pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED,
                    },
                    "readonly": True,
                    "autocommit": False,
                }
            ),
        )
        assert hook.connect_kwargs == {
            "attrs_before": {1: 2, pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED},
            "readonly": True,
            "autocommit": False,
        }

    def test_connect_kwargs_from_conn(self):
        extra = json.dumps(
            dict(
                connect_kwargs={
                    "attrs_before": {
                        1: 2,
                        pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED,
                    },
                    "readonly": True,
                    "autocommit": True,
                }
            )
        )

        hook = self.get_hook(conn_params=dict(extra=extra))
        assert hook.connect_kwargs == {
            "attrs_before": {1: 2, pyodbc.SQL_TXN_ISOLATION: pyodbc.SQL_TXN_READ_UNCOMMITTED},
            "readonly": True,
            "autocommit": True,
        }

    def test_connect_kwargs_from_conn_and_hook(self):
        """
        When connect_kwargs in both hook and conn, should be merged properly.
        Hook beats conn.
        """
        conn_extra = json.dumps(dict(connect_kwargs={"attrs_before": {1: 2, 3: 4}, "readonly": False}))
        hook_params = dict(
            connect_kwargs={"attrs_before": {3: 5, pyodbc.SQL_TXN_ISOLATION: 0}, "readonly": True}
        )

        hook = self.get_hook(conn_params=dict(extra=conn_extra), hook_params=hook_params)
        assert hook.connect_kwargs == {
            "attrs_before": {1: 2, 3: 5, pyodbc.SQL_TXN_ISOLATION: 0},
            "readonly": True,
        }

    def test_connect_kwargs_bool_from_uri(self):
        """
        Bools will be parsed from uri as strings
        """
        conn_extra = json.dumps(dict(connect_kwargs={"ansi": True}))
        hook = self.get_hook(conn_params=dict(extra=conn_extra))
        assert hook.connect_kwargs == {
            "ansi": True,
        }

    def test_driver(self):
        hook = self.get_hook(hook_params=dict(driver="Blah driver"))
        assert hook.driver == "Blah driver"
        hook = self.get_hook(hook_params=dict(driver="{Blah driver}"))
        assert hook.driver == "Blah driver"

    def test_driver_extra_raises_warning_by_default(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow.providers.odbc.hooks.test_odbc"):
            driver = self.get_hook(conn_params=dict(extra='{"driver": "Blah driver"}')).driver
            assert "You have supplied 'driver' via connection extra but it will not be used" in caplog.text
            assert driver is None

    @mock.patch.dict("os.environ", {"AIRFLOW__PROVIDERS_ODBC__ALLOW_DRIVER_IN_EXTRA": "TRUE"})
    def test_driver_extra_works_when_allow_driver_extra(self):
        hook = self.get_hook(
            conn_params=dict(extra='{"driver": "Blah driver"}'), hook_params=dict(allow_driver_extra=True)
        )
        assert hook.driver == "Blah driver"

    def test_default_driver_set(self):
        with patch.object(OdbcHook, "default_driver", "Blah driver"):
            hook = self.get_hook()
            assert hook.driver == "Blah driver"

    def test_driver_extra_works_when_default_driver_set(self):
        with patch.object(OdbcHook, "default_driver", "Blah driver"):
            hook = self.get_hook()
            assert hook.driver == "Blah driver"

    def test_driver_none_by_default(self):
        hook = self.get_hook()
        assert hook.driver is None

    def test_driver_extra_raises_warning_and_returns_default_driver_by_default(self, caplog):
        with patch.object(OdbcHook, "default_driver", "Blah driver"):
            with caplog.at_level(logging.WARNING, logger="airflow.providers.odbc.hooks.test_odbc"):
                driver = self.get_hook(conn_params=dict(extra='{"driver": "Blah driver2"}')).driver
                assert "have supplied 'driver' via connection extra but it will not be used" in caplog.text
                assert driver == "Blah driver"

    def test_database(self):
        hook = self.get_hook(hook_params=dict(database="abc"))
        assert hook.database == "abc"
        hook = self.get_hook()
        assert hook.database == "schema"

    def test_sqlalchemy_scheme_default(self):
        hook = self.get_hook()
        uri = hook.get_uri()
        assert urlsplit(uri).scheme == "mssql+pyodbc"

    def test_sqlalchemy_scheme_param(self):
        hook = self.get_hook(hook_params=dict(sqlalchemy_scheme="my-scheme"))
        uri = hook.get_uri()
        assert urlsplit(uri).scheme == "my-scheme"

    def test_sqlalchemy_scheme_extra(self):
        hook = self.get_hook(conn_params=dict(extra=json.dumps(dict(sqlalchemy_scheme="my-scheme"))))
        uri = hook.get_uri()
        assert urlsplit(uri).scheme == "my-scheme"

    def test_pyodbc_mock(self):
        """Ensure that pyodbc.Row object has a `cursor_description` method.

        In subsequent tests, pyodbc.Row is replaced by pure Python mock object, which implements the above
        method. We want to detect any breaking change in the pyodbc object. If it fails, the 'mock_row'
        needs to be updated.
        """
        assert hasattr(pyodbc.Row, "cursor_description")

    def test_query_return_serializable_result(self, mock_row):
        pyodbc_result = [mock_row(key=1, column="value1"), mock_row(key=2, column="value2")]
        hook_result = [(1, "value1"), (2, "value2")]

        def mock_handler(*_):
            return pyodbc_result

        hook = self.get_hook()
        result = hook.run("SQL", handler=mock_handler)
        assert hook_result == result

    def test_query_no_handler_return_none(self):
        hook = self.get_hook()
        result = hook.run("SQL")
        assert result is None
