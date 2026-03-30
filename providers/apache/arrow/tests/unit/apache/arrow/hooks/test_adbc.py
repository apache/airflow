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

import importlib
import json
import logging
from unittest import mock

import pytest
from adbc_driver_manager import dbapi
from adbc_driver_manager.dbapi import Cursor
from pyarrow import field, schema, string

from airflow.models import Connection
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.common.sql.dialects.dialect import Dialect


class TestAdbcHook:
    def setup_method(self):
        # Create a MagicMock cursor similar to DbApiHook tests
        self.cur = mock.MagicMock(rowcount=0, fast_executemany=False)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        # Schema and extras that the hook might read
        # Provide a real pyarrow Schema so _to_record_batch can build RecordBatch
        self.conn.adbc_get_table_schema.return_value = schema([field("col", string())])
        self.conn.extra_dejson = {}
        conn = self.conn

        logging.root.disabled = True

        # Instantiate the hook under test
        self.hook = self.make_hook_for_conn(conn)

        # Ensure the cursor used in unit tests has a native bind method
        # to simulate native Arrow bind support.
        self.cur.bind = mock.MagicMock()

        # Make fetch_arrow_table().to_pydict() return a simple column mapping
        arrow_table = mock.MagicMock()
        arrow_table.to_pydict.return_value = {"col": [1, 2]}
        self.cur.fetch_arrow_table.return_value = arrow_table

    def make_hook_for_conn(self, conn):
        """
        Return an AdbcHook subclass instance bound to the provided conn.

        Tests previously redefined this subclass locally in multiple places.
        This helper centralizes that logic so tests can simply call
        `self.make_hook_for_conn(conn)`.
        """

        class AdbcHookMock(AdbcHook):
            conn_name_attr = "adbc_default"

            @classmethod
            def get_connection(cls, conn_id: str) -> Connection:
                return conn

            def get_conn(self):
                return conn

            @property
            def dialect(self):
                return Dialect(self)

            def get_db_log_messages(self, _conn) -> None:
                return _conn.get_messages()

        return AdbcHookMock()

    def test_get_records_fetch_all_handler(self):
        result = self.hook.get_records("SELECT 1")
        assert result == [(1,), (2,)]

    def test_insert_rows_native_bind(self):
        table = "table"
        rows = [("a",), ("b",)]

        # Native bind supported (cursor has bind attribute)
        self.hook.insert_rows(table, rows)

        assert self.cur.bind.called
        assert self.cur.execute.called
        assert self.conn.commit.call_count >= 1

    def test_insert_rows_fast_executemany_not_supported(self):
        # Cursor without native bind that doesn't support setting fast_executemany
        class NoFastExecCursor(mock.MagicMock):
            def __setattr__(self, name, value):
                if name == "fast_executemany":
                    raise AttributeError("fast_executemany not supported")
                super().__setattr__(name, value)

        cur = NoFastExecCursor(spec=Cursor)
        delattr(cur, "bind")  # Remove bind to simulate no native bind support
        conn = mock.MagicMock()
        conn.cursor.return_value = cur
        conn.adbc_get_table_schema.return_value = schema([field("col", string())])
        conn.extra_dejson = {}
        hook = self.make_hook_for_conn(conn)

        table = "table"
        rows = [("x",), ("y",)]

        hook.insert_rows(table, rows, executemany=True, fast_executemany=True)

        assert cur.executemany.called
        assert conn.commit.call_count >= 1

    def test_insert_rows_fast_executemany_supported(self):
        # Cursor without native bind but supports setting fast_executemany
        cur = mock.MagicMock(spec=Cursor)
        delattr(cur, "bind")  # Remove bind to simulate no native bind support
        conn = mock.MagicMock()
        conn.cursor.return_value = cur
        conn.adbc_get_table_schema.return_value = schema([field("col", string())])
        conn.extra_dejson = {}
        hook = self.make_hook_for_conn(conn)

        table = "table"
        rows = [("x",), ("y",)]

        hook.insert_rows(table, rows, executemany=True, fast_executemany=True)

        assert cur.fast_executemany
        assert cur.executemany.called
        assert conn.commit.call_count >= 1

    @pytest.mark.skipif(
        importlib.util.find_spec("adbc_driver_sqlite") is None,
        reason="adbc_driver_sqlite not installed",
    )
    def test_dbapi_connection(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="adbc_default",
                conn_type="adbc",
                host="file::memory:?cache=shared",
                extra=json.dumps(
                    {
                        "driver": "adbc_driver_sqlite",
                    }
                ),
            )
        )

        with AdbcHook()._create_autocommit_connection() as adbc_conn:
            assert isinstance(adbc_conn, dbapi.Connection)
