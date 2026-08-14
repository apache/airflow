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
        # Provide a real pyarrow Schema so _to_record_batch can build RecordBatch
        self.conn.adbc_get_table_schema.return_value = schema([field("col", string())])
        self.conn.extra_dejson = {}
        conn = self.conn

        logging.root.disabled = True

        self.hook = self.make_hook_for_conn(conn)

        # Return a real pyarrow Table so fetch_arrow_table().columns works correctly.
        import pyarrow as pa

        self.cur.fetch_arrow_table.return_value = pa.table({"col": [1, 2]})

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

    def test_get_records_duplicate_column_names(self):
        """Duplicate column names (e.g. a JOIN) must not collapse to fewer columns."""
        import pyarrow as pa

        # Two columns both named "id" — as produced by SELECT a.id, b.id FROM a JOIN b.
        table = pa.table({"a_id": [10, 20], "b_id": [30, 40]}).rename_columns(["id", "id"])
        self.cur.fetch_arrow_table.return_value = table

        result = self.hook.get_records("SELECT a.id, b.id FROM a JOIN b ON a.k = b.k")

        assert result == [(10, 30), (20, 40)]

    def test_insert_rows_uses_executemany_with_record_batch(self):
        """insert_rows must call executemany with a RecordBatch — the Arrow-native fast path."""
        rows = [("a",), ("b",)]
        self.hook.insert_rows("table", rows)

        assert self.cur.executemany.called
        batch = self.cur.executemany.call_args[0][1]
        assert batch.num_rows == 2
        assert self.conn.commit.call_count >= 1

    def test_insert_rows_autocommit_skips_explicit_commit(self):
        """With autocommit=True the chunk loop must not call conn.commit() — drivers raise on it."""
        rows = [("a",), ("b",)]
        self.hook.insert_rows("table", rows, autocommit=True)

        assert self.cur.executemany.called
        assert self.conn.commit.call_count == 0

    def test_set_autocommit_applies_adbc_option(self):
        """set_autocommit must set the driver-level option, not only a Python attribute."""
        conn = mock.MagicMock()
        self.hook.set_autocommit(conn, True)
        conn._conn.set_options.assert_called_once_with(**{"adbc.connection.autocommit": "true"})
        assert conn.autocommit is True

        conn.reset_mock()
        self.hook.set_autocommit(conn, False)
        conn._conn.set_options.assert_called_once_with(**{"adbc.connection.autocommit": "false"})
        assert conn.autocommit is False

    def test_insert_rows_commit_every_zero_inserts_all_rows(self):
        """commit_every=0 must insert all rows in a single transaction, not zero rows."""
        rows = [("a",), ("b",), ("c",)]
        self.hook.insert_rows("table", rows, commit_every=0)

        assert self.cur.executemany.called
        batch = self.cur.executemany.call_args[0][1]
        assert batch.num_rows == 3
        assert self.conn.commit.call_count == 1

    def test_insert_rows_fast_executemany_not_supported(self):
        # Cursor that doesn't support setting fast_executemany
        class NoFastExecCursor(mock.MagicMock):
            def __setattr__(self, name, value):
                if name == "fast_executemany":
                    raise AttributeError("fast_executemany not supported")
                super().__setattr__(name, value)

        cur = NoFastExecCursor(spec=Cursor)
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
        # Cursor that supports setting fast_executemany
        cur = mock.MagicMock(spec=Cursor)
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

    def test_insert_rows_target_fields_order_preserved(self):
        """Schema must follow target_fields order so SQL and RecordBatch agree."""
        from pyarrow import int64

        cur = mock.MagicMock(spec=Cursor)
        conn = mock.MagicMock()
        conn.cursor.return_value = cur
        # Table declares (first TEXT, last TEXT, age INT64) — note: first before last.
        conn.adbc_get_table_schema.return_value = schema(
            [field("first", string()), field("last", string()), field("age", int64())]
        )
        conn.extra_dejson = {}
        hook = self.make_hook_for_conn(conn)

        # Insert with reversed column order and omit age.
        rows = [("Smith", "Alice"), ("Jones", "Bob")]
        hook.insert_rows("t", rows, target_fields=["last", "first"])

        assert cur.executemany.called
        batch = cur.executemany.call_args[0][1]  # second positional arg is the RecordBatch
        # Schema order must match target_fields, not the table declaration.
        assert batch.schema.names == ["last", "first"]
        # Values must land in the declared column order.
        assert batch.column("last").to_pylist() == ["Smith", "Jones"]
        assert batch.column("first").to_pylist() == ["Alice", "Bob"]

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

    def _make_real_conn_hook(self, connection: Connection) -> AdbcHook:
        """Return an AdbcHook whose get_connection() returns *connection* but whose
        get_conn() is the real implementation (needed to verify connect() kwargs)."""

        class AdbcHookWithRealGetConn(AdbcHook):
            conn_name_attr = "adbc_default"

            @classmethod
            def get_connection(cls, conn_id: str) -> Connection:
                return connection

        return AdbcHookWithRealGetConn()

    @mock.patch("airflow.providers.apache.arrow.hooks.adbc.connect")
    def test_get_conn_forwards_db_kwargs(self, mock_connect):
        conn = Connection(
            conn_id="adbc_default",
            conn_type="adbc",
            host="file::memory:",
            extra=json.dumps(
                {
                    "driver": "adbc_driver_sqlite",
                    # These are driver-specific database init options (PostgreSQL examples).
                    "db_kwargs": {"username": "admin", "password": "secret"},
                }
            ),
        )
        hook = self._make_real_conn_hook(conn)
        hook.__dict__["_driver_path"] = "adbc_driver_sqlite"

        hook.get_conn()

        mock_connect.assert_called_once()
        kw = mock_connect.call_args.kwargs
        assert kw["db_kwargs"]["username"] == "admin"
        assert kw["db_kwargs"]["password"] == "secret"
        assert kw["db_kwargs"]["uri"] == "file::memory:"

    @mock.patch("airflow.providers.apache.arrow.hooks.adbc.connect")
    def test_get_conn_forwards_conn_kwargs(self, mock_connect):
        conn = Connection(
            conn_id="adbc_default",
            conn_type="adbc",
            host="file::memory:",
            extra=json.dumps(
                {
                    "driver": "adbc_driver_sqlite",
                    # conn_kwargs must use the canonical dotted ADBC option names.
                    "conn_kwargs": {
                        "adbc.connection.autocommit": "true",
                        "adbc.connection.read_only": "true",
                    },
                }
            ),
        )
        hook = self._make_real_conn_hook(conn)
        hook.__dict__["_driver_path"] = "adbc_driver_sqlite"

        hook.get_conn()

        mock_connect.assert_called_once()
        kw = mock_connect.call_args.kwargs
        assert kw["conn_kwargs"] == {
            "adbc.connection.autocommit": "true",
            "adbc.connection.read_only": "true",
        }

    @mock.patch("airflow.providers.apache.arrow.hooks.adbc.connect")
    def test_get_conn_forwards_entrypoint(self, mock_connect):
        conn = Connection(
            conn_id="adbc_default",
            conn_type="adbc",
            host="file::memory:",
            extra=json.dumps(
                {
                    "driver": "adbc_driver_sqlite",
                    "entrypoint": "adbc_driver_sqlite.dbapi.connect",
                }
            ),
        )
        hook = self._make_real_conn_hook(conn)
        hook.__dict__["_driver_path"] = "adbc_driver_sqlite"

        hook.get_conn()

        mock_connect.assert_called_once()
        kw = mock_connect.call_args.kwargs
        assert kw["entrypoint"] == "adbc_driver_sqlite.dbapi.connect"

    def test_dialect_name_from_extra(self):
        conn = Connection(
            conn_id="adbc_default",
            conn_type="adbc",
            host="file::memory:",
            extra=json.dumps({"driver": "adbc_driver_postgresql", "dialect": "postgresql"}),
        )
        hook = self.make_hook_for_conn(conn)
        assert hook.dialect_name == "postgresql"

    def test_dialect_name_defaults_to_default(self):
        conn = Connection(
            conn_id="adbc_default",
            conn_type="adbc",
            host="file::memory:",
            extra=json.dumps({"driver": "adbc_driver_sqlite"}),
        )
        hook = self.make_hook_for_conn(conn)
        assert hook.dialect_name == "default"

    @mock.patch("airflow.providers.apache.arrow.hooks.adbc.connect")
    def test_get_conn_empty_extras_defaults(self, mock_connect):
        conn = Connection(
            conn_id="adbc_default",
            conn_type="adbc",
            host="file::memory:",
            extra=json.dumps({"driver": "adbc_driver_sqlite"}),
        )
        hook = self._make_real_conn_hook(conn)
        hook.__dict__["_driver_path"] = "adbc_driver_sqlite"

        hook.get_conn()

        kw = mock_connect.call_args.kwargs
        assert kw["conn_kwargs"] == {}
        assert kw["entrypoint"] is None
