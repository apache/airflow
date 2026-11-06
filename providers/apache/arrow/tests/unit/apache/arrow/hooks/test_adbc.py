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
from adbc_driver_manager import AdbcStatusCode, dbapi
from adbc_driver_manager.dbapi import Cursor
from pyarrow import RecordBatch, field, schema, string

from airflow.models import Connection
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.common.sql.dialects.dialect import Dialect


class TestAdbcHook:
    def setup_method(self):
        self.cur = mock.MagicMock(spec=Cursor, rowcount=0)
        self.cur.fast_executemany = False
        self.conn = mock.MagicMock(spec=dbapi.Connection)
        self.conn.cursor.return_value = self.cur
        # Provide a real pyarrow Schema so _to_record_batch can build RecordBatch
        self.conn.adbc_get_table_schema.return_value = schema([field("col", string())])
        self.conn.extra_dejson = {}
        # get_messages is not on the dbapi.Connection spec but is called by AdbcHookMock.get_db_log_messages
        self.conn.get_messages = mock.MagicMock(return_value=[])
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

    def test_insert_rows_uses_adbc_ingest_by_default(self):
        """By default insert_rows must hand each RecordBatch to the driver's bulk ingest."""
        rows = [("a",), ("b",)]
        self.hook.insert_rows("table", rows)

        self.cur.executemany.assert_not_called()
        self.cur.adbc_ingest.assert_called_once()
        args, kwargs = self.cur.adbc_ingest.call_args
        assert args[0] == "table"
        assert isinstance(args[1], RecordBatch)
        assert args[1].num_rows == 2
        assert kwargs == {"mode": "append", "db_schema_name": None}
        assert self.conn.commit.call_count == 1

    def test_insert_rows_fast_executemany_false_uses_executemany(self):
        """fast_executemany=False keeps the previous bind path (one INSERT bound to the batch)."""
        rows = [("a",), ("b",)]
        self.hook.insert_rows("table", rows, fast_executemany=False)

        self.cur.adbc_ingest.assert_not_called()
        self.cur.executemany.assert_called_once()
        sql, batch = self.cur.executemany.call_args[0]
        assert sql.startswith("INSERT INTO table")
        assert batch.num_rows == 2

    @mock.patch.object(AdbcHook, "_generate_insert_sql", return_value="REPLACE INTO table (col) VALUES ($1)")
    def test_insert_rows_replace_binds_even_with_fast_executemany(self, generate_insert_sql):
        """replace=True cannot go through bulk ingest, so it must bind to the generated statement."""
        rows = [("a",), ("b",)]
        self.hook.insert_rows("table", rows, replace=True, fast_executemany=True)

        assert generate_insert_sql.call_args[0][3] is True
        self.cur.adbc_ingest.assert_not_called()
        self.cur.executemany.assert_called_once()
        assert self.cur.executemany.call_args[0][0] == "REPLACE INTO table (col) VALUES ($1)"

    @mock.patch.object(AdbcHook, "_generate_insert_sql", return_value="REPLACE INTO table (col) VALUES ($1)")
    def test_insert_rows_replace_with_fast_executemany_logs_debug_not_info(self, _generate_insert_sql):
        """The caller did not ask for ingest explicitly, so binding for replace=True is only a debug note."""
        self.hook._log = mock.MagicMock(spec=logging.Logger)
        self.hook.insert_rows("table", [("a",)], replace=True)

        self.hook._log.debug.assert_any_call(
            "Bulk ingest cannot replace rows; binding them to the statement instead"
        )
        info_messages = [c.args[0] for c in self.hook._log.info.call_args_list]
        assert not any("cannot replace" in m for m in info_messages)

    def test_insert_rows_falls_back_to_executemany_when_ingest_not_supported(self):
        """A driver without bulk ingest must fall back to binding, once, for every remaining batch."""
        self.cur.adbc_ingest.side_effect = dbapi.NotSupportedError("NOT_IMPLEMENTED: ingest")
        self.hook._log = mock.MagicMock(spec=logging.Logger)
        rows = [("a",), ("b",), ("c",)]

        self.hook.insert_rows("table", rows, commit_every=2)

        self.cur.adbc_ingest.assert_called_once()
        assert self.cur.executemany.call_count == 2
        assert [c.args[0].startswith("INSERT INTO table") for c in self.cur.executemany.call_args_list] == [
            True,
            True,
        ]
        assert [c.args[1].num_rows for c in self.cur.executemany.call_args_list] == [2, 1]
        assert self.conn.commit.call_count == 2
        fallback_logs = [
            c for c in self.hook._log.info.call_args_list if "does not support bulk ingest" in c.args[0]
        ]
        assert len(fallback_logs) == 1

    def test_insert_rows_ingest_error_after_first_batch_is_not_swallowed(self):
        """Only an unsupported first ingest falls back; later NotSupportedErrors propagate."""
        self.cur.adbc_ingest.side_effect = [None, dbapi.NotSupportedError("boom")]

        with pytest.raises(dbapi.NotSupportedError, match="boom"):
            self.hook.insert_rows("table", [("a",), ("b",)], commit_every=1)

        self.cur.executemany.assert_not_called()

    def test_insert_rows_ingest_other_errors_propagate(self):
        """Errors other than NotSupportedError from ingest are not hidden by the fallback."""
        self.cur.adbc_ingest.side_effect = dbapi.ProgrammingError(
            "bad table", status_code=AdbcStatusCode.INVALID_ARGUMENT
        )

        with pytest.raises(dbapi.ProgrammingError, match="bad table"):
            self.hook.insert_rows("table", [("a",)])

        self.cur.executemany.assert_not_called()

    def test_insert_rows_schema_qualified_table_uses_db_schema_name(self):
        """``schema.table`` must be split into the ingest target table and db_schema_name."""
        rows = [("a",)]
        self.hook.insert_rows("my_schema.my_table", rows)

        self.conn.adbc_get_table_schema.assert_called_once_with(
            table_name="my_table", db_schema_filter="my_schema"
        )
        args, kwargs = self.cur.adbc_ingest.call_args
        assert args[0] == "my_table"
        assert kwargs == {"mode": "append", "db_schema_name": "my_schema"}

    def test_insert_rows_commit_every_chunks_ingest(self):
        """Chunking and per-chunk commits must be the same on the ingest path."""
        rows = [("a",), ("b",), ("c",)]
        self.hook.insert_rows("table", rows, commit_every=2)

        assert [c.args[1].num_rows for c in self.cur.adbc_ingest.call_args_list] == [2, 1]
        assert self.conn.commit.call_count == 2

    def test_insert_rows_autocommit_skips_explicit_commit(self):
        """With autocommit=True the chunk loop must not call conn.commit() — drivers raise on it."""
        rows = [("a",), ("b",)]
        self.hook.insert_rows("table", rows, autocommit=True)

        assert self.cur.adbc_ingest.called
        assert self.conn.commit.call_count == 0

    def test_set_autocommit_applies_adbc_option(self):
        """set_autocommit must call set_autocommit() on the underlying AdbcConnection."""
        conn = mock.MagicMock()
        self.hook.set_autocommit(conn, True)
        conn.adbc_connection.set_autocommit.assert_called_once_with(True)
        assert conn.autocommit is True

        conn.reset_mock()
        self.hook.set_autocommit(conn, False)
        conn.adbc_connection.set_autocommit.assert_called_once_with(False)
        assert conn.autocommit is False

    def test_insert_rows_commit_every_zero_inserts_all_rows(self):
        """commit_every=0 must insert all rows in a single transaction, not zero rows."""
        rows = [("a",), ("b",), ("c",)]
        self.hook.insert_rows("table", rows, commit_every=0)

        assert self.cur.adbc_ingest.call_count == 1
        batch = self.cur.adbc_ingest.call_args[0][1]
        assert batch.num_rows == 3
        assert self.conn.commit.call_count == 1

    def test_insert_rows_does_not_set_fast_executemany_on_cursor(self):
        """ADBC cursors have no fast_executemany attribute; the flag selects the path instead."""
        cur = mock.MagicMock(spec=Cursor)
        conn = mock.MagicMock()
        conn.cursor.return_value = cur
        conn.adbc_get_table_schema.return_value = schema([field("col", string())])
        conn.extra_dejson = {}
        hook = self.make_hook_for_conn(conn)

        hook.insert_rows("table", [("x",), ("y",)], executemany=True, fast_executemany=True)

        assert not hasattr(cur, "fast_executemany")
        assert cur.adbc_ingest.called
        assert not cur.executemany.called

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

        assert cur.adbc_ingest.called
        batch = cur.adbc_ingest.call_args[0][1]  # second positional arg is the RecordBatch
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

    @pytest.mark.skipif(
        importlib.util.find_spec("adbc_driver_sqlite") is None,
        reason="adbc_driver_sqlite not installed",
    )
    @pytest.mark.parametrize("fast_executemany", [True, False])
    def test_insert_rows_sqlite_round_trip(self, create_connection_without_db, tmp_path, fast_executemany):
        """Both load paths must write the same rows, including a target_fields subset."""
        create_connection_without_db(
            Connection(
                conn_id="adbc_default",
                conn_type="adbc",
                extra=json.dumps({"driver": "adbc_driver_sqlite"}),
            )
        )
        hook = AdbcHook()
        # A file database, so the rows outlive the connection that inserted them.
        with mock.patch.object(AdbcHook, "uri", str(tmp_path / "db.sqlite")):
            # SQLite reports column types from the stored values, so an empty table only
            # yields a usable schema for integer columns.
            hook.run("CREATE TABLE t (id INTEGER, qty INTEGER, note TEXT DEFAULT 'dflt')", autocommit=True)

            hook.insert_rows(
                "t", [(10, 1), (20, 2)], target_fields=["qty", "id"], fast_executemany=fast_executemany
            )

            assert hook.get_records("SELECT id, qty, note FROM t ORDER BY id") == [
                (1, 10, "dflt"),
                (2, 20, "dflt"),
            ]

    @pytest.mark.skipif(
        importlib.util.find_spec("adbc_driver_sqlite") is None,
        reason="adbc_driver_sqlite not installed",
    )
    @pytest.mark.parametrize(
        ("conn_kwargs", "should_raise"),
        [
            pytest.param(
                {"adbc.connection.autocommit": "true"},
                False,
                id="canonical-dotted-name-accepted",
            ),
            pytest.param(
                {"autocommit": "true"},
                True,
                id="short-name-rejected",
            ),
        ],
    )
    def test_conn_kwargs_canonical_names_required(self, conn_kwargs, should_raise):
        """Canonical dotted ADBC option names are required; short names raise NotSupportedError.

        This test documents and enforces actual driver behaviour so that the hook
        docstring and the connection docs stay accurate.
        """
        import adbc_driver_sqlite.dbapi as sqlite_dbapi
        from adbc_driver_manager import NotSupportedError

        if should_raise:
            with sqlite_dbapi.connect("file::memory:") as conn:
                with pytest.raises(NotSupportedError):
                    conn.adbc_connection.set_options(**conn_kwargs)
        else:
            with sqlite_dbapi.connect("file::memory:") as conn:
                conn.adbc_connection.set_options(**conn_kwargs)

    def _make_real_conn_hook(self, connection: Connection) -> AdbcHook:
        """Return an AdbcHook whose get_connection() returns *connection* but whose
        get_conn() is the real implementation (needed to verify connect() kwargs)."""

        class AdbcHookWithRealGetConn(AdbcHook):
            conn_name_attr = "adbc_default"

            @classmethod
            def get_connection(cls, conn_id: str) -> Connection:
                return connection

        return AdbcHookWithRealGetConn()

    @pytest.mark.parametrize(
        ("connection", "expected"),
        [
            pytest.param(
                Connection(
                    conn_id="adbc_default",
                    conn_type="adbc",
                    host="db.example.com",
                    login="user",
                    password="p@ss",
                    port=5432,
                    schema="db",
                    extra=json.dumps({"dialect": "postgresql"}),
                ),
                "postgresql://user:p%40ss@db.example.com:5432/db",
                id="built-from-the-fields",
            ),
            pytest.param(
                Connection(
                    conn_id="adbc_default",
                    conn_type="adbc",
                    host="db.example.com",
                    extra=json.dumps(
                        {"dialect": "postgresql", "conn_kwargs": {"adbc.connection.read_only": "true"}}
                    ),
                ),
                "postgresql://db.example.com",
                id="extras-stay-out-of-the-uri",
            ),
            pytest.param(
                Connection(
                    conn_id="adbc_default",
                    conn_type="adbc",
                    host="postgresql://user:pass@db.example.com:5432/db?sslmode=require",
                    extra=json.dumps({"dialect": "postgresql"}),
                ),
                "postgresql://user:pass@db.example.com:5432/db?sslmode=require",
                id="uri-in-the-host-field",
            ),
            pytest.param(
                Connection(
                    conn_id="adbc_default",
                    conn_type="adbc",
                    host="file::memory:",
                    extra=json.dumps({"dialect": "sqlite"}),
                ),
                "file::memory:",
                id="sqlite-in-memory",
            ),
            pytest.param(
                Connection(
                    conn_id="adbc_default",
                    conn_type="adbc",
                    host="/data/app.db",
                    extra=json.dumps({"dialect": "sqlite"}),
                ),
                "/data/app.db",
                id="sqlite-file",
            ),
        ],
    )
    def test_uri(self, connection, expected):
        assert self._make_real_conn_hook(connection).uri == expected

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
