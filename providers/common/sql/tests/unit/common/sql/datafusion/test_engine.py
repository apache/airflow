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

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from datafusion import SessionContext

from airflow.models import Connection
from airflow.providers.common.sql.config import ConnectionConfig, DataSourceConfig
from airflow.providers.common.sql.datafusion.base import ObjectStorageProvider
from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
from airflow.providers.common.sql.datafusion.exceptions import (
    ObjectStoreCreationException,
    QueryExecutionException,
)

TEST_CONNECTION_CONFIG = ConnectionConfig(
    conn_id="aws_default",
    credentials={
        "access_key_id": "test",
        "secret_access_key": "test",
        "session_token": None,
    },
    extra_config={"region_name": "us-east-1"},
)


class TestDataFusionEngine:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="aws_default",
                conn_type="aws",
                login="fake_id",
                password="fake_secret",
                extra='{"region": "us-east-1"}',
            )
        )

    def test_init(self):
        engine = DataFusionEngine()
        assert engine.df_ctx is not None
        assert engine.registered_tables == {}

    def test_session_context_property(self):
        engine = DataFusionEngine()
        assert isinstance(engine.session_context, SessionContext)
        assert engine.session_context is engine.df_ctx

    def test_register_datasource_invalid_config(self):
        engine = DataFusionEngine()
        with pytest.raises(ValueError, match="datasource_config must be of type DataSourceConfig"):
            engine.register_datasource("invalid")

    @pytest.mark.parametrize(
        ("storage_type", "format", "scheme"),
        [("s3", "parquet", "s3"), ("s3", "csv", "s3"), ("s3", "avro", "s3")],
    )
    @patch("airflow.providers.common.sql.datafusion.engine.get_object_storage_provider", autospec=True)
    @patch.object(DataFusionEngine, "_get_connection_config")
    def test_register_datasource_success(self, mock_get_conn, mock_factory, storage_type, format, scheme):
        mock_get_conn.return_value = TEST_CONNECTION_CONFIG
        mock_provider = MagicMock(spec=ObjectStorageProvider)
        mock_store = MagicMock()
        mock_provider.create_object_store.return_value = mock_store
        mock_provider.get_scheme.return_value = scheme
        mock_factory.return_value = mock_provider

        engine = DataFusionEngine()

        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="test_table", uri=f"{scheme}://bucket/path", format=format
        )

        engine.df_ctx = MagicMock(spec=SessionContext)

        engine.register_datasource(datasource_config)

        mock_factory.assert_called_once()
        mock_provider.create_object_store.assert_called_once_with(
            f"{scheme}://bucket/path", connection_config=mock_get_conn.return_value
        )
        engine.df_ctx.register_object_store.assert_called_once_with(schema=scheme, store=mock_store)

        if format == "parquet":
            engine.df_ctx.register_parquet.assert_called_once_with("test_table", f"{scheme}://bucket/path")
        elif format == "csv":
            engine.df_ctx.register_csv.assert_called_once_with("test_table", f"{scheme}://bucket/path")
        elif format == "avro":
            engine.df_ctx.register_avro.assert_called_once_with("test_table", f"{scheme}://bucket/path")

        assert engine.registered_tables == {"test_table": f"{scheme}://bucket/path"}

    @patch("airflow.providers.common.sql.datafusion.engine.get_object_storage_provider", autospec=True)
    @patch.object(DataFusionEngine, "_get_connection_config")
    def test_register_datasource_object_store_exception(self, mock_get_conn, mock_factory):
        mock_get_conn.return_value = TEST_CONNECTION_CONFIG
        mock_factory.side_effect = Exception("Provider error")

        engine = DataFusionEngine()
        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="test_table", uri="s3://bucket/path", format="parquet"
        )

        with pytest.raises(ObjectStoreCreationException, match="Error while creating object store"):
            engine.register_datasource(datasource_config)

    @patch("airflow.providers.common.sql.datafusion.engine.get_object_storage_provider", autospec=True)
    @patch.object(DataFusionEngine, "_get_connection_config")
    def test_register_datasource_object_store_exception_preserves_cause(self, mock_get_conn, mock_factory):
        mock_get_conn.return_value = TEST_CONNECTION_CONFIG
        mock_factory.side_effect = Exception("Provider error")

        engine = DataFusionEngine()
        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="test_table", uri="s3://bucket/path", format="parquet"
        )

        with pytest.raises(ObjectStoreCreationException) as exc_info:
            engine.register_datasource(datasource_config)

        assert exc_info.value.__cause__ is not None
        assert "Provider error" in str(exc_info.value.__cause__)

    @patch.object(DataFusionEngine, "_get_connection_config")
    def test_register_datasource_duplicate_table(self, mock_get_conn):
        mock_get_conn.return_value = TEST_CONNECTION_CONFIG
        engine = DataFusionEngine()
        engine.registered_tables["test_table"] = "s3://old/path"

        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="test_table", uri="s3://new/path", format="parquet"
        )

        with patch.object(engine, "_register_object_store"):
            with pytest.raises(ValueError, match="Table test_table already registered"):
                engine.register_datasource(datasource_config)

    def test_execute_query_success(self):
        engine = DataFusionEngine()
        engine.df_ctx = MagicMock(spec=SessionContext)
        mock_df = MagicMock(spec=["limit", "to_pydict"])
        mock_df.to_pydict.return_value = {"col1": [1, 2]}
        engine.df_ctx.sql.return_value = mock_df

        result = engine.execute_query("SELECT * FROM test_table")

        engine.df_ctx.sql.assert_called_once_with("SELECT * FROM test_table")
        mock_df.limit.assert_not_called()
        assert result == {"col1": [1, 2]}

    def test_execute_query_with_max_rows(self):
        engine = DataFusionEngine()
        engine.df_ctx = MagicMock(spec=SessionContext)
        mock_df = MagicMock(spec=["limit", "to_pydict"])
        limited_df = MagicMock(spec=["to_pydict"])
        limited_df.to_pydict.return_value = {"col1": [1, 2, 3]}
        mock_df.limit.return_value = limited_df
        engine.df_ctx.sql.return_value = mock_df

        result = engine.execute_query("SELECT * FROM test_table", max_rows=3)

        engine.df_ctx.sql.assert_called_once_with("SELECT * FROM test_table")
        mock_df.limit.assert_called_once_with(4)
        assert result == {"col1": [1, 2, 3]}

    def test_execute_query_with_max_rows_logs_warning_when_exceeded(self):
        engine = DataFusionEngine()
        engine.df_ctx = MagicMock(spec=SessionContext)
        mock_df = MagicMock(spec=["limit", "to_pydict"])
        limited_df = MagicMock(spec=["to_pydict"])
        limited_df.to_pydict.return_value = {"col1": [1, 2, 3, 4]}
        mock_df.limit.return_value = limited_df
        engine.df_ctx.sql.return_value = mock_df

        with patch.object(engine.log, "warning") as mock_warning:
            result = engine.execute_query("SELECT * FROM test_table", max_rows=3)

        engine.df_ctx.sql.assert_called_once_with("SELECT * FROM test_table")
        mock_df.limit.assert_called_once_with(4)
        mock_warning.assert_called_once_with(
            "Query returned more than %s rows. Returning first %s rows.",
            3,
            3,
        )
        assert result == {"col1": [1, 2, 3]}

    def test_execute_query_failure(self):
        engine = DataFusionEngine()
        engine.df_ctx = MagicMock(spec=SessionContext)
        engine.df_ctx.sql.side_effect = Exception("SQL Error")

        with pytest.raises(QueryExecutionException, match="Error while executing query"):
            engine.execute_query("SELECT * FROM test_table")

    @patch.object(DataFusionEngine, "_get_connection_config")
    def test_execute_query_with_local_csv(self, mock_get_conn):
        mock_get_conn.return_value = None

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("name,age\nAlice,30\nBob,25\n")
            csv_path = f.name

        try:
            engine = DataFusionEngine()
            datasource_config = DataSourceConfig(
                table_name="test_csv",
                uri=f"file://{csv_path}",
                format="csv",
                storage_type="local",
                conn_id="",
            )

            engine.register_datasource(datasource_config)

            result = engine.execute_query("SELECT * FROM test_csv ORDER BY name")

            expected = {"name": ["Alice", "Bob"], "age": [30, 25]}
            assert result == expected
        finally:
            os.unlink(csv_path)

    @patch("airflow.providers.common.sql.datafusion.engine.get_object_storage_provider", autospec=True)
    @patch.object(DataFusionEngine, "_get_connection_config")
    def test_register_datasource_with_options(self, mock_get_conn, mock_factory):
        mock_get_conn.return_value = TEST_CONNECTION_CONFIG
        mock_provider = MagicMock(spec=ObjectStorageProvider)
        mock_store = MagicMock()
        mock_provider.create_object_store.return_value = mock_store
        mock_provider.get_scheme.return_value = "s3"
        mock_factory.return_value = mock_provider

        engine = DataFusionEngine()

        datasource_config = DataSourceConfig(
            conn_id="aws_default",
            table_name="test_table",
            uri="s3://bucket/path/",
            format="parquet",
            options={"table_partition_cols": [("year", "integer"), ("month", "integer")]},
        )

        engine.df_ctx = MagicMock(spec=SessionContext)

        engine.register_datasource(datasource_config)

        mock_factory.assert_called_once()
        mock_provider.create_object_store.assert_called_once_with(
            "s3://bucket/path/", connection_config=mock_get_conn.return_value
        )
        engine.df_ctx.register_object_store.assert_called_once_with(schema="s3", store=mock_store)

        engine.df_ctx.register_parquet.assert_called_once_with(
            "test_table",
            "s3://bucket/path/",
            table_partition_cols=[("year", "integer"), ("month", "integer")],
        )

        assert engine.registered_tables == {"test_table": "s3://bucket/path/"}

    def test_remove_none_values(self):
        result = DataFusionEngine._remove_none_values({"a": 1, "b": None, "c": "test", "d": None})
        assert result == {"a": 1, "c": "test"}

    def test_get_connection_config(self):

        engine = DataFusionEngine()

        result = engine._get_connection_config("aws_default")
        expected = ConnectionConfig(
            conn_id="aws_default",
            credentials={
                "access_key_id": "fake_id",
                "secret_access_key": "fake_secret",
            },
            extra_config={"region": "us-east-1"},
        )
        assert result.conn_id == expected.conn_id
        assert result.credentials == expected.credentials
        assert result.extra_config == expected.extra_config

    def test_get_credentials_unknown_type(self):
        mock_conn = MagicMock()
        mock_conn.conn_type = "dummy"
        engine = DataFusionEngine()

        with pytest.raises(ValueError, match="Unknown connection type dummy"):
            engine._get_credentials(mock_conn)

    def test_get_schema_success(self):
        engine = DataFusionEngine()
        engine.df_ctx = MagicMock(spec=SessionContext)
        mock_table = MagicMock()
        mock_schema = MagicMock()
        mock_schema.__str__ = lambda self: "id: int64, name: string"
        mock_table.schema.return_value = mock_schema
        engine.df_ctx.table.return_value = mock_table

        result = engine.get_schema("test_table")

        engine.df_ctx.table.assert_called_once_with("test_table")
        mock_table.schema.assert_called_once()
        assert result == "id: int64, name: string"

    @patch.object(DataFusionEngine, "_get_connection_config")
    def test_get_schema_with_local_csv(self, mock_get_conn):
        mock_get_conn.return_value = None

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("name,age\nAlice,30\nBob,25\n")
            csv_path = f.name

        try:
            engine = DataFusionEngine()
            datasource_config = DataSourceConfig(
                table_name="test_csv",
                uri=f"file://{csv_path}",
                format="csv",
                storage_type="local",
                conn_id="",
            )

            engine.register_datasource(datasource_config)

            result = engine.get_schema("test_csv")

            assert "name: string" in result
            assert "age: int64" in result
        finally:
            os.unlink(csv_path)


class TestIterQueryRowChunks:
    """Unit tests for DataFusionEngine.iter_query_row_chunks()."""

    @staticmethod
    def _make_batch(data: dict) -> MagicMock:
        """Return a mock RecordBatch whose to_pyarrow().to_pydict() returns *data*."""
        batch = MagicMock()
        batch.to_pyarrow.return_value.to_pydict.return_value = data
        return batch

    def _engine(self) -> DataFusionEngine:
        engine = DataFusionEngine()
        engine.df_ctx = MagicMock(spec=SessionContext)
        return engine

    # ------------------------------------------------------------------
    # execute_stream path
    # ------------------------------------------------------------------

    def test_execute_stream_path_yields_all_batches(self):
        engine = self._engine()
        batch1 = self._make_batch({"id": [1, 2], "email": ["a@b.com", "c@d.com"]})
        batch2 = self._make_batch({"id": [3], "email": ["e@f.com"]})
        mock_df = MagicMock(spec=["execute_stream"])
        mock_df.execute_stream.return_value = iter([batch1, batch2])
        engine.df_ctx.sql.return_value = mock_df

        result = list(engine.iter_query_row_chunks("SELECT id, email FROM customers"))

        engine.df_ctx.sql.assert_called_once_with("SELECT id, email FROM customers")
        mock_df.execute_stream.assert_called_once()
        assert result == [
            {"id": [1, 2], "email": ["a@b.com", "c@d.com"]},
            {"id": [3], "email": ["e@f.com"]},
        ]

    def test_execute_stream_path_single_batch(self):
        engine = self._engine()
        batch = self._make_batch({"name": ["Alice"]})
        mock_df = MagicMock(spec=["execute_stream"])
        mock_df.execute_stream.return_value = iter([batch])
        engine.df_ctx.sql.return_value = mock_df

        result = list(engine.iter_query_row_chunks("SELECT name FROM users"))

        assert result == [{"name": ["Alice"]}]

    def test_execute_stream_path_empty_stream(self):
        engine = self._engine()
        mock_df = MagicMock(spec=["execute_stream"])
        mock_df.execute_stream.return_value = iter([])
        engine.df_ctx.sql.return_value = mock_df

        result = list(engine.iter_query_row_chunks("SELECT * FROM empty_table"))

        assert result == []

    # ------------------------------------------------------------------
    # collect fallback path
    # ------------------------------------------------------------------

    def test_collect_fallback_yields_all_batches(self):
        engine = self._engine()
        batch1 = self._make_batch({"id": [10], "val": [99]})
        batch2 = self._make_batch({"id": [20], "val": [88]})
        mock_df = MagicMock(spec=["collect"])
        mock_df.collect.return_value = iter([batch1, batch2])
        engine.df_ctx.sql.return_value = mock_df

        result = list(engine.iter_query_row_chunks("SELECT id, val FROM test"))

        mock_df.collect.assert_called_once()
        assert result == [
            {"id": [10], "val": [99]},
            {"id": [20], "val": [88]},
        ]

    def test_collect_fallback_empty(self):
        engine = self._engine()
        mock_df = MagicMock(spec=["collect"])
        mock_df.collect.return_value = iter([])
        engine.df_ctx.sql.return_value = mock_df

        result = list(engine.iter_query_row_chunks("SELECT * FROM empty_table"))

        assert result == []

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def test_raises_query_execution_exception_on_sql_error(self):
        engine = self._engine()
        engine.df_ctx.sql.side_effect = Exception("SQL Error")

        with pytest.raises(QueryExecutionException, match="Error while executing query"):
            list(engine.iter_query_row_chunks("INVALID SQL"))

    def test_raises_query_execution_exception_on_stream_error(self):
        engine = self._engine()
        mock_df = MagicMock(spec=["execute_stream"])
        mock_df.execute_stream.side_effect = Exception("Stream Error")
        engine.df_ctx.sql.return_value = mock_df

        with pytest.raises(QueryExecutionException, match="Error while executing query"):
            list(engine.iter_query_row_chunks("SELECT * FROM test"))

    def test_raises_query_execution_exception_on_collect_error(self):
        engine = self._engine()
        mock_df = MagicMock(spec=["collect"])
        mock_df.collect.side_effect = Exception("Collect Error")
        engine.df_ctx.sql.return_value = mock_df

        with pytest.raises(QueryExecutionException, match="Error while executing query"):
            list(engine.iter_query_row_chunks("SELECT * FROM test"))

    def test_exception_preserves_original_cause(self):
        engine = self._engine()
        original_error = Exception("original cause")
        engine.df_ctx.sql.side_effect = original_error

        with pytest.raises(QueryExecutionException) as exc_info:
            list(engine.iter_query_row_chunks("SELECT * FROM test"))

        assert exc_info.value.__cause__ is original_error
