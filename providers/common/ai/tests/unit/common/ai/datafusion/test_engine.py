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

from airflow.providers.common.ai.datafusion.engine import DataFusionEngine
from airflow.providers.common.ai.exceptions import ObjectStoreCreationException, QueryExecutionException
from airflow.providers.common.ai.utils.config import ConnectionConfig, DataSourceConfig
from airflow.sdk import Connection


class TestDataFusionEngine:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(Connection(conn_id="aws_default", conn_type="aws"))

    def test_init(self):
        engine = DataFusionEngine()
        assert engine.df_ctx is not None
        assert engine.registered_tables == {}

    def test_register_datasource_invalid_config(self):
        engine = DataFusionEngine()
        with pytest.raises(ValueError, match="datasource_config must be of type DataSourceConfig"):
            engine.register_datasource("invalid", None)

    @pytest.mark.parametrize(
        ("storage_type", "format", "scheme"),
        [("s3", "parquet", "s3"), ("s3", "csv", "s3"), ("s3", "avro", "s3")],
    )
    @patch("airflow.providers.common.ai.datafusion.engine.ObjectStorageProviderFactory")
    def test_register_datasource_success(self, mock_factory, storage_type, format, scheme):
        mock_provider = MagicMock()
        mock_store = MagicMock()
        mock_provider.create_object_store.return_value = mock_store
        mock_provider.get_scheme.return_value = scheme
        mock_factory.create_provider.return_value = mock_provider

        engine = DataFusionEngine()

        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="test_table", uri=f"{scheme}://bucket/path", format=format
        )
        connection_config = ConnectionConfig(conn_id="aws_default")

        engine.df_ctx = MagicMock()

        engine.register_datasource(datasource_config, connection_config)

        mock_factory.create_provider.assert_called_once()
        mock_provider.create_object_store.assert_called_once_with(
            f"{scheme}://bucket/path", connection_config=connection_config
        )
        engine.df_ctx.register_object_store.assert_called_once_with(schema=scheme, store=mock_store)

        if format == "parquet":
            engine.df_ctx.register_parquet.assert_called_once_with("test_table", f"{scheme}://bucket/path")
        elif format == "csv":
            engine.df_ctx.register_csv.assert_called_once_with("test_table", f"{scheme}://bucket/path")
        elif format == "avro":
            engine.df_ctx.register_avro.assert_called_once_with("test_table", f"{scheme}://bucket/path")

        assert engine.registered_tables == {"test_table": f"{scheme}://bucket/path"}

    @patch("airflow.providers.common.ai.datafusion.engine.ObjectStorageProviderFactory")
    def test_register_datasource_object_store_exception(self, mock_factory):
        mock_factory.create_provider.side_effect = Exception("Provider error")

        engine = DataFusionEngine()
        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="test_table", uri="s3://bucket/path", format="parquet"
        )
        connection_config = ConnectionConfig(conn_id="aws_default")

        with pytest.raises(ObjectStoreCreationException, match="Error while creating object store"):
            engine.register_datasource(datasource_config, connection_config)

    def test_register_datasource_duplicate_table(self):
        engine = DataFusionEngine()
        engine.registered_tables["test_table"] = "s3://old/path"

        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="test_table", uri="s3://new/path", format="parquet"
        )
        connection_config = ConnectionConfig(conn_id="aws_default")

        with patch.object(engine, "_register_object_store"):
            with pytest.raises(ValueError, match="Table test_table already registered"):
                engine.register_datasource(datasource_config, connection_config)

    def test_execute_query_success(self):
        engine = DataFusionEngine()
        engine.df_ctx = MagicMock()
        mock_df = MagicMock()
        mock_df.to_pydict.return_value = {"col1": [1, 2]}
        engine.df_ctx.sql.return_value = mock_df

        result = engine.execute_query("SELECT * FROM test_table")

        engine.df_ctx.sql.assert_called_once_with("SELECT * FROM test_table")
        assert result == {"col1": [1, 2]}

    def test_execute_query_failure(self):
        engine = DataFusionEngine()
        engine.df_ctx = MagicMock()
        engine.df_ctx.sql.side_effect = Exception("SQL Error")

        with pytest.raises(QueryExecutionException, match="Error while executing query"):
            engine.execute_query("SELECT * FROM test_table")

    def test_execute_query_with_local_csv(self):
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
            connection_config = ConnectionConfig(conn_id="local")

            engine.register_datasource(datasource_config, connection_config)

            result = engine.execute_query("SELECT * FROM test_csv ORDER BY name")

            expected = {"name": ["Alice", "Bob"], "age": [30, 25]}
            assert result == expected
        finally:
            os.unlink(csv_path)
