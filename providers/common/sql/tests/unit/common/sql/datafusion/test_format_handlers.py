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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.sql.config import DataSourceConfig, FormatType
from airflow.providers.common.sql.datafusion.exceptions import (
    FileFormatRegistrationException,
    IcebergRegistrationException,
)
from airflow.providers.common.sql.datafusion.format_handlers import (
    AvroFormatHandler,
    CsvFormatHandler,
    IcebergFormatHandler,
    ParquetFormatHandler,
    get_format_handler,
)


class TestFormatHandlers:
    @pytest.fixture
    def session_context_mock(self):
        return MagicMock()

    def test_parquet_handler_success(self, session_context_mock):
        datasource_config = DataSourceConfig(
            table_name="table_name",
            uri="file://path/to/file",
            format="parquet",
            conn_id="conn_id",
            options={"key": "value"},
        )
        handler = ParquetFormatHandler(datasource_config)
        handler.register_data_source_format(session_context_mock)
        session_context_mock.register_parquet.assert_called_once_with(
            "table_name", "file://path/to/file", key="value"
        )
        assert handler.get_format == FormatType.PARQUET

    def test_parquet_handler_failure(self, session_context_mock):
        session_context_mock.register_parquet.side_effect = Exception("Error")
        datasource_config = DataSourceConfig(
            table_name="table_name", uri="file://path/to/file", format="parquet", conn_id="conn_id"
        )
        handler = ParquetFormatHandler(datasource_config)
        with pytest.raises(FileFormatRegistrationException, match="Failed to register Parquet data source"):
            handler.register_data_source_format(session_context_mock)

    def test_csv_handler_success(self, session_context_mock):
        datasource_config = DataSourceConfig(
            table_name="table_name",
            uri="file://path/to/file",
            format="csv",
            conn_id="conn_id",
            options={"delimiter": ","},
        )
        handler = CsvFormatHandler(datasource_config)
        handler.register_data_source_format(session_context_mock)
        session_context_mock.register_csv.assert_called_once_with(
            "table_name", "file://path/to/file", delimiter=","
        )
        assert handler.get_format == FormatType.CSV

    def test_csv_handler_failure(self, session_context_mock):
        session_context_mock.register_csv.side_effect = Exception("Error")
        datasource_config = DataSourceConfig(
            table_name="table_name", uri="file://path/to/file", format="csv", conn_id="conn_id"
        )
        handler = CsvFormatHandler(datasource_config)
        with pytest.raises(FileFormatRegistrationException, match="Failed to register csv data source"):
            handler.register_data_source_format(session_context_mock)

    def test_avro_handler_success(self, session_context_mock):
        datasource_config = DataSourceConfig(
            table_name="table_name",
            uri="file://path/to/file",
            format="avro",
            conn_id="conn_id",
            options={"key": "value"},
        )
        handler = AvroFormatHandler(datasource_config)
        handler.register_data_source_format(session_context_mock)
        session_context_mock.register_avro.assert_called_once_with(
            "table_name", "file://path/to/file", key="value"
        )
        assert handler.get_format == FormatType.AVRO

    def test_avro_handler_failure(self, session_context_mock):
        session_context_mock.register_avro.side_effect = Exception("Error")
        datasource_config = DataSourceConfig(
            table_name="table_name", uri="file://path/to/file", format="avro", conn_id="conn_id"
        )
        handler = AvroFormatHandler(datasource_config)
        with pytest.raises(FileFormatRegistrationException, match="Failed to register Avro data source"):
            handler.register_data_source_format(session_context_mock)

    @patch("airflow.providers.apache.iceberg.hooks.iceberg.IcebergHook")
    def test_iceberg_handler_success(self, mock_iceberg_hook_cls, session_context_mock):
        mock_hook = MagicMock()
        mock_iceberg_hook_cls.return_value = mock_hook
        mock_iceberg_table = MagicMock()
        mock_iceberg_table.io.properties = {}
        mock_hook.load_table.return_value = mock_iceberg_table
        datasource_config = DataSourceConfig(
            table_name="my_table",
            format="iceberg",
            conn_id="iceberg_default",
            db_name="default",
        )
        handler = IcebergFormatHandler(datasource_config)
        handler.register_data_source_format(session_context_mock)

        mock_iceberg_hook_cls.assert_called_once_with(iceberg_conn_id="iceberg_default")
        mock_hook.load_table.assert_called_once_with("default.my_table")
        session_context_mock.register_table.assert_called_once_with("my_table", mock_iceberg_table)
        assert handler.get_format == FormatType.ICEBERG

    @patch("airflow.providers.apache.iceberg.hooks.iceberg.IcebergHook")
    def test_iceberg_handler_failure(self, mock_iceberg_hook_cls, session_context_mock):
        mock_hook = MagicMock()
        mock_iceberg_hook_cls.return_value = mock_hook
        mock_hook.load_table.side_effect = Exception("catalog error")
        datasource_config = DataSourceConfig(
            table_name="my_table", format="iceberg", conn_id="iceberg_default", db_name="default"
        )
        handler = IcebergFormatHandler(datasource_config)
        with pytest.raises(IcebergRegistrationException, match="Failed to register Iceberg table"):
            handler.register_data_source_format(session_context_mock)

    def test_iceberg_handler_default_options(self):
        datasource_config = DataSourceConfig(
            table_name="my_table", format="iceberg", conn_id="iceberg_default"
        )
        handler = IcebergFormatHandler(datasource_config)
        assert handler.datasource_config.options == {}
        assert handler.datasource_config.conn_id == "iceberg_default"
        assert handler.get_format == FormatType.ICEBERG

    def test_get_format_handler(self):
        assert isinstance(
            get_format_handler(
                DataSourceConfig(table_name="t", format="parquet", conn_id="c", uri="file://u")
            ),
            ParquetFormatHandler,
        )
        assert isinstance(
            get_format_handler(DataSourceConfig(table_name="t", format="csv", conn_id="c", uri="file://u")),
            CsvFormatHandler,
        )
        assert isinstance(
            get_format_handler(DataSourceConfig(table_name="t", format="avro", conn_id="c", uri="file://u")),
            AvroFormatHandler,
        )
        assert isinstance(
            get_format_handler(DataSourceConfig(table_name="t", format="iceberg", conn_id="iceberg_default")),
            IcebergFormatHandler,
        )

        with pytest.raises(ValueError, match="Unsupported storage type"):
            get_format_handler(DataSourceConfig(table_name="t", format="invalid", conn_id="c"))
