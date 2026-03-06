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

from io import FileIO
from unittest.mock import MagicMock, patch

import pytest
from pyiceberg.table import Table

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

    @pytest.mark.parametrize(
        ("format", "handler_class", "options"),
        [
            ("parquet", ParquetFormatHandler, {"key": "value"}),
            ("csv", CsvFormatHandler, {"delimiter": ","}),
            ("avro", AvroFormatHandler, {"key": "value"}),
        ],
    )
    def test_file_handler_success(self, session_context_mock, format, handler_class, options):
        datasource_config = DataSourceConfig(
            table_name="table_name",
            uri=f"file://path/to/file.{format}",
            format=format,
            conn_id="conn_id",
            options=options,
        )
        handler = handler_class(datasource_config)
        handler.register_data_source_format(session_context_mock)
        register_method = getattr(session_context_mock, f"register_{format}")
        register_method.assert_called_once_with("table_name", f"file://path/to/file.{format}", **options)
        assert handler.get_format == format

    @pytest.mark.parametrize(
        ("format", "handler_class"),
        [
            (FormatType.PARQUET, ParquetFormatHandler),
            (FormatType.CSV, CsvFormatHandler),
            (FormatType.AVRO, AvroFormatHandler),
        ],
    )
    def test_file_handler_failure(self, session_context_mock, format, handler_class):
        datasource_config = DataSourceConfig(
            table_name="table_name",
            uri=f"file://path/to/file.{format}",
            format=format,
            conn_id="conn_id",
        )
        handler = handler_class(datasource_config)
        register_method = getattr(session_context_mock, f"register_{format.name.lower()}")
        register_method.side_effect = Exception("Error")
        with pytest.raises(
            FileFormatRegistrationException, match=f"Failed to register {format} data source:"
        ):
            handler.register_data_source_format(session_context_mock)

    @patch("airflow.providers.apache.iceberg.hooks.iceberg.IcebergHook")
    def test_iceberg_handler_success(self, mock_iceberg_hook_cls, session_context_mock):
        mock_hook = MagicMock()
        mock_iceberg_hook_cls.return_value = mock_hook
        mock_iceberg_table = MagicMock(spec=Table)
        mock_io = MagicMock(spec=FileIO)
        mock_io.properties = {
            "client.access-key-id": "test_key",
            "client.secret-access-key": "test_secret",
        }
        mock_iceberg_table.io = mock_io
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

        # Verify that s3 properties are correctly set
        assert mock_iceberg_table.io.properties["s3.access-key-id"] == "test_key"
        assert mock_iceberg_table.io.properties["s3.secret-access-key"] == "test_secret"

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

    @pytest.mark.parametrize(
        ("config_params", "error_match"),
        [
            (
                {"table_name": "t", "format": "iceberg", "conn_id": "c"},
                "Database name must be provided for table providers",
            ),
            (
                {"table_name": "", "format": "parquet", "conn_id": "c", "uri": "file://u"},
                "Table name must be provided for storage type",
            ),
            (
                {"table_name": "t", "format": "parquet", "conn_id": "c"},
                "Unsupported storage type for URI:",
            ),
        ],
    )
    def test_missing_mandatory_fields(self, config_params, error_match):
        with pytest.raises(ValueError, match=error_match):
            DataSourceConfig(**config_params)

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
            get_format_handler(
                DataSourceConfig(table_name="t", format="iceberg", conn_id="iceberg_default", db_name="d")
            ),
            IcebergFormatHandler,
        )

        with pytest.raises(ValueError, match="Unsupported format: invalid"):
            get_format_handler(
                DataSourceConfig(table_name="t", format="invalid", conn_id="c", uri="file://u")
            )
