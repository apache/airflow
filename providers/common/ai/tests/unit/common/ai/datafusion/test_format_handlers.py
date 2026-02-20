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

from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.datafusion.format_handlers import (
    AvroFormatHandler,
    CsvFormatHandler,
    ParquetFormatHandler,
    get_format_handler,
)
from airflow.providers.common.ai.exceptions import FileFormatRegistrationException
from airflow.providers.common.ai.utils.config import FormatType


class TestFormatHandlers:
    @pytest.fixture
    def session_context_mock(self):
        return MagicMock()

    def test_parquet_handler_success(self, session_context_mock):
        handler = ParquetFormatHandler(options={"key": "value"})
        handler.register_data_source_format(session_context_mock, "table_name", "path/to/file")
        session_context_mock.register_parquet.assert_called_once_with(
            "table_name", "path/to/file", key="value"
        )
        assert handler.get_format() == FormatType.PARQUET.value

    def test_parquet_handler_failure(self, session_context_mock):
        session_context_mock.register_parquet.side_effect = Exception("Error")
        handler = ParquetFormatHandler()
        with pytest.raises(FileFormatRegistrationException, match="Failed to register Parquet data source"):
            handler.register_data_source_format(session_context_mock, "table_name", "path/to/file")

    def test_csv_handler_success(self, session_context_mock):
        handler = CsvFormatHandler(options={"delimiter": ","})
        handler.register_data_source_format(session_context_mock, "table_name", "path/to/file")
        session_context_mock.register_csv.assert_called_once_with("table_name", "path/to/file", delimiter=",")
        assert handler.get_format() == FormatType.CSV.value

    def test_csv_handler_failure(self, session_context_mock):
        session_context_mock.register_csv.side_effect = Exception("Error")
        handler = CsvFormatHandler()
        with pytest.raises(FileFormatRegistrationException, match="Failed to register csv data source"):
            handler.register_data_source_format(session_context_mock, "table_name", "path/to/file")

    def test_avro_handler_success(self, session_context_mock):
        handler = AvroFormatHandler(options={"key": "value"})
        handler.register_data_source_format(session_context_mock, "table_name", "path/to/file")
        session_context_mock.register_avro.assert_called_once_with("table_name", "path/to/file", key="value")
        assert handler.get_format() == FormatType.AVRO.value

    def test_avro_handler_failure(self, session_context_mock):
        session_context_mock.register_avro.side_effect = Exception("Error")
        handler = AvroFormatHandler()
        with pytest.raises(FileFormatRegistrationException, match="Failed to register Avro data source"):
            handler.register_data_source_format(session_context_mock, "table_name", "path/to/file")

    def test_get_format_handler(self):
        assert isinstance(get_format_handler("parquet"), ParquetFormatHandler)
        assert isinstance(get_format_handler("csv"), CsvFormatHandler)
        assert isinstance(get_format_handler("avro"), AvroFormatHandler)

        with pytest.raises(ValueError, match="Unsupported format"):
            get_format_handler("invalid")
