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

from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.datafusion.base import FormatHandler
from airflow.providers.common.ai.exceptions import FileFormatRegistrationException
from airflow.providers.common.ai.utils.config import FormatType

if TYPE_CHECKING:
    from datafusion import SessionContext


class ParquetFormatHandler(FormatHandler):
    """
    Parquet format handler.

    :param options: Additional options for the Parquet format.
        https://datafusion.apache.org/python/autoapi/datafusion/context/index.html#datafusion.context.SessionContext.register_parquet
    """

    def __init__(self, options: dict[str, Any] | None = None):
        self.options = options or {}

    @property
    def get_format(self) -> str:
        """Return the format type."""
        return FormatType.PARQUET.value

    def register_data_source_format(self, ctx: SessionContext, table_name: str, path: str):
        """Register a data source format."""
        try:
            ctx.register_parquet(table_name, path, **self.options)
        except Exception as e:
            raise FileFormatRegistrationException("Failed to register Parquet data source: %s", e)


class CsvFormatHandler(FormatHandler):
    """
    CSV format handler.

    :param options: Additional options for the CSV format.
        https://datafusion.apache.org/python/autoapi/datafusion/context/index.html#datafusion.context.SessionContext.register_csv
    """

    def __init__(self, options: dict[str, Any] | None = None):
        self.options = options or {}

    @property
    def get_format(self) -> str:
        """Return the format type."""
        return FormatType.CSV.value

    def register_data_source_format(self, ctx: SessionContext, table_name: str, path: str):
        """Register a data source format."""
        try:
            ctx.register_csv(table_name, path, **self.options)
        except Exception as e:
            raise FileFormatRegistrationException("Failed to register csv data source: %s", e)


class AvroFormatHandler(FormatHandler):
    """
    Avro format handler.

    :param options: Additional options for the Avro format.
        https://datafusion.apache.org/python/autoapi/datafusion/context/index.html#datafusion.context.SessionContext.register_avro
    """

    def __init__(self, options: dict[str, Any] | None = None):
        self.options = options or {}

    @property
    def get_format(self) -> str:
        """Return the format type."""
        return FormatType.AVRO.value

    def register_data_source_format(self, ctx: SessionContext, table_name: str, path: str) -> None:
        """Register a data source format."""
        try:
            ctx.register_avro(table_name, path, **self.options)
        except Exception as e:
            raise FileFormatRegistrationException("Failed to register Avro data source: %s", e)


def get_format_handler(format_type: str, options: dict[str, Any] | None = None) -> FormatHandler:
    """Get a format handler based on the format type."""
    format_type = format_type.lower()

    match format_type:
        case "parquet":
            return ParquetFormatHandler(options)
        case "csv":
            return CsvFormatHandler(options)
        case "avro":
            return AvroFormatHandler(options)
        case _:
            raise ValueError(f"Unsupported format: {format_type}")
