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

from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.config import DataSourceConfig, FormatType
from airflow.providers.common.sql.datafusion.base import FormatHandler
from airflow.providers.common.sql.datafusion.exceptions import (
    FileFormatRegistrationException,
    IcebergRegistrationException,
)

if TYPE_CHECKING:
    from datafusion import SessionContext


class ParquetFormatHandler(FormatHandler):
    """Parquet format handler."""

    @property
    def get_format(self) -> FormatType:
        """Return the format type."""
        return FormatType.PARQUET

    def register_data_source_format(self, ctx: SessionContext):
        """Register a data source format."""
        try:
            ctx.register_parquet(
                self.datasource_config.table_name,
                self.datasource_config.uri,
                **self.datasource_config.options,
            )
        except Exception as e:
            raise FileFormatRegistrationException(f"Failed to register {self.get_format} data source: {e}")


class CsvFormatHandler(FormatHandler):
    """CSV format handler."""

    @property
    def get_format(self) -> FormatType:
        """Return the format type."""
        return FormatType.CSV

    def register_data_source_format(self, ctx: SessionContext):
        """Register a data source format."""
        try:
            ctx.register_csv(
                self.datasource_config.table_name,
                self.datasource_config.uri,
                **self.datasource_config.options,
            )
        except Exception as e:
            raise FileFormatRegistrationException(f"Failed to register {self.get_format} data source: {e}")


class AvroFormatHandler(FormatHandler):
    """Avro format handler."""

    @property
    def get_format(self) -> FormatType:
        """Return the format type."""
        return FormatType.AVRO

    def register_data_source_format(self, ctx: SessionContext) -> None:
        """Register a data source format."""
        try:
            ctx.register_avro(
                self.datasource_config.table_name,
                self.datasource_config.uri,
                **self.datasource_config.options,
            )
        except Exception as e:
            raise FileFormatRegistrationException(f"Failed to register {self.get_format} data source: {e}")


class IcebergFormatHandler(FormatHandler):
    """
    Iceberg format handler for DataFusion.

    Loads an Iceberg table from a catalog using ``IcebergHook`` and registers
    it with a DataFusion ``SessionContext`` via ``register_table_provider``.
    """

    @property
    def get_format(self) -> FormatType:
        """Return the format type."""
        return FormatType.ICEBERG

    def register_data_source_format(self, ctx: SessionContext) -> None:
        """Register an Iceberg table with the DataFusion session context."""
        try:
            from airflow.providers.apache.iceberg.hooks.iceberg import IcebergHook
        except ImportError:
            raise AirflowOptionalProviderFeatureException(
                "Iceberg format requires the apache-airflow-providers-apache-iceberg package. "
                "Install it with: pip install 'apache-airflow-providers-apache-iceberg'"
            )

        try:
            hook = IcebergHook(iceberg_conn_id=self.datasource_config.conn_id)
            namespace_table = f"{self.datasource_config.db_name}.{self.datasource_config.table_name}"
            iceberg_table = hook.load_table(namespace_table)
            io_properties = iceberg_table.io.properties

            # TODO: Test for other catalog types
            if "client.access-key-id" in io_properties:
                # These properties require working datafusion otherwise it gets error when reading metadata from the s3
                io_properties["s3.access-key-id"] = io_properties.get("client.access-key-id")
                io_properties["s3.secret-access-key"] = io_properties.get("client.secret-access-key")
            iceberg_table.io.properties = io_properties
            ctx.register_table(self.datasource_config.table_name, iceberg_table)
        except Exception as e:
            raise IcebergRegistrationException(
                f"Failed to register Iceberg table '{self.datasource_config.table_name}' "
                f"from connection '{self.datasource_config.conn_id}': {e}"
            )


def get_format_handler(datasource_config: DataSourceConfig) -> FormatHandler:
    """Get a format handler based on the format type."""
    format_type = datasource_config.format.lower()

    match format_type:
        case "parquet":
            return ParquetFormatHandler(datasource_config)
        case "csv":
            return CsvFormatHandler(datasource_config)
        case "avro":
            return AvroFormatHandler(datasource_config)
        case "iceberg":
            return IcebergFormatHandler(datasource_config)
        case _:
            raise ValueError(f"Unsupported format: {format_type}")
