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

from datafusion import SessionContext

from airflow.providers.common.sql.config import ConnectionConfig, DataSourceConfig, StorageType
from airflow.providers.common.sql.datafusion.exceptions import (
    ObjectStoreCreationException,
    QueryExecutionException,
)
from airflow.providers.common.sql.datafusion.format_handlers import get_format_handler
from airflow.providers.common.sql.datafusion.object_storage_provider import get_object_storage_provider
from airflow.utils.log.logging_mixin import LoggingMixin


class DataFusionEngine(LoggingMixin):
    """Apache DataFusion engine."""

    def __init__(self):
        super().__init__()
        self.df_ctx = SessionContext()
        self.registered_tables: dict[str, str] = {}

    @property
    def session_context(self) -> SessionContext:
        """Return the session context."""
        return self.df_ctx

    def register_datasource(self, datasource_config: DataSourceConfig):
        """Register a datasource with the datafusion engine."""
        if not isinstance(datasource_config, DataSourceConfig):
            raise ValueError("datasource_config must be of type DataSourceConfig")

        if not datasource_config.is_table_provider:
            if datasource_config.storage_type == StorageType.LOCAL:
                connection_config = None
            else:
                connection_config = self._get_connection_config(datasource_config.conn_id)

            self._register_object_store(datasource_config, connection_config)

        self._register_data_source_format(datasource_config)

    def _register_object_store(
        self, datasource_config: DataSourceConfig, connection_config: ConnectionConfig | None
    ):
        """Register object stores."""
        if TYPE_CHECKING:
            assert datasource_config.storage_type is not None

        try:
            storage_provider = get_object_storage_provider(datasource_config.storage_type)
            object_store = storage_provider.create_object_store(
                datasource_config.uri, connection_config=connection_config
            )
            schema = storage_provider.get_scheme()
            self.session_context.register_object_store(schema=schema, store=object_store)
            self.log.info("Registered object store for schema: %s", schema)
        except Exception as e:
            raise ObjectStoreCreationException(
                f"Error while creating object store for {datasource_config.storage_type}: {e}"
            )

    def _register_data_source_format(self, datasource_config: DataSourceConfig):
        """Register data source format."""
        if TYPE_CHECKING:
            assert datasource_config.table_name is not None
            assert datasource_config.format is not None

        if datasource_config.table_name in self.registered_tables:
            raise ValueError(
                f"Table {datasource_config.table_name} already registered for {self.registered_tables[datasource_config.table_name]}, please choose different name"
            )

        format_cls = get_format_handler(datasource_config)

        format_cls.register_data_source_format(self.session_context)
        self.registered_tables[datasource_config.table_name] = datasource_config.uri
        self.log.info(
            "Registered data source format %s for table: %s",
            datasource_config.format,
            datasource_config.table_name,
        )

    def execute_query(self, query: str, max_rows: int | None = None) -> dict[str, list[Any]]:
        """Execute a query and return the result as a dictionary."""
        try:
            self.log.info("Executing query: %s", query)
            df = self.session_context.sql(query)

            if max_rows is not None:
                result = df.limit(max_rows + 1).to_pydict()
                if result and len(next(iter(result.values()))) > max_rows:
                    self.log.warning(
                        "Query returned more than %s rows. Returning first %s rows.",
                        max_rows,
                        max_rows,
                    )
                    return {column: values[:max_rows] for column, values in result.items()}
                return result
            return df.to_pydict()
        except Exception as e:
            raise QueryExecutionException(f"Error while executing query: {e}")

    def _get_connection_config(self, conn_id: str) -> ConnectionConfig:
        """Build a ConnectionConfig; credential resolution is delegated to the provider."""
        return ConnectionConfig(conn_id=conn_id)

    def get_schema(self, table_name: str):
        """Get the schema of a table."""
        schema = str(self.session_context.table(table_name).schema())
        return schema
