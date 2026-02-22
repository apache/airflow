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

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


@dataclass(frozen=True)
class ConnectionConfig:
    """Configuration for datafusion object store connections."""

    conn_id: str
    credentials: dict[str, Any] = field(default_factory=dict)
    extra_config: dict[str, Any] = field(default_factory=dict)


class FormatType(str, Enum):
    """Supported data formats."""

    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"


class StorageType(str, Enum):
    """Storage types for Data Fusion."""

    GCS = "gcs"
    S3 = "s3"
    AZURE = "azure"
    LOCAL = "local"
    HTTP = "http"


@dataclass
class DataSourceConfig:
    """
    Configuration for an input data source.

    Attributes:
        conn_id (str): The connection ID to use for accessing the data source.
        uri (str): The URI of the data source (e.g., file path, S3 bucket, etc.).
        format (str | None): The format of the data (e.g., 'parquet', 'csv').
        table_name (str | None): The name of the table if applicable.
        schema (dict[str, str] | None): A dictionary mapping column names to their types.
        db_name (str | None): The database name if applicable.
        storage_type (StorageType | None): The type of storage (automatically inferred from URI).
        options (dict[str, Any]): Additional options for the data source. eg: you can set partition columns to any datasource
            that will be set in while registering the data
    """

    conn_id: str
    uri: str
    format: str | None = None
    table_name: str | None = None
    schema: dict[str, str] | None = None
    db_name: str | None = None
    storage_type: StorageType | None = None
    options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):

        if self.schema is not None and not isinstance(self.schema, dict):
            raise ValueError("Schema must be a dictionary of column names and types")

        self.storage_type = self._extract_storage_type
        if self.storage_type is not None and self.table_name is None:
            raise ValueError("Table name must be provided for storage type")

    @property
    def _extract_storage_type(self) -> StorageType | None:
        """Extract storage type."""
        if self.uri.startswith("s3://"):
            return StorageType.S3
        if self.uri.startswith("http://") or self.uri.startswith("https://"):
            return StorageType.HTTP
        if self.uri.startswith("file://"):
            return StorageType.LOCAL
        return None
