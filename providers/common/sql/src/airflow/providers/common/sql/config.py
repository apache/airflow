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
    AVRO = "avro"
    ICEBERG = "iceberg"


# TODO: Add delta format support
TABLE_PROVIDERS: frozenset[str] = frozenset({FormatType.ICEBERG.value})


class StorageType(str, Enum):
    """Storage types for Data Fusion."""

    S3 = "s3"
    LOCAL = "local"


@dataclass
class DataSourceConfig:
    """
    Configuration for an input data source.

    **File-based formats** (parquet, csv, avro) require ``uri`` and infer
    ``storage_type`` automatically.

    **Catalog-managed formats** (iceberg, and in the future delta, etc.) do not
    require ``uri`` or ``storage_type``; they use ``conn_id`` and format-specific
    keys in ``options`` (e.g. ``catalog_table_name`` for Iceberg).

    :param conn_id: The connection ID to use for accessing the data source.
    :param uri: The URI of the data source (e.g., file path, S3 bucket, etc.).
        Not required for catalog-managed formats.
    :param format: The format of the data (e.g., 'parquet', 'csv', 'iceberg').
    :param table_name: The name to register the table under in DataFusion.
    :param db_name: The namespace for table provider eg: iceberg needs to catalog it to look
    :param storage_type: The type of storage (automatically inferred from URI).
        Not used for catalog-managed formats.
    :param options: Additional options for the data source. e.g. you can set partition columns
        for any file-based datasource, or ``catalog_table_name`` for Iceberg.
    """

    conn_id: str
    table_name: str
    uri: str = ""
    format: str = ""
    db_name: str | None = None
    storage_type: StorageType | None = None
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def is_table_provider(self) -> bool:
        """Whether this format is catalog-managed (no object store needed)."""
        return bool(self.format and self.format.lower() in TABLE_PROVIDERS)

    def __post_init__(self):
        if self.is_table_provider:
            if self.db_name is None:
                raise ValueError(f"Database name must be provided for table providers {TABLE_PROVIDERS}")
            return

        if self.storage_type is None:
            self.storage_type = self._extract_storage_type

        if self.storage_type is not None and (not self.table_name or not self.table_name.strip()):
            raise ValueError("Table name must be provided for storage type")

    @property
    def _extract_storage_type(self) -> StorageType | None:
        """Extract storage type."""
        if self.uri.startswith("s3://"):
            return StorageType.S3
        if self.uri.startswith("file://"):
            return StorageType.LOCAL
        raise ValueError(f"Unsupported storage type for URI: {self.uri}")
