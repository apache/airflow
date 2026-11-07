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

from typing import Any

import pytest

from airflow.providers.common.sql.config import DataSourceConfig, FormatType, StorageType
from airflow.providers.common.sql.datafusion.base import FormatHandler, ObjectStorageProvider


class S3ObjectStorageProvider(ObjectStorageProvider):
    @property
    def get_storage_type(self) -> StorageType:
        return StorageType.S3

    def create_object_store(self, path: str, connection_config=None) -> Any:
        return None

    def get_scheme(self) -> str:
        return "s3://"


class ParquetFormatHandler(FormatHandler):
    @property
    def get_format(self) -> FormatType:
        return FormatType.PARQUET

    def register_data_source_format(self, ctx) -> None:
        pass


@pytest.mark.parametrize(
    ("path", "expected_bucket"),
    [
        ("s3://example-bucket/path/to/data.parquet", "example-bucket"),
        ("s3://example-bucket", "example-bucket"),
        ("s3://example-bucket/", "example-bucket"),
        ("file://example-bucket/data.parquet", None),
        ("example-bucket/data.parquet", None),
        ("", None),
    ],
)
def test_get_bucket(path, expected_bucket):
    assert S3ObjectStorageProvider().get_bucket(path) == expected_bucket


def test_format_handler_stores_datasource_config():
    datasource_config = DataSourceConfig(
        conn_id="aws_default",
        table_name="events",
        uri="s3://example-bucket/events.parquet",
        format=FormatType.PARQUET,
    )

    handler = ParquetFormatHandler(datasource_config)

    assert handler.datasource_config is datasource_config
    assert handler.get_format is FormatType.PARQUET
