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

import pytest

from airflow.providers.common.ai.utils.config import ConnectionConfig, DataSourceConfig, StorageType


class TestDataSourceConfig:
    def test_successful_creation(self):
        config = DataSourceConfig(conn_id="test_conn", uri="s3://bucket/key", table_name="my_table")
        assert config.conn_id == "test_conn"
        assert config.uri == "s3://bucket/key"
        assert config.table_name == "my_table"
        assert config.storage_type == StorageType.S3

    @pytest.mark.parametrize(
        ("uri", "expected_type"),
        [
            ("s3://bucket/path", StorageType.S3),
            ("http://example.com/file", StorageType.HTTP),
            ("file:///path/to/file", StorageType.LOCAL),
            ("unknown://path", None),
        ],
    )
    def test_extract_storage_type(self, uri, expected_type):
        config = DataSourceConfig(conn_id="test", uri=uri, table_name="a_table" if expected_type else None)
        assert config.storage_type == expected_type

    def test_missing_table_name_raises_error(self):
        with pytest.raises(ValueError, match="Table name must be provided for storage type"):
            DataSourceConfig(conn_id="test", uri="s3://bucket/path")

    def test_invalid_schema_raises_error(self):
        with pytest.raises(ValueError, match="Schema must be a dictionary of column names and types"):
            DataSourceConfig(
                conn_id="test",
                uri="s3://bucket/path",
                table_name="my_table",
                schema=["col1", "col2"],  # type: ignore
            )

    def test_parquet_with_partition_cols(self):
        config = DataSourceConfig(
            conn_id="test_conn",
            uri="s3://bucket/path",
            table_name="my_table",
            format="parquet",
            options={"table_partition_cols": [("year", "integer"), ("month", "integer")]},
        )
        assert config.conn_id == "test_conn"
        assert config.uri == "s3://bucket/path"
        assert config.table_name == "my_table"
        assert config.format == "parquet"
        assert config.options == {"table_partition_cols": [("year", "integer"), ("month", "integer")]}
        assert config.storage_type == StorageType.S3


class TestConnectionConfig:
    def test_connection_config_creation(self):
        config = ConnectionConfig(
            conn_id="my_conn", credentials={"key": "value"}, extra_config={"timeout": 30}
        )
        assert config.conn_id == "my_conn"
        assert config.credentials == {"key": "value"}
        assert config.extra_config == {"timeout": 30}

    def test_connection_config_defaults(self):
        config = ConnectionConfig(conn_id="my_conn")
        assert config.credentials == {}
        assert config.extra_config == {}
