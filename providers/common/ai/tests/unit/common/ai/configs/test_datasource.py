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

from airflow.providers.common.ai.configs.datasource import DataSourceConfig


class TestDataSourceConfig:
    def test_valid_config(self):
        config = DataSourceConfig(
            conn_id="fake_conn",
            uri="postgres://",
            table_name="test_table",
            schema={"id": "int", "name": "string"},
        )
        assert config.conn_id == "fake_conn"
        assert config.uri == "postgres://"
        assert config.table_name == "test_table"
        assert config.schema == {"id": "int", "name": "string"}

    def test_invalid_schema_type(self):
        with pytest.raises(ValueError, match="Schema must be a dictionary of column names and types"):
            DataSourceConfig(conn_id="test_conn", uri="test_uri", schema=["invalid_schema"])
