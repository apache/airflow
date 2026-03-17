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

import json
import os
import tempfile
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.sql.config import DataSourceConfig, StorageType
from airflow.providers.common.sql.operators.analytics import AnalyticsOperator


class TestAnalyticsOperator:
    @pytest.fixture
    def mock_engine(self):
        return MagicMock()

    @pytest.fixture
    def operator(self, mock_engine):
        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="users_data", uri="s3://bucket/path", format="parquet"
        )
        return AnalyticsOperator(
            task_id="test_analytics",
            datasource_configs=[datasource_config],
            queries=["SELECT * FROM users_data"],
            engine=mock_engine,
        )

    def test_execute_success(self, operator, mock_engine):
        mock_engine.execute_query.return_value = {
            "col1": [1, 2, 3, 4, 5],
            "col2": ["dave", "bob", "alice", "carol", "eve"],
        }

        result = operator.execute(context={})

        mock_engine.register_datasource.assert_called_once()
        mock_engine.execute_query.assert_called_once_with("SELECT * FROM users_data")
        assert "col1" in result
        assert "col2" in result

    def test_execute_max_rows_exceeded(self, operator, mock_engine):
        operator.max_rows_check = 3
        mock_engine.execute_query.return_value = {"col1": [1, 2, 3, 4]}

        result = operator.execute(context={})

        assert "Skipped" in result
        assert "4 rows exceed max_rows_check (3)" in result

    def test_json_output_format(self, mock_engine):
        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="users_data", uri="s3://bucket/path", format="parquet"
        )
        operator = AnalyticsOperator(
            task_id="test_analytics",
            datasource_configs=[datasource_config],
            queries=["SELECT * FROM users_data"],
            engine=mock_engine,
            result_output_format="json",
        )

        mock_engine.execute_query.return_value = {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10.1, 20.2, 30.3],
        }

        result = operator.execute(context={})

        json_result = json.loads(result)
        assert len(json_result) == 1
        assert json_result[0]["query"] == "SELECT * FROM users_data"
        assert len(json_result[0]["data"]) == 3
        assert json_result[0]["data"][0] == {"id": 1, "name": "A", "value": 10.1}
        assert json_result[0]["data"][1] == {"id": 2, "name": "B", "value": 20.2}
        assert json_result[0]["data"][2] == {"id": 3, "name": "C", "value": 30.3}

    def test_tabulate_output_format(self, mock_engine):
        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="users_data", uri="s3://bucket/path", format="parquet"
        )
        operator = AnalyticsOperator(
            task_id="test_analytics",
            datasource_configs=[datasource_config],
            queries=["SELECT * FROM users_data"],
            engine=mock_engine,
            result_output_format="tabulate",
        )

        mock_engine.execute_query.return_value = {
            "product": ["apple", "banana", "cherry"],
            "quantity": [10, 20, 15],
        }

        result = operator.execute(context={})

        assert "product" in result
        assert "Results: SELECT * FROM users_data" in result

    def test_unsupported_output_format(self, mock_engine):
        datasource_config = DataSourceConfig(
            conn_id="aws_default", table_name="users_data", uri="s3://bucket/path", format="parquet"
        )
        operator = AnalyticsOperator(
            task_id="test_analytics",
            datasource_configs=[datasource_config],
            queries=["SELECT * FROM users_data"],
            engine=mock_engine,
            result_output_format=["invalid"],  # type: ignore
        )

        with pytest.raises(ValueError, match="Unsupported output format"):
            operator.execute(context={})

    def test_execute_with_local_csv(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("name,age\nAlice,30\nBob,25\n")
            csv_path = f.name
        try:
            datasource_config = DataSourceConfig(
                conn_id="",
                table_name="test_csv",
                uri=f"file://{csv_path}",
                format="csv",
                storage_type=StorageType.LOCAL,
            )
            operator = AnalyticsOperator(
                task_id="test_analytics",
                datasource_configs=[datasource_config],
                queries=["SELECT * FROM test_csv ORDER BY name"],
                engine=None,
            )

            result = operator.execute(context={})

            assert "Alice" in result
            assert "Bob" in result
            assert "Results: SELECT * FROM test_csv ORDER BY name" in result
        finally:
            os.unlink(csv_path)
