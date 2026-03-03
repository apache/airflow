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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.sql.config import DataSourceConfig
from airflow.providers.common.sql.decorators.analytics import _AnalyticsDecoratedOperator

DATASOURCE_CONFIGS = [
    DataSourceConfig(conn_id="", table_name="users_data", uri="file:///path/to/", format="parquet")
]


class TestAnalyticsDecoratedOperator:
    def test_custom_operator_name(self):
        assert _AnalyticsDecoratedOperator.custom_operator_name == "@task.analytics"

    @patch(
        "airflow.providers.common.sql.operators.analytics.AnalyticsOperator.execute",
        autospec=True,
    )
    def test_execute_calls_callable_and_sets_queries_from_list(self, mock_execute):
        """The callable return value (list) becomes self.queries."""
        mock_execute.return_value = "mocked output"

        def get_user_queries():
            return ["SELECT * FROM users_data", "SELECT count(*) FROM users_data"]

        op = _AnalyticsDecoratedOperator(
            task_id="test",
            python_callable=get_user_queries,
            datasource_configs=DATASOURCE_CONFIGS,
        )
        result = op.execute(context={})

        assert result == "mocked output"
        assert op.queries == ["SELECT * FROM users_data", "SELECT count(*) FROM users_data"]
        mock_execute.assert_called_once()

    @patch(
        "airflow.providers.common.sql.operators.analytics.AnalyticsOperator.execute",
        autospec=True,
    )
    def test_execute_wraps_single_string_into_list(self, mock_execute):
        """A single string return value is wrapped into a list for self.queries."""
        mock_execute.return_value = "mocked output"

        def get_single_query():
            return "SELECT 1"

        op = _AnalyticsDecoratedOperator(
            task_id="test",
            python_callable=get_single_query,
            datasource_configs=DATASOURCE_CONFIGS,
        )
        op.execute(context={})

        assert op.queries == ["SELECT 1"]

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None, [], [""], ["SELECT 1", ""], ["SELECT 1", "   "], [42]],
        ids=[
            "non-string",
            "empty-string",
            "whitespace-string",
            "none",
            "empty-list",
            "list-with-empty-string",
            "list-with-one-valid-one-empty",
            "list-with-one-valid-one-whitespace",
            "list-with-non-string",
        ],
    )
    def test_execute_raises_on_invalid_return_value(self, return_value):
        """TypeError when the callable returns an invalid value."""
        op = _AnalyticsDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            datasource_configs=DATASOURCE_CONFIGS,
        )
        with pytest.raises(TypeError, match="non-empty string"):
            op.execute(context={})

    @patch(
        "airflow.providers.common.sql.operators.analytics.AnalyticsOperator.execute",
        autospec=True,
    )
    def test_execute_merges_op_kwargs_into_callable(self, mock_execute):
        """op_kwargs are forwarded to the callable to build queries."""
        mock_execute.return_value = "mocked output"

        def get_queries_for_table(table_name):
            return [f"SELECT * FROM {table_name}", f"SELECT count(*) FROM {table_name}"]

        op = _AnalyticsDecoratedOperator(
            task_id="test",
            python_callable=get_queries_for_table,
            datasource_configs=DATASOURCE_CONFIGS,
            op_kwargs={"table_name": "orders"},
        )
        op.execute(context={"task_instance": MagicMock()})

        assert op.queries == ["SELECT * FROM orders", "SELECT count(*) FROM orders"]
