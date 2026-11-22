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

from unittest import mock

import pytest

from airflow.providers.databricks.hooks.data_quality import DatabricksDataQualityHook
from airflow.providers.databricks.operators.data_quality import DatabricksDataQualityRefreshOperator


class TestDatabricksDataQualityRefreshOperator:
    @pytest.mark.parametrize("wait_for_completion", [True, False])
    @mock.patch(
        "airflow.providers.databricks.operators.data_quality.DatabricksDataQualityHook", autospec=True
    )
    def test_execute(self, mock_hook_class, wait_for_completion):
        hook = mock_hook_class.return_value
        hook.get_table_id.return_value = "table-id"
        hook.create_refresh.return_value = 42
        task = DatabricksDataQualityRefreshOperator(
            task_id="refresh",
            table_name="catalog.schema.table",
            databricks_conn_id="custom_databricks",
            wait_for_completion=wait_for_completion,
            polling_period_seconds=7,
            timeout=120,
            databricks_retry_limit=2,
            databricks_retry_delay=3,
            databricks_retry_args={"reraise": True},
        )
        assert task.execute({}) == 42
        mock_hook_class.assert_called_once_with(
            databricks_conn_id="custom_databricks",
            retry_limit=2,
            retry_delay=3,
            retry_args={"reraise": True},
            caller="DatabricksDataQualityRefreshOperator",
        )
        hook.get_table_id.assert_called_once_with("catalog.schema.table")
        hook.create_refresh.assert_called_once_with("table-id")
        if wait_for_completion:
            hook.a_wait_for_refresh.assert_awaited_once_with(
                "table-id", 42, timeout=120, polling_period_seconds=7
            )
        else:
            hook.a_wait_for_refresh.assert_not_called()

    @mock.patch.object(DatabricksDataQualityHook, "a_wait_for_refresh", autospec=True)
    @mock.patch.object(DatabricksDataQualityHook, "create_refresh", autospec=True, return_value=0)
    @mock.patch.object(DatabricksDataQualityHook, "get_table_id", autospec=True, return_value="table-id")
    def test_default_wait_and_single_http_attempt(self, mock_table, mock_create, mock_wait):
        task = DatabricksDataQualityRefreshOperator(task_id="refresh", table_name="catalog.schema.table")
        assert task.execute({}) == 0
        assert task._hook.retry_limit == 1
        mock_wait.assert_awaited_once_with(task._hook, "table-id", 0, timeout=3600, polling_period_seconds=30)

    def test_templating(self):
        task = DatabricksDataQualityRefreshOperator(
            task_id="refresh", table_name="{{ params.table }}", databricks_conn_id="{{ params.connection }}"
        )
        task.render_template_fields({"params": {"table": "catalog.schema.table", "connection": "custom"}})
        assert task.table_name == "catalog.schema.table"
        assert task.databricks_conn_id == "custom"

    @pytest.mark.parametrize("parameter", ["timeout", "polling_period_seconds"])
    @pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")])
    def test_invalid_wait_parameters(self, parameter, value):
        with pytest.raises(ValueError, match=parameter):
            DatabricksDataQualityRefreshOperator(
                task_id="refresh", table_name="catalog.schema.table", **{parameter: value}
            )
