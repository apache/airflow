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
from requests.exceptions import ReadTimeout
from tenacity import stop_after_attempt

from airflow.models.connection import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.operators.vector_search import DatabricksVectorSearchSyncOperator

INDEX_NAME = "catalog.schema.search_index"


class TestDatabricksVectorSearchSyncOperator:
    @mock.patch("airflow.providers.databricks.operators.vector_search.DatabricksHook", autospec=True)
    def test_execute(self, mock_hook):
        retry_args = {"stop": stop_after_attempt(5)}
        operator = DatabricksVectorSearchSyncOperator(
            task_id="sync_index",
            index_name=INDEX_NAME,
            databricks_conn_id="custom_databricks",
            databricks_retry_limit=5,
            databricks_retry_delay=2,
            databricks_retry_args=retry_args,
            wait_for_completion=False,
        )

        assert operator.execute(None) is None

        mock_hook.assert_called_once_with(
            "custom_databricks",
            retry_limit=5,
            retry_delay=2,
            retry_args=retry_args,
            caller="DatabricksVectorSearchSyncOperator",
        )
        assert mock_hook.return_value.mock_calls == [mock.call.sync_vector_search_index(INDEX_NAME)]

    @mock.patch("airflow.providers.databricks.operators.vector_search.DatabricksHook", autospec=True)
    def test_execute_renders_templates(self, mock_hook):
        operator = DatabricksVectorSearchSyncOperator(
            task_id="sync_index",
            index_name="{{ params.index_name }}",
            databricks_conn_id="{{ params.conn_id }}",
            wait_for_completion=False,
        )
        operator.render_template_fields(
            context={"params": {"index_name": INDEX_NAME, "conn_id": "rendered_databricks"}}
        )

        operator.execute(None)

        assert mock_hook.call_args.args == ("rendered_databricks",)
        mock_hook.return_value.sync_vector_search_index.assert_called_once_with(INDEX_NAME)

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.requests.post",
        autospec=True,
        side_effect=ReadTimeout,
    )
    def test_does_not_replay_ambiguous_submission_by_default(self, mock_post):
        operator = DatabricksVectorSearchSyncOperator(task_id="sync_index", index_name=INDEX_NAME)
        operator._hook.databricks_conn = Connection(
            conn_id="databricks_default",
            conn_type="databricks",
            host="https://workspace.cloud.databricks.com",
            extra={"token": "test-token"},
        )
        with pytest.raises(AirflowException):
            operator.execute(None)

        mock_post.assert_called_once()

    @mock.patch("airflow.providers.databricks.operators.vector_search.DatabricksHook", autospec=True)
    def test_waits_by_default(self, mock_hook):
        operator = DatabricksVectorSearchSyncOperator(
            task_id="sync_index",
            index_name=INDEX_NAME,
            timeout=120,
            polling_period_seconds=5,
        )

        assert operator.execute(None) is None

        assert mock_hook.return_value.mock_calls == [
            mock.call.sync_vector_search_index(INDEX_NAME),
            mock.call.a_wait_for_vector_search_index(INDEX_NAME, timeout=120, polling_period_seconds=5),
        ]
        mock_hook.return_value.a_wait_for_vector_search_index.assert_awaited_once()

    @pytest.mark.parametrize("name", ["timeout", "polling_period_seconds"])
    @pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")])
    def test_rejects_invalid_wait_parameters(self, name, value):
        with pytest.raises(ValueError, match=f"{name} must be finite and greater than zero"):
            DatabricksVectorSearchSyncOperator(task_id="sync_index", index_name=INDEX_NAME, **{name: value})
