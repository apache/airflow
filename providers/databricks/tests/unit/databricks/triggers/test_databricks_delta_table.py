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

import time
from unittest.mock import patch

import pytest

from airflow.providers.databricks.triggers.databricks_delta_table import DatabricksDeltaTableVersionTrigger
from airflow.triggers.base import TriggerEvent

TABLE_NAME = "main.default.users"
CONN_ID = "databricks_default"


class TestDatabricksDeltaTableVersionTrigger:
    def test_serialization(self):
        end_time = time.time() + 100
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=5,
            target_version=10,
            allow_recreation=True,
            sql_warehouse_name="test_wh",
            http_path="/sql/1.0/warehouses/abc",
            catalog="main",
            schema="default",
            session_configuration={"spark.sql.shuffle.partitions": "10"},
            http_headers=[("X-Custom-Header", "value")],
            client_parameters={"client_arg": "value"},
            hook_params={"hook_arg": "value"},
            query_tags={"tag_k": "tag_v"},
            polling_period_seconds=15,
            end_time=end_time,
            caller="test_caller",
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.databricks.triggers.databricks_delta_table.DatabricksDeltaTableVersionTrigger"
        )
        assert kwargs == {
            "table_name": TABLE_NAME,
            "databricks_conn_id": CONN_ID,
            "baseline_version": 5,
            "target_version": 10,
            "allow_recreation": True,
            "sql_warehouse_name": "test_wh",
            "http_path": "/sql/1.0/warehouses/abc",
            "catalog": "main",
            "schema": "default",
            "session_configuration": {"spark.sql.shuffle.partitions": "10"},
            "http_headers": [("X-Custom-Header", "value")],
            "client_parameters": {"client_arg": "value"},
            "hook_params": {"hook_arg": "value"},
            "query_tags": {"tag_k": "tag_v"},
            "polling_period_seconds": 15,
            "end_time": end_time,
            "caller": "test_caller",
        }

    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    def test_get_hook_passes_all_configuration(self, mock_hook_cls):
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            sql_warehouse_name="test_wh",
            http_path="/sql/1.0/warehouses/abc",
            catalog="main",
            schema="default",
            session_configuration={"spark.sql.shuffle.partitions": "10"},
            http_headers=[("X-Custom-Header", "value")],
            client_parameters={"client_arg": "value"},
            hook_params={"hook_arg": "value"},
            query_tags={"tag_k": "tag_v"},
            caller="custom_caller",
        )
        hook = trigger._get_hook()
        mock_hook_cls.assert_called_once_with(
            databricks_conn_id=CONN_ID,
            http_path="/sql/1.0/warehouses/abc",
            sql_endpoint_name="test_wh",
            session_configuration={"spark.sql.shuffle.partitions": "10"},
            http_headers=[("X-Custom-Header", "value")],
            catalog="main",
            schema="default",
            caller="custom_caller",
            query_tags={"tag_k": "tag_v"},
            client_arg="value",
            hook_arg="value",
        )
        assert hook == mock_hook_cls.return_value

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_success_newer_version(self, mock_get_version):
        mock_get_version.return_value = (6, "2026-10-05 12:00:00", "WRITE")
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=5,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert isinstance(event, TriggerEvent)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 6
        assert event.payload["baseline_version"] == 5
        assert event.payload["table_name"] == TABLE_NAME

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_initial_enrollment(self, mock_get_version):
        mock_get_version.side_effect = [
            (5, "2026-10-05 12:00:00", "WRITE"),
            (6, "2026-10-05 12:01:00", "WRITE"),
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=None,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 6
        assert event.payload["baseline_version"] == 5

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_recreation_allowed(self, mock_get_version):
        mock_get_version.return_value = (0, "2026-10-05 12:00:00", "CREATE TABLE")
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=10,
            allow_recreation=True,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 0

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_recreation_disallowed(self, mock_get_version):
        mock_get_version.return_value = (0, "2026-10-05 12:00:00", "CREATE TABLE")
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=10,
            allow_recreation=False,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "error"
        assert "was recreated" in event.payload["message"]

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_target_version(self, mock_get_version):
        mock_get_version.side_effect = [
            (8, "2026-10-05 12:00:00", "WRITE"),
            (10, "2026-10-05 12:05:00", "WRITE"),
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            target_version=10,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 10

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_timeout(self, mock_get_version):
        mock_get_version.return_value = (5, "2026-10-05 12:00:00", "WRITE")
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=5,
            end_time=time.time() - 1,  # Already timed out
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "timeout"

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_permanent_error_missing_table(self, mock_get_version):
        mock_get_version.side_effect = Exception(
            "TABLE_OR_VIEW_NOT_FOUND: The table or view cannot be found."
        )
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=5,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "error"
        assert "permanent error" in event.payload["message"]
        assert "TABLE_OR_VIEW_NOT_FOUND" in event.payload["message"]

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_permanent_error_permission_denied(self, mock_get_version):
        mock_get_version.side_effect = Exception("[PERMISSION_DENIED] User lacks SELECT privilege on table.")
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=5,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "error"
        assert "permanent error" in event.payload["message"]
        assert "PERMISSION_DENIED" in event.payload["message"]

    @pytest.mark.asyncio
    @patch.object(DatabricksDeltaTableVersionTrigger, "_get_version")
    async def test_run_transient_error_retries_and_succeeds(self, mock_get_version):
        mock_get_version.side_effect = [
            Exception("Connection reset by peer"),
            (6, "2026-10-05 12:00:00", "WRITE"),
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=5,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 6
