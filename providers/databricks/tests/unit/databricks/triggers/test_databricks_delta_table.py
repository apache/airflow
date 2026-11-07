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
from unittest import mock
from unittest.mock import patch

import pytest

from airflow.providers.databricks.triggers.databricks_delta_table import (
    DatabricksDeltaTableVersionTrigger,
    DeltaVersionInfo,
    parse_oldest_raw_delta_version,
    parse_raw_delta_version,
)
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
        mock_get_version.return_value = DeltaVersionInfo(6, "2026-10-05 12:00:00", "WRITE", 6)
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
            DeltaVersionInfo(5, "2026-10-05 12:00:00", "WRITE", 5),
            DeltaVersionInfo(6, "2026-10-05 12:01:00", "WRITE", 6),
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
        mock_get_version.return_value = DeltaVersionInfo(0, "2026-10-05 12:00:00", "CREATE TABLE", 0)
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
        mock_get_version.return_value = DeltaVersionInfo(0, "2026-10-05 12:00:00", "CREATE TABLE", 0)
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
            DeltaVersionInfo(8, "2026-10-05 12:00:00", "WRITE", 8),
            DeltaVersionInfo(10, "2026-10-05 12:05:00", "WRITE", 10),
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
        mock_get_version.return_value = DeltaVersionInfo(5, "2026-10-05 12:00:00", "WRITE", 5)
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
            DeltaVersionInfo(6, "2026-10-05 12:00:00", "WRITE", 6),
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

    # --- Target/baseline version normalization and maintenance operation tests ---

    def test_target_version_string_normalized_to_int(self):
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            target_version="10",
        )
        assert trigger.target_version == 10
        assert isinstance(trigger.target_version, int)

    def test_target_version_invalid_string_raises(self):
        with pytest.raises(ValueError, match="Invalid target_version 'invalid': must be an integer"):
            DatabricksDeltaTableVersionTrigger(
                table_name=TABLE_NAME,
                databricks_conn_id=CONN_ID,
                target_version="invalid",
            )

    def test_baseline_version_string_normalized_to_int(self):
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version="10",
        )
        assert trigger.baseline_version == 10
        assert isinstance(trigger.baseline_version, int)

    def test_baseline_version_invalid_string_raises(self):
        with pytest.raises(ValueError, match="Invalid baseline_version 'invalid': must be an integer"):
            DatabricksDeltaTableVersionTrigger(
                table_name=TABLE_NAME,
                databricks_conn_id=CONN_ID,
                baseline_version="invalid",
            )

    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    def test_get_version_sql_limit_and_maintenance_filtering(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # History with maintenance rows followed by real write:
        # version 5: VACUUM END
        # version 4: VACUUM START
        # version 3: OPTIMIZE
        # version 2: WRITE
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "VACUUM END"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "VACUUM START"],
            [3, "2026-10-05 12:03:00", "u1", "user1", "OPTIMIZE"],
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
        )
        version_info = trigger._get_version()
        assert version_info.effective_version == 2
        assert version_info.timestamp == "2026-10-05 12:02:00"
        assert version_info.operation == "WRITE"
        assert version_info.raw_version == 5
        mock_hook.run.assert_called_once_with(f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50", handler=mock.ANY)

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_skips_maintenance_delta_operations_end_to_end(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "VACUUM END"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "VACUUM START"],
            [3, "2026-10-05 12:03:00", "u1", "user1", "OPTIMIZE"],
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
            polling_period_seconds=0.01,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 2
        assert event.payload["operation"] == "WRITE"
        assert event.payload["timestamp"] == "2026-10-05 12:02:00"

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_only_maintenance_operations_times_out_without_firing(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "VACUUM END"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "OPTIMIZE"],
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
            polling_period_seconds=0.02,
            end_time=time.time() + 0.05,
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "timeout"

    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    def test_get_version_latest_row_write_selected_normally(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "WRITE"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "OPTIMIZE"],
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=4,
        )
        assert trigger._get_version() == DeltaVersionInfo(5, "2026-10-05 12:05:00", "WRITE", 5)

    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    def test_get_version_maintenance_casing_insensitivity(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "  optimize  "],
            [4, "2026-10-05 12:04:00", "u1", "user1", "Vacuum Start"],
            [3, "2026-10-05 12:03:00", "u1", "user1", " vacuum end "],
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
        )
        assert trigger._get_version() == DeltaVersionInfo(2, "2026-10-05 12:02:00", "WRITE", 5)

    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    def test_get_version_only_maintenance_operations_returns_none(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "VACUUM END"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "OPTIMIZE"],
        ]
        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
        )
        assert trigger._get_version() == DeltaVersionInfo(None, None, None, 5)

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_maintenance_commits_then_later_write_fires(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        maint_history = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "VACUUM END"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "VACUUM START"],
            [3, "2026-10-05 12:03:00", "u1", "user1", "OPTIMIZE"],
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
        ]
        write_v6_history = [
            [6, "2026-10-05 12:06:00", "u1", "user1", "WRITE"],
            *maint_history,
        ]
        # First query returns maintenance over v2 (same as baseline 2, does not fire)
        # Second query returns write v6 (fires success event)
        mock_hook.run.side_effect = [maint_history, write_v6_history]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=2,
            polling_period_seconds=0.01,
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 6
        assert event.payload["operation"] == "WRITE"
        assert event.payload["timestamp"] == "2026-10-05 12:06:00"

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_table_recreation_after_maintenance_detected(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # Recreated at v0, optimized at v1
        mock_hook.run.return_value = [
            [1, "2026-10-05 12:01:00", "u1", "user1", "OPTIMIZE"],
            [0, "2026-10-05 12:00:00", "u1", "user1", "CREATE TABLE"],
        ]
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
        assert event.payload["operation"] == "CREATE TABLE"

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_auto_enrollment_with_only_maintenance_rows_establishes_raw_baseline_and_fires_on_subsequent_write(
        self, mock_hook_cls
    ):
        mock_hook = mock_hook_cls.return_value
        maint_history = [
            [50, "2026-10-05 12:50:00", "u1", "user1", "VACUUM END"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "VACUUM START"],
            [48, "2026-10-05 12:48:00", "u1", "user1", "OPTIMIZE"],
        ]
        more_maint = [
            [51, "2026-10-05 12:51:00", "u1", "user1", "OPTIMIZE"],
            *maint_history,
        ]
        write_v52 = [
            [52, "2026-10-05 12:52:00", "u1", "user1", "WRITE"],
            *more_maint,
        ]
        # Query 1: only maintenance (establishes baseline v50, does not fire)
        # Query 2: more maintenance (v51 OPTIMIZE, does not fire)
        # Query 3: real WRITE v52 (fires success event with version 52 and baseline 50)
        mock_hook.run.side_effect = [maint_history, more_maint, write_v52]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            polling_period_seconds=0.01,
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 52
        assert event.payload["baseline_version"] == 50
        assert event.payload["operation"] == "WRITE"
        assert event.payload["timestamp"] == "2026-10-05 12:52:00"

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_explicit_target_version_with_maintenance_rows(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        maint_history = [
            [50, "2026-10-05 12:50:00", "u1", "user1", "VACUUM END"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "OPTIMIZE"],
            [48, "2026-10-05 12:48:00", "u1", "user1", "WRITE"],
        ]
        write_v51 = [
            [51, "2026-10-05 12:51:00", "u1", "user1", "WRITE"],
            *maint_history,
        ]
        # Query 1: maintenance commit v50 over write v48 does not meet target 50
        # Query 2: subsequent write commit v51 meets target 50 (effective version 51 >= 50)
        mock_hook.run.side_effect = [maint_history, write_v51]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            target_version=50,
            polling_period_seconds=0.01,
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 51
        assert event.payload["operation"] == "WRITE"
        assert event.payload["timestamp"] == "2026-10-05 12:51:00"

    def test_parse_raw_delta_version(self):
        assert parse_raw_delta_version(None) is None
        assert parse_raw_delta_version([]) is None

        rows = [
            [50, "2026-10-05 12:50:00", "u1", "user1", "VACUUM END"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "OPTIMIZE"],
        ]
        assert parse_raw_delta_version(rows) == 50

        rows_str = [["42", "2026-10-05 12:50:00"]]
        assert parse_raw_delta_version(rows_str) == 42

    # -------------------------------------------------------------------------
    # Adaptive Delta History Scan Regression Tests (PR #74225 Reviewer Blocker)
    # -------------------------------------------------------------------------

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_adaptive_history_scan_reviewer_case_write_behind_50_plus_maintenance(
        self, mock_hook_cls
    ):
        mock_hook = mock_hook_cls.return_value
        # Reviewer scenario: baseline_version=1, WRITE v2, followed by 53 maintenance commits (v3..v55)
        history = [[v, f"2026-10-05 12:{v:02d}:00", "u1", "user1", "OPTIMIZE"] for v in range(55, 2, -1)] + [
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
            polling_period_seconds=0.01,
        )
        generator = trigger.run()
        event = await generator.asend(None)

        assert event.payload["status"] == "success"
        assert event.payload["version"] == 2
        assert event.payload["baseline_version"] == 1
        assert event.payload["operation"] == "WRITE"
        assert event.payload["timestamp"] == "2026-10-05 12:02:00"

        # Verify scan expanded: first LIMIT 50 (only maintenance), then LIMIT 100 (finds v2)
        assert mock_hook.run.call_count == 2
        calls = [c.args[0] for c in mock_hook.run.call_args_list]
        assert calls == [
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50",
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 100",
        ]

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_adaptive_scan_write_inside_first_50_rows_single_query(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # WRITE is within the first 50 rows (v2..v5 OPTIMIZE, v2 WRITE)
        history = [[v, f"2026-10-05 12:{v:02d}:00", "u1", "user1", "OPTIMIZE"] for v in range(5, 2, -1)] + [
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
            polling_period_seconds=0.01,
        )
        generator = trigger.run()
        event = await generator.asend(None)

        assert event.payload["status"] == "success"
        assert event.payload["version"] == 2
        assert mock_hook.run.call_count == 1
        assert mock_hook.run.call_args.args[0] == f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50"

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_adaptive_scan_multiple_expansion_steps(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # 120 maintenance commits above v2 WRITE (requires LIMIT 50 -> 100 -> 200)
        history = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(122, 2, -1)] + [
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
            polling_period_seconds=0.01,
        )
        generator = trigger.run()
        event = await generator.asend(None)

        assert event.payload["status"] == "success"
        assert event.payload["version"] == 2
        assert mock_hook.run.call_count == 3
        calls = [c.args[0] for c in mock_hook.run.call_args_list]
        assert calls == [
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50",
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 100",
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 200",
        ]

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_adaptive_scan_stops_when_no_commit_newer_than_baseline_times_out(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # baseline=1, commits v2..v60 are all OPTIMIZE, v1 is WRITE
        history = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(60, 1, -1)] + [
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
            polling_period_seconds=0.01,
            end_time=time.time() + 0.05,
        )
        generator = trigger.run()
        event = await generator.asend(None)

        # Trigger times out without firing false success
        assert event.payload["status"] == "timeout"
        assert mock_hook.run.call_count >= 2

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_adaptive_scan_target_version_behind_50_plus_maintenance(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # target_version=50 behind 55 maintenance commits (v51..v105 OPTIMIZE, v50 WRITE)
        history = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(105, 50, -1)] + [
            [50, "2026-10-05 12:50:00", "u1", "user1", "WRITE"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            target_version=50,
            polling_period_seconds=0.01,
        )
        generator = trigger.run()
        event = await generator.asend(None)

        assert event.payload["status"] == "success"
        assert event.payload["version"] == 50
        assert mock_hook.run.call_count == 2
        calls = [c.args[0] for c in mock_hook.run.call_args_list]
        assert calls == [
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50",
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 100",
        ]

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_adaptive_scan_auto_enrollment_does_not_fire_on_historical_write(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # Table has historical write at v10, then v11..v15 OPTIMIZE
        history_init = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(15, 10, -1)] + [
            [10, "2026-10-05 12:10:00", "u1", "user1", "WRITE"],
        ]
        history_with_v16 = [
            [16, "2026-10-05 12:16:00", "u1", "user1", "WRITE"],
            *history_init,
        ]

        # Iteration 1 enrolls raw baseline 15 without firing on v10; iteration 2 fires on v16
        mock_hook.run.side_effect = [
            history_init[:50],
            history_with_v16[:50],
        ]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            polling_period_seconds=0.01,
        )
        generator = trigger.run()
        event = await generator.asend(None)

        assert event.payload["status"] == "success"
        assert event.payload["version"] == 16
        assert event.payload["baseline_version"] == 15

    @pytest.mark.asyncio
    @patch("airflow.providers.databricks.triggers.databricks_delta_table.DatabricksSqlHook")
    async def test_run_adaptive_scan_recreation_with_maintenance_commits(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # baseline=100, table recreated at v0 WRITE, followed by 55 maintenance commits (v1..v55 OPTIMIZE)
        recreated_history = [
            [v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(55, 0, -1)
        ] + [
            [0, "2026-10-05 12:00:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: recreated_history[: int(sql.split("LIMIT ")[1])]

        # allow_recreation=True yields success
        trigger_allow = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=100,
            allow_recreation=True,
            polling_period_seconds=0.01,
        )
        gen = trigger_allow.run()
        event = await gen.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["version"] == 0

        # allow_recreation=False yields error
        trigger_disallow = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=100,
            allow_recreation=False,
            polling_period_seconds=0.01,
        )
        gen = trigger_disallow.run()
        event = await gen.asend(None)
        assert event.payload["status"] == "error"
        assert "was recreated" in event.payload["message"]

    def test_parse_oldest_raw_delta_version(self):
        assert parse_oldest_raw_delta_version(None) is None
        assert parse_oldest_raw_delta_version([]) is None

        rows = [
            [50, "2026-10-05 12:50:00", "u1", "user1", "VACUUM END"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "OPTIMIZE"],
            [48, "2026-10-05 12:48:00", "u1", "user1", "WRITE"],
        ]
        assert parse_oldest_raw_delta_version(rows) == 48

        rows_with_invalid_tail = [
            [50, "2026-10-05 12:50:00"],
            ["not_an_int", "2026-10-05 12:49:00"],
        ]
        assert parse_oldest_raw_delta_version(rows_with_invalid_tail) == 50

    def test_scan_delta_history_version_parity_with_sensor(self):
        from airflow.providers.databricks.sensors.databricks_delta_table import (
            DatabricksDeltaTableVersionSensor,
        )

        mock_hook = mock.MagicMock()
        history = [
            [55, "2026-10-05 12:55:00", "u1", "user1", "OPTIMIZE"],
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        trigger = DatabricksDeltaTableVersionTrigger(
            table_name=TABLE_NAME,
            databricks_conn_id=CONN_ID,
            baseline_version=1,
        )
        trigger._get_hook = mock.MagicMock(return_value=mock_hook)
        trigger_ver = trigger._get_version()

        sensor = DatabricksDeltaTableVersionSensor(
            task_id="test_parity_sensor",
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        sensor.hook = mock_hook
        sensor_ver = sensor._get_latest_version(TABLE_NAME)

        assert trigger_ver == sensor_ver == DeltaVersionInfo(2, "2026-10-05 12:02:00", "WRITE", 55)
