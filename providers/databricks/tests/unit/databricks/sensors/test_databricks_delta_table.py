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
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.assets.databricks import UnityTableIdentity
from airflow.providers.databricks.exceptions import DatabricksDeltaTableConfigurationError
from airflow.providers.databricks.sensors.databricks_delta_table import DatabricksDeltaTableVersionSensor
from airflow.providers.databricks.triggers.databricks_delta_table import (
    DatabricksDeltaTableVersionTrigger,
    DeltaVersionInfo,
)

TASK_ID = "test-delta-table-sensor"
DEFAULT_CONN_ID = "databricks_default"
TABLE_NAME = "main.default.users"
UNITY_TABLE = UnityTableIdentity(
    host="https://my-workspace.cloud.databricks.com/", catalog="main", schema="default", table="users"
)
ASSET_URI = "databricks://my-workspace.cloud.databricks.com/main/default/users"


def _make_history_row(version=5, timestamp="2026-10-05 12:00:00", operation="WRITE"):
    return [[version, timestamp, "user_id", "user_name", operation]]


class TestDatabricksDeltaTableVersionSensor:
    def test_init_validation(self):
        with pytest.raises(ValueError, match="One of 'table_name' or 'unity_table' must be provided"):
            DatabricksDeltaTableVersionSensor(task_id=TASK_ID)

    def test_init_with_unity_table_sets_outlet_asset(self):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            unity_table=UNITY_TABLE,
        )
        assert sensor.outlets == [Asset(uri=ASSET_URI)]
        assert sensor._resolve_table_name() == "main.default.users"

    def test_init_with_custom_outlets_preserved(self):
        custom_asset = Asset(uri="custom://my-custom-uri")
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            unity_table=UNITY_TABLE,
            outlets=[custom_asset],
        )
        assert sensor.outlets == [custom_asset]

    @pytest.mark.parametrize(
        ("table_name", "valid"),
        [
            ("main.default.users", True),
            ("Main.Default.Users", True),
            ("MAIN.DEFAULT.USERS", True),
            ("default.users", True),
            ("Default.Users", True),
            ("DEFAULT.USERS", True),
            ("users", True),
            ("Users", True),
            ("USERS", True),
            ("other.default.users", False),
            ("Other.Default.Users", False),
            ("main.other.users", False),
            ("main.default.orders", False),
            ("a.b.c.d", False),
        ],
    )
    def test_table_name_validation_against_unity_table(self, table_name, valid):
        if valid:
            sensor = DatabricksDeltaTableVersionSensor(
                task_id=TASK_ID,
                table_name=table_name,
                unity_table=UNITY_TABLE,
            )
            assert sensor._resolve_table_name() == "main.default.users"
        else:
            sensor = DatabricksDeltaTableVersionSensor(
                task_id=TASK_ID,
                table_name=table_name,
                unity_table=UNITY_TABLE,
            )
            with pytest.raises(ValueError, match="does not match unity_table|Invalid table_name"):
                sensor._resolve_table_name()

    @pytest.mark.parametrize(
        ("table_name", "catalog", "schema"),
        [
            ("MAIN.Default.Users", None, None),
            ("Default.Users", "MAIN", None),
            ("Users", "MAIN", "Default"),
        ],
    )
    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_sensor_matches_table_case_insensitively(self, mock_hook_cls, table_name, catalog, schema):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        mock_hook.run.return_value = _make_history_row(version=6)
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=table_name,
            catalog=catalog or "",
            schema=schema or "default",
            unity_table=UNITY_TABLE,
            baseline_version=5,
        )
        context = {"ti": MagicMock()}
        assert sensor.poke(context) is True
        mock_hook.run.assert_called_once_with(
            "DESCRIBE HISTORY main.default.users LIMIT 50", handler=mock.ANY
        )

    @pytest.mark.parametrize("host", ["other-workspace.cloud.databricks.com", None, ""])
    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_sensor_rejects_connection_for_other_workspace_before_sql(self, mock_hook_cls, host):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = host
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            unity_table=UNITY_TABLE,
            baseline_version=5,
        )
        context = {"ti": MagicMock()}
        with pytest.raises(ValueError, match="connection host .* does not match unity_table host"):
            sensor.poke(context)
        assert mock_hook.run.call_args_list == []

        with pytest.raises(ValueError, match="connection host .* does not match unity_table host"):
            sensor.execute(context)

    @pytest.mark.parametrize(
        "host",
        [
            "my-workspace.cloud.databricks.com",
            "My-Workspace.cloud.Databricks.com",
            "MY-WORKSPACE.CLOUD.DATABRICKS.COM",
        ],
    )
    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_sensor_matches_workspace_case_insensitively(self, mock_hook_cls, host):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = host
        mock_hook.run.return_value = _make_history_row(version=6)
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            unity_table=UNITY_TABLE,
            baseline_version=5,
        )
        context = {"ti": MagicMock()}
        assert sensor.poke(context) is True

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_unchanged_version_returns_false(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = _make_history_row(version=5)

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=5,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}
        assert sensor.poke(context) is False
        mock_hook.run.assert_called_once_with(f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50", handler=mock.ANY)

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_newer_version_returns_true_and_pushes_xcom(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = _make_history_row(version=6, operation="MERGE")

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=5,
            sql_warehouse_name="test_wh",
        )
        ti_mock = MagicMock()
        context = {"ti": ti_mock}
        assert sensor.poke(context) is True
        expected_table_identity = {"catalog": "main", "schema": "default", "table": "users"}
        ti_mock.xcom_push.assert_called_once_with(
            key="delta_table_version",
            value={
                "version": 6,
                "baseline_version": 5,
                "table_name": TABLE_NAME,
                "table_identity": expected_table_identity,
                "timestamp": "2026-10-05 12:00:00",
                "operation": "MERGE",
            },
        )

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_newer_version_emits_outlet_events_with_version_and_table_identity(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        mock_hook.run.return_value = _make_history_row(version=7, operation="WRITE")

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            unity_table=UNITY_TABLE,
            baseline_version=5,
            sql_warehouse_name="test_wh",
        )
        outlet_asset = sensor.outlets[0]
        outlet_event_mock = MagicMock()

        class _FakeOutletEvents:
            """Minimal accessor double supporting unhashable Asset keys across Airflow versions."""

            def __init__(self, pairs):
                self._pairs = list(pairs)

            def __getitem__(self, key):
                for k, v in self._pairs:
                    if k is key or k == key:
                        return v
                raise KeyError(key)

        context = {
            "ti": MagicMock(),
            "outlet_events": _FakeOutletEvents([(outlet_asset, outlet_event_mock)]),
        }

        assert sensor.poke(context) is True
        expected_identity = {
            "host": UNITY_TABLE.host,
            "catalog": UNITY_TABLE.catalog,
            "schema": UNITY_TABLE.schema,
            "table": UNITY_TABLE.table,
        }
        assert outlet_event_mock.extra == {
            "version": 7,
            "observed_version": 7,
            "table_identity": expected_identity,
            "operation": "WRITE",
            "timestamp": "2026-10-05 12:00:00",
        }

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_templated_baseline_not_copied_in_init_and_initialized_on_poke(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        mock_hook.run.return_value = _make_history_row(version=10)

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version="{{ params.baseline }}",
            sql_warehouse_name="test_wh",
        )
        assert sensor.baseline_version == "{{ params.baseline }}"
        assert sensor._baseline_version is None
        assert sensor._baseline_initialized is False

        # Rendered by Airflow engine prior to poke/execute
        sensor.baseline_version = 8

        context = {"ti": MagicMock()}
        assert sensor.poke(context) is True
        assert sensor._baseline_version == 8
        assert sensor._baseline_initialized is True

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_user_baseline_retained_across_pokes(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        mock_hook.run.return_value = _make_history_row(version=5)

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=5,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}
        assert sensor.poke(context) is False
        assert sensor._baseline_version == 5

        mock_hook.run.return_value = _make_history_row(version=6)
        assert sensor.poke(context) is True
        assert sensor._baseline_version == 5

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_initial_enrollment_captures_baseline(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = _make_history_row(version=10)

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=None,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}
        # First poke captures baseline=10 and returns False
        assert sensor.poke(context) is False
        assert sensor._baseline_version == 10

        # Second poke with version 10 returns False
        assert sensor.poke(context) is False

        # Third poke with version 11 returns True
        mock_hook.run.return_value = _make_history_row(version=11)
        assert sensor.poke(context) is True

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_table_recreation_handling(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        # Recreated table resets history to version 0
        mock_hook.run.return_value = _make_history_row(version=0)

        # allow_recreation=True (default) succeeds
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=15,
            allow_recreation=True,
            sql_warehouse_name="test_wh",
        )
        assert sensor.poke({"ti": MagicMock()}) is True

        # allow_recreation=False raises AirflowException
        strict_sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=15,
            allow_recreation=False,
            sql_warehouse_name="test_wh",
        )
        with pytest.raises(AirflowException, match="was recreated"):
            strict_sensor.poke({"ti": MagicMock()})

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_target_version(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = _make_history_row(version=7)

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            target_version=10,
            sql_warehouse_name="test_wh",
        )
        assert sensor.poke({"ti": MagicMock()}) is False

        mock_hook.run.return_value = _make_history_row(version=10)
        assert sensor.poke({"ti": MagicMock()}) is True

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_transient_error_reraised(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = ConnectionResetError("Connection reset by peer")

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        with pytest.raises(ConnectionResetError, match="Connection reset by peer"):
            sensor.poke({"ti": MagicMock()})

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_non_delta_or_inaccessible_table_raises(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        with pytest.raises(AirflowException, match="Failed to fetch Delta table history"):
            sensor.poke({"ti": MagicMock()})

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_deferrable_execution_defers_to_trigger(self, mock_hook_cls):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=5,
            sql_warehouse_name="test_wh",
            session_configuration={"spark.sql.shuffle.partitions": "10"},
            http_headers=[("X-Header", "1")],
            client_parameters={"client_param": "abc"},
            hook_params={"hook_param": "xyz"},
            query_tags={"tag": "val"},
            deferrable=True,
        )
        context = {"ti": MagicMock(), "dag": MagicMock(), "task": MagicMock()}
        with pytest.raises(TaskDeferred) as exc_info:
            sensor.execute(context)

        trigger = exc_info.value.trigger
        assert isinstance(trigger, DatabricksDeltaTableVersionTrigger)
        assert trigger.table_name == TABLE_NAME
        assert trigger.baseline_version == 5
        assert trigger.session_configuration == {"spark.sql.shuffle.partitions": "10"}
        assert trigger.http_headers == [("X-Header", "1")]
        assert trigger.client_parameters == {"client_param": "abc"}
        assert trigger.hook_params == {"hook_param": "xyz"}
        assert trigger.query_tags is not None
        assert exc_info.value.method_name == "execute_complete"

    def test_execute_complete_success(self):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=5,
            sql_warehouse_name="test_wh",
        )
        ti_mock = MagicMock()
        context = {"ti": ti_mock}
        event = {
            "status": "success",
            "version": 8,
            "baseline_version": 5,
            "table_name": TABLE_NAME,
            "timestamp": "2026-10-05 12:00:00",
            "operation": "WRITE",
        }
        res = sensor.execute_complete(context, event)
        assert res == 8
        ti_mock.xcom_push.assert_called_once_with(
            key="delta_table_version",
            value={
                "version": 8,
                "baseline_version": 5,
                "table_name": TABLE_NAME,
                "table_identity": {"catalog": "main", "schema": "default", "table": "users"},
                "timestamp": "2026-10-05 12:00:00",
                "operation": "WRITE",
            },
        )

    def test_execute_complete_failure(self):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=5,
            sql_warehouse_name="test_wh",
        )
        event = {"status": "error", "message": "Table was recreated"}
        with pytest.raises(AirflowException, match="Table was recreated"):
            sensor.execute_complete({}, event)

    def test_execute_complete_restores_baseline_version_for_deferred_enrollment(self):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=None,
            sql_warehouse_name="test_wh",
        )
        assert sensor._baseline_version is None
        ti_mock = MagicMock()
        context = {"ti": ti_mock}
        event = {
            "status": "success",
            "version": 8,
            "baseline_version": 5,
            "table_name": TABLE_NAME,
            "timestamp": "2026-10-05 12:00:00",
            "operation": "WRITE",
        }
        res = sensor.execute_complete(context, event)
        assert res == 8
        assert sensor._baseline_version == 5
        ti_mock.xcom_push.assert_called_once_with(
            key="delta_table_version",
            value={
                "version": 8,
                "baseline_version": 5,
                "table_name": TABLE_NAME,
                "table_identity": {"catalog": "main", "schema": "default", "table": "users"},
                "timestamp": "2026-10-05 12:00:00",
                "operation": "WRITE",
            },
        )

    # --- Reschedule mode validation tests ---

    def test_init_mode_reschedule_without_baseline_or_target_raises(self):
        expected_msg = (
            "does not support mode='reschedule' when neither 'baseline_version' nor "
            "'target_version' is specified"
        )
        with pytest.raises(DatabricksDeltaTableConfigurationError, match=expected_msg):
            DatabricksDeltaTableVersionSensor(
                task_id=TASK_ID,
                table_name=TABLE_NAME,
                mode="reschedule",
            )

    def test_init_mode_reschedule_with_explicit_baseline_accepted(self):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            mode="reschedule",
            baseline_version=10,
        )
        assert sensor.mode == "reschedule"
        assert sensor.baseline_version == 10

    def test_init_mode_reschedule_with_target_version_accepted(self):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            mode="reschedule",
            target_version=25,
        )
        assert sensor.mode == "reschedule"
        assert sensor.target_version == 25

    def test_init_mode_poke_and_deferrable_without_baseline_or_target_accepted(self):
        sensor_poke = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            mode="poke",
        )
        assert sensor_poke.mode == "poke"
        assert sensor_poke.baseline_version is None
        assert sensor_poke.target_version is None

        sensor_deferrable = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            deferrable=True,
        )
        assert sensor_deferrable.deferrable is True
        assert sensor_deferrable.baseline_version is None
        assert sensor_deferrable.target_version is None

    # --- Target version templating and normalization tests ---

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_templated_target_version_normalized_to_int_on_poke(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        mock_hook.run.return_value = _make_history_row(version=5)

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            target_version="{{ params.target }}",
            sql_warehouse_name="test_wh",
        )
        assert sensor.target_version == "{{ params.target }}"
        assert sensor._target_version is None
        assert sensor._baseline_initialized is False

        # Jinja renders template expression to string "5" before poke
        sensor.target_version = "5"
        context = {"ti": MagicMock()}
        assert sensor.poke(context) is True
        assert sensor._target_version == 5
        assert isinstance(sensor._target_version, int)
        assert sensor._baseline_initialized is True

    def test_target_version_invalid_string_raises_value_error(self):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            target_version="invalid-version",
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}
        with pytest.raises(ValueError, match="Invalid target_version 'invalid-version': must be an integer"):
            sensor.poke(context)

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_templated_target_version_passed_as_int_to_trigger(self, mock_hook_cls):
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            target_version="{{ params.target }}",
            deferrable=True,
            sql_warehouse_name="test_wh",
        )
        assert sensor.target_version == "{{ params.target }}"
        assert sensor._target_version is None

        # Jinja renders template expression to string "15" before execute
        sensor.target_version = "15"
        with pytest.raises(TaskDeferred) as exc_info:
            sensor.execute({"ti": MagicMock()})
        trigger = exc_info.value.trigger
        assert trigger.target_version == 15
        assert isinstance(trigger.target_version, int)
        assert sensor._target_version == 15

    # --- Delta maintenance operations filtering and baseline enrollment tests ---

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_skips_maintenance_delta_operations_and_selects_effective_version(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # Delta history order: newest first
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
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        ti_mock = MagicMock()
        context = {"ti": ti_mock}
        assert sensor.poke(context) is True
        assert sensor._get_latest_version(TABLE_NAME) == DeltaVersionInfo(
            2, "2026-10-05 12:02:00", "WRITE", 5
        )
        ti_mock.xcom_push.assert_called_once_with(
            key="delta_table_version",
            value={
                "version": 2,
                "baseline_version": 1,
                "table_name": TABLE_NAME,
                "table_identity": {"catalog": "main", "schema": "default", "table": "users"},
                "timestamp": "2026-10-05 12:02:00",
                "operation": "WRITE",
            },
        )

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_latest_row_write_selected_normally(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "WRITE"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "OPTIMIZE"],
        ]
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=4,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}
        assert sensor.poke(context) is True
        assert sensor._get_latest_version(TABLE_NAME) == DeltaVersionInfo(
            5, "2026-10-05 12:05:00", "WRITE", 5
        )

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_maintenance_casing_insensitivity(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "  optimize  "],
            [4, "2026-10-05 12:04:00", "u1", "user1", "Vacuum Start"],
            [3, "2026-10-05 12:03:00", "u1", "user1", " vacuum end "],
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
        ]
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        assert sensor.poke({"ti": MagicMock()}) is True
        assert sensor._get_latest_version(TABLE_NAME) == DeltaVersionInfo(
            2, "2026-10-05 12:02:00", "WRITE", 5
        )

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_only_maintenance_operations_returns_false_without_raising(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        mock_hook.run.return_value = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "VACUUM END"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "OPTIMIZE"],
        ]
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        assert sensor.poke({"ti": MagicMock()}) is False
        assert sensor._get_latest_version(TABLE_NAME) == DeltaVersionInfo(None, None, None, 5)

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_repeated_against_same_effective_version_does_not_fire_then_later_write_fires(
        self, mock_hook_cls
    ):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        maint_history = [
            [5, "2026-10-05 12:05:00", "u1", "user1", "VACUUM END"],
            [4, "2026-10-05 12:04:00", "u1", "user1", "VACUUM START"],
            [3, "2026-10-05 12:03:00", "u1", "user1", "OPTIMIZE"],
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.return_value = maint_history

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}
        # Initial poke enrolls raw latest baseline v5 and returns False
        assert sensor.poke(context) is False
        assert sensor._baseline_version == 5

        # Second poke: history unchanged, should not fire
        assert sensor.poke(context) is False

        # Third poke: real WRITE v6 arrives
        mock_hook.run.return_value = [
            [6, "2026-10-05 12:06:00", "u1", "user1", "WRITE"],
            *maint_history,
        ]
        assert sensor.poke(context) is True
        assert sensor._get_latest_version(TABLE_NAME) == DeltaVersionInfo(
            6, "2026-10-05 12:06:00", "WRITE", 6
        )

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_table_recreation_after_maintenance_detected(self, mock_hook_cls):
        from airflow.providers.common.compat.sdk import AirflowFailException

        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # Table had baseline 10, was recreated at v0, then optimized at v1
        mock_hook.run.return_value = [
            [1, "2026-10-05 12:01:00", "u1", "user1", "OPTIMIZE"],
            [0, "2026-10-05 12:00:00", "u1", "user1", "CREATE TABLE"],
        ]
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=10,
            allow_recreation=True,
            sql_warehouse_name="test_wh",
        )
        assert sensor.poke({"ti": MagicMock()}) is True
        assert sensor._get_latest_version(TABLE_NAME) == DeltaVersionInfo(
            0, "2026-10-05 12:00:00", "CREATE TABLE", 1
        )

        # If allow_recreation=False, backwards version raises
        sensor_no_recreate = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=10,
            allow_recreation=False,
            sql_warehouse_name="test_wh",
        )
        with pytest.raises(AirflowFailException, match="was recreated: current version"):
            sensor_no_recreate.poke({"ti": MagicMock()})

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_auto_enrollment_with_only_maintenance_rows_establishes_raw_baseline_and_fires_on_subsequent_write(
        self, mock_hook_cls
    ):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # Window contains only maintenance rows:
        # v50 VACUUM END, v49 VACUUM START, v48 OPTIMIZE
        maint_history = [
            [50, "2026-10-05 12:50:00", "u1", "user1", "VACUUM END"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "VACUUM START"],
            [48, "2026-10-05 12:48:00", "u1", "user1", "OPTIMIZE"],
        ]
        mock_hook.run.return_value = maint_history

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}

        # 1. Automatic enrollment establishes raw latest version 50 as baseline and returns False
        assert sensor.poke(context) is False
        assert sensor._baseline_version == 50

        # 2. Another maintenance commit (v51 OPTIMIZE) arrives -> does NOT fire
        mock_hook.run.return_value = [
            [51, "2026-10-05 12:51:00", "u1", "user1", "OPTIMIZE"],
            *maint_history,
        ]
        assert sensor.poke(context) is False
        assert sensor._baseline_version == 50

        # 3. First subsequent data-changing commit (v52 WRITE) arrives -> fires immediately
        mock_hook.run.return_value = [
            [52, "2026-10-05 12:52:00", "u1", "user1", "WRITE"],
            [51, "2026-10-05 12:51:00", "u1", "user1", "OPTIMIZE"],
            *maint_history,
        ]
        assert sensor.poke(context) is True
        assert sensor._get_latest_version(TABLE_NAME) == DeltaVersionInfo(
            52, "2026-10-05 12:52:00", "WRITE", 52
        )
        context["ti"].xcom_push.assert_called_once_with(
            key="delta_table_version",
            value={
                "version": 52,
                "baseline_version": 50,
                "table_name": TABLE_NAME,
                "table_identity": {"catalog": "main", "schema": "default", "table": "users"},
                "timestamp": "2026-10-05 12:52:00",
                "operation": "WRITE",
            },
        )

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_deferrable_auto_enrollment_with_only_maintenance_rows_establishes_raw_baseline(
        self, mock_hook_cls
    ):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # Window contains only maintenance rows
        mock_hook.run.return_value = [
            [50, "2026-10-05 12:50:00", "u1", "user1", "VACUUM END"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "VACUUM START"],
            [48, "2026-10-05 12:48:00", "u1", "user1", "OPTIMIZE"],
        ]

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            deferrable=True,
            sql_warehouse_name="test_wh",
        )
        with pytest.raises(TaskDeferred) as exc_info:
            sensor.execute({"ti": MagicMock()})
        trigger = exc_info.value.trigger
        # Trigger receives raw newest version (50) as baseline_version
        assert trigger.baseline_version == 50
        assert sensor._baseline_version == 50

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_explicit_target_version_with_maintenance_rows(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # Monotonic history: v48 WRITE, v49 OPTIMIZE, v50 VACUUM END
        maint_history = [
            [50, "2026-10-05 12:50:00", "u1", "user1", "VACUUM END"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "OPTIMIZE"],
            [48, "2026-10-05 12:48:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.return_value = maint_history
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            target_version=50,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}
        # Target version 50 not met because v50 is maintenance and effective data version is 48
        assert sensor.poke(context) is False

        # Monotonically append v51 WRITE -> succeeds because effective data version 51 >= target 50
        mock_hook.run.return_value = [
            [51, "2026-10-05 12:51:00", "u1", "user1", "WRITE"],
            *maint_history,
        ]
        assert sensor.poke(context) is True
        assert sensor._get_latest_version(TABLE_NAME) == DeltaVersionInfo(
            51, "2026-10-05 12:51:00", "WRITE", 51
        )

    # -------------------------------------------------------------------------
    # Adaptive Delta History Scan Regression Tests (PR #74225 Reviewer Blocker)
    # -------------------------------------------------------------------------

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_history_scan_reviewer_case_write_behind_50_plus_maintenance(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # Reviewer scenario: baseline_version=1, WRITE v2, followed by 53 maintenance commits (v3..v55)
        history = [[v, f"2026-10-05 12:{v:02d}:00", "u1", "user1", "OPTIMIZE"] for v in range(55, 2, -1)] + [
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}

        assert sensor.poke(context) is True
        # Verify scan expanded: first LIMIT 50 (only maintenance), then LIMIT 100 (finds v2)
        assert mock_hook.run.call_count == 2
        calls = [c.args[0] for c in mock_hook.run.call_args_list]
        assert calls == [
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50",
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 100",
        ]
        context["ti"].xcom_push.assert_called_once_with(
            key="delta_table_version",
            value={
                "version": 2,
                "baseline_version": 1,
                "table_name": TABLE_NAME,
                "table_identity": {"catalog": "main", "schema": "default", "table": "users"},
                "timestamp": "2026-10-05 12:02:00",
                "operation": "WRITE",
            },
        )

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_scan_write_inside_first_50_rows_single_query(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # WRITE is within the first 50 rows (v2..v5 OPTIMIZE, v2 WRITE)
        history = [[v, f"2026-10-05 12:{v:02d}:00", "u1", "user1", "OPTIMIZE"] for v in range(5, 2, -1)] + [
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}

        assert sensor.poke(context) is True
        assert mock_hook.run.call_count == 1
        assert mock_hook.run.call_args.args[0] == f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50"

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_scan_multiple_expansion_steps(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # 120 maintenance commits above v2 WRITE (requires LIMIT 50 -> 100 -> 200)
        history = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(122, 2, -1)] + [
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}

        assert sensor.poke(context) is True
        assert mock_hook.run.call_count == 3
        calls = [c.args[0] for c in mock_hook.run.call_args_list]
        assert calls == [
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50",
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 100",
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 200",
        ]

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_scan_stops_when_no_commit_newer_than_baseline(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # baseline=1, commits v2..v60 are all OPTIMIZE, v1 is WRITE
        history = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(60, 1, -1)] + [
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}

        # Does not succeed because effective version 1 is not > baseline 1
        assert sensor.poke(context) is False
        assert mock_hook.run.call_count == 2

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_scan_target_version_behind_50_plus_maintenance(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # target_version=50 behind 55 maintenance commits (v51..v105 OPTIMIZE, v50 WRITE)
        history = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(105, 50, -1)] + [
            [50, "2026-10-05 12:50:00", "u1", "user1", "WRITE"],
            [49, "2026-10-05 12:49:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            target_version=50,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}

        assert sensor.poke(context) is True
        assert mock_hook.run.call_count == 2
        calls = [c.args[0] for c in mock_hook.run.call_args_list]
        assert calls == [
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 50",
            f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 100",
        ]

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_scan_auto_enrollment_does_not_fire_on_historical_write(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # Table has historical write at v10, then v11..v15 OPTIMIZE
        history = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(15, 10, -1)] + [
            [10, "2026-10-05 12:10:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}

        # Poke 1 enrolls raw version 15 and returns False without firing on historical v10 WRITE
        assert sensor.poke(context) is False
        assert sensor._baseline_version == 15
        assert mock_hook.run.call_count == 1

        # Poke 2 with subsequent real WRITE v16 fires
        history_with_v16 = [
            [16, "2026-10-05 12:16:00", "u1", "user1", "WRITE"],
            *history,
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history_with_v16[: int(sql.split("LIMIT ")[1])]
        assert sensor.poke(context) is True

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_scan_recreation_with_maintenance_commits(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # baseline=10, table recreated at v0 WRITE, followed by 55 maintenance commits (v1..v55 OPTIMIZE)
        recreated_history = [
            [v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(55, 0, -1)
        ] + [
            [0, "2026-10-05 12:00:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: recreated_history[: int(sql.split("LIMIT ")[1])]

        # allow_recreation=True succeeds
        sensor_allow = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=100,
            allow_recreation=True,
            sql_warehouse_name="test_wh",
        )
        assert sensor_allow.poke({"ti": MagicMock()}) is True

        # allow_recreation=False fails loudly
        sensor_disallow = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=100,
            allow_recreation=False,
            sql_warehouse_name="test_wh",
        )
        with pytest.raises(AirflowException, match="was recreated"):
            sensor_disallow.poke({"ti": MagicMock()})

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_scan_does_not_misclassify_old_write_as_recreation(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        # baseline=10, commits: v3..v60 OPTIMIZE, v2 WRITE (not recreated, raw version 60 >= 10)
        history = [[v, "2026-10-05 12:00:00", "u1", "user1", "OPTIMIZE"] for v in range(60, 2, -1)] + [
            [2, "2026-10-05 12:02:00", "u1", "user1", "WRITE"],
            [1, "2026-10-05 12:01:00", "u1", "user1", "WRITE"],
        ]
        mock_hook.run.side_effect = lambda sql, handler=None: history[: int(sql.split("LIMIT ")[1])]

        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=10,
            allow_recreation=False,
            sql_warehouse_name="test_wh",
        )
        # Must return False, NOT raise table recreation error
        assert sensor.poke({"ti": MagicMock()}) is False

    @patch("airflow.providers.databricks.sensors.databricks_delta_table.DatabricksSqlHook")
    def test_poke_adaptive_scan_empty_and_malformed_history_raises(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.host = UNITY_TABLE.host
        sensor = DatabricksDeltaTableVersionSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            baseline_version=1,
            sql_warehouse_name="test_wh",
        )
        context = {"ti": MagicMock()}

        mock_hook.run.return_value = []
        with pytest.raises(AirflowException, match="returned empty or malformed history"):
            sensor.poke(context)

        mock_hook.run.return_value = [["not_an_int", "2026-10-05", "u", "u", "WRITE"]]
        with pytest.raises(AirflowException, match="returned empty or malformed history"):
            sensor.poke(context)
