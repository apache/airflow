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
from airflow.providers.databricks.sensors.databricks_delta_table import DatabricksDeltaTableVersionSensor
from airflow.providers.databricks.triggers.databricks_delta_table import DatabricksDeltaTableVersionTrigger

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
        mock_hook.run.assert_called_once_with("DESCRIBE HISTORY main.default.users LIMIT 1", handler=mock.ANY)

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
        mock_hook.run.assert_called_once_with(f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 1", handler=mock.ANY)

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
