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

from airflow.providers.databricks.exceptions import DatabricksWarehouseError
from airflow.providers.databricks.hooks.databricks import DatabricksHook, WarehouseState
from airflow.providers.databricks.operators.databricks_warehouse import (
    DatabricksStartWarehouseOperator,
    DatabricksStopWarehouseOperator,
)

TASK_ID = "warehouse-lifecycle"
WAREHOUSE_ID = "wh-1"


class TestDatabricksStartWarehouseOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks_warehouse.time.sleep")
    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_starts_then_waits_until_running(self, mock_hook_property, mock_sleep):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.side_effect = [
            WarehouseState("STOPPED"),
            WarehouseState("STARTING"),
            WarehouseState("RUNNING"),
        ]
        operator = DatabricksStartWarehouseOperator(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            polling_period_seconds=0,
        )

        operator.execute(None)

        hook.start_warehouse.assert_called_once_with(WAREHOUSE_ID)
        assert hook.get_warehouse_state.call_count == 3
        mock_sleep.assert_called_once_with(0)

    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_skips_start_when_already_running(self, mock_hook_property):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.return_value = WarehouseState("RUNNING")
        operator = DatabricksStartWarehouseOperator(task_id=TASK_ID, warehouse_id=WAREHOUSE_ID)

        operator.execute(None)

        hook.start_warehouse.assert_not_called()
        hook.get_warehouse_state.assert_called_once_with(WAREHOUSE_ID)

    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_skips_start_when_starting_without_waiting(self, mock_hook_property):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.return_value = WarehouseState("STARTING")
        operator = DatabricksStartWarehouseOperator(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            wait_for_termination=False,
        )

        operator.execute(None)

        hook.start_warehouse.assert_not_called()
        hook.get_warehouse_state.assert_called_once_with(WAREHOUSE_ID)

    @mock.patch("airflow.providers.databricks.operators.databricks_warehouse.time.sleep")
    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_starts_while_stopping_and_waits_until_running(self, mock_hook_property, mock_sleep):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.side_effect = [
            WarehouseState("STOPPING"),
            WarehouseState("STOPPING"),
            WarehouseState("RUNNING"),
        ]
        operator = DatabricksStartWarehouseOperator(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            polling_period_seconds=0,
        )

        operator.execute(None)

        hook.start_warehouse.assert_called_once_with(WAREHOUSE_ID)
        assert hook.get_warehouse_state.call_count == 3
        mock_sleep.assert_called_once_with(0)


class TestDatabricksStopWarehouseOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks_warehouse.time.sleep")
    @mock.patch.object(DatabricksStopWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_stops_then_waits_until_stopped(self, mock_hook_property, mock_sleep):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.side_effect = [
            WarehouseState("RUNNING"),
            WarehouseState("STOPPING"),
            WarehouseState("STOPPED"),
        ]
        operator = DatabricksStopWarehouseOperator(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            polling_period_seconds=0,
        )

        operator.execute(None)

        hook.stop_warehouse.assert_called_once_with(WAREHOUSE_ID)
        assert hook.get_warehouse_state.call_count == 3
        mock_sleep.assert_called_once_with(0)

    @mock.patch.object(DatabricksStopWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_skips_stop_when_already_stopped(self, mock_hook_property):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.return_value = WarehouseState("STOPPED")
        operator = DatabricksStopWarehouseOperator(task_id=TASK_ID, warehouse_id=WAREHOUSE_ID)

        operator.execute(None)

        hook.stop_warehouse.assert_not_called()
        hook.get_warehouse_state.assert_called_once_with(WAREHOUSE_ID)

    @mock.patch.object(DatabricksStopWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_skips_stop_when_stopping_without_waiting(self, mock_hook_property):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.return_value = WarehouseState("STOPPING")
        operator = DatabricksStopWarehouseOperator(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            wait_for_termination=False,
        )

        operator.execute(None)

        hook.stop_warehouse.assert_not_called()
        hook.get_warehouse_state.assert_called_once_with(WAREHOUSE_ID)


class TestDatabricksWarehouseOperatorBase:
    @pytest.mark.parametrize(
        ("operator_class", "initial_state", "failure_state", "transition_method", "target_state"),
        [
            (DatabricksStartWarehouseOperator, "STOPPED", "STOPPED", "start_warehouse", "RUNNING"),
            (DatabricksStopWarehouseOperator, "RUNNING", "DELETED", "stop_warehouse", "STOPPED"),
        ],
    )
    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    @mock.patch.object(DatabricksStopWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_execute_raises_on_failure_state(
        self,
        mock_stop_hook_property,
        mock_start_hook_property,
        operator_class,
        initial_state,
        failure_state,
        transition_method,
        target_state,
    ):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_start_hook_property.return_value = hook
        mock_stop_hook_property.return_value = hook
        hook.get_warehouse_state.side_effect = [
            WarehouseState(initial_state),
            WarehouseState(failure_state),
        ]
        operator = operator_class(task_id=TASK_ID, warehouse_id=WAREHOUSE_ID)

        with pytest.raises(
            DatabricksWarehouseError,
            match=f"entered {failure_state} while waiting for {target_state}",
        ):
            operator.execute(None)

        getattr(hook, transition_method).assert_called_once_with(WAREHOUSE_ID)

    @pytest.mark.parametrize(
        ("operator_class", "initial_state", "target_state", "transition_method"),
        [
            (DatabricksStartWarehouseOperator, "STARTING", "RUNNING", "start_warehouse"),
            (DatabricksStopWarehouseOperator, "STOPPING", "STOPPED", "stop_warehouse"),
        ],
    )
    @mock.patch("airflow.providers.databricks.operators.databricks_warehouse.time.sleep")
    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    @mock.patch.object(DatabricksStopWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_transition_in_progress_skips_request_and_continues_waiting(
        self,
        mock_stop_hook_property,
        mock_start_hook_property,
        mock_sleep,
        operator_class,
        initial_state,
        target_state,
        transition_method,
    ):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_start_hook_property.return_value = hook
        mock_stop_hook_property.return_value = hook
        hook.get_warehouse_state.side_effect = [
            WarehouseState(initial_state),
            WarehouseState(initial_state),
            WarehouseState(target_state),
        ]
        operator = operator_class(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            polling_period_seconds=0,
        )

        operator.execute(None)

        getattr(hook, transition_method).assert_not_called()
        assert hook.get_warehouse_state.call_count == 3
        mock_sleep.assert_called_once_with(0)

    @pytest.mark.parametrize(
        ("operator_class", "initial_state", "transition_method"),
        [
            (DatabricksStartWarehouseOperator, "STOPPED", "start_warehouse"),
            (DatabricksStopWarehouseOperator, "RUNNING", "stop_warehouse"),
        ],
    )
    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    @mock.patch.object(DatabricksStopWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_transition_without_waiting(
        self,
        mock_stop_hook_property,
        mock_start_hook_property,
        operator_class,
        initial_state,
        transition_method,
    ):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_start_hook_property.return_value = hook
        mock_stop_hook_property.return_value = hook
        hook.get_warehouse_state.return_value = WarehouseState(initial_state)
        operator = operator_class(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            wait_for_termination=False,
        )

        operator.execute(None)

        getattr(hook, transition_method).assert_called_once_with(WAREHOUSE_ID)
        hook.get_warehouse_state.assert_called_once_with(WAREHOUSE_ID)

    @mock.patch("airflow.providers.databricks.operators.databricks_warehouse.time.sleep")
    @mock.patch(
        "airflow.providers.databricks.operators.databricks_warehouse.time.monotonic",
        side_effect=[0, 0, 0, 11],
    )
    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_wait_caps_sleep_and_does_not_poll_after_deadline(
        self, mock_hook_property, mock_monotonic, mock_sleep
    ):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.side_effect = [WarehouseState("STARTING"), WarehouseState("RUNNING")]
        operator = DatabricksStartWarehouseOperator(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            polling_period_seconds=30,
            timeout=10,
        )

        with pytest.raises(
            DatabricksWarehouseError,
            match="did not reach RUNNING within 10s; last state: STARTING",
        ):
            operator._wait_for_state("RUNNING", {"STOPPED"})

        assert mock_monotonic.call_count == 4
        hook.get_warehouse_state.assert_called_once_with(WAREHOUSE_ID)
        mock_sleep.assert_called_once_with(10)

    @mock.patch("airflow.providers.databricks.operators.databricks_warehouse.time.sleep")
    @mock.patch(
        "airflow.providers.databricks.operators.databricks_warehouse.time.monotonic",
        side_effect=[0, 0, 11],
    )
    @mock.patch.object(DatabricksStartWarehouseOperator, "_hook", new_callable=mock.PropertyMock)
    def test_wait_rejects_target_received_after_deadline(
        self, mock_hook_property, mock_monotonic, mock_sleep
    ):
        hook = mock.MagicMock(spec=DatabricksHook)
        mock_hook_property.return_value = hook
        hook.get_warehouse_state.return_value = WarehouseState("RUNNING")
        operator = DatabricksStartWarehouseOperator(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            timeout=10,
        )

        with pytest.raises(
            DatabricksWarehouseError,
            match="did not reach RUNNING within 10s; last state: RUNNING",
        ):
            operator._wait_for_state("RUNNING", {"STOPPED"})

        assert mock_monotonic.call_count == 3
        mock_sleep.assert_not_called()

    def test_operator_template_fields(self):
        expected = ("databricks_conn_id", "warehouse_id")
        assert DatabricksStartWarehouseOperator.template_fields == expected
        assert DatabricksStopWarehouseOperator.template_fields == expected

    @pytest.mark.parametrize(
        ("operator_class", "warehouse_id"),
        [
            (DatabricksStartWarehouseOperator, ""),
            (DatabricksStartWarehouseOperator, None),
            (DatabricksStopWarehouseOperator, ""),
            (DatabricksStopWarehouseOperator, None),
        ],
    )
    def test_invalid_warehouse_id(self, operator_class, warehouse_id):
        operator = operator_class(task_id=TASK_ID, warehouse_id=warehouse_id)

        with pytest.raises(ValueError, match="warehouse_id must be provided"):
            operator.execute(None)

    @mock.patch("airflow.providers.databricks.operators.databricks_warehouse.DatabricksHook", autospec=True)
    def test_operator_builds_hook(self, mock_hook_class):
        retry_args = {"reraise": True}
        operator = DatabricksStartWarehouseOperator(
            task_id=TASK_ID,
            warehouse_id=WAREHOUSE_ID,
            databricks_conn_id="custom_conn",
            databricks_retry_limit=7,
            databricks_retry_delay=4,
            databricks_retry_args=retry_args,
        )

        assert operator._hook is mock_hook_class.return_value
        mock_hook_class.assert_called_once_with(
            "custom_conn",
            retry_limit=7,
            retry_delay=4,
            retry_args=retry_args,
            caller="DatabricksStartWarehouseOperator",
        )
