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

import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.triggers.base import TriggerEvent
from airflow.utils import db

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip(
        "Event-driven scheduling is only compatible with Airflow versions >= 3.0.0", allow_module_level=True
    )

from airflow.providers.postgres.triggers.postgres_cdc import PostgresCDCEventTrigger

pytestmark = pytest.mark.db_test


class TestPostgresCDCEventTrigger:
    """
    Unit tests for PostgresCDCEventTrigger.

    These tests validate:
    - Correct serialization of trigger parameters.
    - Trigger behavior when new changes are detected.
    - Correct handling when no new changes are present.
    - Proper exception handling on connection failures.
    - Guard behavior when state Variable is not initialized.
    - Proper handling when state Variable is removed during polling.
    """

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="postgres_default",
                conn_type="postgres",
                host="localhost",
                schema="test_db",
                login="user",
                password="pass",
                port=5432,
            )
        )

    def test_trigger_serialization(self):
        trigger = PostgresCDCEventTrigger(
            conn_id="postgres_default",
            table="my_table",
            cdc_column="updated_at",
            polling_interval=30,
            state_key="custom_cdc_state",
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.postgres.triggers.postgres_cdc.PostgresCDCEventTrigger"
        assert kwargs == {
            "conn_id": "postgres_default",
            "table": "my_table",
            "cdc_column": "updated_at",
            "polling_interval": 30,
            "state_key": "custom_cdc_state",
        }

    def test_default_state_key(self):
        """Test that default state key is generated correctly."""
        trigger = PostgresCDCEventTrigger(
            conn_id="postgres_default",
            table="my_table",
            cdc_column="updated_at",
        )
        assert trigger.state_key == "my_table_cdc_last_value"

    @pytest.mark.asyncio
    async def test_trigger_run_without_state_variable(self):
        """Test that trigger logs warning and skips event emission when state Variable doesn't exist."""
        trigger = PostgresCDCEventTrigger(
            conn_id="postgres_default",
            table="my_table",
            cdc_column="updated_at",
            polling_interval=0.1,
            state_key="custom_cdc_state",
        )

        with patch("airflow.sdk.Variable.get", return_value=None):
            # The trigger should return early without yielding any events
            with pytest.raises(StopAsyncIteration):
                await trigger.run().__anext__()

    @pytest.mark.asyncio
    async def test_trigger_run_with_change(self):
        trigger = PostgresCDCEventTrigger(
            conn_id="postgres_default",
            table="my_table",
            cdc_column="updated_at",
            polling_interval=0.1,
            state_key="custom_cdc_state",
        )

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn

        with (
            patch("airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn", return_value=mock_conn),
            patch("airflow.sdk.Variable.get", return_value="2024-01-01T00:00:00"),
            patch("airflow.sdk.Variable.set"),
        ):
            async for result in trigger.run():
                assert isinstance(result, TriggerEvent)
                assert "message" in result.payload
                assert "max_iso" in result.payload
                break

    @pytest.mark.asyncio
    async def test_trigger_run_no_change(self):
        trigger = PostgresCDCEventTrigger(
            conn_id="postgres_default",
            table="my_table",
            cdc_column="updated_at",
            polling_interval=0.1,
            state_key="custom_cdc_state",
        )

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn

        with (
            patch("airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn", return_value=mock_conn),
            patch("airflow.sdk.Variable.get", return_value="2024-01-02T00:00:00"),
        ):
            task = asyncio.create_task(trigger.run().__anext__())
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(task, timeout=1)

    @pytest.mark.asyncio
    async def test_trigger_run_state_variable_removed_during_polling(self):
        """Test that trigger handles state Variable being removed during polling."""
        trigger = PostgresCDCEventTrigger(
            conn_id="postgres_default",
            table="my_table",
            cdc_column="updated_at",
            polling_interval=0.1,
            state_key="custom_cdc_state",
        )

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn

        # First call returns a value, second call returns None (Variable removed)
        with (
            patch("airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn", return_value=mock_conn),
            patch("airflow.sdk.Variable.get", side_effect=["2024-01-01T00:00:00", None]),
        ):
            # The trigger should return early without yielding any events
            with pytest.raises(StopAsyncIteration):
                await trigger.run().__anext__()

    @pytest.mark.asyncio
    async def test_trigger_run_exception(self, mocker):
        trigger = PostgresCDCEventTrigger(
            conn_id="postgres_default",
            table="my_table",
            cdc_column="updated_at",
            polling_interval=0.1,
            state_key="custom_cdc_state",
        )

        mocker.patch(
            "airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn",
            side_effect=Exception("Connection failed"),
        )

        # Mock Variable.get to return a value so the guard doesn't trigger
        mocker.patch("airflow.sdk.Variable.get", return_value="2024-01-01T00:00:00")

        task = asyncio.create_task(trigger.run().__anext__())
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=1)
