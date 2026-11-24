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
from unittest import mock

import pytest

from airflow.providers.databricks.exceptions import DatabricksDataQualityError, DatabricksDataQualityTimeout
from airflow.providers.databricks.hooks.data_quality import DatabricksDataQualityHook

TABLE_ID = "table-id"
REFRESH_ID = 42


class TestDatabricksDataQualityHook:
    @mock.patch.object(DatabricksDataQualityHook, "_do_api_call", autospec=True)
    def test_resolve_table_name(self, mock_api):
        mock_api.return_value = {"table_id": TABLE_ID}
        hook = DatabricksDataQualityHook()
        assert hook.get_table_id("catalog.schema.table /name") == TABLE_ID
        mock_api.assert_called_once_with(
            hook, ("GET", "2.1/unity-catalog/tables/catalog.schema.table%20%2Fname")
        )

    @pytest.mark.parametrize("table_id", [None, "", 5])
    @mock.patch.object(DatabricksDataQualityHook, "_do_api_call", autospec=True)
    def test_reject_missing_table_id(self, mock_api, table_id):
        mock_api.return_value = {"table_id": table_id}
        with pytest.raises(DatabricksDataQualityError, match="without a table_id"):
            DatabricksDataQualityHook().get_table_id("catalog.schema.table")

    @mock.patch.object(DatabricksDataQualityHook, "_do_api_call", autospec=True)
    def test_reject_empty_table_name(self, mock_api):
        with pytest.raises(ValueError, match="table_name must be provided"):
            DatabricksDataQualityHook().get_table_id("")
        mock_api.assert_not_called()

    @pytest.mark.parametrize("refresh_id", [0, REFRESH_ID])
    @mock.patch.object(DatabricksDataQualityHook, "_do_api_call", autospec=True)
    def test_create_refresh(self, mock_api, refresh_id):
        mock_api.return_value = {"refresh_id": refresh_id}
        hook = DatabricksDataQualityHook()
        assert hook.create_refresh("table/id") == refresh_id
        mock_api.assert_called_once_with(
            hook, ("POST", "data-quality/v1/monitors/table/table%2Fid/refreshes"), {}
        )

    @mock.patch.object(DatabricksDataQualityHook, "_do_api_call", autospec=True)
    def test_reject_empty_table_id(self, mock_api):
        with pytest.raises(ValueError, match="table_id must be provided"):
            DatabricksDataQualityHook().create_refresh("")
        mock_api.assert_not_called()

    @pytest.mark.parametrize("refresh_id", [None, "42", True, -1])
    @mock.patch.object(DatabricksDataQualityHook, "_do_api_call", autospec=True)
    def test_reject_invalid_refresh_id(self, mock_api, refresh_id):
        mock_api.return_value = {"refresh_id": refresh_id}
        with pytest.raises(DatabricksDataQualityError, match="valid refresh_id"):
            DatabricksDataQualityHook().create_refresh(TABLE_ID)

    @mock.patch.object(DatabricksDataQualityHook, "_a_do_api_call", autospec=True)
    @pytest.mark.asyncio
    async def test_get_refresh(self, mock_api):
        payload = {"refresh_id": REFRESH_ID, "state": "MONITOR_REFRESH_STATE_RUNNING", "message": "detail"}
        mock_api.return_value = payload
        hook = DatabricksDataQualityHook()
        assert await hook.a_get_refresh("table/id", REFRESH_ID) == payload
        mock_api.assert_awaited_once_with(
            hook, ("GET", "data-quality/v1/monitors/table/table%2Fid/refreshes/42")
        )

    @pytest.mark.parametrize("state", ["MONITOR_REFRESH_STATE_PENDING", "MONITOR_REFRESH_STATE_RUNNING"])
    @mock.patch("airflow.providers.databricks.hooks.data_quality.asyncio.sleep", autospec=True)
    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_wait_for_submitted_refresh(self, mock_get, mock_sleep, state):
        mock_get.side_effect = [
            {"refresh_id": REFRESH_ID, "state": state},
            {
                "refresh_id": REFRESH_ID,
                "object_id": TABLE_ID,
                "object_type": "table",
                "state": "MONITOR_REFRESH_STATE_SUCCESS",
            },
        ]
        hook = DatabricksDataQualityHook()
        await hook.a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=100, polling_period_seconds=7)
        assert mock_get.await_args_list == [mock.call(hook, TABLE_ID, REFRESH_ID)] * 2
        mock_sleep.assert_awaited_once_with(7)
        assert hook._session is None

    @pytest.mark.parametrize("state", ["MONITOR_REFRESH_STATE_FAILED", "MONITOR_REFRESH_STATE_CANCELED"])
    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_refresh_failure(self, mock_get, state):
        mock_get.return_value = {"refresh_id": REFRESH_ID, "state": state}
        with pytest.raises(DatabricksDataQualityError, match=f"entered {state}"):
            await DatabricksDataQualityHook().a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=100)

    @pytest.mark.parametrize("state", [None, "MONITOR_REFRESH_STATE_UNKNOWN", "NEW_STATE", [], {}])
    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_unknown_refresh_state(self, mock_get, state):
        mock_get.return_value = {"refresh_id": REFRESH_ID, "state": state}
        with pytest.raises(DatabricksDataQualityError, match="unrecognized state"):
            await DatabricksDataQualityHook().a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=100)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("refresh_id", None),
            ("refresh_id", 43),
            ("refresh_id", True),
            ("object_id", "other-table"),
            ("object_type", "schema"),
        ],
    )
    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_reject_mismatched_refresh(self, mock_get, field, value):
        payload = {
            "refresh_id": REFRESH_ID,
            "object_id": TABLE_ID,
            "object_type": "table",
            "state": "MONITOR_REFRESH_STATE_SUCCESS",
        }
        payload[field] = value
        mock_get.return_value = payload
        with pytest.raises(DatabricksDataQualityError, match="different refresh identity"):
            await DatabricksDataQualityHook().a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=100)

    @pytest.mark.parametrize(("clock", "requests"), [([0, 10], 0), ([0, 0, 10], 1), ([0, 0, 11], 1)])
    @mock.patch("airflow.providers.databricks.hooks.data_quality.monotonic", autospec=True)
    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_deadline_before_request_and_success(self, mock_get, mock_clock, clock, requests):
        mock_clock.side_effect = clock
        mock_get.return_value = {"refresh_id": REFRESH_ID, "state": "MONITOR_REFRESH_STATE_SUCCESS"}
        with pytest.raises(DatabricksDataQualityTimeout):
            await DatabricksDataQualityHook().a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=10)
        assert mock_get.await_count == requests

    @mock.patch("airflow.providers.databricks.hooks.data_quality.monotonic", autospec=True)
    @mock.patch("airflow.providers.databricks.hooks.data_quality.asyncio.sleep", autospec=True)
    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_clip_sleep(self, mock_get, mock_sleep, mock_clock):
        mock_clock.side_effect = [0, 0, 9, 9, 10]
        mock_get.return_value = {"refresh_id": REFRESH_ID, "state": "MONITOR_REFRESH_STATE_RUNNING"}
        with pytest.raises(DatabricksDataQualityTimeout):
            await DatabricksDataQualityHook().a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=10)
        mock_sleep.assert_awaited_once_with(1)

    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_cancel_slow_request_at_timeout(self, mock_get):
        async def pending(*args):
            await asyncio.Event().wait()

        mock_get.side_effect = pending
        hook = DatabricksDataQualityHook()
        with pytest.raises(DatabricksDataQualityTimeout):
            await hook.a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=0.05)
        mock_get.assert_awaited_once_with(hook, TABLE_ID, REFRESH_ID)
        assert hook._session is None

    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_request_timeout_is_not_wait_timeout(self, mock_get):
        mock_get.side_effect = TimeoutError("request timeout")
        with pytest.raises(TimeoutError, match="request timeout") as exc:
            await DatabricksDataQualityHook().a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=100)
        assert not isinstance(exc.value, DatabricksDataQualityTimeout)

    @mock.patch.object(DatabricksDataQualityHook, "a_get_refresh", autospec=True)
    @pytest.mark.asyncio
    async def test_cancellation_closes_session(self, mock_get):
        mock_get.side_effect = asyncio.CancelledError
        hook = DatabricksDataQualityHook()
        with pytest.raises(asyncio.CancelledError):
            await hook.a_wait_for_refresh(TABLE_ID, REFRESH_ID, timeout=100)
        assert hook._session is None
