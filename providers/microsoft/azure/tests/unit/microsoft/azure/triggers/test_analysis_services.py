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

from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
    RefreshType,
)
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
    validate_completed_refresh_event,
    validate_refresh_event,
)
from airflow.triggers.base import TriggerEvent

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "adventureworks"
REFRESH_ID = "refresh-id"
REFRESH_TYPE: RefreshType = "full"
POKE_INTERVAL = 5
REQUEST_TIMEOUT = 30
MODULE = "airflow.providers.microsoft.azure.triggers.analysis_services"


def build_trigger(refresh_id: str | None = REFRESH_ID) -> AzureAnalysisServicesRefreshTrigger:
    """Build a trigger with standard test arguments."""
    return AzureAnalysisServicesRefreshTrigger(
        conn_id=CONN_ID,
        server_name=SERVER_NAME,
        database=DATABASE,
        refresh_id=refresh_id,
        refresh_type=REFRESH_TYPE,
        poke_interval=POKE_INTERVAL,
        request_timeout=REQUEST_TIMEOUT,
    )


class TestAzureAnalysisServicesRefreshTrigger:
    def test_serializes_all_arguments(self):
        trigger = build_trigger()

        classpath, arguments = trigger.serialize()

        assert (
            classpath
            == f"{AzureAnalysisServicesRefreshTrigger.__module__}.AzureAnalysisServicesRefreshTrigger"
        )
        assert arguments == {
            "conn_id": CONN_ID,
            "server_name": SERVER_NAME,
            "database": DATABASE,
            "refresh_id": REFRESH_ID,
            "refresh_type": REFRESH_TYPE,
            "poke_interval": POKE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
        }

    def test_rejects_invalid_poke_interval(self):
        with pytest.raises(ValueError, match="poke_interval must be greater than zero"):
            AzureAnalysisServicesRefreshTrigger(
                conn_id=CONN_ID,
                server_name=SERVER_NAME,
                database=DATABASE,
                refresh_id=REFRESH_ID,
                poke_interval=0,
            )

    def test_rejects_invalid_request_timeout(self):
        with pytest.raises(ValueError, match="request_timeout must be greater than zero"):
            AzureAnalysisServicesRefreshTrigger(
                conn_id=CONN_ID,
                server_name=SERVER_NAME,
                database=DATABASE,
                refresh_id=REFRESH_ID,
                request_timeout=0,
            )

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @pytest.mark.asyncio
    async def test_emits_success_event(self, hook_class):
        hook = hook_class.return_value
        hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED

        events = [event async for event in build_trigger().run()]

        hook_class.assert_called_once_with(
            azure_analysis_services_conn_id=CONN_ID,
            request_timeout=REQUEST_TIMEOUT,
        )
        hook.get_refresh_status.assert_awaited_once_with(
            server_name=SERVER_NAME,
            database=DATABASE,
            refresh_id=REFRESH_ID,
        )
        assert events == [
            TriggerEvent(
                {
                    "status": "success",
                    "refresh_status": AzureAnalysisServicesRefreshStatus.SUCCEEDED,
                    "message": f"Refresh {REFRESH_ID} completed successfully",
                    "refresh_id": REFRESH_ID,
                }
            )
        ]

    @pytest.mark.parametrize("status", sorted(AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES))
    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @pytest.mark.asyncio
    async def test_emits_error_event_for_failure_status(self, hook_class, status):
        hook_class.return_value.get_refresh_status.return_value = status

        events = [event async for event in build_trigger().run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "refresh_status": status,
                    "message": f"Refresh {REFRESH_ID} finished with status {status}",
                    "refresh_id": REFRESH_ID,
                }
            )
        ]

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @mock.patch(f"{MODULE}.asyncio.sleep", new_callable=mock.AsyncMock)
    @pytest.mark.asyncio
    async def test_polls_again_after_non_terminal_status(self, sleep, hook_class):
        hook = hook_class.return_value
        hook.get_refresh_status.side_effect = [
            AzureAnalysisServicesRefreshStatus.IN_PROGRESS,
            AzureAnalysisServicesRefreshStatus.SUCCEEDED,
        ]

        events = [event async for event in build_trigger().run()]

        sleep.assert_awaited_once_with(POKE_INTERVAL)
        assert hook.get_refresh_status.await_count == 2
        assert events[0].payload["status"] == "success"

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @pytest.mark.asyncio
    async def test_triggers_refresh_and_yields_without_polling(self, hook_class):
        hook = hook_class.return_value
        hook.trigger_refresh.return_value = REFRESH_ID

        events = [event async for event in build_trigger(refresh_id=None).run()]

        hook.trigger_refresh.assert_awaited_once_with(
            server_name=SERVER_NAME,
            database=DATABASE,
            refresh_type=REFRESH_TYPE,
        )
        hook.get_refresh_status.assert_not_awaited()
        assert events == [
            TriggerEvent(
                {
                    "status": "success",
                    "refresh_status": None,
                    "message": f"Refresh {REFRESH_ID} has been triggered",
                    "refresh_id": REFRESH_ID,
                }
            )
        ]

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @pytest.mark.asyncio
    async def test_skips_trigger_when_refresh_id_provided(self, hook_class):
        hook = hook_class.return_value
        hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED

        [event async for event in build_trigger().run()]

        hook.trigger_refresh.assert_not_awaited()

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @pytest.mark.asyncio
    async def test_emits_error_event_when_trigger_refresh_fails(self, hook_class):
        hook_class.return_value.trigger_refresh.side_effect = AzureAnalysisServicesRefreshException(
            "Failed to authenticate with Azure Analysis Services"
        )

        events = [event async for event in build_trigger(refresh_id=None).run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "refresh_status": None,
                    "message": "Failed to authenticate with Azure Analysis Services",
                    "refresh_id": None,
                }
            )
        ]

    @pytest.mark.parametrize("fails", [False, True])
    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @pytest.mark.asyncio
    async def test_closes_hook_on_exit(self, hook_class, fails):
        hook = hook_class.return_value
        if fails:
            hook.get_refresh_status.side_effect = AzureAnalysisServicesRefreshException("boom")
        else:
            hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED

        [event async for event in build_trigger().run()]

        hook.aclose.assert_awaited_once_with()

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @pytest.mark.asyncio
    async def test_converts_hook_exception_to_error_event(self, hook_class):
        hook_class.return_value.get_refresh_status.side_effect = AzureAnalysisServicesRefreshException(
            "API unavailable"
        )

        events = [event async for event in build_trigger().run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "refresh_status": None,
                    "message": "API unavailable",
                    "refresh_id": REFRESH_ID,
                }
            )
        ]

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    @pytest.mark.asyncio
    async def test_uses_exception_class_name_when_message_is_empty(self, hook_class):
        hook_class.return_value.get_refresh_status.side_effect = RuntimeError()

        events = [event async for event in build_trigger().run()]

        assert events[0].payload["message"] == "RuntimeError"


class TestValidateRefreshEvent:
    def test_returns_refresh_id_for_success(self):
        event = {
            "status": "success",
            "refresh_status": AzureAnalysisServicesRefreshStatus.SUCCEEDED,
            "message": "completed",
            "refresh_id": REFRESH_ID,
        }

        assert validate_refresh_event(event) == REFRESH_ID

    @pytest.mark.parametrize("event", [None, [], "event"])
    def test_rejects_non_dictionary_event(self, event):
        with pytest.raises(AzureAnalysisServicesRefreshException, match="valid event"):
            validate_refresh_event(event)

    @pytest.mark.parametrize("refresh_id", [None, "", 1])
    def test_rejects_invalid_refresh_id(self, refresh_id):
        with pytest.raises(AzureAnalysisServicesRefreshException, match="valid refresh ID"):
            validate_refresh_event({"status": "success", "refresh_id": refresh_id})

    def test_raises_error_event_message(self):
        with pytest.raises(AzureAnalysisServicesRefreshException, match="refresh failed"):
            validate_refresh_event(
                {
                    "status": "error",
                    "refresh_status": AzureAnalysisServicesRefreshStatus.FAILED,
                    "message": "refresh failed",
                    "refresh_id": REFRESH_ID,
                }
            )

    @pytest.mark.parametrize("message", [None, "", 1])
    def test_raises_fallback_for_error_without_message(self, message):
        with pytest.raises(AzureAnalysisServicesRefreshException, match="refresh failed"):
            validate_refresh_event(
                {
                    "status": "error",
                    "refresh_status": AzureAnalysisServicesRefreshStatus.FAILED,
                    "message": message,
                    "refresh_id": REFRESH_ID,
                }
            )

    def test_raises_error_message_when_refresh_id_absent(self):
        # A POST that fails before a refresh exists yields no ID; the message is the only diagnostic.
        with pytest.raises(AzureAnalysisServicesRefreshException, match="Failed to authenticate"):
            validate_refresh_event(
                {
                    "status": "error",
                    "refresh_status": None,
                    "message": "Failed to authenticate with Azure Analysis Services",
                    "refresh_id": None,
                }
            )

    def test_rejects_unknown_event_status(self):
        with pytest.raises(AzureAnalysisServicesRefreshException, match="unknown event status"):
            validate_refresh_event({"status": "unknown", "refresh_id": REFRESH_ID})

    def test_base_validator_accepts_null_refresh_status(self):
        event = {"status": "success", "refresh_status": None, "refresh_id": REFRESH_ID}

        assert validate_refresh_event(event) == REFRESH_ID
        with pytest.raises(AzureAnalysisServicesRefreshException, match="unexpected refresh status"):
            validate_completed_refresh_event(event)

    def test_completed_validator_returns_refresh_id(self):
        assert (
            validate_completed_refresh_event(
                {
                    "status": "success",
                    "refresh_status": AzureAnalysisServicesRefreshStatus.SUCCEEDED,
                    "refresh_id": REFRESH_ID,
                }
            )
            == REFRESH_ID
        )

    @pytest.mark.parametrize(
        "refresh_status",
        [None, AzureAnalysisServicesRefreshStatus.IN_PROGRESS, AzureAnalysisServicesRefreshStatus.FAILED],
    )
    def test_completed_validator_rejects_non_success_refresh_status(self, refresh_status):
        with pytest.raises(AzureAnalysisServicesRefreshException, match="unexpected refresh status"):
            validate_completed_refresh_event(
                {
                    "status": "success",
                    "refresh_status": refresh_status,
                    "refresh_id": REFRESH_ID,
                }
            )
