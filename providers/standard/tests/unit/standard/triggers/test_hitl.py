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

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop public API compatible with Airflow >= 3.1.0", allow_module_level=True)

import asyncio
from datetime import datetime, timedelta
from unittest import mock

from uuid6 import uuid7

from airflow._shared.timezones.timezone import utc, utcnow
from airflow.api_fastapi.execution_api.datamodels.hitl import HITLDetailResponse, HITLUser
from airflow.providers.standard.triggers.hitl import (
    HITLTrigger,
    HITLTriggerEventFailurePayload,
    HITLTriggerEventSuccessPayload,
)
from airflow.triggers.base import TriggerEvent

TI_ID = uuid7()
default_trigger_args = {
    "ti_id": TI_ID,
    "options": ["1", "2", "3", "4", "5"],
    "params": {"input": 1},
    "multiple": False,
}


class TestHITLTrigger:
    def test_serialization(self):
        trigger = HITLTrigger(
            defaults=["1"],
            timeout_datetime=None,
            poke_interval=50.0,
            **default_trigger_args,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.standard.triggers.hitl.HITLTrigger"
        assert kwargs == {
            "ti_id": TI_ID,
            "options": ["1", "2", "3", "4", "5"],
            "params": {"input": 1},
            "defaults": ["1"],
            "multiple": False,
            "timeout_datetime": None,
            "poke_interval": 50.0,
        }

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.sdk.execution_time.hitl.update_hitl_detail_response")
    async def test_run_failed_due_to_timeout(self, mock_update, mock_supervisor_comms):
        trigger = HITLTrigger(
            timeout_datetime=utcnow() + timedelta(seconds=0.1),
            poke_interval=5,
            **default_trigger_args,
        )
        mock_supervisor_comms.send.return_value = HITLDetailResponse(
            response_received=False,
            responded_user_id=None,
            responded_user_name=None,
            response_at=None,
            chosen_options=None,
            params_input={},
        )

        gen = trigger.run()
        await asyncio.sleep(0.3)
        trigger_task = asyncio.create_task(gen.__anext__())
        event = await trigger_task
        assert event == TriggerEvent(
            HITLTriggerEventFailurePayload(
                error="The timeout has passed, and the response has not yet been received.",
                error_type="timeout",
            )
        )

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch.object(HITLTrigger, "log")
    @mock.patch("airflow.sdk.execution_time.hitl.update_hitl_detail_response")
    async def test_run_fallback_to_default_due_to_timeout(self, mock_update, mock_log, mock_supervisor_comms):
        trigger = HITLTrigger(
            defaults=["1"],
            timeout_datetime=utcnow() + timedelta(seconds=0.1),
            poke_interval=5,
            **default_trigger_args,
        )
        mock_supervisor_comms.send.return_value = HITLDetailResponse(
            response_received=False,
            responded_by_user=None,
            response_at=None,
            chosen_options=None,
            params_input={},
        )

        gen = trigger.run()
        await asyncio.sleep(0.3)
        trigger_task = asyncio.create_task(gen.__anext__())
        event = await trigger_task

        assert event == TriggerEvent(
            HITLTriggerEventSuccessPayload(
                chosen_options=["1"],
                params_input={"input": 1},
                responded_by_user=None,
                timedout=True,
            )
        )

        assert mock_log.info.call_args == mock.call(
            "[HITL] timeout reached before receiving response, fallback to default %s", ["1"]
        )

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch.object(HITLTrigger, "log")
    @mock.patch("airflow.sdk.execution_time.hitl.update_hitl_detail_response")
    async def test_run_should_check_response_in_timeout_handler(
        self, mock_update, mock_log, mock_supervisor_comms
    ):
        # action time only slightly before timeout
        action_datetime = utcnow() + timedelta(seconds=0.1)
        timeout_datetime = utcnow() + timedelta(seconds=0.1)

        trigger = HITLTrigger(
            defaults=["1"],
            timeout_datetime=timeout_datetime,
            poke_interval=5,
            **default_trigger_args,
        )
        mock_supervisor_comms.send.return_value = HITLDetailResponse(
            response_received=True,
            responded_by_user=HITLUser(id="1", name="test"),
            response_at=action_datetime,
            chosen_options=["2"],
            params_input={},
        )

        gen = trigger.run()
        await asyncio.sleep(0.3)
        trigger_task = asyncio.create_task(gen.__anext__())
        event = await trigger_task

        assert event == TriggerEvent(
            HITLTriggerEventSuccessPayload(
                chosen_options=["2"],
                params_input={},
                responded_by_user={"id": "1", "name": "test"},
                timedout=False,
            )
        )

        assert mock_log.info.call_args == mock.call(
            "[HITL] responded_by=%s (id=%s) options=%s at %s (timeout fallback skipped)",
            "test",
            "1",
            ["2"],
            action_datetime,
        )

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch.object(HITLTrigger, "log")
    @mock.patch("airflow.sdk.execution_time.hitl.update_hitl_detail_response")
    async def test_run(self, mock_update, mock_log, mock_supervisor_comms, time_machine):
        time_machine.move_to(datetime(2025, 7, 29, 2, 0, 0))

        trigger = HITLTrigger(
            defaults=["1"],
            timeout_datetime=None,
            poke_interval=5,
            **default_trigger_args,
        )
        mock_supervisor_comms.send.return_value = HITLDetailResponse(
            response_received=True,
            responded_by_user=HITLUser(id="test", name="test"),
            response_at=utcnow(),
            chosen_options=["3"],
            params_input={"input": 50},
        )

        gen = trigger.run()
        await asyncio.sleep(0.3)
        trigger_task = asyncio.create_task(gen.__anext__())
        event = await trigger_task
        assert event == TriggerEvent(
            HITLTriggerEventSuccessPayload(
                chosen_options=["3"],
                params_input={"input": 50},
                responded_by_user={"id": "test", "name": "test"},
                timedout=False,
            )
        )

        assert mock_log.info.call_args == mock.call(
            "[HITL] responded_by=%s (id=%s) options=%s at %s",
            "test",
            "test",
            ["3"],
            datetime(2025, 7, 29, 2, 0, 0, tzinfo=utc),
        )
