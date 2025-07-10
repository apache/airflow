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
    pytest.skip("Human in the loop public API compatible with Airflow >= 3.0.1", allow_module_level=True)

import asyncio
from datetime import timedelta
from unittest import mock

from uuid6 import uuid7

from airflow.api_fastapi.execution_api.datamodels.hitl import HITLDetailResponse
from airflow.providers.standard.triggers.hitl import (
    HITLTrigger,
    HITLTriggerEventFailurePayload,
    HITLTriggerEventSuccessPayload,
)
from airflow.triggers.base import TriggerEvent
from airflow.utils.timezone import utcnow

TI_ID = uuid7()


class TestHITLTrigger:
    def test_serialization(self):
        trigger = HITLTrigger(
            ti_id=TI_ID,
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
            defaults=["1"],
            multiple=False,
            timeout_datetime=None,
            poke_interval=50.0,
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
    @mock.patch("airflow.sdk.execution_time.hitl.update_htil_detail_response")
    async def test_run_failed_due_to_timeout(self, mock_update, mock_supervisor_comms):
        trigger = HITLTrigger(
            ti_id=TI_ID,
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
            multiple=False,
            timeout_datetime=utcnow() + timedelta(seconds=0.1),
            poke_interval=5,
        )
        mock_supervisor_comms.send.return_value = HITLDetailResponse(
            response_received=False,
            user_id=None,
            response_at=None,
            chosen_options=None,
            params_input={},
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0.3)
        event = await trigger_task
        assert event == TriggerEvent(
            HITLTriggerEventFailurePayload(
                error="The timeout has passed, and the response has not yet been received.",
                error_type="timeout",
            )
        )

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.sdk.execution_time.hitl.update_htil_detail_response")
    async def test_run_fallback_to_default_due_to_timeout(self, mock_update, mock_supervisor_comms):
        trigger = HITLTrigger(
            ti_id=TI_ID,
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
            defaults=["1"],
            multiple=False,
            timeout_datetime=utcnow() + timedelta(seconds=0.1),
            poke_interval=5,
        )
        mock_supervisor_comms.send.return_value = HITLDetailResponse(
            response_received=False,
            user_id=None,
            response_at=None,
            chosen_options=None,
            params_input={},
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0.3)
        event = await trigger_task
        assert event == TriggerEvent(
            HITLTriggerEventSuccessPayload(
                chosen_options=["1"],
                params_input={"input": 1},
            )
        )

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.sdk.execution_time.hitl.update_htil_detail_response")
    async def test_run(self, mock_update, mock_supervisor_comms):
        trigger = HITLTrigger(
            ti_id=TI_ID,
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
            defaults=["1"],
            multiple=False,
            timeout_datetime=None,
            poke_interval=5,
        )
        mock_supervisor_comms.send.return_value = HITLDetailResponse(
            response_received=True,
            user_id="test",
            response_at=utcnow(),
            chosen_options=["3"],
            params_input={"input": 50},
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0.3)
        event = await trigger_task
        assert event == TriggerEvent(
            HITLTriggerEventSuccessPayload(
                chosen_options=["3"],
                params_input={"input": 50},
            )
        )
