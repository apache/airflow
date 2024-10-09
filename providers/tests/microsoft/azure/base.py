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
from contextlib import contextmanager
from copy import deepcopy
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

from kiota_http.httpx_request_adapter import HttpxRequestAdapter

from airflow.exceptions import TaskDeferred
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook

from providers.tests.microsoft.conftest import get_airflow_connection, mock_context

if TYPE_CHECKING:
    from airflow.models import Operator
    from airflow.triggers.base import BaseTrigger, TriggerEvent


class Base:
    def teardown_method(self, method):
        KiotaRequestAdapterHook.cached_request_adapters.clear()

    @contextmanager
    def patch_hook_and_request_adapter(self, response):
        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message") as mock_get_http_response,
        ):
            if isinstance(response, Exception):
                mock_get_http_response.side_effect = response
            else:
                mock_get_http_response.return_value = response
            yield

    @staticmethod
    async def _run_tigger(trigger: BaseTrigger) -> list[TriggerEvent]:
        events = []
        async for event in trigger.run():
            events.append(event)
        return events

    def run_trigger(self, trigger: BaseTrigger) -> list[TriggerEvent]:
        return asyncio.run(self._run_tigger(trigger))

    def execute_operator(self, operator: Operator) -> tuple[Any, Any]:
        context = mock_context(task=operator)
        return asyncio.run(self.deferrable_operator(context, operator))

    async def deferrable_operator(self, context, operator):
        result = None
        triggered_events = []
        try:
            result = operator.execute(context=context)
        except TaskDeferred as deferred:
            task = deferred

            while task:
                events = await self._run_tigger(task.trigger)

                if not events:
                    break

                triggered_events.extend(deepcopy(events))

                try:
                    method = getattr(operator, task.method_name)
                    result = method(context=context, event=next(iter(events)).payload)
                    task = None
                except TaskDeferred as exception:
                    task = exception
        return result, triggered_events
