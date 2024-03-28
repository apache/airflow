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
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterable
from unittest.mock import patch

import pytest
from kiota_http.httpx_request_adapter import HttpxRequestAdapter

from airflow.exceptions import TaskDeferred
from airflow.models import Operator, TaskInstance
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.utils.session import NEW_SESSION
from airflow.utils.state import TaskInstanceState
from airflow.utils.xcom import XCOM_RETURN_KEY
from tests.providers.microsoft.conftest import get_airflow_connection

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.triggers.base import BaseTrigger, TriggerEvent


class MockedTaskInstance(TaskInstance):
    values = {}

    def xcom_pull(
        self,
        task_ids: Iterable[str] | str | None = None,
        dag_id: str | None = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
        *,
        map_indexes: Iterable[int] | int | None = None,
        default: Any | None = None,
    ) -> Any:
        self.task_id = task_ids
        self.dag_id = dag_id
        return self.values.get(f"{task_ids}_{dag_id}_{key}")

    def xcom_push(
        self,
        key: str,
        value: Any,
        execution_date: datetime | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        self.values[f"{self.task_id}_{self.dag_id}_{key}"] = value


class Base:
    _loop = asyncio.get_event_loop()

    def teardown_method(self, method):
        KiotaRequestAdapterHook.cached_request_adapters.clear()
        MockedTaskInstance.values.clear()

    @contextmanager
    def patch_hook_and_request_adapter(self, response):
        with patch(
            "airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection
        ), patch.object(HttpxRequestAdapter, "get_http_response_message") as mock_get_http_response:
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
        return self.run_async(self._run_tigger(trigger))

    def run_async(self, future: Any) -> Any:
        return self._loop.run_until_complete(future)

    def execute_operator(self, operator: Operator) -> tuple[Any, Any]:
        task_instance = MockedTaskInstance(task=operator, run_id="run_id", state=TaskInstanceState.RUNNING)
        context = {"ti": task_instance}
        result = None
        triggered_events = []

        with pytest.raises(TaskDeferred) as deferred:
            operator.execute(context=context)

        task = deferred.value

        while task:
            events = self.run_trigger(deferred.value.trigger)

            if not events:
                break

            triggered_events.extend(deepcopy(events))

            try:
                method = getattr(operator, deferred.value.method_name)
                result = method(context=context, event=next(iter(events)).payload)
                task = None
            except TaskDeferred as exception:
                task = exception

        return result, triggered_events
