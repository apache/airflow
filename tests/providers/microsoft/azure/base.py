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
import asyncio
from copy import deepcopy
from datetime import datetime
from typing import List, Tuple, Any, Iterable, Union, Optional

import pytest
from sqlalchemy.orm import Session

from airflow.exceptions import TaskDeferred
from airflow.models import Operator, TaskInstance
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.session import NEW_SESSION
from airflow.utils.state import TaskInstanceState
from airflow.utils.xcom import XCOM_RETURN_KEY


class MockedTaskInstance(TaskInstance):
    values = {}

    def xcom_pull(
        self,
        task_ids: Optional[Union[Iterable[str], str]] = None,
        dag_id: Optional[str] = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
        *,
        map_indexes: Optional[Union[Iterable[int], int]] = None,
        default: Optional[Any] = None,
    ) -> Any:
        self.task_id = task_ids
        self.dag_id = dag_id
        return self.values.get(f"{task_ids}_{dag_id}_{key}")

    def xcom_push(
        self,
        key: str,
        value: Any,
        execution_date: Optional[datetime] = None,
        session: Session = NEW_SESSION,
    ) -> None:
        self.values[f"{self.task_id}_{self.dag_id}_{key}"] = value


class Base:
    _loop = asyncio.get_event_loop()

    def teardown_method(self, method):
        KiotaRequestAdapterHook.cached_request_adapters.clear()
        MockedTaskInstance.values.clear()

    @staticmethod
    async def run_tigger(trigger: BaseTrigger) -> List[TriggerEvent]:
        events = []
        async for event in trigger.run():
            events.append(event)
        return events

    def execute_operator(self, operator: Operator) -> Tuple[Any, Any]:
        task_instance = MockedTaskInstance(task=operator, run_id="run_id", state=TaskInstanceState.RUNNING)
        context = {"ti": task_instance}
        result = None
        triggered_events = []

        with pytest.raises(TaskDeferred) as deferred:
            operator.execute(context=context)

        task = deferred.value

        while task:
            events = self._loop.run_until_complete(self.run_tigger(deferred.value.trigger))

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
