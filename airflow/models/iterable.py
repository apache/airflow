#
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
import inspect
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.session import provide_session, NEW_SESSION

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.triggers.base import BaseTrigger, run_trigger
    from airflow.utils.context import Context

    from sqlalchemy.orm import Session


class DeferredIterable(Iterator, LoggingMixin):
    """An iterable that lazily fetches XCom values one by one instead of loading all at once."""

    def __init__(self, results: list[Any] | Any, trigger: BaseTrigger, operator: BaseOperator, next_method: str, context: Context | None = None):
        super().__init__()
        self.results = results.copy() if isinstance(results, list) else [results]
        self.trigger = trigger
        self.operator = operator
        self.next_method = next_method
        self.context = context
        self.index = 0
        self._loop = None

    @provide_session
    def resolve(self, context: Context, session: Session = NEW_SESSION, *, include_xcom: bool = True) -> DeferredIterable:
        return DeferredIterable(
            results=self.results,
            trigger=self.trigger,
            operator=self.operator,
            next_method=self.next_method,
            context=context,
        )

    @property
    def loop(self):
        if not self._loop:
            self._loop = asyncio.new_event_loop()
        return self._loop

    def __iter__(self) -> Iterator:
        return self

    def __next__(self):
        if self.index < len(self.results):
            result = self.results[self.index]
            self.index += 1
            return result

        # No more results; attempt to load the next page using the trigger
        self.log.info("No more results. Running trigger: %s", self.trigger)

        event = self.loop.run_until_complete(run_trigger(self.trigger))
        iterator = getattr(self.operator, self.next_method)(self.context, event.payload)
        if not iterator:
            raise StopIteration

        self.trigger = iterator.trigger
        self.results.extend(iterator.results)
        self.index += 1
        return self.results[-1]

    def __len__(self):
        return len(self.results)

    def __getitem__(self, index: int):
        if not (0 <= index < len(self)):
            raise IndexError

        return self.results[index]

    def __del__(self):
        if self._loop:
            self._loop.close()

    def serialize(self):
        """Ensure the object is JSON serializable."""
        return {
            "results": self.results,
            "trigger": self.trigger.serialize(),
            "operator": SerializedBaseOperator.serialize_operator(self.operator),
            "next_method": self.next_method,
        }

    @classmethod
    def deserialize(cls, data: dict, version: int):
        """Ensure the object is JSON deserializable."""
        trigger_class = import_string(data["trigger"][0])
        trigger = trigger_class(**data["trigger"][1])
        operator_class = import_string(f"{data['operator']['_task_module']}.{data['operator']['_task_type']}")
        operator_kwargs = {
            key: value for key, value in data["operator"].items()
            if key in inspect.signature(operator_class.__init__).parameters or key in operator_class._comps
        }
        operator = operator_class(**operator_kwargs)
        return DeferredIterable(
            results=data["results"],
            trigger=trigger,
            operator=operator,
            next_method=data["next_method"]
        )
