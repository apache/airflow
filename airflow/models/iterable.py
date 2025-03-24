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
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import BaseOperator
    from airflow.triggers.base import BaseTrigger, run_trigger
    from airflow.utils.context import Context


class DeferredIterable(Iterator, LoggingMixin):
    """An iterable that lazily fetches XCom values one by one instead of loading all at once."""

    def __init__(
        self,
        results: list[Any] | Any,
        trigger: BaseTrigger,
        operator: BaseOperator,
        next_method: str,
        context: Context | None = None,
    ):
        super().__init__()
        self.results = results.copy() if isinstance(results, list) else [results]
        self.trigger = trigger
        self.operator = operator
        self.next_method = next_method
        self.context = context
        self.index = 0
        self._loop = None

    @provide_session
    def resolve(
        self, context: Context, session: Session = NEW_SESSION, *, include_xcom: bool = True
    ) -> DeferredIterable:
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

        if not self.trigger:
            raise StopIteration

        self.log.info("No more results. Running trigger: %s", self.trigger)

        try:
            event = self.loop.run_until_complete(run_trigger(self.trigger))
            next_method = getattr(self.operator, self.next_method)
            self.log.debug("Triggering next method: %s", self.next_method)
            results = next_method(self.context, event.payload)
        except Exception as e:
            raise AirflowException from e

        if isinstance(results, DeferredIterable):
            self.trigger = results.trigger
            self.results.extend(results.results)
        else:
            self.trigger = None
            self.results.extend(results)

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
            "trigger": self.trigger.serialize() if self.trigger else None,
            "dag_id": self.operator.dag_id,
            "task_id": self.operator.task_id,
            "next_method": self.next_method,
        }

    @classmethod
    def get_operator_from_dag(cls, dag_id: str, task_id: str) -> BaseOperator:
        """Loads a DAG using DagBag and gets the operator by task_id."""

        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=None)
        return dag_bag.dags[dag_id].get_task(task_id)

    @classmethod
    def deserialize(cls, data: dict, version: int):
        """Ensure the object is JSON deserializable."""

        trigger_class = import_string(data["trigger"][0])
        trigger = trigger_class(**data["trigger"][1])
        operator = cls.get_operator_from_dag(data["dag_id"], data["task_id"])
        return DeferredIterable(
            results=data["results"], trigger=trigger, operator=operator, next_method=data["next_method"]
        )
