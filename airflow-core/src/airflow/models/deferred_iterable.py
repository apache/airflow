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
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session
from wrapt import synchronized

from airflow.exceptions import AirflowException
from airflow.models.xcom import XComModel
from airflow.sdk.bases.operator import BaseOperator as Operator
from airflow.sdk.definitions._internal.mixins import ResolveMixin
from airflow.sdk.definitions.context import Context
from airflow.sdk.definitions.xcom_arg import MapXComArg  # noqa: F401
from airflow.serialization import serde
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop


@contextmanager
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    new_event_loop = False
    loop = None
    try:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            new_event_loop = True
        yield loop
    finally:
        if new_event_loop and loop is not None:
            with suppress(AttributeError):
                loop.close()


# TODO: Check _run_inline_trigger method from DAG, could be refactored so it uses this method
@synchronized
async def run_trigger(trigger: BaseTrigger) -> TriggerEvent | None:
    events = []
    async for event in trigger.run():
        events.append(event)
    return next(iter(events), None)


class DeferredIterable(Iterator, ResolveMixin, LoggingMixin):
    """An iterable that lazily fetches XCom values one by one instead of loading all at once."""

    def __init__(
        self,
        results: list[Any] | Any,
        trigger: BaseTrigger,
        operator: Operator,
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

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        yield self.operator, XCOM_RETURN_KEY

    def resolve(self, context: Context, session: Session = None, *, include_xcom: bool = True) -> Any:
        self.log.debug("resolve: %s", self)
        return DeferredIterable(
            results=self.results,
            trigger=self.trigger,
            operator=self.operator,
            next_method=self.next_method,
            context=context,
        )

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

        if not self.context:
            raise AirflowException("Context is required to run the trigger.")

        results = self._execute_trigger()

        if isinstance(results, (list, set)):
            self.results.extend(results)
        else:
            self.results.append(results)

        # We push appended results back to existing XCom
        XComModel.set(
            key=self.operator.output.key,
            value=self.results,
            task_id=self.operator.task_id,
            dag_id=self.operator.dag_id,
            run_id=self.context["run_id"],
            # map_index=self.context["ti"].map_index,
        )

        self.index += 1
        return self.results[-1]

    def _execute_trigger(self):
        try:
            with event_loop() as loop:
                self.log.debug("Running trigger: %s", self.trigger)
                event = loop.run_until_complete(run_trigger(self.trigger))
                self.operator.render_template_fields(context=self.context)
                next_method = getattr(self.operator, self.next_method)
                self.log.debug("Triggering next method: %s", self.next_method)
                results = next_method(self.context, event.payload)
        except Exception as e:
            self.log.exception(e)
            raise AirflowException from e

        if isinstance(results, DeferredIterable):
            self.trigger = results.trigger
            return results.results

        self.trigger = None
        return results

    def serialize(self):
        """Ensure the object is JSON serializable."""
        return {
            "results": self.results,
            "trigger": self.trigger.serialize() if self.trigger else None,
            "dag_fileloc": self.operator.dag.fileloc,
            "dag_id": self.operator.dag_id,
            "task_id": self.operator.task_id,
            "next_method": self.next_method,
        }

    @classmethod
    def get_operator_from_dag(cls, dag_fileloc: str, dag_id: str, task_id: str) -> Operator:
        """Loads a DAG using DagBag and gets the operator by task_id."""

        from airflow.models import DagBag

        dag_bag = DagBag(collect_dags=False)  # Avoid loading all DAGs
        dag_bag.process_file(dag_fileloc)
        return dag_bag.dags[dag_id].get_task(task_id)

    @classmethod
    def deserialize(cls, data: dict, version: int) -> DeferredIterable:
        """Ensure the object is JSON deserializable."""
        trigger_class = import_string(data["trigger"][0])
        trigger = trigger_class(**data["trigger"][1])
        operator = cls.get_operator_from_dag(data["dag_fileloc"], data["dag_id"], data["task_id"])
        cls.logger().info("deserialize: %s", operator)
        return DeferredIterable(
            results=data["results"],
            trigger=trigger,
            operator=operator,
            next_method=data["next_method"],
        )


serde._extra_allowed = serde._extra_allowed.union(
    {
        "infrabel.operators.iterable.DeferredIterable",
    }
)
