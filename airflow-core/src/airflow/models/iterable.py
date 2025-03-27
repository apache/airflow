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
from collections.abc import Generator, Iterator, Sequence
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.serialization import serde
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.xcom import XCOM_RETURN_KEY

try:
    from airflow.sdk.definitions._internal.abstractoperator import Operator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.xcom_arg import MapXComArg, XComArg
    from airflow.sdk.execution_time.xcom import XCom
except ImportError:
    # TODO: Remove once provider drops support for Airflow 2
    from airflow.models import XCom
    from airflow.models.baseoperator import BaseOperator as Operator
    from airflow.utils.context import Context

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from airflow.triggers.base import BaseTrigger, run_trigger


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


class XComIterable(Iterator, Sequence):
    """An iterable that lazily fetches XCom values one by one instead of loading all at once."""

    def __init__(self, task_id: str, dag_id: str, run_id: str, length: int):
        self.task_id = task_id
        self.dag_id = dag_id
        self.run_id = run_id
        self.length = length
        self.index = 0

    def __iter__(self) -> Iterator:
        self.index = 0
        return self

    def __next__(self):
        if self.index >= self.length:
            raise StopIteration

        value = self[self.index]
        self.index += 1
        return value

    def __len__(self):
        return self.length

    def __getitem__(self, index: int):
        """Allows direct indexing, making this work like a sequence."""
        if not (0 <= index < self.length):
            raise IndexError

        return XCom.get_one(
            key=XCOM_RETURN_KEY,
            dag_id=self.dag_id,
            task_id=f"{self.task_id}_{index}",
            run_id=self.run_id,
        )

    def serialize(self):
        """Ensure the object is JSON serializable."""
        return {
            "task_id": self.task_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "length": self.length,
        }

    @classmethod
    def deserialize(cls, data: dict, version: int):
        """Ensure the object is JSON deserializable."""
        return XComIterable(**data)


class DeferredIterable(Iterator, LoggingMixin):
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

    def resolve(self, context: Context) -> DeferredIterable:
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

        try:
            with event_loop() as loop:
                event = loop.run_until_complete(run_trigger(self.trigger))
                next_method = getattr(self.operator, self.next_method)
                self.log.debug("Triggering next method: %s", self.next_method)
                results = next_method(self.context, event.payload)
        except Exception as e:
            self.log.exception(e)
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

        dag_bag = DagBag(dag_folder=None)  # Avoid loading all DAGs
        processed_dags = dag_bag.process_file(dag_fileloc)
        cls.logger().info("processed_dags: %s", processed_dags)
        cls.logger().info("dag_bag: %s", dag_bag)
        cls.logger().info("dags: %s", dag_bag.dags)
        return dag_bag.dags[dag_id].get_task(task_id)

    @classmethod
    def deserialize(cls, data: dict, version: int):
        """Ensure the object is JSON deserializable."""
        trigger_class = import_string(data["trigger"][0])
        trigger = trigger_class(**data["trigger"][1])
        operator = cls.get_operator_from_dag(data["dag_fileloc"], data["dag_id"], data["task_id"])
        return DeferredIterable(
            results=data["results"], trigger=trigger, operator=operator, next_method=data["next_method"]
        )


# This is a workaround to allow the DeferredIterable and XComIterable classes to be serialized
serde._extra_allowed = serde._extra_allowed.union(
    {
        f"{XComIterable.__module__}.{XComIterable.__class__.__name__}",
        f"{DeferredIterable.__module__}.{DeferredIterable.__class__.__name__}",
    }
)
