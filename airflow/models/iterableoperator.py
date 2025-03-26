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
import logging
import os
from abc import abstractmethod
from asyncio import AbstractEventLoop, Future, Semaphore, ensure_future, gather
from collections.abc import Generator, Iterable, Iterator, Sequence
from contextlib import contextmanager, suppress
from datetime import timedelta
from math import ceil
from multiprocessing import TimeoutError
from multiprocessing.pool import ApplyResult, ThreadPool
from queue import Queue
from threading import Thread
from time import sleep
from typing import TYPE_CHECKING, Any

from airflow.exceptions import (
    AirflowException,
    AirflowRescheduleTaskInstanceException,
    AirflowTaskTimeout,
    TaskDeferred,
)
from airflow.models import BaseOperator
from airflow.models.abstractoperator import DEFAULT_TASK_EXECUTION_TIMEOUT
from airflow.models.expandinput import (
    ExpandInput,
    _needs_run_time_resolution,
    is_mappable,
)
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCom
from airflow.sdk.definitions._internal.abstractoperator import Operator
from airflow.sdk.definitions.context import Context
from airflow.sdk.definitions.xcom_arg import XComArg, _MapResult
from airflow.serialization import serde
from airflow.triggers.base import run_trigger
from airflow.utils import timezone
from airflow.utils.context import context_get_outlet_events
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.operator_helpers import ExecutionCallableRunner
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_instance_session import get_current_task_instance_session
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    import jinja2
    from sqlalchemy.orm import Session

serde._extra_allowed = serde._extra_allowed.union({"infrabel.operators.iterableoperator.XComIterable"})


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


class TaskExecutor(LoggingMixin):
    """Base class to run an operator or trigger with given task context and task instance."""

    def __init__(
        self,
        context: Context,
        task_instance: TaskInstance,
    ):
        super().__init__()
        self.__context = dict(context.items())
        self._task_instance = task_instance
        self._is_async_mode: bool = False  # Flag to track sync/async mode

    @property
    def task_instance(self) -> TaskInstance:
        return self._task_instance

    @property
    def task_index(self) -> int:
        return int(self._task_instance.task_id.rsplit("_", 1)[-1])

    @property
    def context(self) -> Context:
        return {**self.__context, **{"ti": self.task_instance}}

    @property
    def operator(self) -> Operator:
        return self.task_instance.task

    @property
    def mode(self) -> str:
        return "async" if self._is_async_mode else "sync"

    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def __enter__(self):
        if self.log.isEnabledFor(logging.INFO):
            self.log.info(
                "Attempting running task %s of %s for %s with map_index %s in %s mode.",
                self.task_instance.try_number,
                self.operator.retries,
                self.task_instance.task_id,
                self.task_index,
                self.mode,
            )

        if self.task_instance.try_number == 0:
            self.operator.render_template_fields(context=self.context)
            self.operator.pre_execute(context=self.context)
            self.task_instance.set_state(TaskInstanceState.SCHEDULED)
            self.task_instance._run_execute_callback(context=self.context, task=self.operator)
        return self

    async def __aenter__(self):
        self._is_async_mode = True
        return self.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value:
            if isinstance(exc_value, AirflowException):
                if self.task_instance.next_try_number > self.operator.retries:
                    self.log.error(
                        "Max number of attempts for %s with map_index %s failed due to: %s",
                        self.task_instance.task_id,
                        self.task_index,
                        exc_value,
                    )
                    raise exc_value

                self.task_instance.try_number += 1
                self.task_instance.end_date = timezone.utcnow()
                self.task_instance.set_state(TaskInstanceState.UP_FOR_RESCHEDULE)
                raise AirflowRescheduleTaskInstanceException(task=self.task_instance)

            self.task_instance.set_state(TaskInstanceState.FAILED)
            raise exc_value
        self.operator.post_execute(context=self.context)
        self.task_instance.set_state(TaskInstanceState.SUCCESS)
        if self.log.isEnabledFor(logging.INFO):
            self.log.info(
                "Task instance %s for %s finished successfully in %s attempts in %s mode.",
                self.task_index,
                self.task_instance.task_id,
                self.task_instance.next_try_number,
                self.mode,
            )

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.__exit__(exc_type, exc_value, traceback)


class OperatorExecutor(TaskExecutor):
    """
    Run an operator with given task context and task instance.

    If the execute function raises a TaskDeferred exception, then the trigger will be executed in an
    async way using the TriggerExecutor.

    :meta private:
    """

    def run(self, *args, **kwargs):
        outlet_events = context_get_outlet_events(self.context)
        # TODO: change back to operator.execute once ExecutorSafeguard is fixed
        if hasattr(self.operator.execute, "__wrapped__"):
            return ExecutionCallableRunner(
                func=self.operator.execute.__wrapped__,
                outlet_events=outlet_events,
                logger=self.log,
            ).run(self.operator, self.context)
        return ExecutionCallableRunner(
            func=self.operator.execute,
            outlet_events=outlet_events,
            logger=self.log,
        ).run(self.context)


class TriggerExecutor(TaskExecutor):
    """
    Run a trigger with given task deferred exception.

    If the next method raises a TaskDeferred exception, then the trigger instance will be re-executed with
    the given TaskDeferred exception until no more TaskDeferred exceptions occur. The trigger will always
    be executed in an async way.

    :meta private:
    """

    async def run(self, task_deferred: TaskDeferred):
        event = await run_trigger(task_deferred.trigger)

        self.log.debug("event: %s", event)

        if event:
            self.log.debug("next_method: %s", task_deferred.method_name)

            if task_deferred.method_name:
                try:
                    next_method = self.operator.next_callable(task_deferred.method_name, task_deferred.kwargs)
                    outlet_events = context_get_outlet_events(self.context)
                    return ExecutionCallableRunner(
                        func=next_method,
                        outlet_events=outlet_events,
                        logger=self.log,
                    ).run(self.context, event.payload)
                except TaskDeferred as task_deferred:
                    return await self.run(task_deferred=task_deferred)


class IterableOperator(BaseOperator):
    """Object representing an iterable operator in a DAG."""

    _operator_class: type[BaseOperator]
    expand_input: ExpandInput
    partial_kwargs: dict[str, Any]
    # each operator should override this class attr for shallow copy attrs.
    shallow_copy_attrs: Sequence[str] = (
        "expand_input",
        "partial_kwargs",
        "_log",
        "_task_queue",
        "_producer_thread",
        # "_semaphore",
    )

    def __init__(
        self,
        *,
        operator_class: type[BaseOperator],
        expand_input: ExpandInput,
        partial_kwargs: dict[str, Any] | None = None,
        timeout: timedelta | None = DEFAULT_TASK_EXECUTION_TIMEOUT,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._operator_class = operator_class
        self.expand_input = expand_input
        self.partial_kwargs = partial_kwargs or {}
        self.timeout = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        self._mapped_kwargs: Iterable[dict] = []
        if not self.max_active_tis_per_dag:
            self.max_active_tis_per_dag = os.cpu_count() or 1
        self._task_queue = Queue()
        self._producer_thread: Thread | None = None
        self._number_of_tasks: int = 0
        XComArg.apply_upstream_relationship(self, self.expand_input.value)

    @property
    def operator_name(self) -> str:
        return self._operator_class.__name__

    def _get_specified_expand_input(self) -> ExpandInput:
        return self.expand_input

    def _unmap_operator(self, index: int, mapped_kwargs: dict):
        kwargs = {
            **self.partial_kwargs,
            **{"task_id": f"{self.task_id}_{index}"},
            **mapped_kwargs,
        }
        self._number_of_tasks += 1
        self.log.debug("index: %s", index)
        self.log.debug("kwargs: %s", kwargs)
        self.log.debug("operator_class: %s", self._operator_class)
        self.log.debug("number_of_tasks: %s", self._number_of_tasks)
        return self._operator_class(**kwargs, _airflow_from_mapped=True)

    def _resolve(self, value, context: Context, session: Session):
        if isinstance(value, dict):
            for key in value:
                item = value[key]
                if _needs_run_time_resolution(item):
                    item = item.resolve(context=context, session=session)

                    if is_mappable(item):
                        item = iter(item)  # type: ignore

                self.log.debug("resolved_value: %s", item)

                value[key] = item

        return value

    def _lazy_mapped_kwargs(self, input, context: Context, session: Session):
        self.log.debug("_lazy_mapped_kwargs value: %s", input)

        value = self._resolve(value=input, context=context, session=session)

        self.log.debug("resolved value: %s", value)

        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(item, (Sequence, Iterable)) or isinstance(item, str):
                    yield {key: item}
                else:
                    for sub_item in item:
                        yield {key: sub_item}

    def _resolve_expand_input(self, context: Context, session: Session):
        self.log.debug("resolve_expand_input: %s", self.expand_input)

        if isinstance(self.expand_input.value, XComArg):
            resolved_input = self.expand_input.value.resolve(context=context, session=session)
        else:
            resolved_input = self.expand_input.value

        self.log.debug("resolved_input: %s", resolved_input)

        if isinstance(resolved_input, _MapResult):
            self._mapped_kwargs = map(
                lambda value: self._resolve(value=value, context=context, session=session), resolved_input
            )
        else:
            self._mapped_kwargs = iter(
                self._lazy_mapped_kwargs(input=resolved_input, context=context, session=session)
            )

        self.log.debug("mapped_kwargs: %s", self._mapped_kwargs)

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        session = get_current_task_instance_session()
        self._resolve_expand_input(context=context, session=session)

    def _run_tasks(
        self,
        context: Context,
    ) -> Iterable[Any] | None:
        exception: Exception | None = None
        reschedule_date = timezone.utcnow()
        failed_tasks: list[TaskInstance] = []

        self.log.info("task_queue : %s", self._task_queue.qsize())

        # Task Consumer
        while not self._task_queue.empty() or self._producer_thread.is_alive():
            futures = {}
            deferred_tasks: list[Future] = []

            with ThreadPool(processes=self.max_active_tis_per_dag) as pool:
                while not self._task_queue.empty():
                    task = self._task_queue.get(timeout=1)
                    self.log.debug("received task : %s", task)

                    # Check if the task is an exception and stop immediately
                    if isinstance(task, Exception):
                        self._producer_thread.join()
                        raise AirflowException("An exception occurred in the producer thread") from task

                    futures[pool.apply_async(self._run_operator, (context, task))] = task

            self.log.debug("futures: %s", futures)

            with event_loop() as loop:
                semaphore = Semaphore(self.max_active_tis_per_dag, loop=loop)

                for future in filter(ApplyResult.ready, list(futures.keys())):
                    task = futures.pop(future)
                    try:
                        result = future.get(timeout=self.timeout)

                        self.log.debug("result: %s", result)

                        if isinstance(result, TaskDeferred):
                            deferred_tasks.append(
                                ensure_future(
                                    self._run_deferrable(
                                        semaphore=semaphore,
                                        context=context,
                                        task_instance=task,
                                        task_deferred=result,
                                    ),
                                    loop=loop,
                                ),
                            )
                    except TimeoutError as e:
                        self.log.warning("A timeout occurred for task_id %s", task.task_id)
                        if task.next_try_number > self.retries:
                            exception = AirflowTaskTimeout(e)
                        else:
                            reschedule_date = task.next_retry_datetime()
                            failed_tasks.append(task)
                    except AirflowRescheduleTaskInstanceException as e:
                        reschedule_date = e.reschedule_date
                        failed_tasks.append(e.task)
                    except AirflowException as e:
                        self.log.error("An exception occurred for task_id %s", task.task_id)
                        exception = e

                if deferred_tasks:
                    self.log.info("Running %s deferred tasks", len(deferred_tasks))

                    for result in loop.run_until_complete(gather(*deferred_tasks, return_exceptions=True)):
                        self.log.debug("result: %s", result)

                        if isinstance(result, Exception):
                            if isinstance(result, AirflowRescheduleTaskInstanceException):
                                reschedule_date = result.reschedule_date
                                failed_tasks.append(result.task)
                            else:
                                exception = result

        self._producer_thread.join()

        if not failed_tasks:
            if exception:
                raise exception

            self.log.info("Finished consuming tasks: %s", self._number_of_tasks)

            if self.do_xcom_push:
                return XComIterable(
                    task_id=self.task_id,
                    dag_id=self.dag_id,
                    run_id=context["run_id"],
                    length=self._number_of_tasks,
                )
            return None

        for failed_task in failed_tasks:
            self._task_queue.put(failed_task)

        # Calculate delay before the next retry
        delay = reschedule_date - timezone.utcnow()
        delay_seconds = ceil(delay.total_seconds())

        self.log.debug("delay_seconds: %s", delay_seconds)

        if delay_seconds > 0:
            self.log.info(
                "Attempting to run %s failed tasks within %s seconds...",
                len(failed_tasks),
                delay_seconds,
            )
            sleep(delay_seconds)

        return self._run_tasks(context)

    def _run_operator(self, context: Context, task_instance: TaskInstance):
        try:
            with OperatorExecutor(context=context, task_instance=task_instance) as executor:
                result = executor.run()
                if self.do_xcom_push:
                    task_instance.xcom_push(key=XCOM_RETURN_KEY, value=result)
                return result
        except TaskDeferred as task_deferred:
            return task_deferred

    async def _run_deferrable(
        self, semaphore: Semaphore, context: Context, task_instance: TaskInstance, task_deferred: TaskDeferred
    ):
        async with semaphore:
            async with TriggerExecutor(context=context, task_instance=task_instance) as executor:
                result = await executor.run(task_deferred)
                self.log.info("_run_deferrable: %s", result)
                if self.do_xcom_push:
                    task_instance.xcom_push(key=XCOM_RETURN_KEY, value=result)
                return result

    def _create_task(self, run_id: str, index: int, mapped_kwargs: dict) -> TaskInstance:
        operator = self._unmap_operator(index, mapped_kwargs)
        return TaskInstance(
            task=operator,
            run_id=run_id,
        )

    def execute(self, context: Context):
        def task_producer():
            tasks = map(
                lambda mapped_kwargs: self._create_task(
                    context["ti"].run_id, mapped_kwargs[0], mapped_kwargs[1]
                ),
                enumerate(self._mapped_kwargs),
            )

            try:
                self.log.info("Started producing tasks")
                for task in tasks:
                    self._task_queue.put(task)
                self.log.info("Finished producing tasks")
            except Exception as e:
                self.log.error("Exception in task_producer: %s", e)
                self._task_queue.put(e)

        self._producer_thread = Thread(target=task_producer)
        self._producer_thread.start()

        return self._run_tasks(context=context)
