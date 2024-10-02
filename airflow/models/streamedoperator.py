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
from asyncio import AbstractEventLoop, Semaphore, ensure_future, iscoroutinefunction
from contextlib import contextmanager
from math import ceil
from time import sleep
from typing import TYPE_CHECKING, Any, Callable, Generator, Sequence, cast

from airflow import XComArg
from airflow.exceptions import (
    AirflowException,
    AirflowRescheduleTaskInstanceException,
    TaskDeferred,
)
from airflow.models.baseoperator import BaseOperator
from airflow.models.expandinput import (
    ExpandInput,
    _needs_run_time_resolution,
)
from airflow.models.taskinstance import TaskInstance
from airflow.triggers.base import run_trigger
from airflow.utils import timezone
from airflow.utils.context import Context, context_get_outlet_events
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.operator_helpers import ExecutionCallableRunner
from airflow.utils.task_instance_session import get_current_task_instance_session

if TYPE_CHECKING:
    import jinja2
    from sqlalchemy.orm import Session


@contextmanager
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    new_event_loop = False
    loop = None
    try:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            new_event_loop = True
        yield loop
    finally:
        if new_event_loop and loop is not None:
            loop.close()


class OperatorExecutor(LoggingMixin):
    """
    Run an operator with given task context and task instance.

    If the execute function raises a TaskDeferred exception, then the trigger instance within the
    TaskDeferred exception will be executed with the given context and task instance. The operator
    or trigger will always be executed in an async way.

    :meta private:
    """

    def __init__(
        self,
        semaphore: Semaphore,
        operator: BaseOperator,
        context: Context,
        task_instance: TaskInstance,
    ):
        super().__init__()
        self._semaphore = semaphore
        self.operator = operator
        self.__context = context
        self._task_instance = task_instance

    @property
    def task_instance(self) -> TaskInstance:
        # TODO: If we want a specialized TaskInstance for the StreamedOperator,
        #       we could inherit from TaskInstanceDependencies
        return self._task_instance

    @property
    def context(self) -> Context:
        return {**self.__context, **{"ti": self.task_instance}}

    async def _run_callable(self, method: Callable, *args, **kwargs):
        self.log.debug("semaphore: %s (%s)", self._semaphore, self._semaphore.locked())
        async with self._semaphore:
            while self.task_instance.try_number <= self.operator.retries:
                if self.log.isEnabledFor(logging.INFO):
                    self.log.info(
                        "Attempting running task %s of %s for %s with map_index %s.",
                        self.task_instance.try_number,
                        self.operator.retries,
                        type(self.operator).__name__,
                        self.task_instance.map_index,
                    )

                try:
                    outlet_events = context_get_outlet_events(self.context)
                    callable_runner = ExecutionCallableRunner(
                        func=method, outlet_events=outlet_events, logger=self.log
                    )
                    if iscoroutinefunction(method):
                        return await callable_runner.run(*args, **kwargs)
                    return callable_runner.run(*args, **kwargs)
                except AirflowException as e:
                    if self.task_instance.next_try_number > self.operator.retries:
                        self.log.error(
                            "Max number of attempts for %s with map_index %s failed due to: %s",
                            type(self.operator).__name__,
                            self.task_instance.map_index,
                            e,
                        )
                        raise e

                    self.log.error("An error occurred: %s", e)
                    self.task_instance.try_number += 1
                    self.task_instance.end_date = timezone.utcnow()

                    raise AirflowRescheduleTaskInstanceException(task_instance=self.task_instance)

    async def _run_deferrable(self, context: Context, task_deferred: TaskDeferred):
        event = await run_trigger(task_deferred.trigger)

        self.log.debug("event: %s", event)

        if event:
            self.log.debug("next_method: %s", task_deferred.method_name)

            if task_deferred.method_name:
                next_method = BaseOperator.next_callable(
                    self.operator, task_deferred.method_name, task_deferred.kwargs
                )
                result = next_method(context, event.payload)
                self.log.debug("result: %s", result)
                return result

    async def run(self, method: Callable, *args, **kwargs):
        self.operator.pre_execute(context=self.context)
        self.task_instance._run_execute_callback(context=self.context, task=self.operator)

        try:
            return await self._run_callable(method, *(list(args or ()) + [self.context]), **kwargs)
        except TaskDeferred as task_deferred:
            return await self._run_callable(self._run_deferrable, *[self.context, task_deferred])
        finally:
            self.operator.post_execute(context=self.context)


class StreamedOperator(BaseOperator):
    """Object representing a streamed operator in a DAG."""

    _operator_class: type[BaseOperator]
    _expand_input: ExpandInput
    _partial_kwargs: dict[str, Any]

    def __init__(
        self,
        *,
        operator_class: type[BaseOperator],
        expand_input: ExpandInput,
        partial_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._operator_class = operator_class
        self._expand_input = expand_input
        self._partial_kwargs = partial_kwargs or {}
        self._mapped_kwargs: list[dict] = []
        self._semaphore = Semaphore(self.max_active_tis_per_dag or os.cpu_count() or 1)
        XComArg.apply_upstream_relationship(self, self._expand_input.value)

    @property
    def operator_name(self) -> str:
        return self._operator_class.__name__

    def _unmap_operator(self, index):
        self.log.debug("index: %s", index)
        kwargs = {
            **self._partial_kwargs,
            **{"task_id": f"{self._partial_kwargs.get('task_id')}_{index}"},
            **self._mapped_kwargs[index],
        }
        self.log.debug("kwargs: %s", kwargs)
        self.log.debug("operator_class: %s", self._operator_class)
        return self._operator_class(**kwargs)

    def _resolve_expand_input(self, context: Context, session: Session):
        if isinstance(self._expand_input.value, dict):
            for key, value in self._expand_input.value.items():
                if _needs_run_time_resolution(value):
                    value = value.resolve(context=context, session=session)

                    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                        value = list(value)

                    self.log.debug("resolved_value: %s", value)

                if isinstance(value, list):
                    self._mapped_kwargs.extend([{key: item} for item in value])
                else:
                    self._mapped_kwargs.append({key: value})
            self.log.debug("resolve_expand_input: %s", self._mapped_kwargs)

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        session = get_current_task_instance_session()
        self._resolve_expand_input(context=context, session=session)

    def _run_futures(self, context: Context, futures, results: list[Any] | None = None) -> list[Any]:
        reschedule_date = timezone.utcnow()
        results = results or []
        failed_futures = []

        with event_loop() as loop:
            for result in loop.run_until_complete(asyncio.gather(*futures, return_exceptions=True)):
                if isinstance(result, Exception):
                    if not isinstance(result, AirflowRescheduleTaskInstanceException):
                        raise result
                    reschedule_date = result.reschedule_date
                    failed_futures.append(ensure_future(self._run_task(context, result.task_instance)))
                else:
                    results.append(result)

            if not failed_futures:
                return list(filter(None, results))

        # session = get_current_task_instance_session()
        # TaskInstance._set_state(context["ti"], TaskInstanceState.UP_FOR_RETRY, session)

        # Calculate delay before the next retry
        delay = reschedule_date - timezone.utcnow()
        delay_seconds = ceil(delay.total_seconds())

        self.log.debug("delay_seconds: %s", delay_seconds)

        if delay_seconds > 0:
            self.log.info(
                "Attempting to run %s failed tasks within %s seconds...",
                len(failed_futures),
                delay_seconds,
            )

            sleep(delay_seconds)

        # TaskInstance._set_state(context["ti"], TaskInstanceState.RUNNING, session)

        return self._run_futures(context, failed_futures, results)

    async def _run_task(self, context: Context, task_instance: TaskInstance):
        operator: BaseOperator = cast(BaseOperator, task_instance.task)
        self.log.debug("operator: %s", operator)
        result = await OperatorExecutor(
            semaphore=self._semaphore,
            operator=operator,
            context=context,
            task_instance=task_instance,
        ).run(operator.execute)
        self.log.debug("result: %s", result)
        self.log.debug("do_xcom_push: %s", operator.do_xcom_push)
        if operator.do_xcom_push:
            return result

    def _create_future(self, context: Context, index: int):
        operator = self._unmap_operator(index)
        operator.render_template_fields(context=context)
        task_instance = TaskInstance(
            task=operator,
            run_id=context["ti"].run_id,
            state=context["ti"].state,
            map_index=index,
        )
        return asyncio.ensure_future(self._run_task(context, task_instance))

    def execute(self, context: Context):
        self.log.info(
            "Executing %s mapped tasks on %s with %s workers",
            len(self._mapped_kwargs),
            self._operator_class.__name__,
            self.max_active_tis_per_dag,
        )

        return self._run_futures(
            context=context,
            futures=[
                self._create_future(context, index) for index, mapped_kwargs in enumerate(self._mapped_kwargs)
            ],
        )
