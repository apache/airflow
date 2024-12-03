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
from contextlib import contextmanager, suppress
from datetime import timedelta
from math import ceil
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool
from time import sleep
from typing import TYPE_CHECKING, Any, Generator, Iterable, Sequence

from airflow import XComArg
from airflow.exceptions import (
    AirflowException,
    AirflowRescheduleTaskInstanceException,
    AirflowTaskTimeout,
    TaskDeferred,
)
from airflow.models.abstractoperator import DEFAULT_TASK_EXECUTION_TIMEOUT
from airflow.models.baseoperator import BaseOperator
from airflow.models.expandinput import (
    ExpandInput,
    is_mappable,
    _needs_run_time_resolution,
)
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.context import Context, context_get_outlet_events
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.operator_helpers import ExecutionCallableRunner
from airflow.utils.task_instance_session import get_current_task_instance_session

from airflow.triggers.base import run_trigger

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
            with suppress(AttributeError):
                loop.close()


class TaskExecutor(LoggingMixin):
    def __init__(
        self,
        context: Context,
        task_instance: TaskInstance,
    ):
        super().__init__()
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

    @property
    def operator(self) -> BaseOperator:
        return self.task_instance.task

    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def __enter__(self):
        if self.log.isEnabledFor(logging.INFO):
            self.log.info(
                "Attempting running task %s of %s for %s with map_index %s.",
                self.task_instance.try_number,
                self.operator.retries,
                type(self.operator).__name__,
                self.task_instance.map_index,
            )

        if self.task_instance.try_number == 0:
            self.operator.render_template_fields(context=self.context)
            self.operator.pre_execute(context=self.context)
            self.task_instance._run_execute_callback(
                context=self.context, task=self.operator
            )

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value:
            if isinstance(exc_value, AirflowException):
                if self.task_instance.next_try_number > self.operator.retries:
                    self.log.error(
                        "Max number of attempts for %s with map_index %s failed due to: %s",
                        type(self.operator).__name__,
                        self.task_instance.map_index,
                        exc_value,
                    )
                    raise exc_value

                self.task_instance.try_number += 1
                self.task_instance.end_date = timezone.utcnow()

                raise AirflowRescheduleTaskInstanceException(task=self.task_instance)
            raise exc_value

        self.operator.post_execute(context=self.context)
        if self.log.isEnabledFor(logging.INFO):
            self.log.info(
                "Task instance %s for %s finished successfully in %s attempts.",
                self.task_instance.map_index,
                type(self.operator).__name__,
                self.task_instance.next_try_number,
            )


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
        return ExecutionCallableRunner(
            func=self.operator.execute.__wrapped__,
            outlet_events=outlet_events,
            logger=self.log,
        ).run(self.operator, self.context)


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
                    next_method = BaseOperator.next_callable(
                        self.operator, task_deferred.method_name, task_deferred.kwargs
                    )
                    outlet_events = context_get_outlet_events(self.context)
                    return ExecutionCallableRunner(
                        func=next_method,
                        outlet_events=outlet_events,
                        logger=self.log,
                    ).run(self.context, event.payload)
                except TaskDeferred as task_deferred:
                    return await self.run(task_deferred=task_deferred)


class StreamedOperator(BaseOperator):
    """Object representing a streamed operator in a DAG."""

    _operator_class: type[BaseOperator]
    expand_input: ExpandInput
    partial_kwargs: dict[str, Any]
    # each operator should override this class attr for shallow copy attrs.
    shallow_copy_attrs: Sequence[str] = (
        "expand_input",
        "partial_kwargs",
        "_log",
        "_semaphore",
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
        self.timeout = (
            timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        )
        self._mapped_kwargs: list[dict] = []
        if not self.max_active_tis_per_dag:
            self.max_active_tis_per_dag = os.cpu_count() or 1
        self._semaphore = Semaphore(self.max_active_tis_per_dag)
        XComArg.apply_upstream_relationship(self, self.expand_input.value)

    @property
    def operator_name(self) -> str:
        return self._operator_class.__name__

    def _get_specified_expand_input(self) -> ExpandInput:
        return self.expand_input

    def _unmap_operator(self, index):
        self.log.debug("index: %s", index)
        kwargs = {
            **self.partial_kwargs,
            **{"task_id": f"{self.task_id}_{index}"},
            **self._mapped_kwargs[index],
        }
        self.log.debug("kwargs: %s", kwargs)
        self.log.debug("operator_class: %s", self._operator_class)
        return self._operator_class(**kwargs)

    def _resolve_expand_input(self, context: Context, session: Session):
        if isinstance(self.expand_input.value, dict):
            for key, value in self.expand_input.value.items():
                if _needs_run_time_resolution(value):
                    value = value.resolve(context=context, session=session)

                    if is_mappable(value):
                        value = list(value)  # type: ignore

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

    def _run_tasks(
        self,
        context: Context,
        tasks: Iterable[TaskInstance],
        results: list[Any] | None = None,
    ) -> list[Any]:
        exception: BaseException | None = None
        results = results or []
        reschedule_date = timezone.utcnow()
        deferred_tasks: list[Future] = []
        failed_tasks: list[TaskInstance] = []

        with ThreadPool(processes=self.max_active_tis_per_dag) as pool:
            futures = [
                (task, pool.apply_async(self._run_operator, (context, task)))
                for task in tasks
            ]

            for task, future in futures:
                try:
                    result = future.get(timeout=self.timeout)
                    if isinstance(result, TaskDeferred):
                        deferred_tasks.append(
                            ensure_future(
                                self._run_deferrable(
                                    context=context,
                                    task=task,
                                    task_deferred=result,
                                )
                            )
                        )
                    elif result:
                        results.append(result)
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

            with event_loop() as loop:
                for result in loop.run_until_complete(
                    gather(*deferred_tasks, return_exceptions=True)
                ):
                    self.log.debug("result: %s", result)

                    if isinstance(result, Exception):
                        if isinstance(result, AirflowRescheduleTaskInstanceException):
                            reschedule_date = result.reschedule_date
                            failed_tasks.append(result.task)
                        else:
                            exception = result
                    elif result:
                        results.append(result)

            deferred_tasks.clear()

        if not failed_tasks:
            if exception:
                raise exception
            return results

        # session = get_current_task_instance_session()
        # TaskInstance._set_state(context["ti"], TaskInstanceState.UP_FOR_RETRY, session)

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

        # TaskInstance._set_state(context["ti"], TaskInstanceState.RUNNING, session)

        return self._run_tasks(context, failed_tasks, results)

    @classmethod
    def _run_operator(cls, context: Context, task_instance: TaskInstance):
        with OperatorExecutor(context=context, task_instance=task_instance) as executor:
            try:
                return executor.run()
            except TaskDeferred as task_deferred:
                return task_deferred

    async def _run_deferrable(
        self, context: Context, task: TaskInstance, task_deferred: TaskDeferred
    ):
        with TriggerExecutor(context=context, task_instance=task) as executor:
            return await executor.run(task_deferred)

    def _create_task(self, context: Context, index: int) -> TaskInstance:
        operator = self._unmap_operator(index)
        task_instance = TaskInstance(
            task=operator,
            run_id=context["ti"].run_id,
            state=context["ti"].state,
            map_index=index,
        )
        return task_instance

    def execute(self, context: Context):
        self.log.info(
            "Executing %s mapped tasks on %s with %s threads and timeout %s",
            len(self._mapped_kwargs),
            self._operator_class.__name__,
            self.max_active_tis_per_dag,
            self.timeout,
        )

        results = self._run_tasks(
            context=context,
            tasks=map(
                lambda index: self._create_task(context, index[0]),
                enumerate(self._mapped_kwargs),
            ),
        )

        if self.do_xcom_push:
            return results
        return None
