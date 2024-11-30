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
from multiprocessing import TimeoutError
from asyncio import iscoroutinefunction, wait_for, ensure_future
from datetime import timedelta
from math import ceil
from multiprocessing.pool import ThreadPool
from time import sleep
from typing import TYPE_CHECKING, Any, Callable, Sequence, cast, Iterable

from airflow import XComArg
from airflow.exceptions import (
    AirflowException,
    AirflowTaskTimeout,
    TaskDeferred, AirflowRescheduleException,
)
from airflow.models.abstractoperator import DEFAULT_TASK_EXECUTION_TIMEOUT
from airflow.models.baseoperator import BaseOperator
from airflow.models.expandinput import (
    ExpandInput,
    _needs_run_time_resolution,
    is_mappable, OperatorExpandArgument, DictOfListsExpandInput,
)
from airflow.models.mappedoperator import validate_mapping_kwargs, ensure_xcomarg_return_value
from airflow.models.taskinstance import TaskInstance
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone
from airflow.utils.context import Context, context_get_outlet_events
from airflow.utils.helpers import prevent_duplicates
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.operator_helpers import ExecutionCallableRunner
from airflow.utils.task_instance_session import get_current_task_instance_session

if TYPE_CHECKING:
    import jinja2
    from sqlalchemy.orm import Session


# TODO: Check _run_inline_trigger method from DAG, could be refactored so it uses this method
async def run_trigger(trigger: BaseTrigger) -> TriggerEvent | None:
    async for event in trigger.run():
        return event


class AirflowRescheduleTaskInstanceException(AirflowRescheduleException):
    """
    Raise when the task should be re-scheduled for a specific TaskInstance at a later time.

    :param task_instance: The task instance that should be rescheduled
    """

    def __init__(self, task: TaskInstance):
        super().__init__(reschedule_date=task.next_retry_datetime())
        self.task = task


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
        operator: BaseOperator,
        context: Context,
        task_instance: TaskInstance,
        timeout,
    ):
        super().__init__()
        self.operator = operator
        self.__context = context
        self._task_instance = task_instance
        self.timeout = timeout

    @property
    def task_instance(self) -> TaskInstance:
        # TODO: If we want a specialized TaskInstance for the StreamedOperator,
        #       we could inherit from TaskInstanceDependencies
        return self._task_instance

    @property
    def context(self) -> Context:
        return {**self.__context, **{"ti": self.task_instance}}

    async def _run_callable(self, method: Callable, *args, **kwargs):
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

            self.task_instance.try_number += 1
            self.task_instance.end_date = timezone.utcnow()

            raise AirflowRescheduleTaskInstanceException(
                task=self.task_instance
            )

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

        try:
            return await wait_for(
                self._run_callable(
                    method, *(list(args or ()) + [self.context]), **kwargs
                ),
                timeout=self.timeout,
            )
        except TaskDeferred as task_deferred:
            return await wait_for(
                self._run_callable(
                    self._run_deferrable, *[self.context, task_deferred]
                ),
                timeout=self.timeout,
            )
        finally:
            self.operator.post_execute(context=self.context)

            if self.log.isEnabledFor(logging.INFO):
                self.log.info(
                    "Task instance %s for %s finished successfully in %s attempts.",
                    self.task_instance.map_index,
                    type(self.operator).__name__,
                    self.task_instance.next_try_number,
                )


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

    def _run_task(self, context: Context, task: TaskInstance):
        # Always open new event loop as this is executed in multithreaded
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(ensure_future(self._run_operator(context, task), loop=loop))
        finally:
            loop.close()

    def _run_tasks(
        self,
        context: Context,
        tasks: Iterable[TaskInstance],
        results: list[Any] | None = None,
    ) -> list[Any]:
        exception: BaseException | None = None
        results = results or []
        reschedule_date = timezone.utcnow()
        failed_tasks: list[TaskInstance] = []

        with ThreadPool(processes=self.max_active_tis_per_dag) as pool:
            futures = [
                (task, pool.apply_async(self._run_task, (context, task)))
                for task in tasks
            ]

            for task, future in futures:
                try:
                    result = future.get(timeout=self.timeout)
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

        if not failed_tasks:
            if exception:
                raise exception
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
                len(failed_tasks),
                delay_seconds,
            )

            sleep(delay_seconds)

        # TaskInstance._set_state(context["ti"], TaskInstanceState.RUNNING, session)

        return self._run_tasks(context, failed_tasks, results)

    async def _run_operator(self, context: Context, task_instance: TaskInstance):
        operator: BaseOperator = cast(BaseOperator, task_instance.task)
        self.log.debug("operator: %s", operator)
        result = await OperatorExecutor(
            operator=operator,
            context=context,
            task_instance=task_instance,
            timeout=self.timeout,
        ).run(
            lambda _context: operator.execute.__wrapped__(
                self=operator, context=_context
            )
        )  # TODO: change back to operator.execute once ExecutorSafeguard is fixed
        self.log.debug("result: %s", result)
        self.log.debug("do_xcom_push: %s", operator.do_xcom_push)
        if operator.do_xcom_push:
            return result

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


def stream(self, **mapped_kwargs: OperatorExpandArgument) -> StreamedOperator:
    if not mapped_kwargs:
        raise TypeError("no arguments to expand against")
    validate_mapping_kwargs(self.operator_class, "stream", mapped_kwargs)
    prevent_duplicates(
        self.kwargs, mapped_kwargs, fail_reason="unmappable or already specified"
    )

    expand_input = DictOfListsExpandInput(mapped_kwargs)
    ensure_xcomarg_return_value(expand_input.value)

    kwargs = {}

    for parameter_name in BaseOperator._comps:
        parameter_value = self.kwargs.get(parameter_name)
        if parameter_value:
            kwargs[parameter_name] = parameter_value

    # We don't retry the whole stream operator, we retry the individual tasks
    kwargs["retries"] = 0
    # We don't want to time out the whole stream operator, we only time out the individual tasks
    kwargs["timeout"] = kwargs.pop("execution_timeout", None)

    return StreamedOperator(
        **kwargs,
        operator_class=self.operator_class,
        expand_input=expand_input,
        partial_kwargs=self.kwargs,
    )
