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

import logging
import os
from abc import abstractmethod
from asyncio import Semaphore, gather
from collections.abc import Coroutine, Iterable, Iterator, Sequence
from contextlib import suppress
from datetime import timedelta
from json import JSONDecodeError
from math import ceil
from multiprocessing import TimeoutError
from multiprocessing.pool import ApplyResult, ThreadPool
from time import sleep
from typing import TYPE_CHECKING, Any

from more_itertools import ichunked

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
from airflow.models.iterable import XComIterable, event_loop
from airflow.models.taskinstance import TaskInstance
from airflow.sdk.definitions._internal.abstractoperator import Operator
from airflow.sdk.definitions.context import Context
from airflow.sdk.definitions.xcom_arg import XComArg, _MapResult
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
        self._result: Any | None = None

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
    def execute(self, *args, **kwargs):
        raise NotImplementedError

    def run(self, *args, **kwargs):
        self._result = self.execute(*args, **kwargs)
        return self._result

    async def run_deferred(self, *args, **kwargs):
        self._result = await self.execute(*args, **kwargs)
        return self._result

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
            self.task_instance._run_execute_callback(
                context=self.context, task=self.operator
            )
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
        if self.operator.do_xcom_push:
            self.task_instance.xcom_push(key=XCOM_RETURN_KEY, value=self._result)
        self.operator.post_execute(context=self.context, result=self._result)
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

    def execute(self, *args, **kwargs):
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

    async def execute(self, task_deferred: TaskDeferred):
        event = await run_trigger(task_deferred.trigger)

        self.log.debug("event: %s", event)

        if event:
            self.log.debug("next_method: %s", task_deferred.method_name)

            if task_deferred.method_name:
                try:
                    next_method = self.operator.next_callable(
                        task_deferred.method_name, task_deferred.kwargs
                    )
                    outlet_events = context_get_outlet_events(self.context)
                    return ExecutionCallableRunner(
                        func=next_method,
                        outlet_events=outlet_events,
                        logger=self.log,
                    ).run(self.context, event.payload)
                except TaskDeferred as task_deferred:
                    return await self.execute(task_deferred=task_deferred)


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
        self._mapped_kwargs: Iterable[dict] = []
        if not self.max_active_tis_per_dag:
            self.max_active_tis_per_dag = os.cpu_count() or 1
        self._semaphore = Semaphore(self.max_active_tis_per_dag)
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
            resolved_input = self.expand_input.value.resolve(
                context=context, session=session
            )
        else:
            resolved_input = self.expand_input.value

        self.log.debug("resolved_input: %s", resolved_input)

        if isinstance(resolved_input, _MapResult):
            self._mapped_kwargs = map(
                lambda value: self._resolve(
                    value=value, context=context, session=session
                ),
                resolved_input,
            )
        else:
            self._mapped_kwargs = iter(
                self._lazy_mapped_kwargs(
                    input=resolved_input, context=context, session=session
                )
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
        tasks: Iterator[TaskInstance],
    ) -> Iterable[Any] | None:
        exception: BaseException | None = None
        reschedule_date = timezone.utcnow()
        prev_futures_count = 0
        futures: dict[ApplyResult, TaskInstance] = {}
        failed_tasks: list[TaskInstance] = []
        chunked_tasks: Iterator[Iterable[TaskInstance]] = ichunked(
            tasks, (self.max_active_tis_per_dag * 2)
        )

        with ThreadPool(processes=self.max_active_tis_per_dag) as pool:
            for task in next(chunked_tasks, []):
                future = pool.apply_async(self._run_operator, (context, task))
                futures[future] = task

            while futures:
                futures_count = len(futures)

                if futures_count != prev_futures_count:
                    self.log.info("Number of remaining futures: %s", futures_count)
                    prev_futures_count = futures_count

                deferred_tasks: list[Coroutine[Any, Any, Any]] = []
                ready_futures = [future for future in futures.keys() if future.ready()]

                for future in ready_futures:
                    task = futures.pop(future)

                    try:
                        result = future.get(timeout=self.timeout)

                        self.log.debug("result: %s", result)

                        if isinstance(result, TaskDeferred):
                            deferred_tasks.append(
                                self._run_deferrable(
                                    context=context,
                                    task_instance=task,
                                    task_deferred=result,
                                )
                            )
                    except TimeoutError as e:
                        self.log.warning(
                            "A timeout occurred for task_id %s", task.task_id
                        )
                        if task.next_try_number > self.retries:
                            exception = AirflowTaskTimeout(e)
                        else:
                            reschedule_date = min(
                                reschedule_date, task.next_retry_datetime()
                            )
                            failed_tasks.append(task)
                    except AirflowRescheduleTaskInstanceException as e:
                        reschedule_date = min(reschedule_date, e.reschedule_date)
                        failed_tasks.append(e.task)
                    except AirflowException as e:
                        self.log.error(
                            "An exception occurred for task_id %s", task.task_id
                        )
                        exception = e

                for task in next(chunked_tasks, []):
                    future = pool.apply_async(self._run_operator, (context, task))
                    futures[future] = task

                if deferred_tasks:
                    self.log.info("Running %s deferred tasks", len(deferred_tasks))

                    with event_loop() as loop:
                        for result in loop.run_until_complete(
                            gather(
                                *[loop.create_task(task) for task in deferred_tasks],
                                return_exceptions=True,
                            )
                        ):
                            self.log.debug("result: %s", result)

                            if isinstance(result, Exception):
                                if isinstance(
                                    result, AirflowRescheduleTaskInstanceException
                                ):
                                    reschedule_date = min(
                                        reschedule_date, result.reschedule_date
                                    )
                                    failed_tasks.append(result.task)
                                else:
                                    exception = result
                elif not ready_futures and futures:
                    sleep(len(futures) * 0.1)

        if not failed_tasks:
            if exception:
                raise exception
            if self.do_xcom_push:
                return XComIterable(
                    task_id=self.task_id,
                    dag_id=self.dag_id,
                    run_id=context["run_id"],
                    length=self._number_of_tasks,
                )
            return None

        # Calculate delay before the next retry
        if reschedule_date > timezone.utcnow():
            delay_seconds = ceil((reschedule_date - timezone.utcnow()).total_seconds())

            self.log.info(
                "Attempting to run %s failed tasks within %s seconds...",
                len(failed_tasks),
                delay_seconds,
            )

            sleep(delay_seconds)

        return self._run_tasks(context, iter(failed_tasks))

    @classmethod
    def _xcom_pull(cls, task_instance: TaskInstance):
        with suppress(JSONDecodeError):
            return task_instance.xcom_pull(
                task_ids=task_instance.task_id, dag_id=task_instance.dag_id
            )
        return None

    def _run_operator(self, context: Context, task_instance: TaskInstance):
        try:
            result = self._xcom_pull(task_instance)

            self.log.debug("result: %s", result)

            if result is None:
                with OperatorExecutor(
                    context=context, task_instance=task_instance
                ) as executor:
                    return executor.run()
            else:
                self.log.info(
                    "Task instance %s already completed.", task_instance.task_id
                )
            return result
        except TaskDeferred as task_deferred:
            return task_deferred

    async def _run_deferrable(
        self, context: Context, task_instance: TaskInstance, task_deferred: TaskDeferred
    ):
        async with self._semaphore:
            async with TriggerExecutor(
                context=context, task_instance=task_instance
            ) as executor:
                return await executor.run_deferred(task_deferred)

    def _create_task(
        self, run_id: str, index: int, mapped_kwargs: dict
    ) -> TaskInstance:
        operator = self._unmap_operator(index, mapped_kwargs)
        return TaskInstance(
            task=operator,
            run_id=run_id,
        )

    def execute(self, context: Context):
        return self._run_tasks(
            context=context,
            tasks=iter(
                map(
                    lambda mapped_kwargs: self._create_task(
                        context["ti"].run_id, mapped_kwargs[0], mapped_kwargs[1]
                    ),
                    enumerate(self._mapped_kwargs),
                )
            ),
        )
