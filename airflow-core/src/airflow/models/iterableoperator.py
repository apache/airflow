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
from collections.abc import Coroutine, Iterable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import timedelta
from math import ceil
from multiprocessing import TimeoutError
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
)
from airflow.models.iterable import XComIterable, event_loop
from airflow.models.taskinstance import TaskInstance
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions._internal.mixins import ResolveMixin
from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
from airflow.sdk.definitions.context import Context
from airflow.sdk.definitions.xcom_arg import XComArg
from airflow.sdk.execution_time.callback_runner import (
    create_executable_runner,
)
from airflow.sdk.execution_time.context import context_get_outlet_events
from airflow.triggers.base import run_trigger
from airflow.utils import timezone
from airflow.utils.context import context_get_outlet_events
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    import jinja2


class VolatileTaskInstance(TaskInstance):
    """Volatile task instance to run an operator which handles XCom's in memory."""

    _xcoms: dict[str, Any] = {}

    def xcom_pull(
        self,
        task_ids: str | Iterable[str] | None = None,
        dag_id: str | None = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,  # TODO: Add support for this
        *,
        map_indexes: int | Iterable[int] | None | ArgNotSet = NOTSET,
        default: Any = None,
        run_id: str | None = None,
    ) -> Any:
        key = f"{self.task_id}_{self.dag_id}_{key}"
        if map_indexes is not None and (not isinstance(map_indexes, int) or map_indexes >= 0):
            key += f"_{map_indexes}"
        return self._xcoms.get(key, default)

    def xcom_push(self, key: str, value: Any):
        key = f"{self.task_id}_{self.dag_id}_{key}"
        if self.map_index is not None and self.map_index >= 0:
            key += f"_{self.map_index}"
        self._xcoms[key] = value

    @property
    def next_try_number(self) -> int:
        return self.try_number + 1

    @property
    def xcom_key(self) -> str:
        return f"{self.task_id}_{self.map_index}"


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
    def dag_id(self) -> str:
        return self._task_instance.dag_id

    @property
    def task_id(self) -> str:
        return self._task_instance.task_id

    @property
    def task_index(self) -> int:
        # return int(self._task_instance.task_id.rsplit("_", 1)[-1])
        return self._task_instance.map_index

    @property
    def key(self):
        return self.task_instance.xcom_key

    @property
    def context(self) -> Context:
        return {
            **self.__context,
            **{"ti": self.task_instance, "task_instance": self.task_instance},
        }

    @property
    def operator(self) -> BaseOperator:
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
                    if self.task_instance.task.on_failure_callback:
                        self.task_instance.task.on_failure_callback(
                            {**self.context, **{"exception": exc_value}}
                        )
                    self.task_instance.state = TaskInstanceState.FAILED
                    raise exc_value

                self.task_instance.try_number += 1
                self.task_instance.end_date = timezone.utcnow()
                if self.task_instance.task.on_retry_callback:
                    self.task_instance.task.on_retry_callback({**self.context, **{"exception": exc_value}})
                self.task_instance.state = TaskInstanceState.UP_FOR_RESCHEDULE
                raise AirflowRescheduleTaskInstanceException(task=self.task_instance)

            raise exc_value

        self.task_instance.state = TaskInstanceState.SUCCESS
        if self.task_instance.task.on_success_callback:
            self.task_instance.task.on_success_callback(self.context)
        self.operator.post_execute(context=self.context, result=self._result)
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
            return create_executable_runner(
                func=self.operator.execute.__wrapped__,
                outlet_events=outlet_events,
                logger=self.log,
            ).run(self.operator, self.context)
        return create_executable_runner(
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
                    next_method = self.operator.next_callable(task_deferred.method_name, task_deferred.kwargs)
                    outlet_events = context_get_outlet_events(self.context)
                    return create_executable_runner(
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
        self.timeout = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        self._mapped_kwargs: Iterable[dict] = []
        if not self.max_active_tis_per_dag:
            self.max_active_tis_per_dag = os.cpu_count() or 1
        self._semaphore = Semaphore(self.max_active_tis_per_dag)
        self._number_of_tasks: int = 0
        XComArg.apply_upstream_relationship(self, self.expand_input.value)

    @property
    def operator_name(self) -> str:
        return self._operator_class.__name__

    @property
    def task_type(self) -> str:
        return self._operator_class.__name__

    @property
    def chunk_size(self) -> int:
        return self.max_active_tis_per_dag * 2

    def _get_specified_expand_input(self) -> ExpandInput:
        return self.expand_input

    def _unmap_operator(self, mapped_kwargs: dict):
        kwargs = {
            **self.partial_kwargs,
            **{"task_id": self.task_id},
            **mapped_kwargs,
        }
        self._number_of_tasks += 1
        self.log.debug("kwargs: %s", kwargs)
        self.log.debug("operator_class: %s", self._operator_class)
        self.log.debug("number_of_tasks: %s", self._number_of_tasks)
        return self._operator_class(**kwargs, _airflow_from_mapped=True)

    def _resolve(self, value, context: Context):
        if isinstance(value, dict):
            for key in value:
                item = value[key]
                if isinstance(item, ResolveMixin):
                    item = item.resolve(context=context)

                self.log.debug("resolved_value: %s", item)

                value[key] = item

        return value

    def _lazy_mapped_kwargs(self, value, context: Context) -> Iterable[dict]:
        self.log.debug("_lazy_mapped_kwargs resolved_value: %s", value)

        resolved_value = self._resolve(value=value, context=context)

        self.log.debug("resolved resolved_value: %s", resolved_value)

        if isinstance(resolved_value, dict):
            for key, item in resolved_value.items():
                if not isinstance(item, (Sequence, Iterable)) or isinstance(item, str):
                    yield {key: item}
                else:
                    for sub_item in item:
                        yield {key: sub_item}

    def _resolve_expand_input(self, context: Context):
        self.log.debug("resolve_expand_input: %s", self.expand_input)

        # TODO: check how to use the correct ResolvMixin type
        if isinstance(self.expand_input.value, ResolveMixin):
            resolved_input = self.expand_input.value.resolve(context=context)
        else:
            resolved_input = self.expand_input.value

        self.log.debug("resolved_input: %s", resolved_input)

        # Once _MapResult inherits from _MappableResult in Airflow, check only for _MappableResult
        if type(resolved_input).__name__ in {
            "_FilterResult",
            "_MapResult",
            "_LazyMapResult",
        }:
            self._mapped_kwargs = map(
                lambda value: self._resolve(value=value, context=context),
                resolved_input,
            )
        else:
            self._mapped_kwargs = iter(self._lazy_mapped_kwargs(value=resolved_input, context=context))

        self.log.debug("mapped_kwargs: %s", self._mapped_kwargs)

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        self._resolve_expand_input(context=context)

    def _xcom_push(self, context: Context, task: TaskInstance, value: Any) -> None:
        self.log.info("Pushing XCom %s", task.map_index)

        context["ti"].xcom_push(key=task.xcom_key, value=value)

    def _run_tasks(
        self,
        context: Context,
        tasks: Iterable[TaskInstance],
    ) -> None:
        exception: BaseException | None = None
        reschedule_date = timezone.utcnow()
        prev_futures_count = 0
        futures: dict[Future, TaskInstance] = {}

        failed_tasks: list[TaskInstance] = []
        chunked_tasks = ichunked(tasks, self.chunk_size)

        with ThreadPoolExecutor(max_workers=self.max_active_tis_per_dag) as pool:
            for task in next(chunked_tasks, []):
                future = pool.submit(self._run_operator, context, task)
                futures[future] = task

            while futures:
                futures_count = len(futures)

                if futures_count != prev_futures_count:
                    self.log.info("Number of remaining futures: %s", futures_count)
                    prev_futures_count = futures_count

                deferred_tasks: dict[Coroutine[Any, Any, Any], TaskInstance] = {}
                ready_futures = False

                with event_loop() as loop:
                    for future in as_completed(futures.keys()):
                        task = futures.pop(future)
                        ready_futures = True

                        try:
                            result = future.result(timeout=self.timeout)

                            self.log.debug("result: %s", result)

                            if isinstance(result, TaskDeferred):
                                deferred_task = loop.create_task(
                                    self._run_deferrable(
                                        context=context,
                                        task_instance=task,
                                        task_deferred=result,
                                    )
                                )
                                deferred_tasks[deferred_task] = task
                            elif result and task.task.do_xcom_push:
                                self._xcom_push(
                                    context=context,
                                    task=task,
                                    value=result,
                                )
                        except TimeoutError as e:
                            self.log.warning("A timeout occurred for task_id %s", task.task_id)
                            if task.next_try_number > self.retries:
                                exception = AirflowTaskTimeout(e)
                            else:
                                reschedule_date = min(reschedule_date, task.next_retry_datetime())
                                failed_tasks.append(task)
                        except AirflowRescheduleTaskInstanceException as e:
                            reschedule_date = min(reschedule_date, e.reschedule_date)
                            failed_tasks.append(e.task)
                        except AirflowException as e:
                            self.log.error("An exception occurred for task_id %s", task.task_id)
                            exception = e

                    if len(futures) < self.chunk_size:
                        for task in next(chunked_tasks, []):
                            future = pool.submit(self._run_operator, context, task)
                            futures[future] = task

                    if deferred_tasks:
                        self.log.info("Running %s deferred tasks", len(deferred_tasks))

                        deferred_task_keys = list(deferred_tasks.keys())
                        results = loop.run_until_complete(gather(*deferred_task_keys, return_exceptions=True))

                        for future, result in zip(deferred_task_keys, results):
                            task = deferred_tasks[future]

                            self.log.debug("result: %s", result)

                            if isinstance(result, Exception):
                                if isinstance(result, AirflowRescheduleTaskInstanceException):
                                    reschedule_date = min(reschedule_date, result.reschedule_date)
                                    failed_tasks.append(task)
                                else:
                                    exception = result
                            elif result and task.task.do_xcom_push:
                                self._xcom_push(
                                    context=context,
                                    task=task,
                                    value=result,
                                )
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

        # Calculate delay before the next retry
        if reschedule_date > timezone.utcnow():
            delay_seconds = ceil((reschedule_date - timezone.utcnow()).total_seconds())

            self.log.info(
                "Attempting to run %s failed tasks within %s seconds...",
                len(failed_tasks),
                delay_seconds,
            )

            sleep(delay_seconds)

        return self._run_tasks(context, failed_tasks)

    def _run_operator(self, context: Context, task_instance: TaskInstance):
        try:
            with OperatorExecutor(context=context, task_instance=task_instance) as executor:
                return executor.run()
        except TaskDeferred as task_deferred:
            return task_deferred

    async def _run_deferrable(
        self, context: Context, task_instance: TaskInstance, task_deferred: TaskDeferred
    ):
        async with self._semaphore:
            async with TriggerExecutor(context=context, task_instance=task_instance) as executor:
                return await executor.run_deferred(task_deferred)

    def _create_task(self, run_id: str, index: int, mapped_kwargs: dict) -> TaskInstance:
        operator = self._unmap_operator(mapped_kwargs)
        return VolatileTaskInstance(
            task=operator,
            run_id=run_id,
            state=TaskInstanceState.SCHEDULED.value,
            map_index=index,
        )

    def execute(self, context: Context):
        return self._run_tasks(
            context=context,
            tasks=iter(
                map(
                    lambda mapped_kwargs: self._create_task(
                        context["ti"].run_id,
                        mapped_kwargs[0],
                        mapped_kwargs[1],
                    ),
                    enumerate(self._mapped_kwargs),
                )
            ),
        )
