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
import time
from collections import deque
from collections.abc import Iterable, Sequence
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any

try:
    # Python 3.12+
    from itertools import batched
except ImportError:
    from more_itertools import batched

try:
    # Python 3.11+
    ExceptionGroup
except NameError:
    from exceptiongroup import ExceptionGroup

from airflow.exceptions import (
    AirflowTaskTimeout,
    TaskDeferred,
)
from airflow.sdk import timezone
from airflow.sdk.bases.operator import BaseOperator, DecoratedDeferredAsyncOperator, event_loop
from airflow.sdk.definitions.xcom_arg import MapXComArg, XComArg  # noqa: F401
from airflow.sdk.exceptions import AirflowRescheduleTaskInstanceException
from airflow.sdk.execution_time.context import context_to_airflow_vars
from airflow.sdk.execution_time.executor import HybridExecutor, _execute_async_task, collect_futures
from airflow.sdk.execution_time.lazy_sequence import XComIterable
from airflow.sdk.execution_time.task_runner import MappedTaskInstance, RuntimeTaskInstance, _execute_task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.sdk.definitions._internal.expandinput import ExpandInput
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.mappedoperator import MappedOperator


class TaskExecutor(LoggingMixin):
    """Base class to run an operator or trigger with given task context and task instance."""

    def __init__(
        self,
        task_instance: MappedTaskInstance,
    ):
        super().__init__()
        self._task_instance = task_instance
        self._result: Any | None = None
        self._start_time: float | None = None

    @property
    def task_instance(self) -> MappedTaskInstance:
        return self._task_instance

    @property
    def dag_id(self) -> str:
        return self.task_instance.dag_id

    @property
    def task_id(self) -> str:
        return self.task_instance.task_id

    @property
    def task_index(self) -> int:
        map_index = self.task_instance.map_index
        assert map_index is not None
        return map_index

    @property
    def key(self):
        return self.task_instance.xcom_key

    @property
    def operator(self) -> BaseOperator:
        return self.task_instance.task

    @property
    def is_async(self) -> bool:
        return self.task_instance.is_async

    def run(self, context: Context):
        return _execute_task(context, self.task_instance, self.log)

    async def arun(self, context: Context):
        return await _execute_async_task(context, self.task_instance, self.log)

    def __enter__(self):
        self._start_time = time.monotonic()

        if self.log.isEnabledFor(logging.INFO):
            self.log.info(
                "Attempting running task %s of %s for %s with map_index %s in %s mode.",
                self.task_instance.try_number,
                self.operator.retries,
                self.task_instance.task_id,
                self.task_index,
                "async" if self.is_async else "sync",
            )
        return self

    async def __aenter__(self):
        return self.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        elapsed = time.monotonic() - self._start_time

        if exc_value:
            if not isinstance(exc_value, TaskDeferred):
                if self.task_instance.next_try_number > self.task_instance.max_tries:
                    self.log.error(
                        "Task instance %s for %s failed after %s attempts in %.2f seconds due to: %s",
                        self.task_index,
                        self.task_instance.task_id,
                        self.task_instance.max_tries,
                        elapsed,
                        exc_value,
                    )
                    self.task_instance.state = TaskInstanceState.FAILED
                    raise exc_value
                self.task_instance.try_number += 1
                self.task_instance.end_date = timezone.utcnow()
                self.task_instance.state = TaskInstanceState.UP_FOR_RESCHEDULE
                raise AirflowRescheduleTaskInstanceException(task=self.task_instance)
            raise exc_value

        self.task_instance.state = TaskInstanceState.SUCCESS
        if self.log.isEnabledFor(logging.INFO):
            self.log.info(
                "Task instance %s for %s finished successfully in %s attempts in %.2f seconds",
                self.task_index,
                self.task_instance.task_id,
                self.task_instance.next_try_number,
                elapsed,
            )

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.__exit__(exc_type, exc_value, traceback)


class IterableOperator(BaseOperator):
    """Object representing an iterable operator in a DAG."""

    _operator: BaseOperator | MappedOperator
    expand_input: ExpandInput
    partial_kwargs: dict[str, Any]
    # each operator should override this class attr for shallow copy attrs.
    shallow_copy_attrs: Sequence[str] = (
        "_operator",
        "expand_input",
        "partial_kwargs",
        "_log",
    )

    def __init__(
        self,
        *,
        operator: BaseOperator | MappedOperator,
        expand_input: ExpandInput,
        **kwargs,
    ):
        super().__init__(
            **{
                **kwargs,
                "task_id": operator.task_id,
                "owner": operator.owner,
                "email": operator.email,
                "email_on_retry": operator.email_on_retry,
                "email_on_failure": operator.email_on_failure,
                "retries": 0,  # We should not retry the IterableOperator, only the mapped ti's should be retried
                "retry_delay": operator.retry_delay,
                "retry_exponential_backoff": operator.retry_exponential_backoff,
                "max_retry_delay": operator.max_retry_delay,
                "start_date": operator.start_date,
                "end_date": operator.end_date,
                "depends_on_past": operator.depends_on_past,
                "ignore_first_depends_on_past": operator.ignore_first_depends_on_past,
                "wait_for_past_depends_before_skipping": operator.wait_for_past_depends_before_skipping,
                "wait_for_downstream": operator.wait_for_downstream,
                "dag": operator.dag,
                "priority_weight": operator.priority_weight,
                "queue": operator.queue,
                "pool": operator.pool,
                "pool_slots": operator.pool_slots,
                "execution_timeout": None,
                "trigger_rule": operator.trigger_rule,
                "resources": operator.resources,
                "run_as_user": operator.run_as_user,
                "map_index_template": operator.map_index_template,
                "max_active_tis_per_dag": operator.max_active_tis_per_dag,
                "max_active_tis_per_dagrun": operator.max_active_tis_per_dagrun,
                "executor": operator.executor,
                "executor_config": operator.executor_config,
                "inlets": operator.inlets,
                "outlets": operator.outlets,
                "task_group": operator.task_group,
                "doc": operator.doc,
                "doc_md": operator.doc_md,
                "doc_json": operator.doc_json,
                "doc_yaml": operator.doc_yaml,
                "doc_rst": operator.doc_rst,
                "task_display_name": operator.task_display_name,
                "allow_nested_operators": operator.allow_nested_operators,
            }
        )
        self._operator = operator
        self.expand_input = expand_input
        self.partial_kwargs = operator.partial_kwargs or {}
        self._number_of_tasks: int = 0
        XComArg.apply_upstream_relationship(self, self.expand_input.value)

    @property
    def task_type(self) -> str:
        """@property: type of the task."""
        return self._operator.__class__.__name__

    @property
    def max_workers(self):
        return self.task_concurrency or os.cpu_count() or 1

    @property
    def timeout(self) -> float | None:
        if self.execution_timeout:
            return self.execution_timeout.total_seconds()
        return None

    def _get_specified_expand_input(self) -> ExpandInput:
        return self.expand_input

    def _unmap_operator(self, mapped_kwargs: dict):
        self._number_of_tasks += 1
        return self._operator.unmap(mapped_kwargs)

    def _xcom_push(self, context: Context, task: RuntimeTaskInstance, value: Any) -> None:
        self.log.debug("Pushing XCom %s", task.map_index)

        context["ti"].xcom_push(key=task.xcom_key, value=value)

    def _run_tasks(
        self,
        context: Context,
        tasks: Iterable[MappedTaskInstance],
    ) -> XComIterable:
        exceptions: list[BaseException] = []
        reschedule_date = timezone.utcnow()
        prev_futures_count = 0
        futures: dict[Future, MappedTaskInstance] = {}
        deferred_tasks: deque[MappedTaskInstance] = deque()
        failed_tasks: deque[MappedTaskInstance] = deque()
        chunked_tasks = batched(tasks, self.max_workers)

        self.log.info("Running tasks with %d workers", self.max_workers)

        # Export context in os.environ to make it available for operators to use.
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        os.environ.update(airflow_context_vars)

        with event_loop() as loop:
            with HybridExecutor(loop=loop, max_workers=self.max_workers) as executor:
                for task in next(chunked_tasks, []):
                    if task.is_async:
                        future = executor.submit(self._run_async_operator, context, task)
                    else:
                        future = executor.submit(self._run_operator, context, task)
                    futures[future] = task

                while futures:
                    futures_count = len(futures)

                    if futures_count != prev_futures_count:
                        self.log.info("Number of remaining futures: %s", futures_count)
                        prev_futures_count = futures_count

                    for future in collect_futures(loop, futures.keys()):
                        task = futures.pop(future)

                        try:
                            if isinstance(future, asyncio.futures.Future):
                                result = future.result()
                            else:
                                result = future.result(timeout=self.timeout)

                            self.log.debug("result: %s", result)

                            if result is not None and task.task.do_xcom_push:
                                self._xcom_push(
                                    context=context,
                                    task=task,
                                    value=result,
                                )
                        except TaskDeferred as task_deferred:
                            operator = DecoratedDeferredAsyncOperator(
                                operator=task.task, task_deferred=task_deferred
                            )
                            deferred_tasks.append(
                                self._create_mapped_task(
                                    run_id=task.run_id,
                                    map_index=task.map_index,
                                    try_number=task.try_number,
                                    operator=operator,
                                )
                            )
                        except asyncio.TimeoutError as e:
                            self.log.warning("A timeout occurred for task_id %s", task.task_id)
                            if task.next_try_number > self.retries:
                                exceptions.append(AirflowTaskTimeout(e))
                            else:
                                reschedule_date = min(reschedule_date, task.next_retry_datetime())
                                failed_tasks.append(task)
                        except AirflowRescheduleTaskInstanceException as e:
                            reschedule_date = min(reschedule_date, e.reschedule_date)
                            self.log.warning(
                                "An exception occurred for task_id %s with map_index %s, it has been rescheduled at %s",
                                task.task_id,
                                task.map_index,
                                reschedule_date,
                            )
                            failed_tasks.append(e.task)
                        except Exception as e:
                            self.log.error(
                                "An exception occurred for task_id %s with map_index %s",
                                task.task_id,
                                task.map_index,
                            )
                            exceptions.append(e)

                    while len(futures) < self.max_workers:
                        if deferred_tasks:
                            task = deferred_tasks.popleft()
                        else:
                            task = next(chunked_tasks, None)

                        if not task:
                            break

                        if task.is_async:
                            future = executor.submit(self._run_async_operator, context, task)
                        else:
                            future = executor.submit(self._run_operator, context, task)
                        futures[future] = task

        if not failed_tasks:
            if exceptions:
                raise ExceptionGroup("Multiple sub-task failures", exceptions)
            if self.do_xcom_push:
                return XComIterable(
                    task_id=self.task_id,
                    dag_id=self.dag_id,
                    run_id=context["run_id"],
                    length=self._number_of_tasks,
                )

        # If the retry time is still in the future we defer the operator so the worker
        # slot is released. If the retry time has already passed we immediately re-run
        # the failed tasks without deferring.
        if reschedule_date > timezone.utcnow():
            # TODO: This is tricky as that import doesn't exist in Task SDK
            from airflow.providers.standard.triggers.temporal import DateTimeTrigger

            self.defer(
                trigger=DateTimeTrigger(reschedule_date),
                method_name=self.execute_failed_tasks.__name__,
                kwargs={
                    "failed_tasks": {failed_task.map_index for failed_task in failed_tasks},
                    "try_number": next(iter(failed_tasks)).try_number,
                },
            )

        return self._run_tasks(context=context, tasks=list(failed_tasks))

    def _run_operator(self, context: Context, task_instance: MappedTaskInstance):
        with TaskExecutor(task_instance=task_instance) as executor:
            return executor.run(
                context={
                    **context,
                    **{
                        "ti": task_instance,
                        "task_instance": task_instance,
                        "map_index": task_instance.map_index,
                    },
                }
            )

    async def _run_async_operator(self, context: Context, task_instance: MappedTaskInstance):
        async with TaskExecutor(task_instance=task_instance) as executor:
            return await executor.arun(
                context={
                    **context,
                    **{
                        "ti": task_instance,
                        "task_instance": task_instance,
                        "map_index": task_instance.map_index,
                    },
                }
            )

    def _create_task(
        self,
        context: Context,
        map_index: int,
        mapped_kwargs: dict,
        try_number: int= 0,
    ) -> MappedTaskInstance:
        run_id = context["ti"].run_id
        operator = self._unmap_operator(mapped_kwargs)
        return self._create_mapped_task(run_id=run_id, map_index=map_index, try_number=try_number, operator=operator)

    def _create_mapped_task(
        self, run_id: str, map_index: int, try_number: int, operator: BaseOperator
    ) -> MappedTaskInstance:
        return MappedTaskInstance.model_construct(
            task_id=operator.task_id,
            dag_id=operator.dag_id,
            run_id=run_id,
            map_index=map_index,
            max_tries=operator.retries,
            start_date=self.start_date,
            state=TaskInstanceState.SCHEDULED.value,
            is_mapped=True,
            task=operator,
            try_number=try_number,
            xcoms={},
        )

    def execute(self, context: Context):
        tasks = (
            self._create_task(context=context, map_index=index, mapped_kwargs=value)
            for index, value in enumerate(
                self.expand_input.iter_values(context=context)
            )
        )
        return self._run_tasks(context=context, tasks=tasks)

    def execute_failed_tasks(self, context: Context, try_number: int, failed_tasks: set[int], event: dict[Any, Any]):
        tasks = (
            self._create_task(context=context, map_index=index, try_number=try_number, mapped_kwargs=value)
            for index, value in enumerate(self.expand_input.iter_values(context=context))
            if index in failed_tasks
        )
        return self._run_tasks(context=context, tasks=tasks)
