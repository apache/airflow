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
import contextvars
import inspect
import logging
import time
from asyncio import AbstractEventLoop, Semaphore
from collections.abc import Callable, Generator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, cast

from airflow.sdk import BaseAsyncOperator, BaseOperator, TaskInstanceState, timezone
from airflow.sdk.bases.operator import ExecutorSafeguard
from airflow.sdk.definitions._internal.logging_mixin import LoggingMixin
from airflow.sdk.exceptions import AirflowRescheduleTaskInstanceException, TaskDeferred
from airflow.sdk.execution_time.callback_runner import create_executable_runner
from airflow.sdk.execution_time.context import context_get_outlet_events
from airflow.sdk.execution_time.task_runner import (
    RuntimeTaskInstance,
    _execute_task,
    _run_task_state_change_callbacks,
)

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk import Context
    from airflow.sdk.execution_time.task_runner import MappedTaskInstance


def collect_futures(loop: AbstractEventLoop, futures: list[Any]) -> Generator[Future | asyncio.futures.Future, None, None]:
    """
    Yield futures as they complete (sync or async).

    :param loop: The asyncio event loop to use for async tasks
    :param futures: List of Future or asyncio.futures.Future objects to collect
    :return: Generator yielding Future or asyncio.futures.Future objects as they complete
    """
    yield from as_completed(f for f in futures if isinstance(f, Future))

    async_tasks = [f for f in futures if isinstance(f, asyncio.futures.Future)]

    if async_tasks:
        for task, _ in zip(
            async_tasks,
            loop.run_until_complete(asyncio.gather(*async_tasks, return_exceptions=True)),
        ):
            yield task


class HybridExecutor:
    """
    Executes both sync and async functions concurrently.

    Sync functions run in a ThreadPoolExecutor.
    Async coroutines run on an asyncio event loop with a semaphore limit.
    """

    def __init__(self, loop: AbstractEventLoop, max_workers: int = 4):
        self._loop = loop
        self._semaphore = Semaphore(max_workers)
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)

    def submit(self, func: Callable, *args, **kwargs):
        if inspect.iscoroutine(func):
            coro = func
        elif inspect.iscoroutinefunction(func):
            coro = func(*args, **kwargs)
        else:
            return self._thread_pool.submit(func, *args, **kwargs)

        async def guarded():
            async with self._semaphore:
                return await coro

        return self._loop.create_task(guarded())


class TaskExecutor(LoggingMixin):
    """Base class to run an operator or trigger with given task context and task instance."""

    def __init__(
        self,
        task_instance: MappedTaskInstance,
    ):
        super().__init__()
        self.task_instance = task_instance
        self._result: Any | None = None
        self._start_time: float | None = None

    @property
    def dag_id(self) -> str:
        return self.task_instance.dag_id

    @property
    def task_id(self) -> str:
        return self.task_instance.task_id

    @property
    def task_index(self) -> int:
        map_index = self.task_instance.map_index
        if map_index is None:
            raise ValueError("MappedTaskInstance.map_index should not be None!")
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
        elapsed = time.monotonic() - self._start_time if self._start_time else 0.0

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


async def _execute_async_task(context: Context, ti: RuntimeTaskInstance, log: Logger):
    """Execute Task (optionally with a Timeout) and push Xcom results."""
    # set-up
    task = cast("BaseAsyncOperator", ti.task)
    execute = task.aexecute  # here we must use aexecute instead of execute

    # async tasks can't originate from deferred operator, so no need to check next_method

    ctx = contextvars.copy_context()
    # Populate the context var so ExecutorSafeguard doesn't complain
    ctx.run(ExecutorSafeguard.tracker.set, task)

    outlet_events = context_get_outlet_events(context)

    if (pre_execute_hook := task._pre_execute_hook) is not None:
        create_executable_runner(pre_execute_hook, outlet_events, logger=log).run(context)
    if getattr(pre_execute_hook := task.pre_execute, "__func__", None) is not BaseOperator.pre_execute:
        create_executable_runner(pre_execute_hook, outlet_events, logger=log).run(context)

    _run_task_state_change_callbacks(task, "on_execute_callback", context, log)

    async def _run_in_context(coro_func, *args, **kwargs):
        """Run async function in contextvars context with optional timeout."""
        coro_in_ctx = ctx.run(lambda: coro_func(*args, **kwargs))

        if task.execution_timeout:
            return await asyncio.wait_for(coro_in_ctx, timeout=task.execution_timeout.total_seconds())
        return await coro_in_ctx

    try:
        result = await _run_in_context(execute, context=context)
    except asyncio.TimeoutError:
        task.on_kill()
        raise

    if (post_execute_hook := task._post_execute_hook) is not None:
        create_executable_runner(post_execute_hook, outlet_events, logger=log).run(context, result)
    if getattr(post_execute_hook := task.post_execute, "__func__", None) is not BaseOperator.post_execute:
        create_executable_runner(post_execute_hook, outlet_events, logger=log).run(context)

    return result
