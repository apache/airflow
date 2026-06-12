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

import contextvars
import inspect
import logging
import os
import time
from asyncio import (
    FIRST_COMPLETED,
    AbstractEventLoop,
    Future,
    Semaphore,
    Task,
    TimeoutError as AsyncTimeoutError,
    gather,
    wait,
    wait_for,
    wrap_future,
)
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Executor, ThreadPoolExecutor
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
    from airflow.sdk.execution_time.task_runner import IndexedTaskInstance


class AsyncAwareExecutor(Executor):
    """
    Executes both sync and async functions concurrently.

    Sync functions run in a ThreadPoolExecutor.
    Async coroutines run on an asyncio event loop with a semaphore limit.

    :param loop: Event loop used to schedule async tasks and coordinate mixed execution.
    :param max_workers: Maximum concurrent workers used by both thread pool and async semaphore.
    """

    def __init__(self, loop: AbstractEventLoop, max_workers: int | None = None):
        if max_workers is None:
            max_workers = os.cpu_count() or 1
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._loop = loop
        self._max_workers = max_workers
        self._semaphore = Semaphore(max_workers)
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._async_tasks: set[Task[Any]] = set()
        self._shutdown = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # On error/timeout path, bail immediately without waiting for in-flight tasks.
            # Note: in-flight threads are abandoned, not killed.
            self.shutdown(wait=False, cancel_futures=True)
        else:
            self.shutdown(wait=True)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        if self._shutdown:
            return

        self._shutdown = True

        if cancel_futures:
            for task in list(self._async_tasks):
                task.cancel()

        if wait and self._async_tasks:
            self._loop.run_until_complete(gather(*self._async_tasks, return_exceptions=True))

        self._thread_pool.shutdown(wait=wait, cancel_futures=cancel_futures)

    def submit(self, func: Callable[..., Any] | Any, *args, **kwargs) -> Future[Any]:  # type: ignore[override]
        """
        Submit a callable for execution.

        Always returns an asyncio.Future for consistency, whether the callable
        is sync (run in thread pool) or async (run on the event loop).
        """
        if self._shutdown:
            raise RuntimeError("cannot schedule new futures after shutdown")

        if inspect.iscoroutine(func):
            coro = func
        elif inspect.iscoroutinefunction(func):
            coro = func(*args, **kwargs)
        else:
            # Wrap thread pool future as asyncio.Future for consistent return type
            return wrap_future(self._thread_pool.submit(func, *args, **kwargs), loop=self._loop)

        async def guarded():
            async with self._semaphore:
                return await coro

        task = self._loop.create_task(guarded())
        self._async_tasks.add(task)
        task.add_done_callback(self._async_tasks.discard)
        return task

    async def run_sync(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Run a sync callable in this executor's thread pool and await its result."""
        future = self._thread_pool.submit(func, *args, **kwargs)
        return await wrap_future(future, loop=self._loop)

    def map(
        self,
        fn: Callable[..., Any],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[Any]:
        """Apply fn to iterables and stream results in completion order."""
        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")

        if self._shutdown:
            raise RuntimeError("cannot schedule new futures after shutdown")

        start = time.monotonic()
        iterator = zip(*iterables)
        pending: dict[Future[Any], Future[Any]] = {}
        exhausted = False

        def _remaining_timeout() -> float | None:
            if timeout is None:
                return None
            remaining = timeout - (time.monotonic() - start)
            if remaining <= 0:
                raise TimeoutError()
            return remaining

        def _submit_next() -> bool:
            nonlocal exhausted

            if exhausted:
                return False

            try:
                args = next(iterator)
            except StopIteration:
                exhausted = True
                return False

            future = self.submit(fn, *args)
            pending[future] = future
            return True

        def _fill_pending() -> None:
            """Submit tasks until pending reaches max_workers or iterator is exhausted."""
            while len(pending) < self._max_workers and _submit_next():
                pass

        # Submit up to max_workers tasks initially
        _fill_pending()

        while pending:
            wait_timeout = _remaining_timeout()
            done, _ = self._loop.run_until_complete(
                wait(set(pending), timeout=wait_timeout, return_when=FIRST_COMPLETED)
            )

            if not done:
                raise TimeoutError()

            for completed in done:
                pending.pop(completed)
                _fill_pending()
                yield completed.result()


class TaskExecutor(LoggingMixin):
    """Base class to run an operator or trigger with given task context and task instance."""

    def __init__(
        self,
        task_instance: IndexedTaskInstance,
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
        return self.task_instance.index

    @property
    def xcom_key(self):
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
                "Attempting running task %s of %s for %s with index %s in %s mode.",
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
            # Non-Exception BaseExceptions (e.g. DeadlockImminentError,
            # KeyboardInterrupt, SystemExit) must never be retried: they
            # signal conditions where continuing is meaningless.
            # Re-raise immediately without retry.
            if not isinstance(exc_value, Exception):
                self.task_instance.state = TaskInstanceState.FAILED
                raise exc_value
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
            return await wait_for(coro_in_ctx, timeout=task.execution_timeout.total_seconds())
        return await coro_in_ctx

    try:
        result = await _run_in_context(execute, context=context)
    except AsyncTimeoutError:
        task.on_kill()
        raise

    if (post_execute_hook := task._post_execute_hook) is not None:
        create_executable_runner(post_execute_hook, outlet_events, logger=log).run(context, result)
    if getattr(post_execute_hook := task.post_execute, "__func__", None) is not BaseOperator.post_execute:
        create_executable_runner(post_execute_hook, outlet_events, logger=log).run(context)

    return result
