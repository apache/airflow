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

import inspect
import logging
import os
import threading
import time
from asyncio import (
    FIRST_COMPLETED,
    AbstractEventLoop,
    CancelledError,
    Future,
    Semaphore,
    Task,
    TimeoutError as AsyncTimeoutError,
    gather,
    wait,
    wait_for,
    wrap_future,
)
from collections.abc import AsyncIterable, Callable, Iterator
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from airflow.sdk import BaseOperator, TaskInstanceState, timezone
from airflow.sdk.definitions._internal.logging_mixin import LoggingMixin
from airflow.sdk.exceptions import TaskDeferred
from airflow.sdk.execution_time.context import set_current_context
from airflow.sdk.execution_time.task_runner import (
    _execute_async_task,
    _execute_task,
    _run_task_state_change_callbacks,
)

if TYPE_CHECKING:
    from airflow.sdk import Context
    from airflow.sdk.execution_time.task_runner import IndexedTaskInstance


_log = logging.getLogger(__name__)


class AsyncAwareExecutor(Executor):
    """
    Executes both sync and async functions concurrently.

    Sync functions run in a ThreadPoolExecutor.
    Async coroutines run on an asyncio event loop with a semaphore limit.

    :param loop: Event loop used to schedule async tasks and coordinate mixed execution.
    :param max_workers: Maximum concurrent workers used by both thread pool and async semaphore.
    :param shutdown_timeout: Maximum time to wait, in seconds, for in-flight async tasks and
        thread-pool workers to finish during ``shutdown(wait=True)``. Python threads cannot be
        forcibly stopped, so a worker stuck in blocking user code (slow HTTP call, blocked C
        extension, a deadlocked DB driver, ...) would otherwise hang ``shutdown()`` forever.
    """

    def __init__(
        self, loop: AbstractEventLoop, max_workers: int | None = None, shutdown_timeout: float = 10.0
    ):
        if max_workers is None:
            max_workers = os.cpu_count() or 1
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._loop = loop
        self._max_workers = max_workers
        self._shutdown_timeout = shutdown_timeout
        self._semaphore = Semaphore(max_workers)
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._async_tasks: set[Task[Any]] = set()
        self._shutdown = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # On error path, cancel futures but still wait briefly for them to
            # process CancelledError and release resources (e.g., threading
            # locks). Without waiting, cancelled tasks that hold _thread_lock
            # never execute their finally blocks, permanently leaking the lock
            # and causing subsequent comms.send() calls to deadlock.
            self.shutdown(wait=True, cancel_futures=True)
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
            with suppress(TimeoutError, AsyncTimeoutError):
                self._loop.run_until_complete(
                    wait_for(
                        gather(*self._async_tasks, return_exceptions=True),
                        timeout=self._shutdown_timeout,
                    )
                )

        # ThreadPoolExecutor.shutdown(wait=True) blocks until every worker thread
        # finishes its current work item, with no way to bound that wait or forcibly
        # stop a thread stuck in blocking user code (slow HTTP call, blocked C
        # extension, a deadlocked DB driver, ...). Ask the pool to stop accepting new
        # work (and cancel anything not yet started) up front, then bound how long we
        # personally wait on the worker threads instead of blocking indefinitely.
        self._thread_pool.shutdown(wait=False, cancel_futures=cancel_futures)

        if wait:
            threads = list(getattr(self._thread_pool, "_threads", ()))
            deadline = time.monotonic() + self._shutdown_timeout
            for thread in threads:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                thread.join(timeout=remaining)

            stuck = [thread.name for thread in threads if thread.is_alive()]
            if stuck:
                _log.error(
                    "%d worker thread(s) still running %.1fs after shutdown was requested; "
                    "giving up waiting to avoid blocking indefinitely. This may leak resources. "
                    "Affected threads: %s",
                    len(stuck),
                    self._shutdown_timeout,
                    stuck,
                )

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
            try:
                async with self._semaphore:
                    return await coro
            except CancelledError:
                # If cancellation occurs while waiting for the semaphore,
                # the inner coroutine was never awaited. Close it to prevent
                # "coroutine was never awaited" RuntimeWarning.
                coro.close()
                raise

        task = self._loop.create_task(guarded())
        self._async_tasks.add(task)
        task.add_done_callback(self._async_tasks.discard)
        return task

    async def run_sync(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Run a sync callable in this executor's thread pool and await its result."""
        future = self._thread_pool.submit(func, *args, **kwargs)
        return await wrap_future(future, loop=self._loop)

    def imap_unordered(
        self,
        fn: Callable[..., Any],
        *iterables: AsyncIterable[Any],
        timeout: float | None = None,
    ) -> Iterator[Any]:
        """
        Apply ``fn`` to async iterables, zipped, and stream results in completion order.

        Named after ``multiprocessing.Pool.imap_unordered`` because that is the contract: results
        come back as calls finish, not in submission order. It deliberately does not override
        ``concurrent.futures.Executor.map``, which promises submission order, so code holding a
        plain ``Executor`` keeps that guarantee and this method has to be asked for by name.

        The iterables are async, unlike ``Executor.map``'s: items are pulled and
        calls submitted from a coroutine on the running loop, never from the main thread between
        two ``run_until_complete`` calls. At such a moment a call can be parked mid-``asend``
        holding the supervisor channel's thread lock; a synchronous SDK call pulling the next item
        (an XCom read behind an iterated task's input) would then take that lock in blocking mode,
        since no loop is running, while the holder needs the loop to run to release it, and the
        process freezes with every thread idle. On the running loop an async iterable reads
        through ``asend``, and a synchronous SDK call from the loop thread meets the SDK's
        running-loop check and raises ``DeadlockImminentError`` instead of hanging.

        Results are handed to the caller while the loop is paused, which is safe: the caller only
        consumes them.
        """
        if self._shutdown:
            raise RuntimeError("cannot schedule new futures after shutdown")

        start = time.monotonic()
        iterators = [iterable.__aiter__() for iterable in iterables]
        pending: set[Future[Any]] = set()
        exhausted = False

        def _remaining_timeout() -> float | None:
            if timeout is None:
                return None
            remaining = timeout - (time.monotonic() - start)
            if remaining <= 0:
                raise TimeoutError()
            return remaining

        async def _next_args() -> tuple[Any, ...] | None:
            """Return the next argument tuple, or None once any iterable is exhausted (like ``zip``)."""
            args = []
            for iterator in iterators:
                try:
                    args.append(await iterator.__anext__())
                except StopAsyncIteration:
                    return None
            return tuple(args)

        async def _fill_pending() -> None:
            """Submit calls until pending reaches max_workers or an iterable is exhausted."""
            nonlocal exhausted
            while not exhausted and len(pending) < self._max_workers:
                args = await _next_args()
                if args is None:
                    exhausted = True
                    return
                pending.add(self.submit(fn, *args))

        # Every pull from the iterables runs on the loop, the initial one included.
        self._loop.run_until_complete(_fill_pending())

        while pending:
            done, _ = self._loop.run_until_complete(
                wait(pending, timeout=_remaining_timeout(), return_when=FIRST_COMPLETED)
            )

            if not done:
                raise TimeoutError()

            for completed in done:
                pending.discard(completed)
                yield completed.result()

            self._loop.run_until_complete(_fill_pending())


class TaskExecutor(LoggingMixin):
    """Base class to run an operator or trigger with given task context and task instance."""

    def __init__(
        self,
        task_instance: IndexedTaskInstance,
        active_operators: set[BaseOperator] | None = None,
        active_operators_lock: threading.Lock | None = None,
    ):
        """
        Run an operator or trigger for one sub-task instance.

        :param active_operators: Optional shared set the caller wants this operator registered
            into for the duration of its execution (e.g. so IterableOperator.on_kill() can
            propagate to whichever sub-tasks are currently in flight). Registration happens in
            __enter__/__exit__ so callers no longer need their own try/finally bookkeeping.
        :param active_operators_lock: Lock guarding ``active_operators``, required whenever
            ``active_operators`` is given since multiple sub-tasks may run concurrently.
        """
        super().__init__()
        self.task_instance = task_instance
        self._result: Any | None = None
        self._start_time: float | None = None
        self._context: Context | None = None
        self._active_operators = active_operators
        self._active_operators_lock = active_operators_lock

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
    def operator(self) -> BaseOperator:
        return self.task_instance.task

    @property
    def is_async(self) -> bool:
        return self.task_instance.is_async

    def run(self, context: Context):
        self._context = context
        with set_current_context(context):
            return _execute_task(context, self.task_instance, self.log)

    async def arun(self, context: Context):
        self._context = context
        with set_current_context(context):
            return await _execute_async_task(context, self.task_instance, self.log)

    def __enter__(self):
        self._start_time = time.monotonic()

        if self._active_operators is not None and self._active_operators_lock is not None:
            with self._active_operators_lock:
                self._active_operators.add(self.operator)

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

    def __exit__(self, exc_type, exc_value, traceback):
        if self._active_operators is not None and self._active_operators_lock is not None:
            with self._active_operators_lock:
                self._active_operators.discard(self.operator)

        elapsed = time.monotonic() - self._start_time if self._start_time else 0.0

        if exc_value:
            # Non-Exception BaseExceptions (e.g. DeadlockImminentError,
            # KeyboardInterrupt, SystemExit) must never be retried: they
            # signal conditions where continuing is meaningless.
            # Re-raise immediately without retry.
            if not isinstance(exc_value, Exception):
                self.task_instance.state = TaskInstanceState.FAILED
                if self._context is not None:
                    _run_task_state_change_callbacks(
                        self.task_instance.task, "on_failure_callback", self._context, self.log
                    )
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
                    if self._context is not None:
                        _run_task_state_change_callbacks(
                            self.task_instance.task, "on_failure_callback", self._context, self.log
                        )
                    raise exc_value
                self.task_instance.try_number += 1
                self.task_instance.end_date = timezone.utcnow()
                self.task_instance.state = TaskInstanceState.UP_FOR_RESCHEDULE
                if self._context is not None:
                    _run_task_state_change_callbacks(
                        self.task_instance.task, "on_retry_callback", self._context, self.log
                    )
            raise exc_value

        self.task_instance.state = TaskInstanceState.SUCCESS
        if self._context is not None:
            _run_task_state_change_callbacks(
                self.task_instance.task, "on_success_callback", self._context, self.log
            )
        if self.log.isEnabledFor(logging.INFO):
            self.log.info(
                "Task instance %s for %s finished successfully in %s attempts in %.2f seconds",
                self.task_index,
                self.task_instance.task_id,
                self.task_instance.next_try_number,
                elapsed,
            )
