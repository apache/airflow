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
import os
from asyncio import AbstractEventLoop, Semaphore
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from logging import Logger
from typing import TYPE_CHECKING, Any

from airflow.sdk import BaseAsyncOperator, BaseOperator
from airflow.sdk.bases.operator import ExecutorSafeguard
from airflow.sdk.execution_time.callback_runner import create_executable_runner
from airflow.sdk.execution_time.context import (
    context_get_outlet_events,
    context_to_airflow_vars,
)
from airflow.sdk.execution_time.task_runner import (
    RuntimeTaskInstance,
    _run_task_state_change_callbacks,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


def collect_futures(loop: AbstractEventLoop, futures: list[Any]):
    """Yield futures as they complete (sync or async)."""
    yield from as_completed(f for f in futures if isinstance(f, Future))

    async_tasks = [f for f in futures if isinstance(f, asyncio.Task)]

    if async_tasks:
        for task, _ in zip(
            async_tasks,
            loop.run_until_complete(asyncio.gather(*async_tasks, return_exceptions=True)),
        ):
            yield task

    return []


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


async def _execute_async_task(context: Context, ti: RuntimeTaskInstance, log: Logger):
    """Execute Task (optionally with a Timeout) and push Xcom results."""
    # set-up
    task: BaseOperator | BaseAsyncOperator = ti.task
    execute = task.aexecute  # here we must use aexecute instead of execute

    # async tasks can't originate from deferred operator, so no need to check next_method

    ctx = contextvars.copy_context()
    # Populate the context var so ExecutorSafeguard doesn't complain
    ctx.run(ExecutorSafeguard.tracker.set, task)

    # Export context in os.environ to make it available for operators to use.
    airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
    os.environ.update(airflow_context_vars)

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
