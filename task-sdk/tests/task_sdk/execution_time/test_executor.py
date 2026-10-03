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
import threading
import time
from concurrent.futures import Executor
from unittest import mock

import pytest

from airflow.sdk.bases.operator import event_loop
from airflow.sdk.execution_time.executor import AsyncAwareExecutor


async def aiter(items):
    """Expose a list as the async iterable ``AsyncAwareExecutor.imap_unordered`` consumes."""
    for item in items:
        yield item


class TestAsyncAwareExecutor:
    def test_submit_sync_function_returns_future(self):
        """Sync callables are dispatched to the thread pool and return an asyncio.Future."""
        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                future = executor.submit(lambda: 42)
                assert isinstance(future, asyncio.Future)
                assert loop.run_until_complete(future) == 42

    def test_submit_async_coroutine_function_returns_task(self):
        """Async callables are scheduled on the event loop and return an asyncio.Task."""
        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:

                async def async_fn():
                    return "async_result"

                task = executor.submit(async_fn)
                assert isinstance(task, asyncio.Task)
                result = loop.run_until_complete(task)
                assert result == "async_result"

    def test_submit_coroutine_object_returns_task(self):
        """Passing a coroutine object (not a function) directly is also scheduled on the event loop."""
        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:

                async def async_fn():
                    return "coro_result"

                coro = async_fn()
                task = executor.submit(coro)
                assert isinstance(task, asyncio.Task)
                result = loop.run_until_complete(task)
                assert result == "coro_result"

    def test_submit_sync_function_propagates_exception(self):
        """Exceptions raised inside sync callables are propagated when the future is resolved."""
        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                future = executor.submit(lambda: (_ for _ in ()).throw(ValueError("boom")))
                assert isinstance(future, asyncio.Future)
                with pytest.raises(ValueError, match="boom"):
                    loop.run_until_complete(future)

    def test_submit_async_function_propagates_exception(self):
        """Exceptions raised inside async callables are propagated when the task is awaited."""
        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:

                async def failing():
                    raise RuntimeError("async boom")

                task = executor.submit(failing)
                with pytest.raises(RuntimeError, match="async boom"):
                    loop.run_until_complete(task)

    def test_semaphore_limits_concurrent_async_tasks(self):
        """The semaphore prevents more than max_workers coroutines from running simultaneously."""
        concurrency_high_watermark = 0
        running = 0

        async def count_concurrent():
            nonlocal concurrency_high_watermark, running
            running += 1
            concurrency_high_watermark = max(concurrency_high_watermark, running)
            await asyncio.sleep(0)
            running -= 1

        max_workers = 2
        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=max_workers) as executor:
                tasks = [executor.submit(count_concurrent) for _ in range(6)]
                loop.run_until_complete(asyncio.gather(*tasks))

        assert concurrency_high_watermark <= max_workers

    def test_exit_shuts_down_thread_pool(self):
        """__exit__ calls shutdown on the thread pool without blocking indefinitely on it."""
        with event_loop() as loop:
            executor = AsyncAwareExecutor(loop=loop, max_workers=2)
            with mock.patch.object(
                executor._thread_pool, "shutdown", wraps=executor._thread_pool.shutdown
            ) as shutdown_mock:
                with executor:
                    pass
                # wait=False: the executor itself bounds how long it waits on worker
                # threads afterwards instead of delegating an unbounded wait to the pool.
                shutdown_mock.assert_called_once_with(wait=False, cancel_futures=False)

    def test_context_manager_returns_self(self):
        """__enter__ returns the executor instance itself."""
        with event_loop() as loop:
            executor = AsyncAwareExecutor(loop=loop, max_workers=2)
            with executor as ctx:
                assert ctx is executor

    def test_does_not_override_executor_map(self):
        """Completion-order streaming is imap_unordered; Executor.map keeps its submission-order contract."""
        assert AsyncAwareExecutor.map is Executor.map

    def test_imap_unordered_streams_completed_sync_results(self):
        """map() yields completed results as work finishes instead of waiting for all items."""

        def sleepy_value(delay: float) -> float:
            time.sleep(delay)
            return delay

        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                started = time.monotonic()
                result_iter = executor.imap_unordered(sleepy_value, aiter([0.25, 0.01]))
                first = next(result_iter)

        assert first == 0.01
        # The faster work (0.01s) should complete well before the slower work (0.25s).
        # Allow overhead for thread pool scheduling (typically ~0.15-0.2s on busy systems).
        assert time.monotonic() - started < 0.35

    def test_shutdown_cancel_futures_cancels_async_tasks(self):
        """shutdown(cancel_futures=True) cancels submitted async tasks."""

        async def long_running():
            await asyncio.sleep(60)

        with event_loop() as loop:
            executor = AsyncAwareExecutor(loop=loop, max_workers=2)
            task = executor.submit(long_running)

            executor.shutdown(wait=False, cancel_futures=True)
            loop.run_until_complete(asyncio.sleep(0))

        assert task.cancelled()

    def test_submit_after_shutdown_raises_runtime_error(self):
        with event_loop() as loop:
            executor = AsyncAwareExecutor(loop=loop, max_workers=2)
            executor.shutdown(wait=False)

            with pytest.raises(RuntimeError, match="cannot schedule new futures after shutdown"):
                executor.submit(lambda: 1)

    def test_imap_unordered_zips_async_iterables_and_stops_at_the_shortest(self):
        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                results = sorted(
                    executor.imap_unordered(lambda a, b: (a, b), aiter([1, 2, 3]), aiter([10, 20]))
                )

        assert results == [(1, 10), (2, 20)]

    def test_imap_unordered_pulls_items_on_the_running_loop_and_lazily(self):
        """
        The next item is pulled from a coroutine while the loop runs, and only when a slot frees up.

        Both are the guard against the deadlock of pulling from the main thread between two
        ``run_until_complete`` calls: a pull that needs the supervisor channel then blocks on the lock
        held by a call parked mid-``asend``, which can only complete once the loop runs again.
        """
        pulled: list[int] = []
        loop_running_at_pull: list[bool] = []

        async def source():
            for item in range(6):
                loop_running_at_pull.append(asyncio.get_running_loop().is_running())
                pulled.append(item)
                yield item

        async def work(item: int) -> int:
            await asyncio.sleep(0.01)
            return item

        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                result_iter = executor.imap_unordered(work, source())
                first = next(result_iter)
                # Only max_workers items were pulled to start; the rest wait for free slots.
                assert first in (0, 1)
                assert pulled == [0, 1] or pulled == [0, 1, 2]
                rest = list(result_iter)

        assert sorted([first, *rest]) == list(range(6))
        assert pulled == list(range(6))
        assert loop_running_at_pull == [True] * 6

    def test_imap_unordered_does_not_deadlock_when_pulling_needs_a_lock_held_by_a_parked_call(self):
        """
        Regression test for the IterableOperator freeze.

        A call holds a thread lock across an ``await`` (the supervisor channel's lock held by a
        parked ``asend``), and pulling the next item takes that same lock from a worker thread (a
        synchronous SDK read behind an iterated input). If the pull happened while the loop was
        paused, the parked call could never release the lock and the process would freeze with
        every thread idle. Pulling on the running loop lets the parked call finish first.
        """
        lock = threading.Lock()

        async def source():
            for item in range(4):
                await asyncio.to_thread(lock.acquire)
                lock.release()
                yield item

        async def work(item: int) -> int:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lock.acquire)
            try:
                await asyncio.sleep(0.02)  # parked while holding the lock
            finally:
                lock.release()
            return item

        results: list[int] = []

        def run() -> None:
            with event_loop() as loop:
                with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                    results.extend(executor.imap_unordered(work, source()))

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=10)

        assert not thread.is_alive(), "map() deadlocked while pulling the next item"
        assert sorted(results) == [0, 1, 2, 3]

    def test_imap_unordered_timeout_raises_timeout_error(self):
        def slow_fn(delay: float) -> float:
            time.sleep(delay)
            return delay

        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=1) as executor:
                with pytest.raises(TimeoutError):
                    list(executor.imap_unordered(slow_fn, aiter([0.2]), timeout=0.01))

    def test_imap_unordered_streams_completed_async_results(self):
        async def async_sleepy_value(delay: float) -> float:
            await asyncio.sleep(delay)
            return delay

        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                started = time.monotonic()
                result_iter = executor.imap_unordered(async_sleepy_value, aiter([0.2, 0.01]))
                first = next(result_iter)

        assert first == 0.01
        # The faster work (0.01s) should complete well before the slower work (0.2s).
        # Allow overhead for event loop scheduling (typically ~0.1-0.15s on busy systems).
        assert time.monotonic() - started < 0.3

    def test_shutdown_wait_true_waits_for_async_tasks(self):
        async def short_running() -> str:
            await asyncio.sleep(0.01)
            return "done"

        with event_loop() as loop:
            executor = AsyncAwareExecutor(loop=loop, max_workers=2)
            task = executor.submit(short_running)

            executor.shutdown(wait=True)

        assert task.done()
        assert not task.cancelled()
        assert task.result() == "done"

    def test_shutdown_does_not_block_forever_on_stuck_worker_thread(self):
        """shutdown(wait=True) must be bounded by shutdown_timeout, not hang on a stuck thread."""
        release = threading.Event()

        def blocking_fn():
            release.wait(timeout=5)
            return "done"

        with event_loop() as loop:
            executor = AsyncAwareExecutor(loop=loop, max_workers=1, shutdown_timeout=0.05)
            executor.submit(blocking_fn)

            started = time.monotonic()
            executor.shutdown(wait=True)
            elapsed = time.monotonic() - started

        release.set()
        assert elapsed < 1.0
