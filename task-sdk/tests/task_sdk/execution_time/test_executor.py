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
import time
from unittest import mock

import pytest
from task_sdk.execution_time.test_task_runner import get_inline_dag

from airflow.sdk import BaseOperator
from airflow.sdk.api.datamodels._generated import TaskInstanceState
from airflow.sdk.bases.operator import event_loop
from airflow.sdk.exceptions import AirflowRescheduleTaskInstanceException, TaskDeferred
from airflow.sdk.execution_time.executor import AsyncAwareExecutor, TaskExecutor

from tests_common.test_utils.mock_context import mock_context


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
        """__exit__ calls shutdown on the thread pool."""
        with event_loop() as loop:
            executor = AsyncAwareExecutor(loop=loop, max_workers=2)
            with mock.patch.object(
                executor._thread_pool, "shutdown", wraps=executor._thread_pool.shutdown
            ) as shutdown_mock:
                with executor:
                    pass
                shutdown_mock.assert_called_once_with(wait=True, cancel_futures=False)

    def test_context_manager_returns_self(self):
        """__enter__ returns the executor instance itself."""
        with event_loop() as loop:
            executor = AsyncAwareExecutor(loop=loop, max_workers=2)
            with executor as ctx:
                assert ctx is executor

    def test_map_streams_completed_sync_results(self):
        """map() yields completed results as work finishes instead of waiting for all items."""

        def sleepy_value(delay: float) -> float:
            time.sleep(delay)
            return delay

        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                started = time.monotonic()
                result_iter = executor.map(sleepy_value, [0.25, 0.01])
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

    def test_map_rejects_non_positive_chunksize(self):
        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                with pytest.raises(ValueError, match="chunksize must be >= 1"):
                    list(executor.map(lambda x: x, [1, 2], chunksize=0))

    def test_map_timeout_raises_timeout_error(self):
        def slow_fn(delay: float) -> float:
            time.sleep(delay)
            return delay

        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=1) as executor:
                with pytest.raises(TimeoutError):
                    list(executor.map(slow_fn, [0.2], timeout=0.01))

    def test_map_streams_completed_async_results(self):
        async def async_sleepy_value(delay: float) -> float:
            await asyncio.sleep(delay)
            return delay

        with event_loop() as loop:
            with AsyncAwareExecutor(loop=loop, max_workers=2) as executor:
                started = time.monotonic()
                result_iter = executor.map(async_sleepy_value, [0.2, 0.01])
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


class TestTaskExecutor:
    def test_dag_id_property(self, make_indexed_ti):
        ti = make_indexed_ti(dag_id="my_dag")
        executor = TaskExecutor(task_instance=ti)
        assert executor.dag_id == "my_dag"

    def test_task_id_property(self, make_indexed_ti):
        ti = make_indexed_ti(task_id="my_task")
        executor = TaskExecutor(task_instance=ti)
        assert executor.task_id == "my_task"

    def test_task_index(self, make_indexed_ti):
        ti = make_indexed_ti(index=3)
        executor = TaskExecutor(task_instance=ti)
        assert executor.task_index == ti.index
        assert executor.task_index == 3

    def test_operator_property(self, make_indexed_ti):
        ti = make_indexed_ti()
        executor = TaskExecutor(task_instance=ti)
        assert executor.operator is ti.task

    def test_is_async_property_sync(self, make_indexed_ti):
        ti = make_indexed_ti(is_async=False)
        executor = TaskExecutor(task_instance=ti)
        assert executor.is_async is False

    def test_is_async_property_async(self, make_indexed_ti):
        ti = make_indexed_ti(is_async=True)
        executor = TaskExecutor(task_instance=ti)
        assert executor.is_async is True

    def test_enter_sets_start_time(self, make_indexed_ti):
        ti = make_indexed_ti()
        executor = TaskExecutor(task_instance=ti)
        assert executor._start_time is None
        executor.__enter__()
        assert executor._start_time is not None

    def test_enter_returns_self(self, make_indexed_ti):
        ti = make_indexed_ti()
        executor = TaskExecutor(task_instance=ti)
        with executor as ctx:
            assert ctx is executor

    def test_exit_success_sets_state(self, make_indexed_ti):
        """__exit__ without an exception marks the task instance as SUCCESS."""
        ti = make_indexed_ti()
        with TaskExecutor(task_instance=ti):
            pass  # no exception
        assert ti.state == TaskInstanceState.SUCCESS

    def test_exit_with_task_deferred_reraises(self, make_indexed_ti):
        """TaskDeferred must propagate unchanged through __exit__."""
        ti = make_indexed_ti()
        trigger = mock.Mock()
        deferred = TaskDeferred(trigger=trigger, method_name="resume")

        with pytest.raises(TaskDeferred):
            with TaskExecutor(task_instance=ti):
                raise deferred

    def test_exit_reschedules_when_retries_remain(self, make_indexed_ti):
        """
        When a retryable exception occurs and retries are not exhausted,
        the task state is set to UP_FOR_RESCHEDULE and
        AirflowRescheduleTaskInstanceException is raised.
        """
        # try_number=0 → next_try_number=1; max_tries=3 → 1 <= 3, reschedule
        ti = make_indexed_ti(try_number=0, max_tries=3)

        with pytest.raises(AirflowRescheduleTaskInstanceException):
            with TaskExecutor(task_instance=ti):
                raise RuntimeError("transient failure")

        assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
        assert ti.try_number == 1  # incremented

    def test_exit_fails_when_retries_exhausted(self, make_indexed_ti):
        """
        When a retryable exception occurs and all retries are exhausted,
        the task state is set to FAILED and the original exception is re-raised.
        """
        # try_number=3 → next_try_number=4; max_tries=3 → 4 > 3, fail
        ti = make_indexed_ti(try_number=3, max_tries=3)
        original_error = RuntimeError("permanent failure")

        with pytest.raises(RuntimeError, match="permanent failure"):
            with TaskExecutor(task_instance=ti):
                raise original_error

        assert ti.state == TaskInstanceState.FAILED

    @pytest.mark.parametrize(
        ("try_number", "max_tries", "should_fail"),
        [
            (0, 0, True),  # next=1, max=0 → 1>0 → fail
            (0, 1, False),  # next=1, max=1 → 1<=1 → reschedule
            (1, 1, True),  # next=2, max=1 → 2>1 → fail
            (2, 3, False),  # next=3, max=3 → 3<=3 → reschedule
            (3, 3, True),  # next=4, max=3 → 4>3 → fail
        ],
    )
    def test_exit_retry_boundary(self, make_indexed_ti, try_number, max_tries, should_fail):
        """Exhaustive boundary checks for the retry/fail decision in __exit__."""
        ti = make_indexed_ti(try_number=try_number, max_tries=max_tries)
        if should_fail:
            with pytest.raises(RuntimeError):
                with TaskExecutor(task_instance=ti):
                    raise RuntimeError("err")
            assert ti.state == TaskInstanceState.FAILED
        else:
            with pytest.raises(AirflowRescheduleTaskInstanceException):
                with TaskExecutor(task_instance=ti):
                    raise RuntimeError("err")
            assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE

    def test_run_delegates_to_execute_task(self, make_indexed_ti):
        """run() must call _execute_task with the given context."""
        ti = make_indexed_ti()
        task = BaseOperator(task_id="test_task")
        get_inline_dag("test_dag", task)
        context = mock_context(task)
        executor = TaskExecutor(task_instance=ti)

        with mock.patch(
            "airflow.sdk.execution_time.executor._execute_task",
            autospec=True,
            return_value="result",
        ) as mock_execute:
            result = executor.run(context)

        mock_execute.assert_called_once_with(context, ti, executor.log)
        assert result == "result"

    @pytest.mark.asyncio
    async def test_arun_delegates_to_execute_async_task(self, make_indexed_ti):
        """arun() must call _execute_async_task with the given context."""
        ti = make_indexed_ti(is_async=True)
        task = BaseOperator(task_id="test_task")
        get_inline_dag("test_dag", task)
        context = mock_context(task)
        executor = TaskExecutor(task_instance=ti)

        with mock.patch(
            "airflow.sdk.execution_time.executor._execute_async_task",
            new=mock.AsyncMock(return_value="async_result"),
        ) as mock_async_execute:
            result = await executor.arun(context)

        mock_async_execute.assert_called_once_with(context, ti, executor.log)
        assert result == "async_result"

    @pytest.mark.parametrize(
        "base_exception",
        [
            SystemExit(1),
            KeyboardInterrupt(),
            GeneratorExit(),
        ],
        ids=["SystemExit", "KeyboardInterrupt", "GeneratorExit"],
    )
    def test_exit_base_exception_not_retried(self, make_indexed_ti, base_exception):
        """
        BaseException subclasses (e.g., SystemExit, KeyboardInterrupt) must never
        be retried—they signal conditions where continuing is meaningless.
        They should be re-raised immediately and mark the task as FAILED.
        """
        ti = make_indexed_ti(try_number=0, max_tries=3)

        with pytest.raises(type(base_exception)):
            with TaskExecutor(task_instance=ti):
                raise base_exception

        assert ti.state == TaskInstanceState.FAILED
        # try_number should NOT be incremented for BaseException
        assert ti.try_number == 0
