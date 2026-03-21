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
from concurrent.futures import Future
from datetime import timedelta
from unittest import mock

import pytest
from task_sdk.execution_time.test_task_runner import get_inline_dag
from uuid6 import uuid7

from airflow.sdk import BaseAsyncOperator, BaseOperator
from airflow.sdk.api.datamodels._generated import TaskInstanceState
from airflow.sdk.bases.operator import event_loop
from airflow.sdk.exceptions import AirflowRescheduleTaskInstanceException, TaskDeferred
from airflow.sdk.execution_time.executor import ConcurrentExecutor, TaskExecutor, collect_futures
from airflow.sdk.execution_time.task_runner import MappedTaskInstance

from tests_common.test_utils.mock_context import mock_context


def _make_mapped_ti(
    *,
    task_id: str = "my_task",
    dag_id: str = "my_dag",
    run_id: str = "run_1",
    map_index: int = 0,
    try_number: int = 0,
    max_tries: int = 3,
    is_async: bool = False,
) -> MappedTaskInstance:
    """Create a MappedTaskInstance via model_construct to bypass full Pydantic validation."""
    operator_cls = BaseAsyncOperator if is_async else BaseOperator
    operator = mock.create_autospec(operator_cls, instance=True)
    operator.task_id = task_id
    operator.dag_id = dag_id
    operator.is_async = is_async
    operator.retries = max_tries
    operator.retry_delay = timedelta(seconds=300)
    operator.retry_exponential_backoff = False
    operator.max_retry_delay = None

    return MappedTaskInstance.model_construct(
        id=uuid7(),
        task_id=task_id,
        dag_id=dag_id,
        run_id=run_id,
        map_index=map_index,
        try_number=try_number,
        max_tries=max_tries,
        state=TaskInstanceState.SCHEDULED,
        is_mapped=True,
        task=operator,
        xcoms={},
        dag_version_id=uuid7(),
    )


class TestConcurrentExecutor:
    def test_submit_sync_function_returns_future(self):
        """Sync callables are dispatched to the thread pool and return a concurrent.futures.Future."""
        with event_loop() as loop:
            with ConcurrentExecutor(loop=loop, max_workers=2) as executor:
                future = executor.submit(lambda: 42)
                assert isinstance(future, Future)
                assert future.result() == 42

    def test_submit_async_coroutine_function_returns_task(self):
        """Async callables are scheduled on the event loop and return an asyncio.Task."""
        with event_loop() as loop:
            with ConcurrentExecutor(loop=loop, max_workers=2) as executor:

                async def async_fn():
                    return "async_result"

                task = executor.submit(async_fn)
                assert isinstance(task, asyncio.Task)
                result = loop.run_until_complete(task)
                assert result == "async_result"

    def test_submit_coroutine_object_returns_task(self):
        """Passing a coroutine object (not a function) directly is also scheduled on the event loop."""
        with event_loop() as loop:
            with ConcurrentExecutor(loop=loop, max_workers=2) as executor:

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
            with ConcurrentExecutor(loop=loop, max_workers=2) as executor:
                future = executor.submit(lambda: (_ for _ in ()).throw(ValueError("boom")))
                assert isinstance(future, Future)
                with pytest.raises(ValueError, match="boom"):
                    future.result()

    def test_submit_async_function_propagates_exception(self):
        """Exceptions raised inside async callables are propagated when the task is awaited."""
        with event_loop() as loop:
            with ConcurrentExecutor(loop=loop, max_workers=2) as executor:

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
            with ConcurrentExecutor(loop=loop, max_workers=max_workers) as executor:
                tasks = [executor.submit(count_concurrent) for _ in range(6)]
                loop.run_until_complete(asyncio.gather(*tasks))

        assert concurrency_high_watermark <= max_workers

    def test_exit_shuts_down_thread_pool(self):
        """__exit__ calls shutdown on the thread pool."""
        with event_loop() as loop:
            executor = ConcurrentExecutor(loop=loop, max_workers=2)
            with mock.patch.object(
                executor._thread_pool, "shutdown", wraps=executor._thread_pool.shutdown
            ) as shutdown_mock:
                with executor:
                    pass
                shutdown_mock.assert_called_once_with(wait=True)

    def test_context_manager_returns_self(self):
        """__enter__ returns the executor instance itself."""
        with event_loop() as loop:
            executor = ConcurrentExecutor(loop=loop, max_workers=2)
            with executor as ctx:
                assert ctx is executor


class TestCollectFutures:
    def test_yields_sync_futures(self):
        """collect_futures yields completed concurrent.futures.Future objects."""
        with event_loop() as loop:
            f1: Future = Future()
            f2: Future = Future()
            f1.set_result("a")
            f2.set_result("b")

            results = list(collect_futures(loop, [f1, f2]))
            assert set(results) == {f1, f2}

    def test_yields_async_tasks(self):
        """collect_futures yields completed asyncio.Task objects."""
        with event_loop() as loop:

            async def coro(val):
                return val

            t1 = loop.create_task(coro("x"))
            t2 = loop.create_task(coro("y"))

            results = list(collect_futures(loop, [t1, t2]))
            assert set(results) == {t1, t2}

    def test_yields_mixed_futures_and_tasks(self):
        """collect_futures handles a mix of concurrent.futures.Future and asyncio.Task."""
        with event_loop() as loop:
            f: Future = Future()
            f.set_result(1)

            async def coro():
                return 2

            t = loop.create_task(coro())

            results = list(collect_futures(loop, [f, t]))
            assert len(results) == 2
            assert f in results
            assert t in results

    def test_empty_list_yields_nothing(self):
        """collect_futures with an empty list yields nothing."""
        with event_loop() as loop:
            results = list(collect_futures(loop, []))
            assert results == []


class TestTaskExecutor:
    def test_dag_id_property(self):
        ti = _make_mapped_ti(dag_id="my_dag")
        executor = TaskExecutor(task_instance=ti)
        assert executor.dag_id == "my_dag"

    def test_task_id_property(self):
        ti = _make_mapped_ti(task_id="my_task")
        executor = TaskExecutor(task_instance=ti)
        assert executor.task_id == "my_task"

    def test_task_index_returns_map_index(self):
        ti = _make_mapped_ti(map_index=3)
        executor = TaskExecutor(task_instance=ti)
        assert executor.task_index == 3

    def test_task_index_raises_when_map_index_is_none(self):
        """task_index must raise ValueError when map_index is None (e.g. unmapped task)."""
        ti = _make_mapped_ti()
        ti.map_index = None  # force None via attribute assignment after construction
        executor = TaskExecutor(task_instance=ti)
        with pytest.raises(ValueError, match="map_index should not be None"):
            _ = executor.task_index

    def test_key_property_returns_xcom_key(self):
        ti = _make_mapped_ti(task_id="op", map_index=2)
        executor = TaskExecutor(task_instance=ti)
        assert executor.key == ti.xcom_key
        assert executor.key == "op_2"

    def test_operator_property(self):
        ti = _make_mapped_ti()
        executor = TaskExecutor(task_instance=ti)
        assert executor.operator is ti.task

    def test_is_async_property_sync(self):
        ti = _make_mapped_ti(is_async=False)
        executor = TaskExecutor(task_instance=ti)
        assert executor.is_async is False

    def test_is_async_property_async(self):
        ti = _make_mapped_ti(is_async=True)
        executor = TaskExecutor(task_instance=ti)
        assert executor.is_async is True

    def test_enter_sets_start_time(self):
        ti = _make_mapped_ti()
        executor = TaskExecutor(task_instance=ti)
        assert executor._start_time is None
        executor.__enter__()
        assert executor._start_time is not None

    def test_enter_returns_self(self):
        ti = _make_mapped_ti()
        executor = TaskExecutor(task_instance=ti)
        with executor as ctx:
            assert ctx is executor

    def test_exit_success_sets_state(self):
        """__exit__ without an exception marks the task instance as SUCCESS."""
        ti = _make_mapped_ti()
        with TaskExecutor(task_instance=ti):
            pass  # no exception
        assert ti.state == TaskInstanceState.SUCCESS

    def test_exit_with_task_deferred_reraises(self):
        """TaskDeferred must propagate unchanged through __exit__."""
        ti = _make_mapped_ti()
        trigger = mock.Mock()
        deferred = TaskDeferred(trigger=trigger, method_name="resume")

        with pytest.raises(TaskDeferred):
            with TaskExecutor(task_instance=ti):
                raise deferred

    def test_exit_reschedules_when_retries_remain(self):
        """
        When a retryable exception occurs and retries are not exhausted,
        the task state is set to UP_FOR_RESCHEDULE and
        AirflowRescheduleTaskInstanceException is raised.
        """
        # try_number=0 → next_try_number=1; max_tries=3 → 1 <= 3, reschedule
        ti = _make_mapped_ti(try_number=0, max_tries=3)

        with pytest.raises(AirflowRescheduleTaskInstanceException):
            with TaskExecutor(task_instance=ti):
                raise RuntimeError("transient failure")

        assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
        assert ti.try_number == 1  # incremented

    def test_exit_fails_when_retries_exhausted(self):
        """
        When a retryable exception occurs and all retries are exhausted,
        the task state is set to FAILED and the original exception is re-raised.
        """
        # try_number=3 → next_try_number=4; max_tries=3 → 4 > 3, fail
        ti = _make_mapped_ti(try_number=3, max_tries=3)
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
    def test_exit_retry_boundary(self, try_number, max_tries, should_fail):
        """Exhaustive boundary checks for the retry/fail decision in __exit__."""
        ti = _make_mapped_ti(try_number=try_number, max_tries=max_tries)
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

    def test_run_delegates_to_execute_task(self):
        """run() must call _execute_task with the given context."""
        ti = _make_mapped_ti()
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
    async def test_arun_delegates_to_execute_async_task(self):
        """arun() must call _execute_async_task with the given context."""
        ti = _make_mapped_ti(is_async=True)
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

    @pytest.mark.asyncio
    async def test_async_context_manager_enter_returns_self(self):
        ti = _make_mapped_ti()
        executor = TaskExecutor(task_instance=ti)
        async with executor as ctx:
            assert ctx is executor

    @pytest.mark.asyncio
    async def test_async_context_manager_exit_success(self):
        ti = _make_mapped_ti()
        async with TaskExecutor(task_instance=ti):
            pass
        assert ti.state == TaskInstanceState.SUCCESS

    @pytest.mark.asyncio
    async def test_async_context_manager_exit_reschedules(self):
        ti = _make_mapped_ti(try_number=0, max_tries=3)
        with pytest.raises(AirflowRescheduleTaskInstanceException):
            async with TaskExecutor(task_instance=ti):
                raise RuntimeError("async transient failure")
        assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
