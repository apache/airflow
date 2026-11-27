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
import os
import threading
from contextvars import ContextVar

import httpx2
import pytest

from airflow.providers.common.ai.utils.coroutines import (
    run_coroutine_on_thread_loop,
    run_coroutine_sync,
    run_off_loop,
)

from unit.common.ai.default_executor import default_executor_taken

_value: ContextVar[str] = ContextVar("value", default="unset")


async def _thread_name() -> str:
    return threading.current_thread().name


async def _read_value() -> str:
    return _value.get()


async def _get_running_loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_running_loop()


def _raise_keyboard_interrupt() -> None:
    raise KeyboardInterrupt


class TestRunCoroutineSync:
    def test_runs_on_the_calling_thread_when_no_loop_is_running(self):
        assert run_coroutine_sync(_thread_name()) == threading.current_thread().name

    def test_runs_on_another_thread_when_called_from_a_running_loop(self):
        async def caller() -> str:
            return run_coroutine_sync(_thread_name())

        assert asyncio.run(caller()) != threading.current_thread().name

    def test_sees_the_callers_context_variables_from_a_running_loop(self):
        async def caller() -> str:
            _value.set("caller")
            return run_coroutine_sync(_read_value())

        assert asyncio.run(caller()) == "caller"

    def test_an_error_from_the_coroutine_is_not_chained_onto_the_loop_probe(self):
        async def fails() -> None:
            raise ValueError("bad request")

        with pytest.raises(ValueError, match="bad request") as raised:
            run_coroutine_sync(fails())

        assert raised.value.__context__ is None


@pytest.mark.usefixtures("close_thread_event_loop")
class TestRunCoroutineOnThreadLoop:
    def test_an_async_client_works_across_calls(self, keep_alive_url):
        # Like the OpenAI Agents SDK's shared HTTP client: the connection it pools on the first
        # call belongs to that call's loop, so the second call needs the same loop, still open.
        client = httpx2.AsyncClient()
        try:
            first = run_coroutine_on_thread_loop(client.get(keep_alive_url))
            second = run_coroutine_on_thread_loop(client.get(keep_alive_url))
        finally:
            run_coroutine_on_thread_loop(client.aclose())

        assert (first.text, second.text) == ("ok", "ok")

    def test_an_asyncio_run_between_calls_keeps_the_loop(self):
        first = run_coroutine_on_thread_loop(_get_running_loop())
        asyncio.run(asyncio.sleep(0))

        assert run_coroutine_on_thread_loop(_get_running_loop()) is first

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
    def test_a_forked_child_does_not_reuse_the_parents_loop(self):
        # Python 3.11 keeps the parent's loop as the child's current loop; 3.12+ clears it.
        parent_loop = run_coroutine_on_thread_loop(_get_running_loop())
        read_end, write_end = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                child_loop = run_coroutine_on_thread_loop(_get_running_loop())
                os.write(write_end, b"same" if child_loop is parent_loop else b"new")
            finally:
                os._exit(0)
        os.close(write_end)
        os.waitpid(pid, 0)
        with os.fdopen(read_end, "rb") as reader:
            assert reader.read() == b"new"

    def test_replaces_a_closed_current_loop(self):
        closed = asyncio.new_event_loop()
        closed.close()
        asyncio.set_event_loop(closed)

        loop = run_coroutine_on_thread_loop(_get_running_loop())

        assert loop is not closed
        assert not loop.is_closed()

    def test_an_interrupted_call_cleans_up_before_it_returns(self):
        # execution_timeout and SIGTERM raise from a signal handler while the loop waits, which
        # leaves the call's task pending. A loop callback raising KeyboardInterrupt does the same.
        events: list[str] = []

        async def wait_then_record() -> None:
            asyncio.get_running_loop().call_soon(_raise_keyboard_interrupt)
            try:
                await asyncio.sleep(0.05)
                events.append("resumed")
            finally:
                events.append("cleaned up")

        with pytest.raises(KeyboardInterrupt):
            run_coroutine_on_thread_loop(wait_then_record())

        assert events == ["cleaned up"]
        run_coroutine_on_thread_loop(asyncio.sleep(0.1))
        assert events == ["cleaned up"]

    def test_a_failed_gather_leaves_no_child_running(self):
        events: list[str] = []

        async def fail() -> None:
            raise ValueError("boom")

        async def record_later() -> None:
            try:
                await asyncio.sleep(0.05)
                events.append("ran")
            except asyncio.CancelledError:
                events.append("cancelled")
                raise

        async def gather_both() -> None:
            await asyncio.gather(fail(), record_later())

        with pytest.raises(ValueError, match="boom"):
            run_coroutine_on_thread_loop(gather_both())

        assert events == ["cancelled"]
        run_coroutine_on_thread_loop(asyncio.sleep(0.1))
        assert events == ["cancelled"]

    def test_leaves_tasks_started_before_the_call_alone(self):
        loop = run_coroutine_on_thread_loop(_get_running_loop())
        earlier = loop.create_task(asyncio.sleep(0.1))
        try:
            run_coroutine_on_thread_loop(asyncio.sleep(0))

            assert not earlier.cancelled()
        finally:
            earlier.cancel()

    def test_concurrent_calls_from_two_threads_run_on_separate_loops(self):
        barrier = threading.Barrier(2)
        main_loop = run_coroutine_on_thread_loop(_get_running_loop())
        loops: list[asyncio.AbstractEventLoop] = []

        async def get_loop_after_overlap() -> asyncio.AbstractEventLoop:
            await asyncio.sleep(0.05)  # overlaps the other thread's call
            return asyncio.get_running_loop()

        def call() -> None:
            barrier.wait()
            loops.append(run_coroutine_on_thread_loop(get_loop_after_overlap()))

        threads = [threading.Thread(target=call) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len({main_loop, *loops}) == 3
        for loop in loops:
            loop.close()

    def test_runs_on_another_thread_when_called_from_a_running_loop(self):
        async def caller() -> str:
            return run_coroutine_on_thread_loop(_thread_name())

        assert asyncio.run(caller()) != threading.current_thread().name


class TestRunOffLoop:
    @pytest.mark.asyncio
    async def test_runs_on_another_thread_than_the_event_loop(self):
        assert await run_off_loop(threading.get_ident) != threading.get_ident()

    @pytest.mark.asyncio
    async def test_sees_the_callers_context_variables(self):
        _value.set("caller")

        assert await run_off_loop(_value.get) == "caller"

    @pytest.mark.asyncio
    async def test_passes_the_arguments_and_raises_what_the_call_raises(self):
        assert await run_off_loop(int, "ff", base=16) == 255
        with pytest.raises(ValueError, match="invalid literal"):
            await run_off_loop(int, "not a number")

    @pytest.mark.asyncio
    async def test_does_not_need_the_default_executor(self):
        async with default_executor_taken():
            result = await asyncio.wait_for(run_off_loop(threading.get_ident), timeout=5)

        assert result != threading.get_ident()
