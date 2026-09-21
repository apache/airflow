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

from airflow.providers.common.ai.batch.base import BatchAdapter, BatchState
from airflow.providers.common.ai.triggers import llm_batch as trigger_module
from airflow.providers.common.ai.triggers.llm_batch import MAX_CONSECUTIVE_POLL_FAILURES, LLMBatchTrigger

IN_PROGRESS = BatchState(status="in_progress", counts=None, error_message=None)
COMPLETED = BatchState(
    status="completed",
    counts={"succeeded": 1, "errored": 0, "expired": 0, "cancelled": 0},
    error_message=None,
)


def _trigger(*, end_time=None, poll_interval=1, cancel_on_kill=True, cancel_on_timeout=True, timeout=3600):
    return LLMBatchTrigger(
        llm_conn_id="my_openai",
        adapter="openai",
        batch_id="batch_1",
        poll_interval=poll_interval,
        end_time=end_time if end_time is not None else time.time() + 3600,
        timeout=timeout,
        cancel_on_kill=cancel_on_kill,
        cancel_on_timeout=cancel_on_timeout,
    )


def _adapter(*get_batch_results) -> mock.Mock:
    adapter = mock.Mock(spec=BatchAdapter)
    adapter.get_batch.side_effect = list(get_batch_results)
    return adapter


async def _run_to_single_event(trigger: LLMBatchTrigger) -> dict:
    """Consume exactly one event and prove the generator then stops, without ever sleeping for real."""
    gen = trigger.run()
    event = await asyncio.wait_for(gen.__anext__(), 5)
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(gen.__anext__(), 5)
    return event.payload


@pytest.fixture
def no_sleep():
    with mock.patch.object(trigger_module.asyncio, "sleep", autospec=True) as sleep:
        yield sleep


@pytest.fixture
def build_adapter():
    with mock.patch.object(trigger_module, "build_adapter", autospec=True) as build:
        yield build


class TestSerialize:
    def test_round_trips_every_constructor_argument(self):
        trigger = _trigger(
            end_time=1234.5, poll_interval=45, cancel_on_kill=False, cancel_on_timeout=False, timeout=99
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.common.ai.triggers.llm_batch.LLMBatchTrigger"
        assert kwargs == {
            "llm_conn_id": "my_openai",
            "adapter": "openai",
            "batch_id": "batch_1",
            "poll_interval": 45,
            "end_time": 1234.5,
            "timeout": 99,
            "cancel_on_kill": False,
            "cancel_on_timeout": False,
        }
        assert LLMBatchTrigger(**kwargs).serialize() == (classpath, kwargs)

    def test_timeout_defaults_for_triggers_serialized_before_it_existed(self):
        _, kwargs = _trigger().serialize()
        kwargs.pop("timeout")
        assert LLMBatchTrigger(**kwargs).timeout == 0


class TestBlockingCallsRunOffTheEventLoop:
    @pytest.mark.asyncio
    async def test_build_adapter_get_batch_and_close_go_through_to_thread(self, build_adapter):
        adapter = _adapter(COMPLETED)
        build_adapter.return_value = adapter

        async def passthrough(func, *args, **kwargs):
            return func(*args, **kwargs)

        with mock.patch.object(
            trigger_module.asyncio, "to_thread", autospec=True, side_effect=passthrough
        ) as to_thread:
            payload = await _run_to_single_event(_trigger())

        to_thread.assert_any_call(build_adapter, "openai", llm_conn_id="my_openai")
        to_thread.assert_any_call(adapter.get_batch, "batch_1")
        to_thread.assert_any_call(adapter.close)
        assert payload["status"] == "success"


class TestTerminalStatusMapping:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "expected"),
        [("completed", "success"), ("failed", "failed"), ("expired", "expired"), ("cancelled", "cancelled")],
    )
    async def test_each_terminal_status_yields_its_own_event_and_stops(self, build_adapter, status, expected):
        counts = {"succeeded": 2, "errored": 1, "expired": 0, "cancelled": 0}
        adapter = _adapter(BatchState(status=status, counts=counts, error_message=None))
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(_trigger())

        assert payload["status"] == expected
        assert payload["batch_id"] == "batch_1"
        assert payload["counts"] == counts
        assert adapter.cancel_batch.call_count == 0
        adapter.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_failed_carries_the_providers_message_or_a_fallback(self, build_adapter):
        build_adapter.return_value = _adapter(
            BatchState(status="failed", counts=None, error_message="bad request")
        )
        assert (await _run_to_single_event(_trigger()))["message"] == "bad request"

        build_adapter.return_value = _adapter(BatchState(status="failed", counts=None, error_message=None))
        payload = await _run_to_single_event(_trigger())
        assert payload["message"] == "Batch batch_1 reached status 'failed'."
        assert payload["counts"] is None

    @pytest.mark.asyncio
    async def test_polls_until_terminal_sleeping_poll_interval_between_checks(self, build_adapter, no_sleep):
        adapter = _adapter(IN_PROGRESS, IN_PROGRESS, COMPLETED)
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(_trigger(poll_interval=7))

        assert payload["status"] == "success"
        assert adapter.get_batch.call_count == 3
        assert no_sleep.await_count == 2
        no_sleep.assert_awaited_with(7)


class TestDeadlineTimeout:
    @pytest.mark.asyncio
    async def test_a_terminal_status_on_the_final_poll_wins_over_the_deadline(self, build_adapter):
        adapter = _adapter(COMPLETED)
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(_trigger(end_time=time.time() - 1))

        assert payload["status"] == "success"
        assert adapter.cancel_batch.call_count == 0

    @pytest.mark.asyncio
    async def test_cancel_on_timeout_true_cancels_and_says_so(self, build_adapter):
        adapter = _adapter(IN_PROGRESS)
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(
            _trigger(end_time=time.time() - 1, timeout=60, cancel_on_timeout=True)
        )

        adapter.cancel_batch.assert_called_once_with("batch_1")
        assert payload["status"] == "timeout"
        assert "timeout=60s" in payload["message"]
        assert "deadline" in payload["message"]
        assert "was cancelled (cancel_on_timeout=True)" in payload["message"]

    @pytest.mark.asyncio
    async def test_cancel_on_timeout_false_leaves_the_batch_running_and_says_so(self, build_adapter):
        adapter = _adapter(IN_PROGRESS)
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(_trigger(end_time=time.time() - 1, cancel_on_timeout=False))

        assert adapter.cancel_batch.call_count == 0
        assert payload["status"] == "timeout"
        assert "left running (cancel_on_timeout=False)" in payload["message"]

    @pytest.mark.asyncio
    async def test_a_failing_cancel_still_yields_timeout_and_reports_the_failure(self, build_adapter):
        adapter = _adapter(IN_PROGRESS)
        adapter.cancel_batch.side_effect = RuntimeError("cancel endpoint down")
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(_trigger(end_time=time.time() - 1))

        assert payload["status"] == "timeout"
        assert "Cancelling the batch failed (cancel endpoint down)" in payload["message"]


class TestOnKill:
    @pytest.mark.asyncio
    async def test_cancel_on_kill_true_cancels_and_closes(self, build_adapter):
        adapter = _adapter()
        build_adapter.return_value = adapter

        await _trigger(cancel_on_kill=True).on_kill()

        adapter.cancel_batch.assert_called_once_with("batch_1")
        adapter.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_cancel_on_kill_false_does_not_even_build_an_adapter(self, build_adapter):
        await _trigger(cancel_on_kill=False).on_kill()
        build_adapter.assert_not_called()

    @pytest.mark.asyncio
    async def test_build_adapter_failure_is_caught_not_raised(self, build_adapter):
        build_adapter.side_effect = RuntimeError("no such connection")
        await _trigger(cancel_on_kill=True).on_kill()

    @pytest.mark.asyncio
    async def test_cancel_failure_is_caught_and_the_adapter_is_still_closed(self, build_adapter):
        adapter = _adapter()
        adapter.cancel_batch.side_effect = RuntimeError("cancel endpoint down")
        build_adapter.return_value = adapter

        await _trigger(cancel_on_kill=True).on_kill()

        adapter.close.assert_called_once_with()


class TestCancelOnKillTimeoutMatrix:
    """The two flags are independent: neither may borrow the other's setting."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cancel_on_kill", [True, False])
    @pytest.mark.parametrize("cancel_on_timeout", [True, False])
    async def test_kill_and_timeout_cancellation_follow_their_own_flag_only(
        self, build_adapter, cancel_on_kill, cancel_on_timeout
    ):
        killed_adapter = _adapter()
        build_adapter.return_value = killed_adapter
        await _trigger(cancel_on_kill=cancel_on_kill, cancel_on_timeout=cancel_on_timeout).on_kill()
        assert killed_adapter.cancel_batch.call_count == (1 if cancel_on_kill else 0)

        timeout_adapter = _adapter(IN_PROGRESS)
        build_adapter.return_value = timeout_adapter
        payload = await _run_to_single_event(
            _trigger(
                end_time=time.time() - 1, cancel_on_kill=cancel_on_kill, cancel_on_timeout=cancel_on_timeout
            )
        )
        assert payload["status"] == "timeout"
        assert timeout_adapter.cancel_batch.call_count == (1 if cancel_on_timeout else 0)


class TestConsecutivePollFailures:
    @pytest.mark.asyncio
    async def test_gives_up_after_max_consecutive_failures_with_an_error_event(self, build_adapter, no_sleep):
        adapter = _adapter(*[RuntimeError("Connection reset by peer")] * MAX_CONSECUTIVE_POLL_FAILURES)
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(_trigger())

        assert payload["status"] == "error"
        assert f"after {MAX_CONSECUTIVE_POLL_FAILURES} consecutive failures" in payload["message"]
        assert "Connection reset by peer" in payload["message"]
        assert adapter.get_batch.call_count == MAX_CONSECUTIVE_POLL_FAILURES
        assert adapter.cancel_batch.call_count == 0
        assert no_sleep.await_count == MAX_CONSECUTIVE_POLL_FAILURES - 1

    @pytest.mark.asyncio
    async def test_a_healthy_poll_resets_the_failure_counter(self, build_adapter, no_sleep):
        failures = [RuntimeError("blip")] * (MAX_CONSECUTIVE_POLL_FAILURES - 1)
        adapter = _adapter(*failures, IN_PROGRESS, *failures, COMPLETED)
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(_trigger())

        assert payload["status"] == "success"
        assert adapter.get_batch.call_count == 2 * len(failures) + 2

    @pytest.mark.asyncio
    async def test_giving_up_past_the_deadline_is_a_real_timeout_on_the_first_failure(self, build_adapter):
        adapter = _adapter(RuntimeError("Connection reset by peer"))
        build_adapter.return_value = adapter

        payload = await _run_to_single_event(_trigger(end_time=time.time() - 1, timeout=30))

        assert payload["status"] == "timeout"
        assert adapter.get_batch.call_count == 1
        adapter.cancel_batch.assert_called_once_with("batch_1")
        assert "timeout=30s" in payload["message"]
        assert "The last 1 status check(s) failed: Connection reset by peer" in payload["message"]
