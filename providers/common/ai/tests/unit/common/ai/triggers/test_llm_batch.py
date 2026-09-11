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

import time
from unittest import mock

import pytest

from airflow.providers.common.ai.batch.base import BatchState
from airflow.providers.common.ai.triggers.llm_batch import MAX_CONSECUTIVE_POLL_FAILURES, LLMBatchTrigger

TRIGGER_PATH = "airflow.providers.common.ai.triggers.llm_batch"


def _trigger(*, end_time=None, poll_interval=1, cancel_on_kill=True, cancel_on_timeout=True):
    return LLMBatchTrigger(
        llm_conn_id="my_openai",
        adapter="openai",
        batch_id="batch_1",
        poll_interval=poll_interval,
        end_time=end_time if end_time is not None else time.time() + 3600,
        cancel_on_kill=cancel_on_kill,
        cancel_on_timeout=cancel_on_timeout,
    )


class TestSerialize:
    def test_excludes_result_path_requests_output_type(self):
        """§4: the trigger never downloads or validates results, so these have no business here."""
        _, kwargs = _trigger().serialize()
        assert "result_path" not in kwargs
        assert "requests" not in kwargs
        assert "idempotency_key" not in kwargs
        assert "output_type" not in kwargs

    def test_includes_the_fields_the_trigger_actually_needs(self):
        classpath, kwargs = _trigger().serialize()
        assert classpath == f"{TRIGGER_PATH}.LLMBatchTrigger"
        assert kwargs["llm_conn_id"] == "my_openai"
        assert kwargs["adapter"] == "openai"
        assert kwargs["batch_id"] == "batch_1"


class TestPollingUsesToThread:
    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    @mock.patch(f"{TRIGGER_PATH}.asyncio.to_thread")
    async def test_get_batch_is_invoked_through_to_thread(self, mock_to_thread, mock_build_adapter):
        """
        Must go through ``asyncio.to_thread`` (providers/anthropic's pattern), never call the
        blocking SDK client directly on the event loop (the anti-pattern in
        providers/openai/.../triggers/openai.py:100).
        """
        fake_adapter = mock.Mock()
        mock_build_adapter.return_value = fake_adapter

        async def fake_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)

        mock_to_thread.side_effect = fake_to_thread
        fake_adapter.get_batch.return_value = BatchState(
            status="completed",
            counts={"succeeded": 1, "errored": 0, "expired": 0, "cancelled": 0},
            error_message=None,
        )

        events = [event async for event in _trigger().run()]

        mock_to_thread.assert_any_call(fake_adapter.get_batch, "batch_1")
        assert events[0].payload["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    @mock.patch(f"{TRIGGER_PATH}.asyncio.to_thread")
    async def test_build_adapter_is_also_invoked_through_to_thread(self, mock_to_thread, mock_build_adapter):
        """
        M13: ``build_adapter`` does blocking I/O (connection lookup, SDK client construction) --
        it must not run directly on the event loop either, same reasoning as ``get_batch``.
        """
        fake_adapter = mock.Mock()
        mock_build_adapter.return_value = fake_adapter

        async def fake_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)

        mock_to_thread.side_effect = fake_to_thread
        fake_adapter.get_batch.return_value = BatchState(status="completed", counts=None, error_message=None)

        [event async for event in _trigger().run()]

        mock_to_thread.assert_any_call(mock_build_adapter, "openai", llm_conn_id="my_openai")


class TestTerminalStatusMapping:
    async def _run_with_status(self, status, *, counts=None, error_message=None):
        with mock.patch(f"{TRIGGER_PATH}.build_adapter") as mock_build_adapter:
            fake_adapter = mock.Mock()
            fake_adapter.get_batch.return_value = BatchState(
                status=status, counts=counts, error_message=error_message
            )
            mock_build_adapter.return_value = fake_adapter
            events = [event async for event in _trigger().run()]
        return events, fake_adapter

    @pytest.mark.asyncio
    async def test_completed_maps_to_success(self):
        events, _ = await self._run_with_status(
            "completed", counts={"succeeded": 5, "errored": 0, "expired": 0, "cancelled": 0}
        )
        assert len(events) == 1
        assert events[0].payload == {
            "status": "success",
            "batch_id": "batch_1",
            "counts": {"succeeded": 5, "errored": 0, "expired": 0, "cancelled": 0},
            "message": mock.ANY,
        }

    @pytest.mark.asyncio
    async def test_failed_maps_to_failed(self):
        events, _ = await self._run_with_status("failed", error_message="bad request")
        assert events[0].payload["status"] == "failed"
        assert events[0].payload["message"] == "bad request"

    @pytest.mark.asyncio
    async def test_provider_expired_maps_to_its_own_status_not_timeout(self):
        """
        M5: "expired" is its own event status, not folded into "timeout" -- it must route
        through finalize (partial results, already billed), which "timeout" explicitly does not.
        """
        events, _ = await self._run_with_status("expired")
        assert events[0].payload["status"] == "expired"

    @pytest.mark.asyncio
    async def test_provider_cancelled_maps_to_cancelled(self):
        events, _ = await self._run_with_status("cancelled")
        assert events[0].payload["status"] == "cancelled"


class TestDeadlineTimeout:
    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    async def test_cancel_on_timeout_true_cancels_the_batch(self, mock_build_adapter):
        fake_adapter = mock.Mock()
        fake_adapter.get_batch.return_value = BatchState(
            status="in_progress", counts=None, error_message=None
        )
        mock_build_adapter.return_value = fake_adapter

        events = [event async for event in _trigger(end_time=time.time() - 1, cancel_on_timeout=True).run()]

        assert events[0].payload["status"] == "timeout"
        fake_adapter.cancel_batch.assert_called_once_with("batch_1")

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    async def test_cancel_on_timeout_false_leaves_the_batch_running(self, mock_build_adapter):
        """§8: 'stop waiting, but let the paid-for batch finish' -- the batch must not be cancelled."""
        fake_adapter = mock.Mock()
        fake_adapter.get_batch.return_value = BatchState(
            status="in_progress", counts=None, error_message=None
        )
        mock_build_adapter.return_value = fake_adapter

        events = [event async for event in _trigger(end_time=time.time() - 1, cancel_on_timeout=False).run()]

        assert events[0].payload["status"] == "timeout"
        fake_adapter.cancel_batch.assert_not_called()


class TestOnKill:
    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    async def test_cancel_on_kill_true_cancels(self, mock_build_adapter):
        fake_adapter = mock.Mock()
        mock_build_adapter.return_value = fake_adapter
        await _trigger(cancel_on_kill=True).on_kill()
        fake_adapter.cancel_batch.assert_called_once_with("batch_1")

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    async def test_cancel_on_kill_false_is_a_no_op(self, mock_build_adapter):
        await _trigger(cancel_on_kill=False).on_kill()
        mock_build_adapter.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    async def test_build_adapter_failure_is_caught_not_raised(self, mock_build_adapter):
        """M12: build_adapter must be inside the same try as the cancel call, not outside it."""
        mock_build_adapter.side_effect = RuntimeError("cannot resolve connection")
        await _trigger(cancel_on_kill=True).on_kill()  # must not raise


class TestCancelOnKillTimeoutMatrix:
    """
    §8: the plan's four-combination matrix for ``cancel_on_kill`` x ``cancel_on_timeout`` --
    each combination is tested for *both* columns (killed, and defer-budget-exhausted), not just
    each flag in isolation. ``TestOnKill``/``TestDeadlineTimeout`` above already show each flag
    governs its own path alone; this test exists to catch a *combination* bug those two classes
    structurally cannot -- e.g. a future change that made one flag leak into the other's branch.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("cancel_on_kill", "cancel_on_timeout"),
        [
            (True, True),
            (True, False),
            (False, True),
            (False, False),
        ],
    )
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    async def test_kill_and_timeout_cancellation_follow_their_own_flag_only(
        self, mock_build_adapter, cancel_on_kill, cancel_on_timeout
    ):
        # Column 1: killed -- governed by cancel_on_kill alone (plan §8).
        killed_adapter = mock.Mock()
        mock_build_adapter.return_value = killed_adapter
        await _trigger(cancel_on_kill=cancel_on_kill, cancel_on_timeout=cancel_on_timeout).on_kill()
        if cancel_on_kill:
            killed_adapter.cancel_batch.assert_called_once_with("batch_1")
        else:
            killed_adapter.cancel_batch.assert_not_called()

        # Column 2: defer budget exhausted -- governed by cancel_on_timeout alone (plan §8).
        timeout_adapter = mock.Mock()
        timeout_adapter.get_batch.return_value = BatchState(
            status="in_progress", counts=None, error_message=None
        )
        mock_build_adapter.return_value = timeout_adapter
        events = [
            event
            async for event in _trigger(
                end_time=time.time() - 1,
                cancel_on_kill=cancel_on_kill,
                cancel_on_timeout=cancel_on_timeout,
            ).run()
        ]
        assert events[0].payload["status"] == "timeout"
        if cancel_on_timeout:
            timeout_adapter.cancel_batch.assert_called_once_with("batch_1")
        else:
            timeout_adapter.cancel_batch.assert_not_called()


class TestConsecutivePollFailures:
    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.asyncio.sleep", new_callable=mock.AsyncMock)
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    async def test_gives_up_after_max_consecutive_failures(self, mock_build_adapter, mock_sleep):
        fake_adapter = mock.Mock()
        fake_adapter.get_batch.side_effect = RuntimeError("transient")
        mock_build_adapter.return_value = fake_adapter

        events = [event async for event in _trigger().run()]

        assert events[0].payload["status"] == "error"
        assert fake_adapter.get_batch.call_count == MAX_CONSECUTIVE_POLL_FAILURES
        fake_adapter.cancel_batch.assert_not_called()  # batch health unknown -- do not act on it

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.asyncio.sleep", new_callable=mock.AsyncMock)
    @mock.patch(f"{TRIGGER_PATH}.build_adapter")
    async def test_giving_up_past_the_deadline_is_a_real_timeout_and_cancels(
        self, mock_build_adapter, mock_sleep
    ):
        """
        M11: giving up on poll failures *while past end_time* is a real timeout (§8) and must
        honor cancel_on_timeout, not silently skip cancellation the way a bare "error" would.
        """
        fake_adapter = mock.Mock()
        fake_adapter.get_batch.side_effect = RuntimeError("transient")
        mock_build_adapter.return_value = fake_adapter

        events = [event async for event in _trigger(end_time=time.time() - 1, cancel_on_timeout=True).run()]

        assert events[0].payload["status"] == "timeout"
        fake_adapter.cancel_batch.assert_called_once_with("batch_1")
