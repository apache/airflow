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

import pytest

from airflow.providers.common.ai.batch.base import BatchState
from airflow.providers.common.ai.batch.polling import (
    MAX_CONSECUTIVE_POLL_FAILURES,
    BatchPoller,
    terminal_event,
)

IN_PROGRESS = BatchState(status="in_progress", counts=None, error_message=None)


def _poller(*, end_time=1_000_000.0, cancel_on_timeout=True) -> BatchPoller:
    return BatchPoller(
        batch_id="batch_1", end_time=end_time, timeout=600, cancel_on_timeout=cancel_on_timeout
    )


class TestOnState:
    def test_in_progress_inside_the_deadline_continues(self):
        assert _poller().on_state(IN_PROGRESS, now=1.0).event is None

    def test_terminal_state_is_an_event_regardless_of_the_deadline(self):
        state = BatchState(status="completed", counts={"succeeded": 3}, error_message=None)
        outcome = _poller().on_state(state, now=2_000_000.0)
        assert outcome.event == terminal_event("batch_1", state)
        assert outcome.cancel is False

    @pytest.mark.parametrize("cancel_on_timeout", [True, False])
    def test_in_progress_past_the_deadline_is_a_timeout_that_asks_for_cancel_per_flag(
        self, cancel_on_timeout
    ):
        outcome = _poller(cancel_on_timeout=cancel_on_timeout).on_state(IN_PROGRESS, now=1_000_001.0)
        assert outcome.event["status"] == "timeout"
        assert outcome.cancel is cancel_on_timeout


class TestOnError:
    def test_fewer_than_max_failures_inside_the_deadline_continue(self):
        poller = _poller()
        for _ in range(MAX_CONSECUTIVE_POLL_FAILURES - 1):
            assert poller.on_error(RuntimeError("blip"), now=1.0).event is None

    def test_max_failures_inside_the_deadline_give_up_with_error_and_no_cancel(self):
        poller = _poller()
        outcome = None
        for _ in range(MAX_CONSECUTIVE_POLL_FAILURES):
            outcome = poller.on_error(RuntimeError("Connection reset"), now=1.0)
        assert outcome.event["status"] == "error"
        assert outcome.cancel is False
        assert f"after {MAX_CONSECUTIVE_POLL_FAILURES} consecutive failures" in outcome.event["message"]
        assert "Connection reset" in outcome.event["message"]
        assert "re-attaches" in outcome.event["message"]

    def test_a_healthy_state_resets_the_counter(self):
        poller = _poller()
        for _ in range(MAX_CONSECUTIVE_POLL_FAILURES - 1):
            poller.on_error(RuntimeError("blip"), now=1.0)
        poller.on_state(IN_PROGRESS, now=1.0)
        assert poller.consecutive_failures == 0

    def test_a_failure_past_the_deadline_is_a_timeout_immediately(self):
        outcome = _poller().on_error(RuntimeError("blip"), now=1_000_001.0)
        assert outcome.event["status"] == "timeout"
        assert outcome.cancel is True


class TestFinishTimeout:
    def test_message_names_budget_deadline_and_that_the_batch_was_cancelled(self):
        event = _poller().finish_timeout(cancelled=True)
        assert event["status"] == "timeout"
        assert "timeout=600s" in event["message"]
        assert "deadline 1970-01-12T13:46:40+00:00" in event["message"]
        assert "was cancelled (cancel_on_timeout=True); a retry submits a new one" in event["message"]

    def test_message_reports_a_failed_cancel(self):
        event = _poller().finish_timeout(cancelled=False, cancel_error="boom")
        assert "Cancelling the batch failed (boom)" in event["message"]
        assert "may still be running and billing" in event["message"]

    def test_message_says_the_batch_was_left_running_when_not_cancelling(self):
        event = _poller(cancel_on_timeout=False).finish_timeout(cancelled=False)
        assert "left running (cancel_on_timeout=False)" in event["message"]

    def test_message_includes_the_last_poll_error_when_polling_was_failing(self):
        poller = _poller()
        poller.on_error(RuntimeError("Connection reset"), now=1_000_001.0)
        event = poller.finish_timeout(cancelled=True)
        assert "The last 1 status check(s) failed: Connection reset" in event["message"]


class TestTerminalEvent:
    def test_maps_status_and_copies_counts(self):
        state = BatchState(status="expired", counts={"succeeded": 1, "expired": 2}, error_message=None)
        event = terminal_event("batch_1", state)
        assert event == {
            "status": "expired",
            "batch_id": "batch_1",
            "counts": {"succeeded": 1, "expired": 2},
            "message": "Batch batch_1 reached status 'expired'.",
        }
        assert event["counts"] is not state.counts

    def test_prefers_the_providers_error_message(self):
        state = BatchState(status="failed", counts=None, error_message="token_limit_exceeded: too many")
        assert terminal_event("batch_1", state)["message"] == "token_limit_exceeded: too many"
