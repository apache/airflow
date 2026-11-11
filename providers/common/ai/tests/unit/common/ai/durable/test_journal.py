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
"""The framework-neutral journal, driven directly the way an adapter for any agent framework would."""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.common.ai.durable import journal as journal_module
from airflow.providers.common.ai.durable.base import (
    RUN_ID_KEY,
    RUNS_KEY,
    build_run_meta_key,
    build_step_key,
)
from airflow.providers.common.ai.durable.journal import (
    DurableJournal,
    DurableRun,
    current_journal,
    journal_scope,
)


async def ok(value):
    return value


async def boom():
    raise RuntimeError("tool failed")


async def raise_(error: BaseException):
    raise error


class TestReplay:
    @pytest.mark.asyncio
    async def test_a_matching_step_replays_and_its_body_does_not_run(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run(
            "agent__model.request", kind="model", fingerprint="fp", body=lambda: ok({"text": "hi"})
        )
        body = mock.AsyncMock(spec=ok)

        retry = DurableJournal(memory_storage).start_run()
        payload = await retry.run("agent__model.request", kind="model", fingerprint="fp", body=body)

        assert payload == {"text": "hi"}
        body.assert_not_called()
        assert retry.journal.stats.replayed["model"] == 1

    @pytest.mark.parametrize(
        ("name", "fingerprint"),
        [
            pytest.param("agent__other.call", "fp", id="different step at the position"),
            pytest.param("agent__model.request", "changed", id="same step asked something else"),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_mismatch_runs_that_step_and_every_later_one_live(
        self, memory_storage, name, fingerprint
    ):
        first = DurableJournal(memory_storage).start_run()
        await first.run("agent__model.request", kind="model", fingerprint="fp", body=lambda: ok(1))
        await first.run("agent__tool.call", kind="tool", fingerprint="t", body=lambda: ok(2))

        retry = DurableJournal(memory_storage).start_run()
        mismatched = await retry.run(name, kind="model", fingerprint=fingerprint, body=lambda: ok("live"))
        # Would match position 1 of the first attempt, but the run has already diverged.
        after = await retry.run("agent__tool.call", kind="tool", fingerprint="t", body=lambda: ok("live too"))

        assert (mismatched, after) == ("live", "live too")
        assert retry.diverged
        assert memory_storage.steps()[1]["payload"] == "live too"

    @pytest.mark.asyncio
    async def test_a_step_without_a_fingerprint_replays_on_its_name(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run(
            "agent__capability__ledger.accrue", kind="other", fingerprint=None, body=lambda: ok(5)
        )

        retry = DurableJournal(memory_storage).start_run()
        payload = await retry.run(
            "agent__capability__ledger.accrue", kind="other", fingerprint=None, body=lambda: ok(6)
        )

        assert payload == 5

    @pytest.mark.asyncio
    async def test_a_step_that_was_not_recorded_runs_again_and_the_rest_still_replay(self, memory_storage):
        memory_storage.refuse.add("__commonai_durable__run_0_step_0")
        first = DurableJournal(memory_storage).start_run()
        await first.run("a", kind="tool", fingerprint="1", body=lambda: ok("first"))
        await first.run("b", kind="tool", fingerprint="2", body=lambda: ok("second"))
        memory_storage.refuse.clear()

        retry = DurableJournal(memory_storage).start_run()
        a = await retry.run("a", kind="tool", fingerprint="1", body=lambda: ok("first again"))
        b = await retry.run("b", kind="tool", fingerprint="2", body=lambda: ok("not replayed"))

        assert (a, b) == ("first again", "second")
        assert first.journal.stats.not_recorded == [("tool", "a")]

    @pytest.mark.asyncio
    async def test_a_step_that_is_not_replayable_keeps_its_position_but_is_never_recorded(
        self, memory_storage
    ):
        first = DurableJournal(memory_storage).start_run()
        await first.run("managed", kind="tool", fingerprint=None, body=lambda: ok("acted"), replayable=False)
        await first.run("after", kind="tool", fingerprint="a", body=lambda: ok("recorded"))

        retry = DurableJournal(memory_storage).start_run()
        managed = await retry.run(
            "managed", kind="tool", fingerprint=None, body=lambda: ok("acted again"), replayable=False
        )
        after = await retry.run("after", kind="tool", fingerprint="a", body=lambda: ok("live"))

        assert (managed, after) == ("acted again", "recorded")
        assert [step["name"] for step in memory_storage.steps()] == ["after"]

    @pytest.mark.asyncio
    async def test_to_record_transforms_only_what_is_recorded(self, memory_storage):
        durable_run = DurableJournal(memory_storage).start_run()

        payload = await durable_run.run(
            "t", kind="tool", fingerprint=None, body=lambda: ok("secret"), to_record=lambda value: "***"
        )

        assert payload == "secret"
        assert memory_storage.steps()[0]["payload"] == "***"


class TestFailures:
    @pytest.mark.asyncio
    async def test_error_handling_of_a_failed_run_is_not_replayed(self, memory_storage):
        """What error handling recorded after the step that failed the run must not replay into the retry."""
        first = DurableJournal(memory_storage).start_run()
        error = RuntimeError("tool failed")
        with pytest.raises(RuntimeError):
            await first.run("flaky", kind="tool", fingerprint="f", body=lambda: raise_(error))
        await first.run("record_effect", kind="other", fingerprint="e", body=lambda: ok("failed"))
        first.fail(error)

        retry = DurableJournal(memory_storage).start_run()
        flaky = await retry.run("flaky", kind="tool", fingerprint="f", body=lambda: ok("ok"))
        effect = await retry.run("record_effect", kind="other", fingerprint="e", body=lambda: ok("completed"))

        assert (flaky, effect) == ("ok", "completed")

    @pytest.mark.asyncio
    async def test_steps_started_before_the_failure_still_replay(self, memory_storage):
        """A tool call running alongside the one that failed had already started, so it replays."""
        first = DurableJournal(memory_storage).start_run()
        error = RuntimeError("tool failed")
        failing = first.claim("flaky", kind="tool", fingerprint="f")
        sibling = first.claim("sibling", kind="tool", fingerprint="s")
        failing.fail(error)
        sibling.record("sibling result")
        await first.run("on_error", kind="other", fingerprint="e", body=lambda: ok("cleanup"))
        first.fail(error)

        retry = DurableJournal(memory_storage).start_run()
        await retry.run("flaky", kind="tool", fingerprint="f", body=lambda: ok("ok"))
        replayed = await retry.run("sibling", kind="tool", fingerprint="s", body=lambda: ok("ran again"))
        after = await retry.run("on_error", kind="other", fingerprint="e", body=lambda: ok("live"))

        assert (replayed, after) == ("sibling result", "live")

    @pytest.mark.asyncio
    async def test_a_failure_the_run_recovered_from_does_not_stop_replay(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        with pytest.raises(RuntimeError):
            await first.run("flaky", kind="tool", fingerprint="f", body=boom)
        await first.run("next", kind="tool", fingerprint="n", body=lambda: ok("recorded"))

        retry = DurableJournal(memory_storage).start_run()
        flaky = await retry.run("flaky", kind="tool", fingerprint="f", body=lambda: ok("ok"))
        after = await retry.run("next", kind="tool", fingerprint="n", body=lambda: ok("ran again"))

        assert (flaky, after) == ("ok", "recorded")

    @pytest.mark.asyncio
    async def test_a_run_failure_no_step_raised_keeps_everything_recorded(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run("a", kind="tool", fingerprint="a", body=lambda: ok("recorded"))
        first.fail(RuntimeError("usage limit"))

        retry = DurableJournal(memory_storage).start_run()

        assert await retry.run("a", kind="tool", fingerprint="a", body=lambda: ok("live")) == "recorded"

    @pytest.mark.asyncio
    async def test_a_failure_the_model_caused_reruns_its_last_request(self, memory_storage):
        """The model's recorded answers would replay into the same failure, so it is asked again."""
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("bad query"))
        await first.run("query", kind="tool", fingerprint="q1", body=lambda: ok("retry: no column"))
        await first.run("model", kind="model", fingerprint="m2", body=lambda: ok("bad query again"))
        await first.run("query", kind="tool", fingerprint="q2", body=lambda: ok("retry: no column"))
        first.fail(RuntimeError("exceeded max retries"), model_at_fault=True)

        retry = DurableJournal(memory_storage).start_run()
        results = [
            await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("live")),
            await retry.run("query", kind="tool", fingerprint="q1", body=lambda: ok("live")),
            await retry.run("model", kind="model", fingerprint="m2", body=lambda: ok("good query")),
            await retry.run("query", kind="tool", fingerprint="q2", body=lambda: ok("3 rows")),
        ]

        assert results == ["bad query", "retry: no column", "good query", "3 rows"]
        assert dict(retry.journal.stats.replayed) == {"model": 1, "tool": 1}

    def test_a_failure_the_model_caused_is_noted_for_the_retrys_log(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        first.claim("model", kind="model", fingerprint="m1").record("bad query")

        first.fail(RuntimeError("exceeded max retries"), model_at_fault=True)

        meta = memory_storage.load_step(build_run_meta_key(first.key))
        assert (meta["diverge_from"], meta["model_at_fault"]) == (0, True)

    @pytest.mark.asyncio
    async def test_a_failure_the_model_caused_while_replaying_reruns_its_last_request(self, memory_storage):
        """Otherwise an attempt that replayed everything would leave the next one replaying it too."""
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("bad"))
        await first.run("query", kind="tool", fingerprint="q1", body=lambda: ok("retry"))
        first.fail(RuntimeError("worker died"))
        second = DurableJournal(memory_storage).start_run()
        await second.run("model", kind="model", fingerprint="m1", body=lambda: ok("live"))
        await second.run("query", kind="tool", fingerprint="q1", body=lambda: ok("live"))
        second.fail(RuntimeError("exceeded max retries"), model_at_fault=True)

        third = DurableJournal(memory_storage).start_run()
        answer = await third.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again"))

        assert answer == "asked again"

    @pytest.mark.asyncio
    async def test_a_model_request_asked_again_that_raises_is_asked_again_on_the_next_retry(
        self, memory_storage
    ):
        """The rejected answer is still recorded at that position, so it must not replay."""
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("bad query"))
        await first.run("query", kind="tool", fingerprint="q1", body=lambda: ok("retry: no column"))
        first.fail(RuntimeError("exceeded max retries"), model_at_fault=True)
        second = DurableJournal(memory_storage).start_run()
        rate_limited = RuntimeError("429")
        with pytest.raises(RuntimeError):
            await second.run("model", kind="model", fingerprint="m1", body=lambda: raise_(rate_limited))
        second.fail(rate_limited)

        third = DurableJournal(memory_storage).start_run()
        answer = await third.run("model", kind="model", fingerprint="m1", body=lambda: ok("good query"))

        assert answer == "good query"

    @pytest.mark.asyncio
    async def test_a_step_that_raised_the_error_decides_even_when_the_model_is_blamed(self, memory_storage):
        """An agent a tool runs can give up on its own model; the outer model's answer was fine."""
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("call both tools"))
        error = RuntimeError("nested agent exceeded max retries")
        nested = first.claim("nested_agent", kind="tool", fingerprint="n")
        sibling = first.claim("charge_card", kind="tool", fingerprint="c")
        nested.fail(error)
        sibling.record("charged")
        first.fail(error, model_at_fault=True)

        retry = DurableJournal(memory_storage).start_run()
        model = await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again"))
        await retry.run("nested_agent", kind="tool", fingerprint="n", body=lambda: ok("ok"))
        charge = await retry.run(
            "charge_card", kind="tool", fingerprint="c", body=lambda: ok("charged twice")
        )

        assert (model, charge) == ("call both tools", "charged")

    @pytest.mark.asyncio
    async def test_a_step_without_a_fingerprint_does_not_replay_after_a_live_step(self, memory_storage):
        """The live step may have produced something else, which the unfingerprinted step would depend on."""
        memory_storage.refuse.add(build_step_key(0, 0))
        first = DurableJournal(memory_storage).start_run()
        await first.run("tool", kind="tool", fingerprint="t", body=lambda: ok("rows v1"))
        await first.run("summarize", kind="other", fingerprint=None, body=lambda: ok("summary of v1"))
        memory_storage.refuse.clear()

        retry = DurableJournal(memory_storage).start_run()
        await retry.run("tool", kind="tool", fingerprint="t", body=lambda: ok("rows v2"))
        summary = await retry.run(
            "summarize", kind="other", fingerprint=None, body=lambda: ok("summary of v2")
        )

        assert summary == "summary of v2"

    @pytest.mark.asyncio
    async def test_an_entry_a_completed_run_could_not_delete_is_not_trusted(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m", body=lambda: ok("old answer"))
        await first.run("tool", kind="tool", fingerprint="t", body=lambda: ok("old rows"))
        first.fail(RuntimeError("the answers failed the checks"), model_at_fault=True)

        retry = DurableJournal(memory_storage).start_run()
        await retry.run("model", kind="model", fingerprint="m", body=lambda: ok("new answer"))
        memory_storage.refuse.add(build_step_key(0, 1))
        await retry.run("tool", kind="tool", fingerprint="t", body=lambda: ok("new rows"))
        with mock.patch.object(memory_storage, "delete_steps", autospec=True):
            retry.complete()
        memory_storage.refuse.clear()

        later = DurableJournal(memory_storage).start_run()
        answer = await later.run("model", kind="model", fingerprint="m", body=lambda: ok("asked again"))
        rows = await later.run("tool", kind="tool", fingerprint="t", body=lambda: ok("run again"))

        assert (answer, rows) == ("new answer", "run again")


async def run_model_then_failing_tool(
    durable_run: DurableRun, answer: str, *, fingerprinted: bool = True
) -> tuple[str, ValueError]:
    """
    Fail ``durable_run`` in the tool call its model chose, and return the model's choice and the error.

    ``answer`` is what the model chooses when it is asked rather than replayed. The tool call's
    fingerprint is the model's choice, as a real tool call's carries the model's call id.
    """
    chosen = await durable_run.run("model", kind="model", fingerprint="m", body=lambda: ok(answer))
    error = ValueError(f"bad arguments: {chosen}")
    with pytest.raises(ValueError, match="bad arguments"):
        await durable_run.run(
            "query", kind="tool", fingerprint=chosen if fingerprinted else None, body=lambda: raise_(error)
        )
    durable_run.fail(error)
    return chosen, error


async def run_attempt_failing_in_tool(storage, answer: str, *, fingerprinted: bool = True) -> str:
    """Run one attempt that fails in the tool call the model chose, and return the model's choice."""
    chosen, _ = await run_model_then_failing_tool(
        DurableJournal(storage).start_run(), answer, fingerprinted=fingerprinted
    )
    return chosen


def collect_ask_model_again_attempts(log: mock.MagicMock) -> list[int]:
    """The ``attempts`` of each warning that a retry asks the model again."""
    return [
        warning.kwargs["attempts"]
        for warning in log.warning.call_args_list
        if "same tool call failed" in warning.args[0]
    ]


class TestRepeatedToolFailures:
    @pytest.mark.parametrize("fingerprinted", [True, False], ids=["fingerprinted", "no fingerprint"])
    @pytest.mark.asyncio
    async def test_a_tool_call_that_fails_twice_in_a_row_makes_the_next_retry_ask_the_model_again(
        self, memory_storage, fingerprinted
    ):
        """Each call the model chooses is retried once as it was, then the model is asked for another."""
        with mock.patch.object(journal_module, "log", spec=["debug", "warning"]) as log:
            chosen = [
                await run_attempt_failing_in_tool(memory_storage, f"call {n}", fingerprinted=fingerprinted)
                for n in range(1, 6)
            ]

        assert chosen == ["call 1", "call 1", "call 3", "call 3", "call 5"]
        assert collect_ask_model_again_attempts(log) == [2, 2]

    @pytest.mark.asyncio
    async def test_an_attempt_that_fails_before_the_tool_starts_the_count_over(self, memory_storage):
        await run_attempt_failing_in_tool(memory_storage, "call 1")
        killed = DurableJournal(memory_storage).start_run()
        await killed.run("model", kind="model", fingerprint="m", body=lambda: ok("replayed, never asked"))
        killed.fail(TimeoutError("killed"))
        chosen = [await run_attempt_failing_in_tool(memory_storage, f"call {n}") for n in range(3, 6)]

        assert chosen == ["call 1", "call 1", "call 5"]

    @pytest.mark.parametrize("through_operation", [False, True], ids=["from the tool", "from an operation"])
    @pytest.mark.asyncio
    async def test_a_tool_inside_a_nested_agent_failing_twice_asks_only_the_nested_model_again(
        self, memory_storage, through_operation
    ):
        """The outer model chose the agent tool, not the arguments that failed, so its answer still replays."""

        async def attempt(answer: str) -> tuple[str, str]:
            journal = DurableJournal(memory_storage)
            outer = journal.start_run()
            outer_choice = await outer.run(
                "model", kind="model", fingerprint="outer", body=lambda: ok(f"outer {answer}")
            )
            inner_choice: list[str] = []

            async def run_agent():
                chosen, error = await run_model_then_failing_tool(journal.start_run(), f"inner {answer}")
                inner_choice.append(chosen)
                raise error

            async def ask_agent():
                if through_operation:
                    # Such as another capability's durable operation that the tool calls.
                    await outer.run("operation", kind="other", fingerprint=None, body=run_agent)
                else:
                    await run_agent()

            with pytest.raises(ValueError, match="bad arguments") as raised:
                await outer.run("ask_agent", kind="tool", fingerprint=outer_choice, body=ask_agent)
            outer.fail(raised.value)
            return outer_choice, inner_choice[0]

        choices = [await attempt(str(n)) for n in range(1, 4)]

        assert choices == [("outer 1", "inner 1"), ("outer 1", "inner 1"), ("outer 1", "inner 3")]

    @pytest.mark.asyncio
    async def test_a_nested_agent_whose_model_request_keeps_failing_leaves_it_to_the_tool_that_ran_it(
        self, memory_storage
    ):
        """No tool call of the nested agent failed, so the outer model's arguments to it are what to change."""

        async def attempt(answer: str) -> str:
            journal = DurableJournal(memory_storage)
            outer = journal.start_run()
            outer_choice = await outer.run(
                "model", kind="model", fingerprint="outer", body=lambda: ok(f"outer {answer}")
            )

            async def ask_agent():
                inner = journal.start_run()
                error = ValueError("bad arguments: the request is too long for the model")
                try:
                    await inner.run("model", kind="model", fingerprint="inner", body=lambda: raise_(error))
                except ValueError:
                    inner.fail(error)
                    raise

            with pytest.raises(ValueError, match="bad arguments") as raised:
                await outer.run("ask_agent", kind="tool", fingerprint=outer_choice, body=ask_agent)
            outer.fail(raised.value)
            return outer_choice

        choices = [await attempt(str(n)) for n in range(1, 4)]

        assert choices == ["outer 1", "outer 1", "outer 3"]

    @pytest.mark.asyncio
    async def test_a_fallback_agent_started_while_handling_another_runs_failure_counts_its_own(
        self, memory_storage
    ):
        """Its error carries the first run's error as context, but that run is not inside its tool call."""

        async def attempt(answer: str) -> str:
            journal = DurableJournal(memory_storage)
            _, primary_error = await run_model_then_failing_tool(journal.start_run(), f"primary {answer}")
            try:
                raise primary_error
            except ValueError:
                fallback_choice, _ = await run_model_then_failing_tool(
                    journal.start_run(), f"fallback {answer}"
                )
            return fallback_choice

        choices = [await attempt(str(n)) for n in range(1, 4)]

        assert choices == ["fallback 1", "fallback 1", "fallback 3"]

    @pytest.mark.asyncio
    async def test_a_tool_call_no_model_request_chose_never_reports_asking_the_model(self, memory_storage):
        with mock.patch.object(journal_module, "log", spec=["debug", "warning"]) as log:
            for _ in range(3):
                durable_run = DurableJournal(memory_storage).start_run()
                error = ValueError("bad arguments")
                with pytest.raises(ValueError, match="bad arguments"):
                    await durable_run.run(
                        "query", kind="tool", fingerprint="q", body=lambda error=error: raise_(error)
                    )
                durable_run.fail(error)

        assert collect_ask_model_again_attempts(log) == []

    @pytest.mark.asyncio
    async def test_a_model_request_that_fails_twice_does_not_rerun_the_requests_before_it(
        self, memory_storage
    ):
        for _ in range(2):
            run = DurableJournal(memory_storage).start_run()
            await run.run("model", kind="model", fingerprint="m1", body=lambda: ok("call query"))
            await run.run("query", kind="tool", fingerprint="q", body=lambda: ok("3 rows"))
            rate_limited = RuntimeError("429")
            with pytest.raises(RuntimeError):
                await run.run(
                    "model", kind="model", fingerprint="m2", body=lambda error=rate_limited: raise_(error)
                )
            run.fail(rate_limited)

        retry = DurableJournal(memory_storage).start_run()
        first = await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again"))

        assert first == "call query"


class TestReject:
    @pytest.mark.asyncio
    async def test_a_rejected_step_and_every_step_after_it_run_live(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run("a", kind="model", fingerprint="a", body=lambda: ok("old a"))
        await first.run("b", kind="tool", fingerprint="b", body=lambda: ok("old b"))

        retry = DurableJournal(memory_storage).start_run()
        step = retry.claim("a", kind="model", fingerprint="a")
        retry.reject(step, reason="the recorded result no longer loads")
        b = await retry.run("b", kind="tool", fingerprint="b", body=lambda: ok("live b"))

        assert (step.replayed, step.payload, b) == (False, None, "live b")
        assert retry.journal.stats.replayed.total() == 0


class TestReserve:
    @staticmethod
    async def record_tool(memory_storage, *names):
        first = DurableJournal(memory_storage).start_run()
        for name in names:
            await first.run(name, kind="tool", fingerprint=name, body=lambda: ok("old"))

    @pytest.mark.asyncio
    async def test_a_reserved_step_counts_as_replayed_only_once_taken(self, memory_storage):
        await self.record_tool(memory_storage, "t")

        retry = DurableJournal(memory_storage).start_run()
        retry.reserve([("c", "t", "t")], kind="tool")
        assert retry.journal.stats.replayed.total() == 0

        step = retry.claim_reserved("c", "t", kind="tool", fingerprint="t")
        step.count_as_replayed()

        assert (step.position, step.replayed, step.payload) == (0, True, "old")
        assert dict(retry.journal.stats.replayed) == {"tool": 1}

    @pytest.mark.asyncio
    async def test_a_reserved_id_is_claimed_once(self, memory_storage):
        await self.record_tool(memory_storage, "t")

        retry = DurableJournal(memory_storage).start_run()
        retry.reserve([("c", "t", "t")], kind="tool")
        retry.claim_reserved("c", "t", kind="tool", fingerprint="t")
        again = retry.claim_reserved("c", "t", kind="tool", fingerprint="t")

        assert (again.position, again.replayed) == (1, False)
        assert dict(retry.journal.stats.replayed) == {"tool": 1}

    @pytest.mark.asyncio
    async def test_a_later_reserve_drops_the_steps_nothing_took(self, memory_storage):
        await self.record_tool(memory_storage, "t")

        retry = DurableJournal(memory_storage).start_run()
        retry.reserve([("old", "t", "t")], kind="tool")
        retry.reserve([("new", "u", "u")], kind="tool")
        stale = retry.claim_reserved("old", "t", kind="tool", fingerprint="t")

        assert (stale.position, stale.replayed) == (2, False)

    @pytest.mark.asyncio
    async def test_a_reserve_without_calls_drops_the_steps_nothing_took(self, memory_storage):
        await self.record_tool(memory_storage, "t")

        retry = DurableJournal(memory_storage).start_run()
        retry.reserve([("old", "t", "t")], kind="tool")
        retry.reserve([], kind="tool")
        stale = retry.claim_reserved("old", "t", kind="tool", fingerprint="t")

        assert (stale.position, stale.replayed) == (1, False)

    @pytest.mark.asyncio
    async def test_a_call_that_shares_an_id_leaves_the_reservation_to_the_matching_call(self, memory_storage):
        await self.record_tool(memory_storage, "t")

        retry = DurableJournal(memory_storage).start_run()
        retry.reserve([("c", "t", "t")], kind="tool")
        other = retry.claim_reserved("c", "t", kind="tool", fingerprint="other")
        own = retry.claim_reserved("c", "t", kind="tool", fingerprint="t")

        assert (other.position, other.replayed) == (1, False)
        assert (own.position, own.replayed) == (0, True)

    def test_duplicate_ids_in_one_reserve_take_one_position(self, memory_storage):
        run = DurableJournal(memory_storage).start_run()
        run.reserve([("c", "t", "a"), ("c", "t", "b"), ("d", "t", "d")], kind="tool")

        assert run.position == 2
        assert run.claim_reserved("c", "t", kind="tool", fingerprint="a").position == 0
        assert run.claim_reserved("d", "t", kind="tool", fingerprint="d").position == 1


class TestFailuresDuringReplay:
    @pytest.mark.asyncio
    async def test_an_attempt_killed_while_replaying_leaves_the_earlier_steps_replayable(
        self, memory_storage
    ):
        first = DurableJournal(memory_storage).start_run()
        for name in ("a", "b", "c"):
            await first.run(
                name, kind="tool", fingerprint=name, body=lambda name=name: ok(f"{name} recorded")
            )
        second = DurableJournal(memory_storage).start_run()
        await second.run("a", kind="tool", fingerprint="a", body=lambda: ok("live"))
        second.fail(TimeoutError("killed while replaying"))

        third = DurableJournal(memory_storage).start_run()
        results = [await third.run(n, kind="tool", fingerprint=n, body=lambda: ok("live")) for n in "abc"]

        assert results == ["a recorded", "b recorded", "c recorded"]


async def _failed_attempt_then_retry_that_diverges_at_once(memory_storage):
    """
    Return a retry about to pause at position 6 that ran 0..5 live and left 1 unrecorded.

    The failed attempt recorded 0..5, with ``try1-1`` at 1 and its model's answer at 4, and
    failed because of that answer, so it stopped being trusted from 4. The retry's first step
    asks something else, so it stops replaying at 0, below that; its tool at 1 raises.
    """
    first = DurableJournal(memory_storage).start_run()
    for position in range(6):
        await first.run(
            f"s{position}",
            kind="model" if position == 4 else "tool",
            fingerprint=f"s{position}",
            body=lambda p=position: ok(f"try1-{p}"),
        )
    first.fail(RuntimeError("the model's answers failed the run"), model_at_fault=True)
    assert memory_storage.entries[build_run_meta_key(first.key)]["diverge_from"] == 4

    retry = DurableJournal(memory_storage).start_run()
    await retry.run("s0", kind="tool", fingerprint="changed", body=lambda: ok("retry-0"))
    with pytest.raises(RuntimeError):
        await retry.run("s1", kind="tool", fingerprint="s1", body=boom)
    for position in range(2, 6):
        await retry.run(
            f"s{position}",
            kind="model" if position == 4 else "tool",
            fingerprint=f"s{position}",
            body=lambda p=position: ok(f"retry-{p}"),
        )
    return retry


class TestPause:
    @pytest.mark.asyncio
    async def test_entries_the_attempt_left_unrecorded_below_the_stored_boundary_do_not_replay(
        self, memory_storage
    ):
        retry = await _failed_attempt_then_retry_that_diverges_at_once(memory_storage)

        retry.pause()

        resumed = DurableJournal(memory_storage).start_run()
        replayed = await resumed.run("s0", kind="tool", fingerprint="changed", body=lambda: ok("live"))
        rerun = await resumed.run("s1", kind="tool", fingerprint="s1", body=lambda: ok("live"))
        assert (replayed, rerun) == ("retry-0", "live")
        assert memory_storage.entries[build_run_meta_key(retry.key)]["diverge_from"] == 6

    @pytest.mark.asyncio
    async def test_an_entry_that_could_not_be_deleted_is_not_trusted(self, memory_storage):
        retry = await _failed_attempt_then_retry_that_diverges_at_once(memory_storage)

        with mock.patch.object(memory_storage, "delete_steps", autospec=True):
            retry.pause()

        # The failed attempt's entry at 1 is still there, so the resume runs everything from 1 live.
        assert memory_storage.entries[build_run_meta_key(retry.key)]["diverge_from"] == 1

    @pytest.mark.asyncio
    async def test_a_resume_still_replaying_leaves_the_boundary_where_it_is(self, memory_storage):
        retry = await _failed_attempt_then_retry_that_diverges_at_once(memory_storage)
        retry.pause()
        meta = dict(memory_storage.entries[build_run_meta_key(retry.key)])

        resumed = DurableJournal(memory_storage).start_run()
        await resumed.run("s0", kind="tool", fingerprint="changed", body=lambda: ok("live"))
        resumed.pause()

        assert memory_storage.entries[build_run_meta_key(resumed.key)] == meta
        assert await resumed.run("s1", kind="tool", fingerprint="s1", body=lambda: ok("live")) == "live"
        assert await resumed.run("s2", kind="tool", fingerprint="s2", body=lambda: ok("live")) == "retry-2"

    @pytest.mark.asyncio
    async def test_a_pause_before_the_failed_tool_call_runs_again_leaves_its_count(self, memory_storage):
        await run_attempt_failing_in_tool(memory_storage, "call 1")
        paused = DurableJournal(memory_storage).start_run()
        await paused.run("model", kind="model", fingerprint="m", body=lambda: ok("replayed, never asked"))
        paused.pause()

        chosen = [await run_attempt_failing_in_tool(memory_storage, f"call {n}") for n in range(3, 5)]

        assert chosen == ["call 1", "call 4"]

    @pytest.mark.asyncio
    async def test_a_tool_call_failing_before_a_pause_and_after_the_resume_failed_two_attempts(
        self, memory_storage
    ):
        """
        A pause is not a failure, and the task resumes in the same attempt, so the count carries over.

        The tool always runs live, so it runs the same call again after the resume. The first
        attempt took a different path from the one before it, which makes the pause save the
        run's entry rather than leave it as it was.
        """
        error = ValueError("bad arguments")
        earlier = DurableJournal(memory_storage).start_run()
        await earlier.run("model", kind="model", fingerprint="old prompt", body=lambda: ok("call 0"))
        earlier.fail(TimeoutError("killed"))

        first = DurableJournal(memory_storage).start_run()
        assert await first.run("model", kind="model", fingerprint="m", body=lambda: ok("call 1")) == "call 1"
        with pytest.raises(ValueError, match="bad arguments"):
            await first.run(
                "query", kind="tool", fingerprint="call 1", body=lambda: raise_(error), replayable=False
            )
        first.fail(error)

        second = DurableJournal(memory_storage).start_run()
        meta_key = build_run_meta_key(second.key)
        await second.run("model", kind="model", fingerprint="m", body=lambda: ok("replayed, never asked"))
        await second.run(
            "query", kind="tool", fingerprint="call 1", body=lambda: ok("rows"), replayable=False
        )
        await second.run("model", kind="model", fingerprint="m2", body=lambda: ok("call needing approval"))
        second.pause()
        assert memory_storage.entries[meta_key]["paused"] is True
        assert memory_storage.entries[meta_key]["tool_failure"]["attempts"] == 1

        resumed = DurableJournal(memory_storage).start_run()
        await resumed.run("model", kind="model", fingerprint="m", body=lambda: ok("replayed, never asked"))
        with pytest.raises(ValueError, match="bad arguments"):
            await resumed.run(
                "query", kind="tool", fingerprint="call 1", body=lambda: raise_(error), replayable=False
            )
        resumed.fail(error)

        retry = DurableJournal(memory_storage).start_run()
        assert await retry.run("model", kind="model", fingerprint="m", body=lambda: ok("call 3")) == "call 3"


class TestCompletedRuns:
    """A task can keep the journal after a run succeeds, such as while a reviewer reads its output."""

    @pytest.mark.asyncio
    async def test_a_run_that_succeeded_after_a_failure_replays_in_full_on_a_later_attempt(
        self, memory_storage
    ):
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("look it up"))
        with pytest.raises(RuntimeError) as failed:
            await first.run("lookup", kind="tool", fingerprint="l", body=boom)
        first.fail(failed.value)

        retry = DurableJournal(memory_storage).start_run()
        await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("live"))
        await retry.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("$10"))
        await retry.run("model", kind="model", fingerprint="m2", body=lambda: ok("it costs $10"))
        retry.complete()

        later = DurableJournal(memory_storage).start_run()
        replayed = [
            await later.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again")),
            await later.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("looked up again")),
            await later.run("model", kind="model", fingerprint="m2", body=lambda: ok("asked again")),
        ]

        assert replayed == ["look it up", "$10", "it costs $10"]
        assert dict(later.journal.stats.replayed) == {"model": 2, "tool": 1}

    @pytest.mark.asyncio
    async def test_an_entry_left_where_a_run_that_took_another_path_recorded_nothing_runs_live(
        self, memory_storage
    ):
        first = DurableJournal(memory_storage).start_run()
        await first.run("a", kind="model", fingerprint="a", body=lambda: ok("old a"))
        await first.run("b", kind="tool", fingerprint="b", body=lambda: ok("old b"))
        first.complete()

        retry = DurableJournal(memory_storage).start_run()
        await retry.run("a", kind="model", fingerprint="a2", body=lambda: ok("new a"))
        memory_storage.refuse.add(build_step_key(0, 1))
        await retry.run("b", kind="tool", fingerprint="b", body=lambda: ok("new b"))
        retry.complete()
        memory_storage.refuse.clear()

        later = DurableJournal(memory_storage).start_run()
        replayed = await later.run("a", kind="model", fingerprint="a2", body=lambda: ok("asked again"))
        b = await later.run("b", kind="tool", fingerprint="b", body=lambda: ok("run again"))

        assert (replayed, b) == ("new a", "run again")

    @pytest.mark.asyncio
    async def test_an_entry_that_could_not_be_deleted_on_completion_is_not_trusted(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run("a", kind="model", fingerprint="a", body=lambda: ok("old a"))
        await first.run("b", kind="tool", fingerprint="b", body=lambda: ok("old b"))
        first.complete()

        retry = DurableJournal(memory_storage).start_run()
        await retry.run("a", kind="model", fingerprint="a2", body=lambda: ok("new a"))
        memory_storage.refuse.add(build_step_key(0, 1))
        await retry.run("b", kind="tool", fingerprint="b", body=lambda: ok("new b"))
        with mock.patch.object(memory_storage, "delete_steps", autospec=True):
            retry.complete()
        memory_storage.refuse.clear()

        later = DurableJournal(memory_storage).start_run()
        replayed = await later.run("a", kind="model", fingerprint="a2", body=lambda: ok("asked again"))
        b = await later.run("b", kind="tool", fingerprint="b", body=lambda: ok("run again"))

        assert (replayed, b) == ("new a", "run again")

    @pytest.mark.asyncio
    async def test_a_run_that_replayed_in_full_writes_nothing_on_completion(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m", body=lambda: ok("answer"))
        first.complete()
        retry = DurableJournal(memory_storage).start_run()
        await retry.run("model", kind="model", fingerprint="m", body=lambda: ok("live"))
        retry.complete()

        assert build_run_meta_key(0) not in memory_storage.entries

    @pytest.mark.asyncio
    async def test_ask_model_again_replays_up_to_the_last_model_request_and_asks_it_again(
        self, memory_storage
    ):
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("look it up"))
        await first.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("$10"))
        await first.run("model", kind="model", fingerprint="m2", body=lambda: ok("it costs $10"))
        first.complete()

        # In a later process, as when a reviewer turns the answer down.
        DurableJournal(memory_storage).ask_model_again(0)

        retry = DurableJournal(memory_storage).start_run()
        steps = [
            await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again")),
            await retry.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("looked up again")),
            await retry.run("model", kind="model", fingerprint="m2", body=lambda: ok("a new answer")),
        ]

        assert steps == ["look it up", "$10", "a new answer"]

    @pytest.mark.asyncio
    async def test_ask_model_again_finds_the_last_model_request_past_a_step_that_was_not_recorded(
        self, memory_storage
    ):
        sent = []

        async def send():
            sent.append("sent")
            return "sent"

        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("check the date"))
        await first.run("today", kind="tool", fingerprint="t", body=lambda: ok("Monday"), replayable=False)
        await first.run("model", kind="model", fingerprint="m2", body=lambda: ok("send it"))
        await first.run("send", kind="tool", fingerprint="s", body=send)
        await first.run("model", kind="model", fingerprint="m3", body=lambda: ok("sent on Monday"))
        first.complete()

        DurableJournal(memory_storage).ask_model_again(0)

        retry = DurableJournal(memory_storage).start_run()
        await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again"))
        await retry.run("today", kind="tool", fingerprint="t", body=lambda: ok("Monday"), replayable=False)
        await retry.run("model", kind="model", fingerprint="m2", body=lambda: ok("asked again"))
        await retry.run("send", kind="tool", fingerprint="s", body=send)
        final = await retry.run("model", kind="model", fingerprint="m3", body=lambda: ok("a new answer"))

        assert (sent, final) == (["sent"], "a new answer")

    @pytest.mark.asyncio
    async def test_ask_model_again_asks_for_a_final_answer_the_storage_did_not_keep(self, memory_storage):
        sent = []

        async def send():
            sent.append("sent")
            return "sent"

        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("send it"))
        await first.run("send", kind="tool", fingerprint="s", body=send)
        memory_storage.refuse.add(build_step_key(0, 2))
        await first.run("model", kind="model", fingerprint="m2", body=lambda: ok("sent"))
        first.complete()
        memory_storage.refuse.clear()

        DurableJournal(memory_storage).ask_model_again(0)

        retry = DurableJournal(memory_storage).start_run()
        chose = await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again"))
        await retry.run("send", kind="tool", fingerprint="s", body=send)
        await retry.run("model", kind="model", fingerprint="m2", body=lambda: ok("a new answer"))

        assert (chose, sent) == ("send it", ["sent"])

    @pytest.mark.asyncio
    async def test_a_note_left_by_a_refused_write_does_not_outlive_a_longer_run(self, memory_storage):
        sent = []

        async def send(what):
            sent.append(what)
            return what

        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("send it"))
        await first.run("send", kind="tool", fingerprint="s1", body=lambda: send("first"))
        memory_storage.refuse.add(build_step_key(0, 2))
        await first.run("model", kind="model", fingerprint="m2", body=lambda: ok("sent"))
        first.complete()
        memory_storage.refuse.clear()
        # A retry (the review went on, then failed) runs the missing answer live, which goes further.
        retry = DurableJournal(memory_storage).start_run()
        await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again"))
        await retry.run("send", kind="tool", fingerprint="s1", body=lambda: send("first"))
        await retry.run("model", kind="model", fingerprint="m2", body=lambda: ok("send more"))
        await retry.run("send", kind="tool", fingerprint="s2", body=lambda: send("second"))
        await retry.run("model", kind="model", fingerprint="m3", body=lambda: ok("sent both"))
        retry.complete()

        DurableJournal(memory_storage).ask_model_again(0)

        third = DurableJournal(memory_storage).start_run()
        await third.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again"))
        await third.run("send", kind="tool", fingerprint="s1", body=lambda: send("first"))
        await third.run("model", kind="model", fingerprint="m2", body=lambda: ok("asked again"))
        await third.run("send", kind="tool", fingerprint="s2", body=lambda: send("second"))
        final = await third.run("model", kind="model", fingerprint="m3", body=lambda: ok("a new answer"))

        assert (sent, final) == (["first", "second"], "a new answer")

    @pytest.mark.asyncio
    async def test_a_note_left_by_a_refused_write_does_not_outlive_a_shorter_run(self, memory_storage):
        """Replaying the shorter run's answer would show the reviewer the output they turned down."""
        first = DurableJournal(memory_storage).start_run()
        memory_storage.refuse.add(build_step_key(0, 0))
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("look it up"))
        memory_storage.refuse.clear()
        await first.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("$10"))
        await first.run("model", kind="model", fingerprint="m2", body=lambda: ok("first answer"))
        first.complete()
        retry = DurableJournal(memory_storage).start_run()
        await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("rejected answer"))
        retry.complete()

        DurableJournal(memory_storage).ask_model_again(0)

        third = DurableJournal(memory_storage).start_run()
        answer = await third.run("model", kind="model", fingerprint="m1", body=lambda: ok("a new answer"))

        assert answer == "a new answer"

    @pytest.mark.asyncio
    async def test_ask_model_again_skips_steps_after_the_last_model_request(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run("model", kind="model", fingerprint="m1", body=lambda: ok("look it up"))
        await first.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("$10"))
        await first.run("model", kind="model", fingerprint="m2", body=lambda: ok("save it"))
        await first.run("save", kind="tool", fingerprint="s", body=lambda: ok("saved"))
        first.complete()

        DurableJournal(memory_storage).ask_model_again(0)

        retry = DurableJournal(memory_storage).start_run()
        await retry.run("model", kind="model", fingerprint="m1", body=lambda: ok("asked again"))
        await retry.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("looked up again"))
        answer = await retry.run("model", kind="model", fingerprint="m2", body=lambda: ok("a new answer"))

        assert answer == "a new answer"

    @pytest.mark.asyncio
    async def test_drop_runs_from_also_drops_the_runs_started_inside_them(self, memory_storage):
        journal = DurableJournal(memory_storage)
        await journal.start_run().run("model", kind="model", fingerprint="m", body=lambda: ok("first"))
        regeneration = journal.start_run()

        async def tool_running_an_agent():
            return await journal.start_run().run("model", kind="model", fingerprint="n", body=lambda: ok("x"))

        await regeneration.run("agent_tool", kind="tool", fingerprint="t", body=tool_running_an_agent)

        DurableJournal(memory_storage).drop_runs_from(1)

        assert memory_storage.entries[RUNS_KEY] == {"runs": ["0"]}
        assert [key for key in memory_storage.entries if "run_1" in key] == []

    @pytest.mark.asyncio
    async def test_ask_model_again_on_a_run_with_no_model_step_runs_it_again_from_its_start(
        self, memory_storage
    ):
        first = DurableJournal(memory_storage).start_run()
        await first.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("$10"))

        DurableJournal(memory_storage).ask_model_again(0)

        retry = DurableJournal(memory_storage).start_run()
        assert await retry.run("lookup", kind="tool", fingerprint="l", body=lambda: ok("live")) == "live"

    @pytest.mark.asyncio
    async def test_ask_model_again_starts_the_later_runs_over(self, memory_storage):
        """A later run (a regeneration after a review round) followed from the answer turned down."""
        journal = DurableJournal(memory_storage)
        for _ in range(2):
            run = journal.start_run()
            await run.run("model", kind="model", fingerprint="m", body=lambda: ok("recorded"))

        DurableJournal(memory_storage).ask_model_again(0)

        retry = DurableJournal(memory_storage, first_run=1).start_run()
        assert await retry.run("model", kind="model", fingerprint="m", body=lambda: ok("live")) == "live"


class TestCleanup:
    @pytest.mark.asyncio
    async def test_cleanup_reaches_a_run_started_by_a_step_that_now_replays(self, memory_storage):
        """The nested run never starts again on the retry, but its steps are still deleted."""
        first_journal = DurableJournal(memory_storage)
        first = first_journal.start_run()

        async def tool_that_runs_an_agent():
            await first_journal.start_run().run("inner", kind="tool", fingerprint="i", body=lambda: ok(1))
            return "done"

        await first.run("tool", kind="tool", fingerprint="t", body=tool_that_runs_an_agent)
        first.fail(RuntimeError("worker died"))
        retry_journal = DurableJournal(memory_storage)
        replayed = await retry_journal.start_run().run(
            "tool", kind="tool", fingerprint="t", body=tool_that_runs_an_agent
        )

        retry_journal.cleanup()

        assert replayed == "done"
        assert memory_storage.entries == {}

    @pytest.mark.asyncio
    async def test_cleanup_deletes_what_an_earlier_attempt_recorded_beyond_this_one(self, memory_storage):
        """Left behind, those steps would replay into a run started by clearing the task."""
        first = DurableJournal(memory_storage).start_run()
        for name in ("a", "b", "c"):
            await first.run(name, kind="tool", fingerprint=name, body=lambda: ok(1))
        first.fail(RuntimeError("worker died"))
        retry_journal = DurableJournal(memory_storage)
        await retry_journal.start_run().run("other", kind="tool", fingerprint="o", body=lambda: ok(2))

        retry_journal.cleanup()

        assert memory_storage.entries == {}

    @pytest.mark.asyncio
    async def test_cleanup_finds_steps_of_an_attempt_that_was_killed(self, memory_storage):
        """A killed attempt never records how far it got."""
        first = DurableJournal(memory_storage).start_run()
        for name in ("a", "b", "c"):
            await first.run(name, kind="tool", fingerprint=name, body=lambda: ok(1))
        retry_journal = DurableJournal(memory_storage)
        await retry_journal.start_run().run("other", kind="tool", fingerprint="o", body=lambda: ok(2))

        retry_journal.cleanup()

        assert memory_storage.entries == {}


class TestStoreCalls:
    @pytest.mark.asyncio
    async def test_a_first_attempt_reads_back_only_the_run_id_and_the_run_list(self, memory_storage):
        """No earlier attempt started the run, so every lookup of a step or of the run's entry would miss."""
        journal = DurableJournal(memory_storage)
        journal.get_run_id(default="ti-try-1")
        durable_run = journal.start_run()
        for name in "abc":
            await durable_run.run(name, kind="tool", fingerprint=name, body=lambda: ok(1))
        with pytest.raises(RuntimeError):
            await durable_run.run("d", kind="tool", fingerprint="d", body=boom)
        durable_run.fail(RuntimeError("tool failed"))

        assert memory_storage.loads() == [RUN_ID_KEY, RUNS_KEY]
        assert memory_storage.saves() == [
            RUN_ID_KEY,
            RUNS_KEY,
            *(build_step_key(0, position) for position in range(3)),
            build_run_meta_key(0),
        ]

    @pytest.mark.asyncio
    async def test_a_retry_reads_back_only_the_runs_an_earlier_attempt_started(self, memory_storage):
        first = DurableJournal(memory_storage).start_run()
        await first.run("a", kind="tool", fingerprint="a", body=lambda: ok("recorded"))
        first.fail(RuntimeError("worker died"))
        memory_storage.calls.clear()

        retry_journal = DurableJournal(memory_storage)
        replayed = await retry_journal.start_run().run(
            "a", kind="tool", fingerprint="a", body=lambda: ok("live")
        )
        new_run = retry_journal.start_run()
        await new_run.run("b", kind="tool", fingerprint="b", body=lambda: ok("live"))

        assert replayed == "recorded"
        assert memory_storage.loads() == [RUNS_KEY, build_run_meta_key(0), build_step_key(0, 0)]

    @pytest.mark.asyncio
    async def test_cleanup_deletes_every_run_in_one_call_without_reading_anything(self, memory_storage):
        journal = DurableJournal(memory_storage)
        journal.get_run_id(default="ti-try-1")
        for _ in range(2):
            durable_run = journal.start_run()
            for name in "ab":
                await durable_run.run(name, kind="tool", fingerprint=name, body=lambda: ok(1))
        before_cleanup = len(memory_storage.calls)

        journal.cleanup()

        assert memory_storage.calls[before_cleanup:] == [
            (
                "delete",
                [
                    build_step_key(0, 0),
                    build_step_key(0, 1),
                    build_step_key(1, 0),
                    build_step_key(1, 1),
                    RUNS_KEY,
                    RUN_ID_KEY,
                ],
            )
        ]
        assert memory_storage.entries == {}

    @pytest.mark.asyncio
    async def test_a_run_that_cleans_up_after_itself_deletes_its_steps_and_the_run_list_in_one_call(
        self, memory_storage
    ):
        durable_run = DurableJournal(memory_storage, clean_up_after_run=True).start_run()
        for name in "ab":
            await durable_run.run(name, kind="tool", fingerprint=name, body=lambda: ok(1))
        before_cleanup = len(memory_storage.calls)

        durable_run.cleanup()

        assert memory_storage.calls[before_cleanup:] == [
            ("delete", [build_step_key(0, 0), build_step_key(0, 1), RUNS_KEY])
        ]
        assert memory_storage.entries == {}

    @pytest.mark.asyncio
    async def test_a_run_that_cleans_up_while_another_is_listed_leaves_the_other_on_the_list(
        self, memory_storage
    ):
        journal = DurableJournal(memory_storage, clean_up_after_run=True)
        journal.start_run()
        second = journal.start_run()
        await second.run("a", kind="tool", fingerprint="a", body=lambda: ok(1))
        before_cleanup = len(memory_storage.calls)

        second.cleanup()

        assert memory_storage.calls[before_cleanup:] == [
            ("delete", [build_step_key(1, 0)]),
            ("save", RUNS_KEY),
        ]
        assert memory_storage.entries == {RUNS_KEY: {"runs": ["0"]}}

    @pytest.mark.asyncio
    async def test_a_run_the_run_list_never_listed_records_nothing(self, memory_storage):
        """A retry looks up only listed runs, so steps of an unlisted run would never replay or be deleted."""
        memory_storage.refuse.add(RUNS_KEY)
        durable_run = DurableJournal(memory_storage).start_run()
        await durable_run.run("a", kind="tool", fingerprint="a", body=lambda: ok(1))
        durable_run.fail(RuntimeError("worker died"))

        assert memory_storage.entries == {}
        assert durable_run.journal.stats.not_recorded == [("tool", "a")]

    @pytest.mark.asyncio
    async def test_a_run_list_write_that_failed_is_retried_before_the_first_step_is_recorded(
        self, memory_storage
    ):
        memory_storage.refuse.add(RUNS_KEY)
        first = DurableJournal(memory_storage).start_run()
        memory_storage.refuse.clear()
        await first.run("a", kind="tool", fingerprint="a", body=lambda: ok("recorded"))
        first.fail(RuntimeError("worker died"))

        retry = DurableJournal(memory_storage).start_run()

        assert await retry.run("a", kind="tool", fingerprint="a", body=lambda: ok("live")) == "recorded"

    @pytest.mark.asyncio
    async def test_a_first_attempt_cleanup_deletes_every_write_it_sent(self, memory_storage):
        """
        A write reported as failed may still have landed, so it is deleted.

        A step claimed with ``replayable=False`` sends no write, so nothing is deleted for it.
        """
        memory_storage.lose_ack.update({build_step_key(0, 2), build_run_meta_key(0)})
        durable_run = DurableJournal(memory_storage, clean_up_after_run=True).start_run()
        await durable_run.run("a", kind="tool", fingerprint="a", body=lambda: ok(1))
        await durable_run.run("live", kind="tool", fingerprint=None, body=lambda: ok(2), replayable=False)
        await durable_run.run("lost", kind="tool", fingerprint="l", body=lambda: ok(3))
        await durable_run.run("d", kind="tool", fingerprint="d", body=lambda: ok(4))
        durable_run.fail(RuntimeError("an agent a tool ran failed, and the tool went on"))
        before_cleanup = len(memory_storage.calls)

        durable_run.cleanup()

        assert memory_storage.calls[before_cleanup:] == [
            (
                "delete",
                [
                    build_step_key(0, 0),
                    build_step_key(0, 2),
                    build_step_key(0, 3),
                    build_run_meta_key(0),
                    RUNS_KEY,
                ],
            )
        ]
        assert memory_storage.entries == {}

    @pytest.mark.parametrize("key", [build_step_key(0, 0), build_run_meta_key(0)], ids=["step", "run entry"])
    @pytest.mark.asyncio
    async def test_a_write_that_raised_after_landing_is_still_deleted(self, memory_storage, key):
        memory_storage.raise_after_storing.add(key)
        durable_run = DurableJournal(memory_storage, clean_up_after_run=True).start_run()

        async def run_that_fails_and_is_caught():
            await durable_run.run("a", kind="tool", fingerprint="a", body=lambda: ok(1))
            durable_run.fail(RuntimeError("an agent a tool ran failed, and the tool went on"))

        with pytest.raises(OSError, match="could not write"):
            await run_that_fails_and_is_caught()

        durable_run.cleanup()

        assert memory_storage.entries == {}

    @pytest.mark.asyncio
    async def test_cleanup_deletes_the_entry_of_a_run_that_failed_in_this_attempt(self, memory_storage):
        """An agent a tool ran can fail while the task goes on to succeed."""
        journal = DurableJournal(memory_storage)
        durable_run = journal.start_run()
        with pytest.raises(RuntimeError):
            await durable_run.run("a", kind="tool", fingerprint="a", body=boom)
        durable_run.fail(RuntimeError("tool failed"))

        journal.cleanup()

        assert memory_storage.entries == {}


class TestRuns:
    @pytest.mark.asyncio
    async def test_runs_number_their_steps_separately(self, memory_storage):
        journal = DurableJournal(memory_storage)
        await journal.start_run().run("a", kind="other", fingerprint=None, body=lambda: ok(1))
        await journal.start_run().run("b", kind="other", fingerprint=None, body=lambda: ok(2))

        assert memory_storage.steps(0)[0]["name"] == "a"
        assert memory_storage.steps(1)[0]["name"] == "b"

    @pytest.mark.asyncio
    async def test_a_run_started_by_a_step_is_numbered_under_it(self, memory_storage):
        """So replaying a step that started a run never shifts the runs later steps start."""
        journal = DurableJournal(memory_storage)
        outer = journal.start_run()
        nested_keys = []

        async def tool_that_runs_an_agent():
            nested_keys.append(journal.start_run().key)
            return "done"

        await outer.run("tool", kind="tool", fingerprint="t", body=tool_that_runs_an_agent)

        assert nested_keys == ["0.0.1"]
        assert journal.start_run().key == "1"

    @pytest.mark.asyncio
    async def test_a_running_tool_call_is_seen_from_a_thread_without_its_context(self, memory_storage):
        """``run_in_executor`` does not copy the context, so only the journal can tell code there runs in the tool."""
        journal = DurableJournal(memory_storage)
        run = journal.start_run()
        seen = {}

        async def look(key):
            seen[key] = await asyncio.get_running_loop().run_in_executor(
                None, journal.find_executing_tool_steps
            )
            return key

        await run.run("event", kind="other", fingerprint=None, body=lambda: look("other"))
        await run.run("tool", kind="tool", fingerprint="t", body=lambda: look("tool"))

        assert seen["other"] == []
        assert [step.name for step in seen["tool"]] == ["tool"]
        assert journal.find_executing_tool_steps() == []

    def test_the_run_id_is_the_first_attempts_on_every_retry(self, memory_storage):
        first = DurableJournal(memory_storage).get_run_id(default="ti-try-1")
        retry = DurableJournal(memory_storage).get_run_id(default="ti-try-2")

        assert (first, retry) == ("ti-try-1", "ti-try-1")

    @pytest.mark.asyncio
    async def test_cleanup_deletes_the_steps_and_the_run_id(self, memory_storage):
        journal = DurableJournal(memory_storage)
        journal.get_run_id(default="ti-try-1")
        await journal.start_run().run("a", kind="other", fingerprint=None, body=lambda: ok(1))

        journal.cleanup()

        assert memory_storage.entries == {}
        assert DurableJournal(memory_storage).get_run_id(default="ti-try-2") == "ti-try-2"

    @pytest.mark.asyncio
    async def test_cleanup_deletes_replayed_steps_too(self, memory_storage):
        await (
            DurableJournal(memory_storage)
            .start_run()
            .run("a", kind="other", fingerprint=None, body=lambda: ok(1))
        )
        retry = DurableJournal(memory_storage)
        await retry.start_run().run("a", kind="other", fingerprint=None, body=lambda: ok(2))

        retry.cleanup()

        assert memory_storage.entries == {}


class TestSummary:
    @pytest.mark.asyncio
    async def test_summary_names_the_tools_a_retry_runs_again(self, memory_storage, caplog):
        memory_storage.refuse.update({"__commonai_durable__run_0_step_1", "__commonai_durable__run_0_step_2"})
        journal = DurableJournal(memory_storage)
        durable_run = journal.start_run()
        await durable_run.run("m", kind="model", fingerprint=None, body=lambda: ok(1))
        await durable_run.run("send_email", kind="tool", fingerprint=None, body=lambda: ok(1))
        await durable_run.run("send_email", kind="tool", fingerprint=None, body=lambda: ok(1))
        logger = logging.getLogger("test_summary")

        with caplog.at_level(logging.INFO, logger="test_summary"):
            journal.log_summary(logger)

        assert "recorded 1 new steps (1 model, 0 tool, 0 other)" in caplog.text
        assert "2 tool results were not recorded, and a retry runs them again: send_email (x2)" in caplog.text


class TestCurrentJournal:
    def test_a_scoped_journal_wins(self, memory_storage):
        journal = DurableJournal(memory_storage)

        with journal_scope(journal):
            assert current_journal() is journal

    def test_outside_a_task_there_is_no_journal(self):
        assert current_journal() is None

    def test_inside_a_task_the_attempt_gets_one_journal_that_cleans_up_after_each_run(
        self, memory_storage, monkeypatch
    ):
        monkeypatch.setattr(journal_module, "_task_journal", None)
        context = {"task_instance": SimpleNamespace(id="ti-1")}
        with (
            mock.patch.object(journal_module, "get_current_context", autospec=True, return_value=context),
            mock.patch.object(
                journal_module, "build_task_storage", autospec=True, return_value=memory_storage
            ),
        ):
            journal = current_journal()

            assert journal is not None
            assert journal.clean_up_after_run
            assert current_journal() is journal

    def test_a_new_attempt_in_the_same_process_gets_a_new_journal(self, memory_storage, monkeypatch):
        monkeypatch.setattr(journal_module, "_task_journal", None)
        ti = SimpleNamespace(id="ti-1")
        with (
            mock.patch.object(
                journal_module, "get_current_context", autospec=True, return_value={"task_instance": ti}
            ),
            mock.patch.object(
                journal_module, "build_task_storage", autospec=True, return_value=memory_storage
            ),
        ):
            first = current_journal()
            ti.id = "ti-2"

            assert current_journal() is not first
