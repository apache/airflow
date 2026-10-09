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

import logging
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.common.ai.durable import journal as journal_module
from airflow.providers.common.ai.durable.base import build_step_key
from airflow.providers.common.ai.durable.journal import (
    DurableJournal,
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
