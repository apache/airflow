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
import json
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("task state store needs Airflow >= 3.3", allow_module_level=True)

pytest.importorskip("strands")

from strands import Agent, tool
from strands.storage import InMemoryStorage
from strands.types.exceptions import MaxTokensReachedException

from airflow.providers.common.ai.durable import strands as durable_strands
from airflow.providers.common.ai.durable.strands import invoke_durably
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

from unit.common.ai.durable.test_task_state_store import FakeTaskStateStore
from unit.common.ai.tools.test_strands import _OneToolCallModel

if TYPE_CHECKING:
    from strands.types.agent import AgentInput


class _WorkerKilled(SystemExit):
    """Stands in for the worker dying: Strands hands an exception from a tool to the model."""


class _ScriptedModel(_OneToolCallModel):
    """
    Turn 0 calls ``first``, turn 1 calls ``second`` and ``third`` together, turn 2 answers.

    The turn is the number of tool results in the conversation, so an agent rebuilt from a
    snapshot carries on from where the last one stopped.
    """

    def __init__(
        self, turns: list[int], fail_at_turn: int | None, truncate_at_turn: int | None = None
    ) -> None:
        super().__init__("first", {})
        self._turns = turns
        self._fail_at_turn = fail_at_turn
        self._truncate_at_turn = truncate_at_turn

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs: Any):
        turn = sum(1 for message in messages if any("toolResult" in block for block in message["content"]))
        self._turns.append(turn)
        if turn == self._fail_at_turn:
            raise RuntimeError(f"model unavailable at turn {turn}")
        yield {"messageStart": {"role": "assistant"}}
        if turn == self._truncate_at_turn:
            # Strands keeps the cut-off answer in the conversation, then raises.
            yield {"contentBlockDelta": {"delta": {"text": "cut off"}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "max_tokens"}}
            return
        calls = {0: ["first"], 1: ["second", "third"]}.get(turn, [])
        for name in calls:
            start = {"toolUse": {"toolUseId": f"{name}-call", "name": name}}
            yield {"contentBlockStart": {"start": start}}
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps({})}}}}
            yield {"contentBlockStop": {}}
        if calls:
            yield {"messageStop": {"stopReason": "tool_use"}}
            return
        yield {"contentBlockDelta": {"delta": {"text": "done"}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}


@dataclass
class _TaskInstance:
    dag_id: str = "research"
    run_id: str = "manual__2026-10-07T00:00:00+00:00"
    task_id: str = "answer"
    map_index: int = -1
    # Every try of a task instance gets a new id; the agent's state must not be keyed on it.
    id: uuid.UUID = field(default_factory=uuid.uuid4)


class _Task:
    """A task running a durable agent. Each ``run`` is one try; only stored state carries over."""

    def __init__(self) -> None:
        self.store = FakeTaskStateStore()
        self.tool_calls: Counter[str] = Counter()
        self.model_turns: list[int] = []
        self.agents: list[Agent] = []

    def run(
        self,
        prompt: AgentInput = "go",
        *,
        fail_at_turn: int | None = None,
        truncate_at_turn: int | None = None,
        kill_in_tool: str | None = None,
        map_index: int = -1,
        **kwargs: Any,
    ) -> str:
        ti = _TaskInstance(map_index=map_index)
        model = _ScriptedModel(self.model_turns, fail_at_turn, truncate_at_turn)

        def call(name: str) -> str:
            self.tool_calls[name] += 1
            if name == kill_in_tool:
                raise _WorkerKilled
            return f"{name} done"

        @tool
        def first() -> str:
            """First tool."""
            return call("first")

        @tool
        def second() -> str:
            """Second tool."""
            return call("second")

        @tool
        def third() -> str:
            """Third tool."""
            return call("third")

        def factory(**durable_kwargs: Any) -> Agent:
            agent = Agent(model=model, tools=[first, second, third], callback_handler=None, **durable_kwargs)
            self.agents.append(agent)
            return agent

        context = {"ti": ti, "task_state_store": self.store}
        with mock.patch.object(durable_strands, "get_current_context", autospec=True, return_value=context):
            return str(invoke_durably(factory, prompt, **kwargs)).strip()


def _texts(agent: Agent) -> list[str]:
    """The text blocks of the agent's conversation: the prompts it was given and its answers."""
    return [block["text"] for message in agent.messages for block in message["content"] if "text" in block]


@pytest.fixture
def task():
    return _Task()


class TestInvokeDurably:
    def test_runs_the_agent_to_its_answer(self, task):
        assert task.run() == "done"
        assert task.tool_calls == {"first": 1, "second": 1, "third": 1}

    def test_a_retry_resumes_after_the_last_finished_cycle(self, task):
        with pytest.raises(RuntimeError, match="model unavailable at turn 1"):
            task.run(fail_at_turn=1)

        assert task.run() == "done"

        # Cycle 1's model call and tool ran once; the retry started from cycle 2's model call.
        assert task.tool_calls == {"first": 1, "second": 1, "third": 1}
        assert task.model_turns == [0, 1, 1, 2]
        assert _texts(task.agents[-1]) == ["go", "done"]

    def test_a_retry_after_the_last_tool_cycle_only_asks_for_the_answer(self, task):
        with pytest.raises(RuntimeError):
            task.run(fail_at_turn=2)

        assert task.run() == "done"

        assert task.tool_calls == {"first": 1, "second": 1, "third": 1}
        assert task.model_turns == [0, 1, 2, 2]

    def test_a_failed_invocation_leaves_nothing_of_itself_in_the_saved_state(self, task):
        """Strands appends a cut-off answer before raising; the retry must not inherit it."""
        with pytest.raises(MaxTokensReachedException):
            task.run(truncate_at_turn=1)

        assert task.run() == "done"

        assert _texts(task.agents[-1]) == ["go", "done"]
        assert task.tool_calls == {"first": 1, "second": 1, "third": 1}

    @pytest.mark.parametrize(
        ("killed_in", "tool_calls"),
        [
            pytest.param("first", {"first": 2, "second": 1, "third": 1}, id="cycle 1"),
            pytest.param("third", {"first": 1, "second": 2, "third": 2}, id="cycle 2"),
        ],
    )
    def test_a_crash_inside_a_cycle_runs_all_of_its_tools_again(self, task, killed_in, tool_calls):
        """Strands resumes from a cycle boundary, so a cycle's tool batch is all or nothing."""
        with pytest.raises(_WorkerKilled):
            task.run(kill_in_tool=killed_in)

        assert task.run() == "done"

        assert task.tool_calls == tool_calls
        assert _texts(task.agents[-1]) == ["go", "done"]

    def test_a_prompt_with_bytes_resumes(self, task):
        """A document in the prompt is fingerprinted the way Strands saves it."""
        prompt = [
            {"text": "go"},
            {"document": {"format": "txt", "name": "orders", "source": {"bytes": b"42"}}},
        ]
        with pytest.raises(RuntimeError):
            task.run(prompt, fail_at_turn=1)

        assert task.run(prompt) == "done"

        assert task.tool_calls == {"first": 1, "second": 1, "third": 1}

    def test_a_failure_before_the_first_checkpoint_starts_over_with_one_prompt(self, task):
        with pytest.raises(RuntimeError):
            task.run(fail_at_turn=0)

        assert task.run() == "done"

        assert _texts(task.agents[-1]) == ["go", "done"]

    def test_a_retry_with_a_different_prompt_starts_over(self, task):
        with pytest.raises(RuntimeError):
            task.run(fail_at_turn=1)

        assert task.run("go, but differently") == "done"

        assert task.tool_calls == {"first": 2, "second": 1, "third": 1}
        assert _texts(task.agents[-1]) == ["go, but differently", "done"]

    @pytest.mark.parametrize("fail_at_turn", [None, 1], ids=["first try", "after a retry"])
    def test_the_saved_state_is_deleted_once_the_agent_finishes(self, task, fail_at_turn):
        if fail_at_turn is not None:
            with pytest.raises(RuntimeError):
                task.run(fail_at_turn=fail_at_turn)
            assert task.store.store

        task.run()

        assert task.store.store == {}

    def test_a_failed_delete_after_the_agent_finishes_does_not_fail_the_task(self, task):
        class _DeleteFailsStore(FakeTaskStateStore):
            def delete(self, key):
                if key in self.store:
                    raise RuntimeError("api down")
                super().delete(key)

        task.store = _DeleteFailsStore()

        assert task.run() == "done"

    def test_a_strands_storage_can_replace_the_task_state_store(self, task):
        storage = InMemoryStorage()
        with pytest.raises(RuntimeError):
            task.run(fail_at_turn=1, storage=storage)

        assert task.run(storage=storage) == "done"

        assert task.tool_calls == {"first": 1, "second": 1, "third": 1}
        assert task.store.store == {}
        assert asyncio.run(storage.list("")) == []

    def test_mapped_task_instances_sharing_a_storage_keep_their_own_state(self, task):
        storage = InMemoryStorage()
        with pytest.raises(RuntimeError):
            task.run(fail_at_turn=1, map_index=0, storage=storage)

        # Another mapped task instance starts its own agent rather than resuming the first one's.
        assert task.run(map_index=1, storage=storage) == "done"
        assert task.tool_calls["first"] == 2

        # And the first one still resumes where it stopped.
        assert task.run(map_index=0, storage=storage) == "done"
        assert task.tool_calls["first"] == 2

    def test_refuses_a_stateful_model(self, task):
        """Strands drops a restored conversation for a model that keeps it server-side."""
        with (
            mock.patch.object(_ScriptedModel, "stateful", new_callable=mock.PropertyMock, return_value=True),
            pytest.raises(ValueError, match="stateful"),
        ):
            # The model fails at once if it is ever called, rather than loop on lost history.
            task.run(fail_at_turn=0)

        assert task.model_turns == []

    def test_needs_a_storage_below_airflow_3_3(self, task):
        with (
            mock.patch.object(durable_strands, "AIRFLOW_V_3_3_PLUS", False),
            pytest.raises(AirflowOptionalProviderFeatureException, match="Airflow 3.3"),
        ):
            task.run()
