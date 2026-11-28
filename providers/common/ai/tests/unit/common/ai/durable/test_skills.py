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
"""
AirflowDurability with ``AirflowSkills``, through a real agent loop.

A skill script runs on the worker, so a retry that ran it again would repeat its side effects.
"""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from airflow.providers.common.ai.durable import AirflowDurability
from airflow.providers.common.ai.durable.journal import DurableJournal, journal_scope
from airflow.providers.common.ai.skills import AirflowSkills, GitSkills

from unit.common.ai.durable.test_capability import Calls, attempt, responses_so_far

COUNT_SCRIPT = """\
import pathlib
import sys

counter = pathlib.Path(sys.argv[2])
with counter.open("a") as handle:
    handle.write("ran\\n")
print("counted")
"""


def write_counting_skill(library: Path) -> None:
    skill = library / "counter"
    (skill / "scripts").mkdir(parents=True)
    (skill / "SKILL.md").write_text(
        "---\nname: counter\ndescription: counts runs\n---\nRun scripts/count.py\n"
    )
    (skill / "scripts" / "count.py").write_text(COUNT_SCRIPT)


def load_run_then_answer(calls: Calls, counter: Path, *, fail_final: list[bool]):
    def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.bump("model")
        match responses_so_far(messages):
            case 0:
                return ModelResponse(parts=[ToolCallPart("load_capability", {"id": "counter"})])
            case 1:
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            "run_skill_script",
                            {
                                "skill_name": "counter",
                                "script": "scripts/count.py",
                                "args": {"counter": str(counter)},
                            },
                        )
                    ]
                )
        if fail_final[0]:
            raise RuntimeError("worker died")
        return ModelResponse(parts=[TextPart("done")])

    return model_fn


class TestWorkerScripts:
    @pytest.mark.asyncio
    async def test_a_retry_replays_the_script_instead_of_running_it_again(self, memory_storage, tmp_path):
        library = tmp_path / "library"
        write_counting_skill(library)
        counter = tmp_path / "counter.txt"
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(load_run_then_answer(calls, counter, fail_final=fail_final)),
                name="skills-agent",
                capabilities=[AirflowSkills([library]), AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        assert counter.read_text() == "ran\n"

        fail_final[0] = False
        with journal_scope(journal := DurableJournal(memory_storage)):
            result = await build().run("go")

        assert result.output == "done"
        assert counter.read_text() == "ran\n"
        assert dict(journal.stats.replayed) == {"model": 2, "tool": 1}

    @mock.patch("airflow.providers.common.ai.skills._clone_git", autospec=True)
    @pytest.mark.asyncio
    async def test_the_replay_holds_when_each_attempt_clones_into_a_different_directory(
        self, mock_clone, memory_storage, tmp_path
    ):
        counter = tmp_path / "counter.txt"
        clones: list[Path] = []

        def clone(source: GitSkills):
            root = tmp_path / f"clone-{len(clones)}"
            clones.append(root)
            write_counting_skill(root / "skills")
            return str(root / "skills"), str(root), "abc123"

        mock_clone.side_effect = clone
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(load_run_then_answer(calls, counter, fail_final=fail_final)),
                name="skills-agent",
                capabilities=[
                    AirflowSkills([GitSkills(repo_url="https://github.com/org/repo")]),
                    AirflowDurability(),
                ],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        with journal_scope(journal := DurableJournal(memory_storage)):
            result = await build().run("go")

        assert result.output == "done"
        assert len(clones) == 2
        assert counter.read_text() == "ran\n"
        assert journal.stats.replayed["tool"] == 1
