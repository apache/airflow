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
"""Tests for the pydantic-ai AgentSkillsToolset binding.

Async methods are driven with ``asyncio.run`` to avoid depending on a particular
pytest-asyncio mode.
"""

from __future__ import annotations

import asyncio
import os
import sys
import types
from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai import Agent, DeferredToolRequests, DeferredToolResults
from pydantic_ai.capabilities import ProcessHistory
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.toolsets import DynamicToolset

from airflow.providers.common.ai.skills import GitSkills, _SkillsLocation
from airflow.providers.common.ai.toolsets.skills import (
    AgentSkillsToolset,
    _count_skill_calls,
    _describe_skill_source,
)
from airflow.providers.common.ai.utils.logging import SKILLS_XCOM_KEY

pytest.importorskip("pydantic_ai_skills")


class _FakeInner:
    """Minimal async-context-manager stand-in for pydantic_ai_skills.SkillsToolset."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *args):
        self.exited = True
        return None


def _write_skill(directory, name="demo-skill"):
    skill_dir = directory / name
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: A {name} skill for tests.\n---\n\n# Demo\n"
    )
    return skill_dir


class TestConstruction:
    def test_is_abstract_toolset_and_lazy(self):
        from pydantic_ai.toolsets.abstract import AbstractToolset

        toolset = AgentSkillsToolset(sources=["./skills", GitSkills(repo_url="https://x/y", conn_id="c")])
        assert isinstance(toolset, AbstractToolset)
        # Nothing resolved or cloned at construction.
        assert toolset._inner is None

    def test_get_tools_before_enter_raises(self):
        toolset = AgentSkillsToolset(sources=["./skills"])
        with pytest.raises(RuntimeError, match="must be entered"):
            asyncio.run(toolset.get_tools(MagicMock()))


class TestLifecycle:
    def test_enter_builds_inner_and_exit_tears_down(self, tmp_path):
        from pydantic_ai_skills import SkillsToolset

        _write_skill(tmp_path)
        toolset = AgentSkillsToolset(sources=[str(tmp_path)])

        async def run():
            async with toolset:
                assert isinstance(toolset._inner, SkillsToolset)
                assert "demo-skill" in toolset._inner._skills
            assert toolset._inner is None

        asyncio.run(run())

    def test_exclude_tools_passed_to_inner(self):
        captured: dict = {}

        def fake_skillstoolset(**kwargs):
            captured.update(kwargs)
            return _FakeInner(**kwargs)

        toolset = AgentSkillsToolset(sources=["/x"], exclude_tools={"run_skill_script"})
        with patch(
            "airflow.providers.common.ai.toolsets.skills._materialize_skills",
            return_value=([_SkillsLocation("/x")], lambda: None),
        ):
            with patch("pydantic_ai_skills.SkillsToolset", fake_skillstoolset):
                asyncio.run(_enter_exit(toolset))

        assert captured["exclude_tools"] == {"run_skill_script"}
        assert captured["directories"] == ["/x"]

    def test_exclude_resources_passed_to_inner(self):
        captured: dict = {}

        def fake_skillstoolset(**kwargs):
            captured.update(kwargs)
            return _FakeInner(**kwargs)

        toolset = AgentSkillsToolset(sources=["/x"], exclude_resources=["*.env", "secrets/*"])
        with patch(
            "airflow.providers.common.ai.toolsets.skills._materialize_skills",
            autospec=True,
            return_value=([_SkillsLocation("/x")], lambda: None),
        ):
            with patch("pydantic_ai_skills.SkillsToolset", fake_skillstoolset):  # noqa: spec
                asyncio.run(_enter_exit(toolset))

        assert captured["exclude_resources"] == ["*.env", "secrets/*"]

    def test_no_optional_kwargs_when_unset(self):
        # Omit exclude_tools/exclude_resources entirely so an older pydantic-ai-skills
        # that lacks the kwarg still works when the feature is not used.
        captured: dict = {}

        def fake_skillstoolset(**kwargs):
            captured.update(kwargs)
            return _FakeInner(**kwargs)

        toolset = AgentSkillsToolset(sources=["/x"])
        with patch(
            "airflow.providers.common.ai.toolsets.skills._materialize_skills",
            autospec=True,
            return_value=([_SkillsLocation("/x")], lambda: None),
        ):
            with patch("pydantic_ai_skills.SkillsToolset", fake_skillstoolset):  # noqa: spec
                asyncio.run(_enter_exit(toolset))

        assert "exclude_tools" not in captured
        assert "exclude_resources" not in captured
        assert "max_retries" not in captured

    def test_negative_max_retries_is_rejected(self):
        with pytest.raises(ValueError, match="max_retries must not be negative"):
            AgentSkillsToolset(sources=["/x"], max_retries=-1)

    def test_for_run_propagates_optional_kwargs(self):
        # for_run hands each run its own instance; dropping a kwarg here would
        # silently expose excluded files in concurrent runs.
        toolset = AgentSkillsToolset(
            sources=["/x"], exclude_tools={"run_skill_script"}, exclude_resources=["*.env"], max_retries=3
        )
        per_run = asyncio.run(toolset.for_run(MagicMock()))  # noqa: spec  (for_run ignores ctx)
        assert per_run is not toolset
        assert per_run._exclude_tools == {"run_skill_script"}
        assert per_run._exclude_resources == ["*.env"]
        assert per_run._max_retries == 3

    @pytest.mark.parametrize(
        ("max_retries", "agent_retries", "fails"),
        [
            pytest.param(None, 1, True, id="agent_default_allows_one_correction"),
            pytest.param(None, 3, False, id="follows_agent_retries"),
            pytest.param(2, 1, False, id="own_budget_wins"),
        ],
    )
    def test_max_retries_bounds_corrections_in_a_real_run(self, tmp_path, max_retries, agent_retries, fails):
        """Two unknown resource names in a row need a budget of at least two corrections."""
        _write_skill(tmp_path)
        calls = iter(
            [
                {"skill_name": "demo-skill", "resource_name": "missing-1.md"},
                {"skill_name": "demo-skill", "resource_name": "missing-2.md"},
            ]
        )

        def model(messages, info):
            if (args := next(calls, None)) is None:
                return ModelResponse(parts=[TextPart("done")])
            return ModelResponse(parts=[ToolCallPart("read_skill_resource", args)])

        agent = Agent(
            FunctionModel(model),
            toolsets=[AgentSkillsToolset(sources=[str(tmp_path)], max_retries=max_retries)],
            retries={"tools": agent_retries},
        )

        if fails:
            with pytest.raises(UnexpectedModelBehavior, match="exceeded max retries count of 1"):
                agent.run_sync("go")
        else:
            assert agent.run_sync("go").output == "done"


class TestCleanup:
    def test_cleanup_runs_if_inner_construction_fails(self):
        cleanup = MagicMock()

        def boom(**kwargs):
            raise RuntimeError("inner build failed")

        toolset = AgentSkillsToolset(sources=["/x"])
        with patch(
            "airflow.providers.common.ai.toolsets.skills._materialize_skills",
            return_value=([_SkillsLocation("/x")], cleanup),
        ):
            with patch("pydantic_ai_skills.SkillsToolset", boom):
                with pytest.raises(RuntimeError, match="inner build failed"):
                    asyncio.run(toolset.__aenter__())

        cleanup.assert_called_once()


class TestMissingExtra:
    def test_helpful_error_when_package_missing(self):
        toolset = AgentSkillsToolset(sources=["./skills"])
        with patch.dict(sys.modules, {"pydantic_ai_skills": None}):
            with pytest.raises(ValueError, match=r"\[skills\]"):
                asyncio.run(toolset.__aenter__())

    def test_unsupported_installed_version_names_version_and_range(self, tmp_path):
        """pydantic-ai-skills 2.x imports but has no SkillsToolset; the error must not blame the extra."""
        agent = Agent(
            FunctionModel(lambda messages, info: ModelResponse(parts=[TextPart("done")])),
            toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])],
        )
        without_skills_toolset = types.ModuleType("pydantic_ai_skills")
        with (
            patch.dict(sys.modules, {"pydantic_ai_skills": without_skills_toolset}),
            patch("importlib.metadata.version", autospec=True, return_value="2.1.0"),
        ):
            with pytest.raises(
                ValueError, match=r"pydantic-ai-skills>=1\.2\.0,<2, but pydantic-ai-skills 2\.1\.0"
            ):
                agent.run_sync("go")


async def _enter_exit(toolset):
    await toolset.__aenter__()
    await toolset.__aexit__(None, None, None)


def _calling(*tool_calls):
    """A model that makes each (tool, args) call in turn, then answers "done"."""
    calls = iter(tool_calls)

    def model(messages, info):
        if (call := next(calls, None)) is None:
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart(*call)])

    return FunctionModel(model)


_GET_CONTEXT = "airflow.providers.common.ai.toolsets.skills.get_current_context"


class _FakeTaskInstance:
    """Keeps what is pushed, so a later pull in the same task instance reads it back."""

    task_id = "agent"
    map_index = -1

    def __init__(self, stored=None):
        self.task = types.SimpleNamespace(do_xcom_push=True)
        self.xcoms = {} if stored is None else {SKILLS_XCOM_KEY: stored}
        self.pushes = 0

    def xcom_pull(self, key, task_ids, map_indexes):
        assert (task_ids, map_indexes) == (self.task_id, self.map_index)
        return self.xcoms.get(key)

    def xcom_push(self, key, value):
        self.xcoms[key] = value
        self.pushes += 1


def _skill_entry(name, path, calls_by_run):
    calls = sum((Counter(calls) for calls in calls_by_run.values()), Counter())
    return {
        "name": name,
        "description": f"A {name} skill for tests.",
        "source": {"path": os.path.realpath(path)},
        "calls": dict(calls),
        "calls_by_run": calls_by_run,
    }


class TestSkillsXCom:
    @patch(_GET_CONTEXT, autospec=True)
    def test_pushes_available_skills_and_the_calls_made_for_each(self, mock_get_context, tmp_path):
        alpha = _write_skill(tmp_path, "alpha")
        (alpha / "notes.md").write_text("notes")
        beta = _write_skill(tmp_path, "beta")
        ti = _FakeTaskInstance()
        mock_get_context.return_value = {"ti": ti}
        model = _calling(
            ("load_skill", {"skill_name": "alpha"}),
            ("read_skill_resource", {"skill_name": "alpha", "resource_name": "notes.md"}),
            # Not a skill: the failed lookup is not counted against any skill.
            ("load_skill", {"skill_name": "missing"}),
        )

        Agent(model, toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])]).run_sync("go", run_id="run")

        assert ti.xcoms[SKILLS_XCOM_KEY] == [
            _skill_entry("alpha", alpha, {"run": {"load_skill": 1, "read_skill_resource": 1}}),
            _skill_entry("beta", beta, {}),
        ]

    @patch(_GET_CONTEXT, autospec=True)
    def test_counts_a_skill_call_approved_after_a_pause(self, mock_get_context, tmp_path):
        # The resumed run gets the paused run's transcript as history: the approved call was made
        # there, and only its return is in the resumed run's own messages.
        alpha = _write_skill(tmp_path, "alpha")
        ti = _FakeTaskInstance()
        mock_get_context.return_value = {"ti": ti}
        agent = Agent(
            _calling(("load_skill", {"skill_name": "alpha"})),
            toolsets=[
                AgentSkillsToolset(sources=[str(tmp_path)]).approval_required(
                    lambda ctx, tool_def, args: tool_def.name == "load_skill"
                )
            ],
            output_type=[str, DeferredToolRequests],
        )
        paused = agent.run_sync("go", run_id="paused")
        approvals = {call.tool_call_id: True for call in paused.output.approvals}

        agent.run_sync(
            message_history=paused.all_messages(),
            deferred_tool_results=DeferredToolResults(approvals=approvals),
            run_id="resumed",
        )

        assert ti.xcoms[SKILLS_XCOM_KEY] == [_skill_entry("alpha", alpha, {"resumed": {"load_skill": 1}})]

    @patch(_GET_CONTEXT, autospec=True)
    def test_history_seeded_from_another_task_is_not_counted(self, mock_get_context, tmp_path):
        alpha = _write_skill(tmp_path, "alpha")
        mock_get_context.return_value = {"ti": _FakeTaskInstance()}
        earlier = Agent(
            _calling(("load_skill", {"skill_name": "alpha"})),
            toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])],
        ).run_sync("go", run_id="other-task")
        ti = _FakeTaskInstance()
        mock_get_context.return_value = {"ti": ti}

        Agent(_calling(), toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])]).run_sync(
            "go on", message_history=earlier.all_messages(), run_id="run"
        )

        assert ti.xcoms[SKILLS_XCOM_KEY] == [_skill_entry("alpha", alpha, {})]

    @patch(_GET_CONTEXT, autospec=True)
    def test_counts_calls_after_a_history_processor_trims_the_transcript(self, mock_get_context, tmp_path):
        alpha = _write_skill(tmp_path, "alpha")
        mock_get_context.return_value = {"ti": _FakeTaskInstance()}
        seeded = Agent(_calling(), toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])]).run_sync(
            "go", run_id="other-task"
        )
        ti = _FakeTaskInstance()
        mock_get_context.return_value = {"ti": ti}
        agent = Agent(
            _calling(("load_skill", {"skill_name": "alpha"})),
            toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])],
            capabilities=[ProcessHistory(lambda messages: messages[-3:])],
        )

        agent.run_sync("go on", message_history=seeded.all_messages() * 3, run_id="run")

        assert ti.xcoms[SKILLS_XCOM_KEY] == [_skill_entry("alpha", alpha, {"run": {"load_skill": 1}})]

    @patch(_GET_CONTEXT, autospec=True)
    def test_adds_this_run_to_the_stored_runs_and_keeps_the_other_skills(self, mock_get_context, tmp_path):
        alpha = _write_skill(tmp_path, "alpha")
        other = _skill_entry("other", tmp_path / "elsewhere", {"earlier": {"load_skill": 3}})
        stored_alpha = _skill_entry("alpha", alpha, {"earlier": {"load_skill": 1, "read_skill_resource": 1}})
        ti = _FakeTaskInstance(stored=[stored_alpha, other])
        mock_get_context.return_value = {"ti": ti}
        model = _calling(("load_skill", {"skill_name": "alpha"}))

        Agent(model, toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])]).run_sync("go", run_id="run")

        assert ti.xcoms[SKILLS_XCOM_KEY] == [
            _skill_entry(
                "alpha",
                alpha,
                {"earlier": {"load_skill": 1, "read_skill_resource": 1}, "run": {"load_skill": 1}},
            ),
            other,
        ]

    @patch(_GET_CONTEXT, autospec=True)
    def test_toolset_built_per_run_by_a_factory_publishes(self, mock_get_context, tmp_path):
        # DynamicToolset, which AgentOperator also wraps a toolset factory in, never calls the
        # built toolset's for_run.
        alpha = _write_skill(tmp_path, "alpha")
        ti = _FakeTaskInstance()
        mock_get_context.return_value = {"ti": ti}
        toolset = DynamicToolset(lambda ctx: AgentSkillsToolset(sources=[str(tmp_path)]))

        Agent(_calling(("load_skill", {"skill_name": "alpha"})), toolsets=[toolset]).run_sync(
            "go", run_id="run"
        )

        assert ti.xcoms[SKILLS_XCOM_KEY] == [_skill_entry("alpha", alpha, {"run": {"load_skill": 1}})]

    @patch(_GET_CONTEXT, autospec=True)
    def test_entering_the_agent_publishes_only_from_the_run(self, mock_get_context, tmp_path):
        # ``async with agent:`` enters the toolset itself, besides the copy for_run hands the run.
        alpha = _write_skill(tmp_path, "alpha")
        ti = _FakeTaskInstance()
        mock_get_context.return_value = {"ti": ti}
        agent = Agent(
            _calling(("load_skill", {"skill_name": "alpha"})),
            toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])],
        )

        async def run():
            async with agent:
                await agent.run("go", run_id="run")

        asyncio.run(run())

        assert ti.xcoms[SKILLS_XCOM_KEY] == [_skill_entry("alpha", alpha, {"run": {"load_skill": 1}})]
        assert ti.pushes == 1

    @pytest.mark.parametrize(
        ("second_skill", "expected_second_calls"),
        [
            pytest.param("beta", {"load_skill": 1}, id="different_skills"),
            # Each toolset counts both calls to "alpha"; publishing twice must not count them twice.
            pytest.param("alpha", None, id="same_skill_name"),
        ],
    )
    @patch(_GET_CONTEXT, autospec=True)
    def test_two_prefixed_toolsets_each_count_their_own_skills(
        self, mock_get_context, tmp_path, second_skill, expected_second_calls
    ):
        alpha = _write_skill(tmp_path / "one", "alpha")
        second = _write_skill(tmp_path / "two", second_skill)
        ti = _FakeTaskInstance()
        mock_get_context.return_value = {"ti": ti}
        toolsets = [
            AgentSkillsToolset(sources=[str(tmp_path / "one")]).prefixed("one"),
            AgentSkillsToolset(sources=[str(tmp_path / "two")]).prefixed("two"),
        ]
        model = _calling(
            ("one_load_skill", {"skill_name": "alpha"}),
            ("two_load_skill", {"skill_name": second_skill}),
        )

        Agent(model, toolsets=toolsets).run_sync("go", run_id="run")

        if expected_second_calls is None:
            # Toolsets exit in reverse order, so the first one's source is published last.
            assert ti.xcoms[SKILLS_XCOM_KEY] == [_skill_entry("alpha", alpha, {"run": {"load_skill": 2}})]
        else:
            assert ti.xcoms[SKILLS_XCOM_KEY] == [
                _skill_entry("alpha", alpha, {"run": {"load_skill": 1}}),
                _skill_entry(second_skill, second, {"run": expected_second_calls}),
            ]

    @pytest.mark.parametrize(
        "in_task",
        [pytest.param(False, id="outside_a_task"), pytest.param(True, id="do_xcom_push_off")],
    )
    @patch(_GET_CONTEXT, autospec=True)
    def test_nothing_pushed_without_a_task_that_pushes_xcom(self, mock_get_context, tmp_path, in_task):
        _write_skill(tmp_path, "alpha")
        ti = _FakeTaskInstance()
        ti.task.do_xcom_push = False
        if in_task:
            mock_get_context.return_value = {"ti": ti}
        else:
            mock_get_context.side_effect = RuntimeError("no task")
        model = _calling(("load_skill", {"skill_name": "alpha"}))

        assert (
            Agent(model, toolsets=[AgentSkillsToolset(sources=[str(tmp_path)])]).run_sync("go").output
            == "done"
        )
        mock_get_context.assert_called_once()
        assert ti.pushes == 0

    @patch.object(
        _FakeTaskInstance, "xcom_push", autospec=True, side_effect=RuntimeError("xcom backend down")
    )
    @patch(_GET_CONTEXT, autospec=True)
    def test_failed_push_does_not_fail_the_run_or_skip_cleanup(self, mock_get_context, mock_push, tmp_path):
        _write_skill(tmp_path, "alpha")
        mock_get_context.return_value = {"ti": _FakeTaskInstance()}
        cleanup = MagicMock()
        toolset = AgentSkillsToolset(sources=[str(tmp_path)])

        with patch(
            "airflow.providers.common.ai.toolsets.skills._materialize_skills",
            return_value=([_SkillsLocation(str(tmp_path))], cleanup),
        ):
            result = Agent(_calling(), toolsets=[toolset]).run_sync("go")

        assert result.output == "done"
        mock_push.assert_called_once()
        cleanup.assert_called_once()


class TestCountSkillCalls:
    @pytest.mark.parametrize(
        ("outcome", "expected"),
        [
            pytest.param("success", {"alpha": {"run_skill_script": 1}}, id="success"),
            pytest.param("denied", {}, id="denied"),
            pytest.param("failed", {}, id="failed"),
            pytest.param("interrupted", {}, id="interrupted"),
            # A missing resource or script: pydantic-ai records the ModelRetry as a "retried" return.
            pytest.param("retried", {}, id="retried"),
        ],
    )
    def test_counts_only_calls_that_succeeded(self, outcome, expected):
        args = {"skill_name": "alpha", "script_name": "run.py"}
        messages = [
            ModelResponse(
                parts=[ToolCallPart("run_skill_script", args, tool_call_id="call-1")], run_id="run"
            ),
            ModelRequest(
                parts=[ToolReturnPart("run_skill_script", "output", tool_call_id="call-1", outcome=outcome)],
                run_id="run",
            ),
        ]

        assert _count_skill_calls(messages, "run") == expected


class TestDescribeSkillSource:
    @pytest.mark.parametrize(
        ("repo_path", "expected_path"),
        [
            pytest.param("skills", "skills/alpha", id="sub_path"),
            pytest.param("", "alpha", id="repo_root"),
        ],
    )
    def test_git_skill_names_repository_path_and_commit(self, tmp_path, repo_path, expected_path):
        skills_dir = tmp_path / "clone" / repo_path
        skill_dir = _write_skill(skills_dir, "alpha")
        source = GitSkills(repo_url="https://github.com/org/repo", path=repo_path, branch="main")
        locations = [_SkillsLocation(str(skills_dir), git=source, commit="0123abc")]

        assert _describe_skill_source(str(skill_dir), locations) == {
            "repo_url": "https://github.com/org/repo",
            "path": expected_path,
            "branch": "main",
            "commit": "0123abc",
        }

    @pytest.mark.parametrize("nested_first", [False, True], ids=["containing_first", "nested_first"])
    def test_nested_source_wins_over_the_one_containing_it(self, tmp_path, nested_first):
        skill_dir = _write_skill(tmp_path / "clone", "alpha")
        source = GitSkills(repo_url="https://github.com/org/repo")
        locations = [
            _SkillsLocation(str(tmp_path)),
            _SkillsLocation(str(tmp_path / "clone"), git=source, commit="0123abc"),
        ]
        if nested_first:
            locations.reverse()

        assert _describe_skill_source(str(skill_dir), locations)["repo_url"] == "https://github.com/org/repo"
