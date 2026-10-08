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
import sys
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import FunctionModel

from airflow.providers.common.ai.skills import GitSkills
from airflow.providers.common.ai.toolsets.skills import AgentSkillsToolset

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


def _write_skill(directory):
    skill_dir = directory / "demo-skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: demo-skill\ndescription: A demo skill for tests.\n---\n\n# Demo\n"
    )


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
            return_value=(["/x"], lambda: None),
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
            return_value=(["/x"], lambda: None),
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
            return_value=(["/x"], lambda: None),
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
            return_value=(["/x"], cleanup),
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


async def _enter_exit(toolset):
    await toolset.__aenter__()
    await toolset.__aexit__(None, None, None)
