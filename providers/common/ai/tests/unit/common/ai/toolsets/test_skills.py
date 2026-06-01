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
