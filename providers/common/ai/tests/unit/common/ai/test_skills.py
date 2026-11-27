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
"""Tests for the framework-agnostic skill source resolver."""

from __future__ import annotations

import asyncio
import gc
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest import mock
from unittest.mock import patch

import pytest
from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.skills import (
    AirflowSkills,
    GitSkills,
    _materialize_skills,
    _SkillsLocation,
    resolve_skills,
)
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.ai.utils.toolset_base import MaskingCapability

pytest.importorskip("git")
pytest.importorskip("airflow.providers.git.hooks.git")


class TestGitSkills:
    def test_defaults(self):
        source = GitSkills(repo_url="https://github.com/org/repo")
        assert source.repo_url == "https://github.com/org/repo"
        assert source.conn_id is None
        assert source.path == ""
        assert source.branch is None

    def test_holds_no_secret(self):
        source = GitSkills(repo_url="u", conn_id="github_skills")
        assert "github_skills" in repr(source)
        assert not hasattr(source, "token")
        assert not hasattr(source, "password")


class TestResolveLocal:
    def test_local_dirs_pass_through_untouched(self, tmp_path):
        d1, d2 = str(tmp_path / "a"), str(tmp_path / "b")
        with resolve_skills([d1, d2]) as dirs:
            assert dirs == [d1, d2]

    def test_unsupported_source_raises_typeerror(self):
        with pytest.raises(TypeError, match="Unsupported skill source"):
            with resolve_skills([123]):
                pass


class TestResolveGitWithHook:
    """Credentials come from the git connection via GitHook; never the environment."""

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_conn_id_uses_githook_and_scrubs_config(self, mock_repo, mock_githook):
        mock_githook.return_value.repo_url = "https://user:tok@github.com/org/repo"
        mock_githook.return_value.env = {}
        src = GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default", path="skills")

        with resolve_skills([src]) as dirs:
            assert dirs[0].endswith(os.sep + "skills")

        mock_githook.assert_called_once_with(
            git_conn_id="git_default", repo_url="https://github.com/org/repo"
        )
        # Cloned via the hook's token-bearing URL...
        assert mock_repo.clone_from.call_args.args[0] == "https://user:tok@github.com/org/repo"
        # ...with interactive credential prompts disabled...
        assert mock_repo.clone_from.call_args.kwargs["env"]["GIT_TERMINAL_PROMPT"] == "0"
        # ...then .git/config is scrubbed back to the credential-free URL.
        mock_repo.clone_from.return_value.remote.return_value.set_url.assert_called_once_with(
            "https://github.com/org/repo"
        )

    @pytest.mark.parametrize(
        "repo_url",
        ["https://user:tok@github.com/org/repo", "https://tok@github.com/org/repo"],
    )
    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_repo_url_with_embedded_credentials_rejected(self, mock_repo, mock_githook, repo_url):
        # Credentials in the URL would land in the serialized DAG and be written
        # back to .git/config by the scrub -- reject them outright.
        with pytest.raises(ValueError, match="must not embed"):
            with resolve_skills([GitSkills(repo_url=repo_url, conn_id="git_default")]):
                pass
        mock_githook.assert_not_called()
        mock_repo.clone_from.assert_not_called()

    @pytest.mark.parametrize("bad_path", ["/etc", "../outside", "a/../../b"])
    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_absolute_or_traversing_path_rejected(self, mock_repo, mock_githook, bad_path):
        with pytest.raises(ValueError, match="relative sub-path"):
            with resolve_skills(
                [GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default", path=bad_path)]
            ):
                pass
        mock_repo.clone_from.assert_not_called()

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_http_with_conn_id_is_rejected(self, mock_repo, mock_githook):
        with pytest.raises(ValueError, match="http://"):
            with resolve_skills([GitSkills(repo_url="http://github.com/org/repo", conn_id="git_default")]):
                pass
        mock_githook.assert_not_called()
        mock_repo.clone_from.assert_not_called()

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_no_conn_id_clones_anonymously_without_hook(self, mock_repo, mock_githook):
        with resolve_skills([GitSkills(repo_url="https://github.com/org/repo")]):
            pass
        mock_githook.assert_not_called()
        assert mock_repo.clone_from.call_args.args[0] == "https://github.com/org/repo"

    @patch("git.Repo")
    def test_records_the_cloned_commit(self, mock_repo):
        mock_repo.clone_from.return_value.head.commit.hexsha = "0123abc"
        source = GitSkills(repo_url="https://github.com/org/repo", path="skills")

        locations, cleanup = _materialize_skills([source, "/local/skills"])
        cleanup()

        assert locations[0].git is source
        assert locations[0].commit == "0123abc"
        assert locations[0].directory.endswith(os.sep + "skills")
        assert locations[1] == _SkillsLocation("/local/skills")

    def test_empty_repository_clones_with_no_commit(self, tmp_path):
        empty = tmp_path / "empty.git"
        subprocess.run(["git", "init", "--bare", "-q", str(empty)], check=True)
        source = GitSkills(repo_url=empty.as_uri())

        locations, cleanup = _materialize_skills([source])
        cleanup()

        assert locations[0].commit is None

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_failed_clone_removes_temp_dir(self, mock_repo, mock_githook, tmp_path):
        mock_githook.return_value.repo_url = "https://github.com/org/repo"
        mock_githook.return_value.env = {}
        mock_repo.clone_from.side_effect = RuntimeError("boom")
        created = str(tmp_path / "clone")
        os.makedirs(created)
        with patch("airflow.providers.common.ai.skills.tempfile.mkdtemp", return_value=created):
            with pytest.raises(RuntimeError):
                with resolve_skills(
                    [GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default")]
                ):
                    pass
        assert not os.path.exists(created)

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_clone_error_scrubs_token_from_message(self, mock_repo, mock_githook):
        mock_githook.return_value.repo_url = "https://user:ghp_secret@github.com/org/repo"
        mock_githook.return_value.env = {}
        mock_repo.clone_from.side_effect = Exception(
            "fatal: unable to access https://user:ghp_secret@github.com/org/repo"
        )
        with pytest.raises(RuntimeError) as exc_info:
            with resolve_skills([GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default")]):
                pass
        assert "ghp_secret" not in str(exc_info.value)


class TestResolveGitCleanup:
    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_clone_dir_removed_on_exit(self, mock_repo, mock_githook):
        mock_githook.return_value.repo_url = "https://github.com/org/repo"
        mock_githook.return_value.env = {}
        with resolve_skills([GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default")]):
            dest = mock_repo.clone_from.call_args.args[1]
            assert os.path.isdir(dest)
        assert not os.path.exists(dest)


def write_skill(
    library: Path, name: str, *, description: str | None = None, files: dict[str, str] | None = None
):
    skill = library / name
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: {description or f'the {name} skill'}\n---\nInstructions for {name}\n"
    )
    for relative, content in (files or {}).items():
        (skill / relative).parent.mkdir(parents=True, exist_ok=True)
        (skill / relative).write_text(content)
    return skill


def model_calling(
    *calls: tuple[str, dict[str, Any]], answer: str = "done", seen: list[AgentInfo] | None = None
):
    """A model that makes each call in turn, one per response, then answers."""

    def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if seen is not None:
            seen.append(info)
        sent = sum(isinstance(message, ModelResponse) for message in messages)
        if sent < len(calls):
            name, args = calls[sent]
            return ModelResponse(parts=[ToolCallPart(name, args)])
        return ModelResponse(parts=[TextPart(answer)])

    return FunctionModel(model_fn)


def tool_returns(result) -> dict[str, Any]:
    return {
        part.tool_name: part.content
        for message in result.all_messages()
        for part in message.parts
        if isinstance(part, ToolReturnPart)
    }


def retried_returns(result) -> list[str]:
    return [
        str(part.content)
        for message in result.all_messages()
        for part in message.parts
        if isinstance(part, ToolReturnPart) and part.outcome == "retried"
    ]


def make_clone(tmp_path: Path, *skill_names: str, content: str = "x"):
    """A stand-in for ``_clone_git`` that makes a new clone directory with the given skills on every call."""
    clones: list[Path] = []

    def clone(source: GitSkills):
        root = tmp_path / f"clone-{len(clones)}"
        clones.append(root)
        for name in skill_names:
            write_skill(root / "skills", name, files={"references/which.txt": f"{content}-{len(clones) - 1}"})
        return str(root / "skills"), str(root), "abc123"

    return clone, clones


GIT_SOURCE = GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default")


class TestAirflowSkillsConstruction:
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            pytest.param({"sources": []}, "at least one source", id="no-sources"),
            pytest.param(
                {"sources": ["skills"], "include": ["a"], "exclude": ["b"]},
                "cannot be used together",
                id="include-and-exclude",
            ),
            pytest.param(
                {"sources": ["skills"], "scripts": "sandbox"}, "must be 'worker' or None", id="text"
            ),
            pytest.param(
                {"sources": ["skills"], "scripts": mock.create_autospec(SandboxToolset, instance=True)},
                "Running scripts in a sandbox",
                id="sandbox-toolset",
            ),
        ],
    )
    def test_rejects_invalid_arguments(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            AirflowSkills(**kwargs)

    @mock.patch("airflow.providers.common.ai.skills._clone_git", autospec=True)
    def test_does_not_resolve_sources_when_built(self, mock_clone):
        AirflowSkills([GIT_SOURCE])

        mock_clone.assert_not_called()

    @pytest.mark.parametrize(
        ("resources", "scripts", "expected"),
        [
            pytest.param(True, "worker", True, id="both"),
            pytest.param(False, None, False, id="neither"),
        ],
    )
    def test_has_a_toolset_only_when_a_tool_is_on(self, resources, scripts, expected):
        capability = AirflowSkills(["skills"], resources=resources, scripts=scripts)

        assert (capability.get_toolset() is not None) is expected

    def test_two_of_them_cannot_be_combined(self):
        with pytest.raises(ValueError, match="only one AirflowSkills"):
            AirflowSkills.combine([AirflowSkills(["a"]), AirflowSkills(["b"])])

    @pytest.mark.asyncio
    async def test_a_missing_harness_is_reported(self, tmp_path):
        write_skill(tmp_path, "alpha")
        agent = Agent(model_calling(), capabilities=[AirflowSkills([tmp_path])])

        with mock.patch.dict(sys.modules, {"pydantic_ai_harness.skills": None}):
            with pytest.raises(ValueError, match=r"apache-airflow-providers-common-ai\[skills\]"):
                await agent.run("go")


class TestAirflowSkillsRun:
    @pytest.mark.asyncio
    async def test_catalog_load_resource_and_script(self, tmp_path):
        write_skill(
            tmp_path,
            "alpha",
            files={
                "references/a.md": "alpha notes",
                "scripts/hello.py": "import sys\nprint('hello', *sys.argv[1:])\n",
            },
        )
        write_skill(tmp_path, "beta", description="the beta skill")
        seen: list[AgentInfo] = []
        agent = Agent(
            model_calling(
                ("load_capability", {"id": "alpha"}),
                ("read_skill_resource", {"skill_name": "alpha", "resource": "${SKILL_DIR}/references/a.md"}),
                ("run_skill_script", {"skill_name": "alpha", "script": "scripts/hello.py", "args": {"n": 1}}),
                seen=seen,
            ),
            capabilities=[AirflowSkills([tmp_path])],
        )

        result = await agent.run("go")

        assert tool_returns(result)["read_skill_resource"] == "alpha notes"
        assert tool_returns(result)["run_skill_script"] == "hello --n 1"
        instructions = seen[0].instructions or ""
        assert "- alpha: the alpha skill" in instructions
        assert "- beta: the beta skill" in instructions

    @pytest.mark.asyncio
    async def test_a_skill_that_was_not_loaded_is_refused_until_it_is(self, tmp_path):
        write_skill(tmp_path, "alpha", files={"references/a.md": "alpha notes"})
        call = {"skill_name": "alpha", "resource": "references/a.md"}
        agent = Agent(
            model_calling(
                ("read_skill_resource", call),
                ("load_capability", {"id": "alpha"}),
                ("read_skill_resource", call),
            ),
            capabilities=[AirflowSkills([tmp_path])],
        )

        result = await agent.run("go")

        assert "load_capability" in retried_returns(result)[0]
        assert tool_returns(result)["read_skill_resource"] == "alpha notes"

    @pytest.mark.parametrize(
        ("options", "expected"),
        [
            pytest.param({}, {"read_skill_resource", "run_skill_script"}, id="default"),
            pytest.param({"resources": False}, {"run_skill_script"}, id="no-resources"),
            pytest.param({"scripts": None}, {"read_skill_resource"}, id="no-scripts"),
            pytest.param({"resources": False, "scripts": None}, set(), id="no-file-tools"),
        ],
    )
    @pytest.mark.asyncio
    async def test_the_model_sees_only_the_enabled_tools(self, tmp_path, options, expected):
        write_skill(tmp_path, "alpha")
        seen: list[AgentInfo] = []
        agent = Agent(model_calling(seen=seen), capabilities=[AirflowSkills([tmp_path], **options)])

        await agent.run("go")

        assert {tool.name for tool in seen[0].function_tools} - {"load_capability"} == expected

    @pytest.mark.parametrize(
        ("selection", "expected"),
        [
            pytest.param({"include": ["alpha"]}, {"alpha"}, id="include"),
            pytest.param({"exclude": ["alpha"]}, {"beta"}, id="exclude"),
        ],
    )
    @pytest.mark.asyncio
    async def test_include_and_exclude_choose_the_skills(self, tmp_path, selection, expected):
        write_skill(tmp_path, "alpha")
        write_skill(tmp_path, "beta")
        seen: list[AgentInfo] = []
        agent = Agent(model_calling(seen=seen), capabilities=[AirflowSkills([tmp_path], **selection)])

        await agent.run("go")

        listed = {
            line.split(":")[0].removeprefix("- ")
            for line in (seen[0].instructions or "").splitlines()
            if line.startswith("- ")
        }
        assert listed == expected

    @pytest.mark.asyncio
    async def test_a_later_source_wins_a_name_clash(self, tmp_path):
        for label in ("first", "second"):
            write_skill(tmp_path / label, "dup", description=label, files={"references/which.txt": label})
        agent = Agent(
            model_calling(
                ("load_capability", {"id": "dup"}),
                ("read_skill_resource", {"skill_name": "dup", "resource": "references/which.txt"}),
            ),
            capabilities=[AirflowSkills([tmp_path / "first", tmp_path / "second"])],
        )

        with pytest.warns(UserWarning, match="dup"):
            result = await agent.run("go")

        assert tool_returns(result)["read_skill_resource"] == "second"

    @pytest.mark.asyncio
    async def test_resolves_relative_local_sources_against_the_working_directory(self, tmp_path, monkeypatch):
        write_skill(tmp_path / "skills", "alpha")
        monkeypatch.chdir(tmp_path)
        seen: list[AgentInfo] = []
        agent = Agent(model_calling(seen=seen), capabilities=[AirflowSkills(["skills"])])

        await agent.run("go")

        assert "- alpha:" in (seen[0].instructions or "")

    @pytest.mark.enable_redact
    @pytest.mark.asyncio
    async def test_a_tool_result_is_masked(self, tmp_path, registered_secret):
        write_skill(tmp_path, "alpha", files={"references/creds.txt": f"password={registered_secret}"})
        agent = Agent(
            model_calling(
                ("load_capability", {"id": "alpha"}),
                ("read_skill_resource", {"skill_name": "alpha", "resource": "references/creds.txt"}),
            ),
            capabilities=[AirflowSkills([tmp_path]), MaskingCapability()],
        )

        result = await agent.run("go")

        assert tool_returns(result)["read_skill_resource"] == "password=***"

    @mock.patch("airflow.providers.common.ai.skills._clone_git", autospec=True)
    @pytest.mark.asyncio
    async def test_concurrent_runs_see_only_their_own_clone(self, mock_clone, tmp_path):
        clone, clones = make_clone(tmp_path, "alpha")
        mock_clone.side_effect = clone
        capability = AirflowSkills([GIT_SOURCE])
        gate = asyncio.Event()
        toolset = FunctionToolset()

        @toolset.tool_plain
        async def wait_for_the_other_run() -> str:
            if len(clones) < 2:
                await gate.wait()
            gate.set()
            return "ok"

        def build() -> Agent[None, str]:
            return Agent(
                model_calling(
                    ("load_capability", {"id": "alpha"}),
                    ("wait_for_the_other_run", {}),
                    ("read_skill_resource", {"skill_name": "alpha", "resource": "references/which.txt"}),
                ),
                capabilities=[capability],
                toolsets=[toolset],
            )

        first, second = await asyncio.gather(build().run("go"), build().run("go"))

        assert {tool_returns(first)["read_skill_resource"], tool_returns(second)["read_skill_resource"]} == {
            "x-0",
            "x-1",
        }
        assert capability._runs == {}
        assert not any(clone_dir.exists() for clone_dir in clones)


class TestAirflowSkillsCleanup:
    @staticmethod
    def build_agent(model, capability, *extra_tools):
        toolset = FunctionToolset()
        for tool in extra_tools:
            toolset.tool_plain(tool)
        return Agent(model, capabilities=[capability], toolsets=[toolset])

    @staticmethod
    def hold_run_capabilities(monkeypatch) -> list[Any]:
        """Keep every per-run capability alive, so only the end of the run can have removed its clone."""
        held: list[Any] = []
        original = AirflowSkills.for_run

        async def for_run(self, ctx):
            run = await original(self, ctx)
            held.append(run)
            return run

        monkeypatch.setattr(AirflowSkills, "for_run", for_run)
        return held

    @mock.patch("airflow.providers.common.ai.skills._clone_git", autospec=True)
    @pytest.mark.asyncio
    async def test_a_successful_run_removes_the_clone(self, mock_clone, tmp_path, monkeypatch):
        mock_clone.side_effect, clones = make_clone(tmp_path, "alpha")
        held = self.hold_run_capabilities(monkeypatch)
        capability = AirflowSkills([GIT_SOURCE])
        agent = Agent(model_calling(("load_capability", {"id": "alpha"})), capabilities=[capability])

        await agent.run("go")

        assert held
        assert len(clones) == 1
        assert not clones[0].exists()
        assert capability._runs == {}

    @mock.patch("airflow.providers.common.ai.skills._clone_git", autospec=True)
    @pytest.mark.asyncio
    async def test_a_failed_run_removes_the_clone(self, mock_clone, tmp_path, monkeypatch):
        mock_clone.side_effect, clones = make_clone(tmp_path, "alpha")
        held = self.hold_run_capabilities(monkeypatch)

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            raise RuntimeError("worker died")

        agent = Agent(FunctionModel(model_fn), capabilities=[AirflowSkills([GIT_SOURCE])])

        with pytest.raises(RuntimeError, match="worker died"):
            await agent.run("go")

        assert held
        assert not clones[0].exists()

    @mock.patch("airflow.providers.common.ai.skills._clone_git", autospec=True)
    @pytest.mark.asyncio
    async def test_a_cancelled_run_removes_the_clone(self, mock_clone, tmp_path):
        mock_clone.side_effect, clones = make_clone(tmp_path, "alpha")
        started = asyncio.Event()

        async def hang() -> str:
            started.set()
            await asyncio.sleep(60)
            return "unreachable"

        agent = self.build_agent(model_calling(("hang", {})), AirflowSkills([GIT_SOURCE]), hang)
        task = asyncio.create_task(agent.run("go"))
        await asyncio.wait_for(started.wait(), timeout=10)
        assert clones[0].exists()

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert not clones[0].exists()

    @mock.patch("airflow.providers.common.ai.skills._clone_git", autospec=True)
    @pytest.mark.asyncio
    async def test_a_harness_failure_while_the_run_starts_removes_the_clone(self, mock_clone, tmp_path):
        mock_clone.side_effect, clones = make_clone(tmp_path, "alpha")
        capability = AirflowSkills([GIT_SOURCE], exclude=["no-such-skill"])
        agent = Agent(model_calling(), capabilities=[capability])

        with pytest.raises(ValueError, match="no-such-skill"):
            await agent.run("go")

        assert not clones[0].exists()
        assert capability._runs == {}

    @pytest.mark.asyncio
    async def test_a_missing_local_source_fails_the_run_and_registers_nothing(self, tmp_path):
        capability = AirflowSkills([tmp_path / "missing"])
        agent = Agent(model_calling(), capabilities=[capability])

        with pytest.raises(ValueError, match="library directory does not exist"):
            await agent.run("go")

        assert capability._runs == {}

    @pytest.mark.asyncio
    async def test_local_sources_are_never_removed(self, tmp_path):
        library = tmp_path / "library"
        write_skill(library, "alpha")

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            raise RuntimeError("worker died")

        agent = Agent(FunctionModel(model_fn), capabilities=[AirflowSkills([library])])

        with pytest.raises(RuntimeError):
            await agent.run("go")

        assert (library / "alpha" / "SKILL.md").exists()

    @mock.patch("airflow.providers.common.ai.skills._clone_git", autospec=True)
    @pytest.mark.asyncio
    async def test_a_run_that_never_starts_is_cleaned_up_when_it_is_dropped(self, mock_clone, tmp_path):
        mock_clone.side_effect, clones = make_clone(tmp_path, "alpha")
        capability = AirflowSkills([GIT_SOURCE])
        ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage(), run_id="run-1")

        offered = await capability.for_run(ctx)
        assert clones[0].exists()
        assert "run-1" in capability._runs

        del offered
        gc.collect()

        assert not clones[0].exists()
        assert capability._runs == {}
