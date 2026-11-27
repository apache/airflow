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
import shutil
import stat
import sys
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from pydantic_ai import ModelRetry, RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.utils import skill_files
from airflow.providers.common.ai.utils.skill_files import SkillRunState, build_skill_files_toolset

RUN_ID = "run-1"


def make_context(*, loaded: set[str] | None = None, run_id: str = RUN_ID, max_retries: int = 3):
    return RunContext(
        deps=None,
        model=TestModel(),
        usage=RunUsage(),
        run_id=run_id,
        loaded_capability_ids={"demo"} if loaded is None else loaded,
        max_retries=max_retries,
    )


def make_toolset(skill_dir: Path, **overrides: Any):
    runs = {RUN_ID: SkillRunState(skill_dirs={"demo": skill_dir})}
    options: dict[str, Any] = {
        "runs": runs,
        "resources": True,
        "scripts": True,
        "exclude_resources": [],
        "max_retries": None,
    }
    options.update(overrides)
    toolset = build_skill_files_toolset(**options)
    assert toolset is not None
    return toolset


async def call(toolset, name: str, ctx: RunContext[Any] | None = None, **tool_args: Any) -> Any:
    ctx = ctx or make_context()
    tools = await toolset.get_tools(ctx)
    return await toolset.call_tool(name, tool_args, ctx, tools[name])


@pytest.fixture
def skill_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "demo"
    (directory / "scripts").mkdir(parents=True)
    (directory / "references").mkdir()
    (directory / "SKILL.md").write_text("---\nname: demo\ndescription: demo skill\n---\nBody\n")
    return directory


class TestToolRegistration:
    @pytest.mark.parametrize(
        ("resources", "scripts", "expected"),
        [
            pytest.param(True, True, {"read_skill_resource", "run_skill_script"}, id="both"),
            pytest.param(True, False, {"read_skill_resource"}, id="resources-only"),
            pytest.param(False, True, {"run_skill_script"}, id="scripts-only"),
        ],
    )
    @pytest.mark.asyncio
    async def test_registers_only_the_enabled_tools(self, skill_dir, resources, scripts, expected):
        toolset = make_toolset(skill_dir, resources=resources, scripts=scripts)

        assert set(await toolset.get_tools(make_context())) == expected

    def test_returns_none_when_both_tools_are_off(self):
        assert (
            build_skill_files_toolset(
                runs={}, resources=False, scripts=False, exclude_resources=[], max_retries=None
            )
            is None
        )

    def test_toolset_id_is_stable(self, skill_dir):
        assert make_toolset(skill_dir).id == "airflow_skills"

    @pytest.mark.parametrize(
        ("max_retries", "expected"),
        [pytest.param(None, 3, id="from-the-agent"), pytest.param(0, 0, id="explicit-zero"), (5, 5)],
    )
    @pytest.mark.asyncio
    async def test_max_retries(self, skill_dir, max_retries, expected):
        toolset = make_toolset(skill_dir, max_retries=max_retries)

        tools = await toolset.get_tools(make_context(max_retries=3))

        assert {tool.max_retries for tool in tools.values()} == {expected}

    def test_negative_max_retries_is_rejected(self, skill_dir):
        with pytest.raises(ValueError, match="max_retries must not be negative"):
            make_toolset(skill_dir, max_retries=-1)


class TestSkillLookup:
    @pytest.mark.parametrize("tool_args", [{"resource": "references/a.md"}, {"script": "scripts/a.py"}])
    @pytest.mark.asyncio
    async def test_refuses_a_skill_that_is_not_loaded(self, skill_dir, tool_args):
        (skill_dir / "references" / "a.md").write_text("a")
        name = "read_skill_resource" if "resource" in tool_args else "run_skill_script"

        with pytest.raises(ModelRetry, match="load_capability"):
            await call(
                make_toolset(skill_dir),
                name,
                make_context(loaded=set()),
                skill_name="demo",
                **tool_args,
            )

    @pytest.mark.asyncio
    async def test_unknown_skill_lists_the_available_ones(self, skill_dir):
        with pytest.raises(ModelRetry, match="Available skills: demo"):
            await call(make_toolset(skill_dir), "read_skill_resource", skill_name="nope", resource="a.md")

    @pytest.mark.asyncio
    async def test_a_run_only_sees_its_own_skills(self, skill_dir):
        with pytest.raises(ModelRetry, match="Unknown skill"):
            await call(
                make_toolset(skill_dir),
                "read_skill_resource",
                make_context(run_id="another-run"),
                skill_name="demo",
                resource="a.md",
            )


class TestReadSkillResource:
    @pytest.mark.parametrize(
        ("name", "content", "expected"),
        [
            pytest.param("notes.md", "# Notes\n", "# Notes\n", id="text"),
            pytest.param("data.json", '{"a": 1}', {"a": 1}, id="json"),
            pytest.param("broken.json", "{not json", "{not json", id="invalid-json-stays-text"),
            pytest.param("data.yaml", "a: 1\nb: [x]\n", {"a": 1, "b": ["x"]}, id="yaml"),
            pytest.param("data.yml", "a: 1\n", {"a": 1}, id="yml"),
            pytest.param("broken.yaml", "a: [1\n", "a: [1\n", id="invalid-yaml-stays-text"),
        ],
    )
    @pytest.mark.asyncio
    async def test_reads_a_resource(self, skill_dir, name, content, expected):
        (skill_dir / "references" / name).write_text(content)

        result = await call(
            make_toolset(skill_dir),
            "read_skill_resource",
            skill_name="demo",
            resource=f"references/{name}",
        )

        assert result == expected

    @pytest.mark.parametrize("prefix", ["${SKILL_DIR}/", "${CLAUDE_SKILL_DIR}/"])
    @pytest.mark.asyncio
    async def test_skill_dir_prefix_names_the_skill_root(self, skill_dir, prefix):
        (skill_dir / "references" / "a.md").write_text("alpha")

        result = await call(
            make_toolset(skill_dir),
            "read_skill_resource",
            skill_name="demo",
            resource=f"{prefix}references/a.md",
        )

        assert result == "alpha"

    @pytest.mark.parametrize(
        ("resource", "message"),
        [
            pytest.param("/etc/hostname", "not a path inside the skill", id="absolute"),
            pytest.param("../other/secret.md", "not a path inside the skill", id="parent"),
            pytest.param("references/../../x", "not a path inside the skill", id="nested-parent"),
            pytest.param("", "not a path inside the skill", id="empty"),
            pytest.param("${SKILL_DIR}/", "not a path inside the skill", id="prefix-only"),
            pytest.param("references/__pycache__/a.md", "excluded", id="default-exclude-directory"),
            pytest.param("references/.DS_Store", "excluded", id="default-exclude-name"),
            pytest.param("references/old.draft", "excluded", id="custom-exclude"),
            pytest.param("references", "not a file", id="directory"),
            pytest.param("references/missing.md", "not a file", id="missing"),
            pytest.param("references/link.md", "outside the skill", id="symlink-escape"),
            pytest.param("references/link-to-excluded", "excluded", id="symlink-to-excluded-name"),
            pytest.param("references/binary.bin", "not UTF-8", id="binary"),
            pytest.param("references/.GIT/config", "excluded", id="exclude-ignores-case"),
            pytest.param("references/a\0.md", "NUL character", id="nul"),
        ],
    )
    @pytest.mark.asyncio
    async def test_refuses_a_path_it_should_not_read(self, tmp_path, skill_dir, resource, message):
        secret = tmp_path / "secret.md"
        secret.write_text("secret")
        (skill_dir / "references" / "__pycache__").mkdir()
        (skill_dir / "references" / "__pycache__" / "a.md").write_text("x")
        (skill_dir / "references" / ".DS_Store").write_text("x")
        (skill_dir / "references" / "old.draft").write_text("x")
        (skill_dir / "references" / "link.md").symlink_to(secret)
        (skill_dir / "references" / "real.md").write_text("x")
        (skill_dir / "references" / ".git").write_text("x")
        (skill_dir / "references" / "link-to-excluded").symlink_to(skill_dir / "references" / ".git")
        (skill_dir / "references" / "binary.bin").write_bytes(b"\xff\xfe\x00\x80")

        with pytest.raises(ModelRetry, match=message):
            await call(
                make_toolset(skill_dir, exclude_resources=["*.draft"]),
                "read_skill_resource",
                skill_name="demo",
                resource=resource,
            )

    @pytest.mark.asyncio
    async def test_a_resource_over_the_size_cap_is_refused(self, skill_dir, monkeypatch):
        monkeypatch.setattr(skill_files, "_MAX_RESOURCE_BYTES", 10)
        (skill_dir / "references" / "exact.md").write_text("x" * 10)
        (skill_dir / "references" / "big.md").write_text("x" * 11)
        toolset = make_toolset(skill_dir)

        exact = await call(toolset, "read_skill_resource", skill_name="demo", resource="references/exact.md")
        with pytest.raises(ModelRetry, match="larger than 10 bytes"):
            await call(toolset, "read_skill_resource", skill_name="demo", resource="references/big.md")

        assert exact == "x" * 10

    @pytest.mark.asyncio
    async def test_a_symlink_inside_the_skill_is_followed(self, skill_dir):
        (skill_dir / "references" / "real.md").write_text("real")
        (skill_dir / "references" / "alias.md").symlink_to(skill_dir / "references" / "real.md")

        result = await call(
            make_toolset(skill_dir), "read_skill_resource", skill_name="demo", resource="references/alias.md"
        )

        assert result == "real"

    @pytest.mark.asyncio
    async def test_exclude_resources_extend_the_defaults(self, skill_dir):
        (skill_dir / "references" / ".DS_Store").write_text("x")
        (skill_dir / "references" / "a.md").write_text("alpha")
        toolset = make_toolset(skill_dir, exclude_resources=["private/*"])

        with pytest.raises(ModelRetry, match="excluded"):
            await call(toolset, "read_skill_resource", skill_name="demo", resource="references/.DS_Store")
        assert (
            await call(toolset, "read_skill_resource", skill_name="demo", resource="references/a.md")
            == "alpha"
        )


class TestRunSkillScript:
    @staticmethod
    def write_script(skill_dir: Path, name: str, body: str, *, executable: bool = False) -> Path:
        path = skill_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
        if executable:
            path.chmod(path.stat().st_mode | stat.S_IXUSR)
        return path

    @pytest.mark.asyncio
    async def test_runs_a_python_script_in_the_skill_directory(self, skill_dir):
        self.write_script(
            skill_dir, "scripts/where.py", "import os, sys\nprint(os.getcwd())\nprint(sys.executable)\n"
        )

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/where.py"
        )

        cwd, executable = output.splitlines()
        assert Path(cwd) == skill_dir.resolve()
        assert executable == sys.executable

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            pytest.param(None, [], id="none"),
            pytest.param({}, [], id="empty"),
            pytest.param({"week": 42}, ["--week", "42"], id="number"),
            pytest.param({"max-papers": 5}, ["--max-papers", "5"], id="key-kept-as-is"),
            pytest.param({"verbose": True, "quiet": False}, ["--verbose"], id="booleans"),
            pytest.param({"tag": ["a", "b"]}, ["--tag", "a", "--tag", "b"], id="list-repeats-the-flag"),
            pytest.param({"skip": None, "name": "x"}, ["--name", "x"], id="none-value-is-dropped"),
        ],
    )
    @pytest.mark.asyncio
    async def test_arguments_become_flags(self, skill_dir, args, expected):
        self.write_script(skill_dir, "scripts/echo.py", "import json, sys\nprint(json.dumps(sys.argv[1:]))\n")

        output = await call(
            make_toolset(skill_dir),
            "run_skill_script",
            skill_name="demo",
            script="scripts/echo.py",
            args=args,
        )

        assert output == str(expected).replace("'", '"')

    @pytest.mark.parametrize(
        "args",
        [
            pytest.param({"a": "b\0"}, id="value"),
            pytest.param({"a": ["b", "c\0"]}, id="list-item"),
        ],
    )
    @pytest.mark.asyncio
    async def test_arguments_with_a_nul_character_are_refused(self, skill_dir, args):
        self.write_script(skill_dir, "scripts/echo.py", "print('ran')\n")

        with pytest.raises(ModelRetry, match="NUL character"):
            await call(
                make_toolset(skill_dir),
                "run_skill_script",
                skill_name="demo",
                script="scripts/echo.py",
                args=args,
            )

    @pytest.mark.parametrize(
        "key",
        [
            pytest.param("", id="empty"),
            pytest.param("-x", id="leading-dash"),
            pytest.param("--rm", id="double-dash"),
            pytest.param("a=b", id="equals"),
            pytest.param("a b", id="space"),
            pytest.param("a\0", id="nul"),
            pytest.param("a\n", id="trailing-newline"),
        ],
    )
    @pytest.mark.asyncio
    async def test_argument_names_that_are_not_flag_names_are_refused(self, skill_dir, key):
        self.write_script(skill_dir, "scripts/echo.py", "print('ran')\n")

        with pytest.raises(ModelRetry, match="Argument names must start with a letter"):
            await call(
                make_toolset(skill_dir),
                "run_skill_script",
                skill_name="demo",
                script="scripts/echo.py",
                args={key: 1},
            )

    @pytest.mark.asyncio
    async def test_scripts_in_the_skill_root_and_the_skill_dir_prefix(self, skill_dir):
        self.write_script(skill_dir, "root.py", "print('root')\n")

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="${SKILL_DIR}/root.py"
        )

        assert output == "root"

    @pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX shell")
    @pytest.mark.asyncio
    async def test_a_shebang_picks_the_interpreter(self, skill_dir):
        self.write_script(skill_dir, "scripts/tool", "#!/usr/bin/env sh\necho shebang\n", executable=True)

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/tool"
        )

        assert output == "shebang"

    @pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX shell")
    @pytest.mark.asyncio
    async def test_a_shell_script_without_a_shebang_runs_under_sh(self, skill_dir):
        self.write_script(skill_dir, "scripts/hi.sh", 'echo hi "$1"\n')

        output = await call(
            make_toolset(skill_dir),
            "run_skill_script",
            skill_name="demo",
            script="scripts/hi.sh",
            args={"a": 1},
        )

        assert output == "hi --a"

    @pytest.mark.parametrize(
        ("suffix", "interpreter"),
        [
            pytest.param(".sh", "sh", id="sh"),
            pytest.param(".bash", "bash", id="bash"),
            pytest.param(".zsh", "zsh", id="zsh"),
        ],
    )
    @pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX shell")
    @pytest.mark.asyncio
    async def test_a_script_suffix_picks_its_interpreter(self, skill_dir, suffix, interpreter):
        if shutil.which(interpreter) is None:
            pytest.skip(f"{interpreter} is not installed")
        self.write_script(skill_dir, f"scripts/hi{suffix}", "echo hello\n")

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script=f"scripts/hi{suffix}"
        )

        assert output == "hello"

    @pytest.mark.parametrize("suffix", [".ps1", ".bat", ".cmd", ".fish"])
    @pytest.mark.asyncio
    async def test_a_suffix_that_cannot_run_on_posix_is_not_a_script(self, skill_dir, suffix):
        self.write_script(skill_dir, f"scripts/tool{suffix}", "echo x\n")

        with pytest.raises(ModelRetry, match="not a runnable script"):
            await call(
                make_toolset(skill_dir), "run_skill_script", skill_name="demo", script=f"scripts/tool{suffix}"
            )

    @pytest.mark.asyncio
    async def test_an_executable_with_an_unresolvable_shebang_is_reported(self, skill_dir):
        self.write_script(skill_dir, "scripts/tool", "#!/nonexistent/interpreter\n", executable=True)

        with pytest.raises(ModelRetry, match="Failed to start"):
            await call(make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/tool")

    @pytest.mark.asyncio
    async def test_stderr_is_merged_and_a_failure_is_reported_as_text(self, skill_dir):
        self.write_script(
            skill_dir,
            "scripts/fail.py",
            "import sys\nprint('out')\nprint('err', file=sys.stderr)\nsys.exit(3)\n",
        )

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/fail.py"
        )

        assert "out" in output
        assert "err" in output
        assert output.endswith("Script exited with code 3")

    @pytest.mark.asyncio
    async def test_a_script_with_no_output(self, skill_dir):
        self.write_script(skill_dir, "scripts/quiet.py", "pass\n")

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/quiet.py"
        )

        assert output == "(no output)"

    @pytest.mark.asyncio
    async def test_undecodable_output_is_replaced(self, skill_dir):
        self.write_script(
            skill_dir, "scripts/bytes.py", "import sys\nsys.stdout.buffer.write(b'ok \\xff done')\n"
        )

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/bytes.py"
        )

        assert output == "ok � done"

    @pytest.mark.asyncio
    async def test_long_output_is_truncated(self, skill_dir, monkeypatch):
        monkeypatch.setattr(skill_files, "_MAX_SCRIPT_OUTPUT_BYTES", 10)
        self.write_script(skill_dir, "scripts/long.py", "print('x' * 100)\n")

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/long.py"
        )

        assert output == "x" * 10 + "\n[output truncated at 10 bytes and the script was killed]"

    @pytest.mark.skipif(sys.platform == "win32", reason="needs process groups")
    @pytest.mark.asyncio
    async def test_a_script_that_floods_the_pipe_is_cut_off_at_the_output_cap(self, skill_dir, monkeypatch):
        """Writes far more than the stream reader buffers at once, so the pipe backs up behind the cap."""
        monkeypatch.setattr(skill_files, "_MAX_SCRIPT_OUTPUT_BYTES", 1000)
        monkeypatch.setattr(skill_files, "_SCRIPT_TIMEOUT_SECONDS", 3)
        # Longer than the timeout above: if the killed script's pipe is not drained, waiting for it
        # outlasts the timeout and the model gets the timeout note instead of the truncation note.
        monkeypatch.setattr(skill_files, "_REAP_TIMEOUT_SECONDS", 6)
        self.write_script(
            skill_dir, "scripts/flood.py", "import os\nwhile True:\n    os.write(1, b'x' * 262144)\n"
        )

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/flood.py"
        )

        assert output.endswith("[output truncated at 1000 bytes and the script was killed]")

    @pytest.mark.skipif(sys.platform == "win32", reason="needs process groups")
    @pytest.mark.asyncio
    async def test_a_script_that_outlives_the_timeout_is_killed(self, skill_dir, monkeypatch):
        monkeypatch.setattr(skill_files, "_SCRIPT_TIMEOUT_SECONDS", 0.5)
        marker = skill_dir / "survived"
        self.write_script(
            skill_dir,
            "scripts/slow.py",
            f"import time, pathlib\ntime.sleep(3)\npathlib.Path({str(marker)!r}).write_text('x')\n",
        )

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/slow.py"
        )

        assert output == "Script 'slow.py' timed out after 0.5 seconds and was killed."
        assert not marker.exists()

    @pytest.mark.skipif(sys.platform == "win32", reason="needs process groups")
    @pytest.mark.asyncio
    async def test_a_timeout_keeps_the_output_printed_before_it(self, skill_dir, monkeypatch):
        """A background child keeps the pipe open after the script exits, so the read ends only at the timeout."""
        monkeypatch.setattr(skill_files, "_SCRIPT_TIMEOUT_SECONDS", 0.5)
        self.write_script(
            skill_dir,
            "scripts/spawn.py",
            "import subprocess\nprint('started', flush=True)\nsubprocess.Popen(['sleep', '30'])\n",
        )

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/spawn.py"
        )

        assert output == "started\nScript 'spawn.py' timed out after 0.5 seconds and was killed."

    @pytest.mark.parametrize(
        ("script", "message"),
        [
            pytest.param(
                "scripts/nested/deep.py", "scripts live in the skill root or in scripts/", id="nested"
            ),
            pytest.param(
                "references/tool.py", "scripts live in the skill root or in scripts/", id="other-directory"
            ),
            pytest.param("scripts/__init__.py", "not a runnable script", id="package-marker"),
            pytest.param("scripts/data.txt", "not a runnable script", id="not-a-script"),
            pytest.param("SKILL.md", "not a runnable script", id="skill-file"),
            pytest.param("../escape.py", "not a path inside the skill", id="parent"),
            pytest.param("scripts/__pycache__/x.py", "excluded", id="excluded"),
            pytest.param("scripts/missing.py", "not a file", id="missing"),
        ],
    )
    @pytest.mark.asyncio
    async def test_refuses_what_is_not_a_script(self, skill_dir, script, message):
        for name in (
            "scripts/nested/deep.py",
            "references/tool.py",
            "scripts/__init__.py",
            "scripts/data.txt",
        ):
            self.write_script(skill_dir, name, "print('x')\n")
        self.write_script(skill_dir, "scripts/__pycache__/x.py", "print('x')\n")

        with pytest.raises(ModelRetry, match=message):
            await call(make_toolset(skill_dir), "run_skill_script", skill_name="demo", script=script)

    @pytest.mark.skipif(sys.platform == "win32", reason="needs symlinks")
    @pytest.mark.asyncio
    async def test_a_symlinked_script_pointing_outside_is_refused(self, tmp_path, skill_dir):
        outside = tmp_path / "outside.py"
        outside.write_text("print('escaped')\n")
        (skill_dir / "scripts" / "link.py").symlink_to(outside)

        with pytest.raises(ModelRetry, match="outside the skill directory"):
            await call(
                make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/link.py"
            )

    @pytest.mark.asyncio
    async def test_a_script_runs_with_the_workers_environment(self, skill_dir, monkeypatch):
        monkeypatch.setenv("SKILL_TEST_FLAG", "set")
        self.write_script(
            skill_dir, "scripts/env.py", "import os\nprint(os.environ.get('SKILL_TEST_FLAG', 'unset'))\n"
        )

        output = await call(
            make_toolset(skill_dir), "run_skill_script", skill_name="demo", script="scripts/env.py"
        )

        assert output == "set"


class TestReapProcess:
    @pytest.mark.asyncio
    @mock.patch.object(skill_files, "_REAP_TIMEOUT_SECONDS", 0.05)
    @mock.patch.object(skill_files, "_kill_process", autospec=True)
    async def test_it_reads_the_pipe_to_eof_before_waiting_for_the_process(self, mock_kill):
        """A process whose wait only returns once its pipe is closed hangs unless the pipe is drained."""
        pipe_closed = asyncio.Event()
        chunks = [b"x" * 10, b""]

        async def read(_size):
            chunk = chunks.pop(0)
            if not chunk:
                pipe_closed.set()
            return chunk

        waited = False

        async def wait():
            nonlocal waited
            await pipe_closed.wait()
            waited = True
            return -9

        process = mock.create_autospec(asyncio.subprocess.Process, instance=True)
        process.stdout = mock.create_autospec(asyncio.StreamReader, instance=True)
        process.stdout.read.side_effect = read
        process.wait.side_effect = wait

        await skill_files._reap_process(process)

        mock_kill.assert_called_once_with(process)
        assert not chunks
        assert waited
