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
from unittest.mock import MagicMock

import pytest
from pydantic_ai._run_context import RunContext
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.toolsets.abstract import ToolsetTool
from pydantic_core import ValidationError

from airflow.providers.common.ai.sandbox.base import (
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset

TOOL_NAMES = ["list_directory", "read_file", "run_command", "write_file"]


class _RecordingBackend(SandboxBackend):
    """Backend double that records calls and can be told to fail on demand."""

    name = "rec"

    def __init__(self, *, destroy_error: Exception | None = None, run_result=None, run_error=None):
        self.created: list[SandboxSpec | None] = []
        self.destroyed: list[str] = []
        self.commands: list[tuple[str, str, float, int]] = []
        self.written: dict[str, bytes] = {}
        self.entries: list[tuple[str, bool]] = []
        self.destroy_error = destroy_error
        self.run_result = run_result
        self.run_error = run_error
        self.read_payload = b""
        self.read_error: Exception | None = None

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        self.created.append(spec)
        return f"box-{len(self.created)}"

    def run_command(self, sandbox, command, *, timeout, max_output_bytes):
        self.commands.append((sandbox, command, timeout, max_output_bytes))
        if self.run_error is not None:
            raise self.run_error
        if self.run_result is not None:
            return self.run_result
        return SandboxExecResult(exit_code=0, stdout="out\n", stderr="")

    def read_file(self, sandbox, path, *, max_bytes):
        if self.read_error is not None:
            raise self.read_error
        return self.read_payload

    def write_file(self, sandbox, path, content):
        self.written[path] = content

    def list_directory(self, sandbox, path):
        return list(self.entries)

    def destroy(self, sandbox):
        self.destroyed.append(sandbox)
        if self.destroy_error is not None:
            raise self.destroy_error


def _ctx():
    return MagicMock(spec=RunContext)


def _tool():
    return MagicMock(spec=ToolsetTool)


async def _call(ts: SandboxToolset, name: str, args: dict):
    return await ts.call_tool(name, args, ctx=_ctx(), tool=_tool())


class TestInit:
    @pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan")])
    @pytest.mark.parametrize(
        "field",
        [
            "default_command_timeout",
            "max_command_timeout",
            "max_output_lines",
            "max_output_bytes",
            "max_read_bytes",
        ],
    )
    def test_rejects_non_positive_bounds(self, field, bad):
        with pytest.raises(ValueError, match=field):
            SandboxToolset(_RecordingBackend(), **{field: bad})

    def test_rejects_default_timeout_above_the_ceiling(self):
        with pytest.raises(ValueError, match="must not exceed"):
            SandboxToolset(_RecordingBackend(), default_command_timeout=100, max_command_timeout=10)

    @pytest.mark.parametrize("bad", ["not-an-identifier", "9lives", "has space", "a-b"])
    def test_rejects_a_prefix_that_is_not_an_identifier(self, bad):
        # The prefixed names are rendered as Python signatures under code mode.
        with pytest.raises(ValueError, match="tool_prefix"):
            SandboxToolset(_RecordingBackend(), tool_prefix=bad)

    def test_id_includes_backend_name_and_prefix(self):
        assert SandboxToolset(_RecordingBackend()).id == "sandbox-rec"
        assert SandboxToolset(_RecordingBackend(), tool_prefix="local").id == "sandbox-rec-local"


class TestGetTools:
    @pytest.mark.asyncio
    async def test_exposes_the_four_tools(self):
        tools = await SandboxToolset(_RecordingBackend()).get_tools(_ctx())

        assert sorted(tools) == TOOL_NAMES

    @pytest.mark.asyncio
    async def test_tool_prefix_renames_every_tool(self):
        tools = await SandboxToolset(_RecordingBackend(), tool_prefix="local").get_tools(_ctx())

        assert sorted(tools) == [f"local_{n}" for n in TOOL_NAMES]

    @pytest.mark.asyncio
    async def test_only_run_command_is_marked_as_a_code_surface(self):
        # code_arg_name is what keeps a code-execution tool out of code mode's
        # run_code. The file tools are more useful folded in, so they must not
        # carry it.
        tools = await SandboxToolset(_RecordingBackend()).get_tools(_ctx())

        assert tools["run_command"].tool_def.metadata == {
            "code_arg_name": "command",
            "code_arg_language": "shell",
        }
        for name in ("read_file", "write_file", "list_directory"):
            assert not tools[name].tool_def.metadata

    @pytest.mark.asyncio
    async def test_all_tools_are_sequential(self):
        # They share one sandbox and later calls depend on files earlier ones wrote.
        tools = await SandboxToolset(_RecordingBackend()).get_tools(_ctx())

        assert all(t.tool_def.sequential for t in tools.values())

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_args", [{}, {"command": 1}, {"command": None}])
    async def test_run_command_validator_rejects_malformed_args(self, bad_args):
        tools = await SandboxToolset(_RecordingBackend()).get_tools(_ctx())

        with pytest.raises(ValidationError):
            tools["run_command"].args_validator.validate_python(bad_args)


class TestRunCommand:
    @pytest.mark.asyncio
    async def test_labels_streams_and_reports_a_nonzero_exit(self):
        backend = _RecordingBackend(run_result=SandboxExecResult(exit_code=3, stdout="hi\n", stderr="bad\n"))
        ts = SandboxToolset(backend)

        async with ts:
            result = await _call(ts, "run_command", {"command": "x"})

        assert "[stdout]" in result
        assert "hi" in result
        assert "[stderr]" in result
        assert "bad" in result
        assert "[exit code: 3]" in result

    @pytest.mark.asyncio
    async def test_no_output_is_reported_explicitly(self):
        backend = _RecordingBackend(run_result=SandboxExecResult(exit_code=0, stdout="", stderr=""))
        ts = SandboxToolset(backend)

        async with ts:
            assert await _call(ts, "run_command", {"command": "x"}) == "(no output)"

    @pytest.mark.asyncio
    async def test_uses_the_default_timeout_when_the_model_omits_one(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend, default_command_timeout=42, max_command_timeout=100)

        async with ts:
            await _call(ts, "run_command", {"command": "x"})

        assert backend.commands[0][2] == 42

    @pytest.mark.asyncio
    async def test_clamps_a_model_supplied_timeout_to_the_ceiling(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend, default_command_timeout=10, max_command_timeout=30)

        async with ts:
            await _call(ts, "run_command", {"command": "x", "timeout_seconds": 9999})

        assert backend.commands[0][2] == 30

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad", [0, -5, float("nan")])
    async def test_rejects_a_nonsense_timeout_instead_of_silently_flooring_it(self, bad):
        ts = SandboxToolset(_RecordingBackend())

        async with ts:
            with pytest.raises(ModelRetry, match="timeout_seconds must be greater than 0"):
                await _call(ts, "run_command", {"command": "x", "timeout_seconds": bad})

    @pytest.mark.asyncio
    async def test_timeout_is_normal_output_not_an_exception(self):
        backend = _RecordingBackend(
            run_result=SandboxExecResult(exit_code=-1, stdout="", stderr="", timed_out=True)
        )
        ts = SandboxToolset(backend, default_command_timeout=5, max_command_timeout=5)

        async with ts:
            result = await _call(ts, "run_command", {"command": "x"})

        assert "[timed out after 5s]" in result

    @pytest.mark.asyncio
    async def test_a_replaced_sandbox_is_announced_and_recreated(self):
        backend = _RecordingBackend(
            run_result=SandboxExecResult(
                exit_code=-1, stdout="", stderr="", timed_out=True, sandbox_terminated=True
            )
        )
        ts = SandboxToolset(backend)

        async with ts:
            result = await _call(ts, "run_command", {"command": "x"})
            assert "sandbox was replaced" in result
            backend.run_result = SandboxExecResult(exit_code=0, stdout="ok\n", stderr="")
            await _call(ts, "run_command", {"command": "y"})

        assert [c[0] for c in backend.commands] == ["box-1", "box-2"]

    @pytest.mark.asyncio
    async def test_backend_truncation_is_surfaced_to_the_model(self):
        backend = _RecordingBackend(
            run_result=SandboxExecResult(exit_code=0, stdout="tail", stderr="", stdout_truncated=True)
        )
        ts = SandboxToolset(backend)

        async with ts:
            result = await _call(ts, "run_command", {"command": "x"})

        assert "truncated" in result


class TestFileTools:
    @pytest.mark.asyncio
    async def test_write_then_read_round_trips(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        async with ts:
            written = await _call(ts, "write_file", {"path": "/w/a.txt", "content": "hello"})
            backend.read_payload = backend.written["/w/a.txt"]
            read = await _call(ts, "read_file", {"path": "/w/a.txt"})

        assert written == "Wrote 5 bytes to '/w/a.txt'."
        assert read == "hello"

    @pytest.mark.asyncio
    async def test_oversized_file_becomes_a_retry_pointing_at_the_shell(self):
        backend = _RecordingBackend()
        backend.read_error = SandboxFileTooLargeError("/w/big", 6 * 1024 * 1024, 5 * 1024 * 1024)
        ts = SandboxToolset(backend)

        async with ts:
            with pytest.raises(ModelRetry, match="over the 5.0MB read limit"):
                await _call(ts, "read_file", {"path": "/w/big"})

    @pytest.mark.asyncio
    async def test_listing_sorts_by_name_and_marks_directories(self):
        backend = _RecordingBackend()
        backend.entries = [("b.txt", False), ("sub", True), ("a.txt", False)]
        ts = SandboxToolset(backend)

        async with ts:
            result = await _call(ts, "list_directory", {})

        assert result == "a.txt\nb.txt\nsub/"

    @pytest.mark.asyncio
    async def test_listing_is_bounded_like_every_other_tool_result(self):
        # An unpacked dataset or a node_modules is tens of thousands of names;
        # unbounded, one call overflows the model's context mid-run.
        backend = _RecordingBackend()
        backend.entries = [(f"file{i:05d}.txt", False) for i in range(5000)]
        ts = SandboxToolset(backend, max_output_lines=20, max_output_bytes=4096)

        async with ts:
            result = await _call(ts, "list_directory", {})

        assert len(result.splitlines()) <= 21  # 20 entries plus the truncation marker
        assert "truncated" in result

    @pytest.mark.asyncio
    async def test_empty_listing_is_reported_explicitly(self):
        ts = SandboxToolset(_RecordingBackend())

        async with ts:
            assert await _call(ts, "list_directory", {"path": "/empty"}) == "(empty)"


class TestErrorMapping:
    @pytest.mark.asyncio
    async def test_recoverable_failure_becomes_a_retry(self):
        ts = SandboxToolset(_RecordingBackend(run_error=SandboxError("no such image")))

        async with ts:
            with pytest.raises(ModelRetry, match="no such image"):
                await _call(ts, "run_command", {"command": "x"})

    @pytest.mark.asyncio
    async def test_terminal_failure_propagates_so_airflow_retries_the_task(self):
        ts = SandboxToolset(_RecordingBackend(run_error=SandboxTerminalError("creds rejected")))

        async with ts:
            with pytest.raises(SandboxTerminalError):
                await _call(ts, "run_command", {"command": "x"})

    @pytest.mark.asyncio
    async def test_unknown_tool_raises(self):
        ts = SandboxToolset(_RecordingBackend())

        async with ts:
            with pytest.raises(ValueError, match="Unknown tool"):
                await _call(ts, "nope", {})

    @pytest.mark.asyncio
    async def test_unprefixed_name_is_unknown_when_a_prefix_is_set(self):
        ts = SandboxToolset(_RecordingBackend(), tool_prefix="local")

        async with ts:
            with pytest.raises(ValueError, match="Unknown tool"):
                await _call(ts, "run_command", {"command": "x"})


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_sandbox_is_created_lazily_and_reused(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        async with ts:
            assert backend.created == []
            await _call(ts, "run_command", {"command": "a"})
            await _call(ts, "run_command", {"command": "b"})

        assert len(backend.created) == 1
        assert [c[0] for c in backend.commands] == ["box-1", "box-1"]

    @pytest.mark.asyncio
    async def test_an_unused_run_provisions_nothing(self):
        backend = _RecordingBackend()

        async with SandboxToolset(backend):
            pass

        assert backend.created == []
        assert backend.destroyed == []

    @pytest.mark.asyncio
    async def test_the_default_spec_is_enforceable_not_none(self):
        # The docs promise "no environment, no egress" by default. Passing None
        # through would skip the backend's contract check entirely and hand back
        # an unrestricted sandbox, so the default must be a concrete spec.
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        async with ts:
            await _call(ts, "run_command", {"command": "x"})

        assert backend.created == [SandboxSpec()]
        assert backend.created[0].block_network is True

    @pytest.mark.asyncio
    async def test_the_spec_reaches_the_backend(self):
        backend = _RecordingBackend()
        spec = SandboxSpec(env={"A": "1"}, block_network=False)
        ts = SandboxToolset(backend, spec=spec)

        async with ts:
            await _call(ts, "run_command", {"command": "x"})

        assert backend.created == [spec]

    @pytest.mark.asyncio
    async def test_sandbox_is_destroyed_even_when_a_call_raises(self):
        backend = _RecordingBackend(run_error=SandboxTerminalError("boom"))
        ts = SandboxToolset(backend)

        with pytest.raises(SandboxTerminalError):
            async with ts:
                await _call(ts, "run_command", {"command": "x"})

        assert backend.destroyed == ["box-1"]

    @pytest.mark.asyncio
    async def test_a_teardown_failure_does_not_fail_a_finished_run(self, caplog):
        # The model work is done and paid for by the time __aexit__ runs, so a
        # delete blip must not turn success into a task failure.
        backend = _RecordingBackend(destroy_error=RuntimeError("delete failed"))
        ts = SandboxToolset(backend)

        async with ts:
            await _call(ts, "run_command", {"command": "x"})

        assert "may need manual cleanup" in caplog.text

    @pytest.mark.asyncio
    async def test_reentry_creates_a_fresh_sandbox(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        for _ in range(2):
            async with ts:
                await _call(ts, "run_command", {"command": "x"})

        assert backend.created == [SandboxSpec(), SandboxSpec()]
        assert backend.destroyed == ["box-1", "box-2"]

    @pytest.mark.asyncio
    async def test_cancellation_during_create_still_destroys_the_sandbox(self):
        started = asyncio.Event()

        class SlowBackend(_RecordingBackend):
            def create(self, *, spec=None):
                started.set()
                import time

                time.sleep(0.2)
                return super().create(spec=spec)

        backend = SlowBackend()
        ts = SandboxToolset(backend)

        async def run():
            async with ts:
                await _call(ts, "run_command", {"command": "x"})

        task = asyncio.create_task(run())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert backend.destroyed == ["box-1"]


class TestForRun:
    @pytest.mark.asyncio
    async def test_each_run_gets_its_own_instance_and_sandbox(self):
        backend = _RecordingBackend()
        base = SandboxToolset(backend)

        first = await base.for_run(_ctx())
        second = await base.for_run(_ctx())
        assert first is not second
        assert first is not base

        async with first, second:
            await first.call_tool("run_command", {"command": "a"}, ctx=_ctx(), tool=_tool())
            await second.call_tool("run_command", {"command": "b"}, ctx=_ctx(), tool=_tool())

        assert [c[0] for c in backend.commands] == ["box-1", "box-2"]

    @pytest.mark.asyncio
    async def test_configuration_is_carried_across(self):
        base = SandboxToolset(
            _RecordingBackend(), tool_prefix="local", default_command_timeout=7, max_command_timeout=9
        )

        forked = await base.for_run(_ctx())

        assert sorted(await forked.get_tools(_ctx())) == [f"local_{n}" for n in TOOL_NAMES]
        assert forked.id == base.id

    @pytest.mark.asyncio
    async def test_a_subclass_does_not_degrade_to_the_base_class(self):
        class CustomToolset(SandboxToolset):
            pass

        forked = await CustomToolset(_RecordingBackend()).for_run(_ctx())

        assert isinstance(forked, CustomToolset)
