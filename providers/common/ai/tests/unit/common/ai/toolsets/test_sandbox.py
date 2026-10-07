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
import re
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import time_machine
from pydantic_ai import Agent
from pydantic_ai._run_context import RunContext
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.toolsets.abstract import ToolsetTool
from pydantic_core import ValidationError

from airflow.providers.common.ai.sandbox.base import (
    EXPIRES_AT_TAG,
    HOLDER_TAG,
    NETWORK_TAG,
    OWNER_TAG,
    AttachableSandboxBackend,
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
    encode_network_policy,
)
from airflow.providers.common.ai.tools import ToolCallError
from airflow.providers.common.ai.toolsets import sandbox as sandbox_module
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.compat.sdk import ObjectStoragePath

from unit.common.ai.sandbox.fake_tags import InMemoryTagStore

TOOL_NAMES = ["list_directory", "read_file", "run_command", "write_file"]


class _RecordingBackend(SandboxBackend):
    """Backend double that records calls and can be told to fail on demand."""

    name = "rec"

    def __init__(
        self,
        *,
        destroy_error: Exception | None = None,
        run_result=None,
        run_error=None,
        create_error: Exception | None = None,
    ):
        self.created: list[SandboxSpec | None] = []
        self.destroyed: list[str] = []
        self.commands: list[tuple[str, str, float, int]] = []
        self.written: dict[str, bytes] = {}
        self.entries: list[tuple[str, bool]] = []
        self.destroy_error = destroy_error
        self.run_result = run_result
        self.run_error = run_error
        self.create_error = create_error
        self.read_payload = b""
        self.read_error: Exception | None = None
        # What the agent left in each sandbox, for export_file to hand back.
        self.files: dict[str, bytes] = {}
        # (sandbox, path, max_bytes, whether the sandbox was already destroyed)
        self.exported: list[tuple[str, str, int, bool]] = []
        # Paths whose copy writes some bytes and then fails, as a copy cut off partway does.
        self.fail_partway: set[str] = set()

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        self.created.append(spec)
        if self.create_error is not None:
            raise self.create_error
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

    def export_file(self, sandbox, path, dest, *, max_bytes):
        self.exported.append((sandbox, path, max_bytes, sandbox in self.destroyed))
        if path not in self.files:
            raise SandboxError(f"{path!r} does not exist in the sandbox, or is not readable.")
        if path in self.fail_partway:
            dest.write(self.files[path][:1])
            raise SandboxError(f"the sandbox stopped sending {path!r}")
        dest.write(self.files[path])
        return len(self.files[path])

    def destroy(self, sandbox):
        self.destroyed.append(sandbox)
        if self.destroy_error is not None:
            raise self.destroy_error


class _AttachableRecordingBackend(InMemoryTagStore, _RecordingBackend, AttachableSandboxBackend):
    """The recording double plus an in-memory tag store, so a toolset can attach to it."""

    name = "attachable"

    def __init__(self, *, tags=None, tags_errors: list[Exception] | None = None, **kwargs):
        super().__init__(tags=tags, **kwargs)
        # Consumed one per read, so a test can fail the release a set number of times.
        self.tags_errors = list(tags_errors or [])

    def read_tags(self, sandbox):
        if self.tags_errors:
            raise self.tags_errors.pop(0)
        return super().read_tags(sandbox)


def _owned(owner: str = "me", **extra: str) -> dict[str, dict[str, str]]:
    return {"sb-1": {OWNER_TAG: owner, **extra}}


def _task_context(dag_id="d", run_id="r", task_id="t", map_index=-1):
    return {"ti": SimpleNamespace(dag_id=dag_id, run_id=run_id, task_id=task_id, map_index=map_index)}


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
            "max_export_bytes",
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


class TestNetworkNote:
    """
    The model reads tool descriptions and nothing else, so the egress policy belongs
    there. Measured without it: a ``pip install`` under the default deny costs a turn
    and returns a DNS error, and under an allowlist a reach for plain HTTP is never
    refused, so the model burns its whole command budget and reads a timeout, which
    it takes to mean its command was slow.
    """

    @pytest.mark.asyncio
    async def test_the_default_spec_tells_the_model_there_is_no_network(self):
        tools = await SandboxToolset(_RecordingBackend()).get_tools(_ctx())

        description = tools["run_command"].tool_def.description
        assert "NO network access" in description
        assert "DNS" in description

    @pytest.mark.asyncio
    async def test_an_allowlist_names_the_hosts_and_the_port(self):
        toolset = SandboxToolset(
            _RecordingBackend(),
            spec=SandboxSpec(block_network=True, allow_egress_to=["pypi.org", "files.pythonhosted.org"]),
        )

        description = (await toolset.get_tools(_ctx()))["run_command"].tool_def.description
        assert "pypi.org, files.pythonhosted.org" in description
        # Plain HTTP is the trap worth naming, because nothing refuses it.
        assert "plain HTTP" in description

    @pytest.mark.asyncio
    async def test_an_address_allowlist_names_the_ranges_and_that_names_still_resolve(self):
        # Measured: hostnames resolve under a CIDR list, but only listed addresses answer.
        # Without saying so the model reads a successful lookup as a reachable host.
        toolset = SandboxToolset(
            _RecordingBackend(),
            spec=SandboxSpec(block_network=True, allow_egress_to_cidrs=["10.20.0.0/16", "203.0.113.7/32"]),
        )

        description = (await toolset.get_tools(_ctx()))["run_command"].tool_def.description
        assert "10.20.0.0/16, 203.0.113.7/32" in description
        assert "on any port" in description
        assert "hostnames still resolve" in description
        assert "plain HTTP" not in description, "plain HTTP is only a trap under the hostname list"

    @pytest.mark.asyncio
    async def test_both_lists_are_described_together(self):
        toolset = SandboxToolset(
            _RecordingBackend(),
            spec=SandboxSpec(
                block_network=True, allow_egress_to=["pypi.org"], allow_egress_to_cidrs=["10.20.0.0/16"]
            ),
        )

        description = (await toolset.get_tools(_ctx()))["run_command"].tool_def.description
        assert (
            "over HTTPS on port 443 only: pypi.org; and these address ranges, on any port: 10.20.0.0/16"
            in (description)
        )
        # Plain HTTP fails for the hosts but not for the ranges, which take any port, so the
        # blanket "plain HTTP to any host will fail" of the hosts-only note would be wrong here.
        assert "plain HTTP to them fails" in description
        assert "address ranges accept any port" in description
        assert "plain HTTP to any host" not in description

    @pytest.mark.asyncio
    async def test_an_open_sandbox_says_so(self):
        toolset = SandboxToolset(_RecordingBackend(), spec=SandboxSpec(block_network=False))

        description = (await toolset.get_tools(_ctx()))["run_command"].tool_def.description
        assert "has outbound network access" in description

    @pytest.mark.asyncio
    async def test_only_run_command_carries_the_note(self):
        tools = await SandboxToolset(_RecordingBackend()).get_tools(_ctx())

        for name in ("read_file", "write_file", "list_directory"):
            assert "network" not in tools[name].tool_def.description


class TestRunCommand:
    @pytest.mark.asyncio
    @pytest.mark.enable_redact
    async def test_output_is_masked_before_it_is_truncated(self, registered_secret):
        """Cut first, a secret split at the cut would no longer match and would leak in part."""
        output = "x" * 40 + registered_secret + "y" * 40
        backend = _RecordingBackend(run_result=SandboxExecResult(exit_code=0, stdout=output, stderr=""))
        ts = SandboxToolset(backend, max_output_bytes=60)

        async with ts:
            result = await _call(ts, "run_command", {"command": "x"})

        assert registered_secret[len(registered_secret) // 2 :] not in result

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
    async def test_a_timeout_reports_the_budget_the_command_actually_had(self):
        """
        A backend may have to shorten a deadline, and then the request is the wrong number.

        The Modal backend does this when a sandbox has less life left than the command asked
        for. Reporting the request would send the model back asking for more time when the
        constraint was never its request, and the sandbox may well have survived, so there is
        no replacement note to explain the difference either.
        """
        backend = _RecordingBackend(
            run_result=SandboxExecResult(
                exit_code=-1, stdout="", stderr="", timed_out=True, applied_timeout=23
            )
        )
        ts = SandboxToolset(backend, default_command_timeout=60, max_command_timeout=60)

        async with ts:
            result = await _call(ts, "run_command", {"command": "x"})

        assert "[timed out after 23s]" in result
        assert "60s" not in result

    @pytest.mark.asyncio
    async def test_a_timeout_falls_back_to_the_requested_budget(self):
        """A backend that shortens nothing sets no applied_timeout, and `sbx` never does."""
        backend = _RecordingBackend(
            run_result=SandboxExecResult(exit_code=-1, stdout="", stderr="", timed_out=True)
        )
        ts = SandboxToolset(backend, default_command_timeout=5, max_command_timeout=5)

        async with ts:
            result = await _call(ts, "run_command", {"command": "x"})

        assert "[timed out after 5s]" in result

    @pytest.mark.asyncio
    async def test_a_replaced_sandbox_is_announced_after_an_ordinary_failure_too(self):
        """
        A backend can lose the sandbox under a command that did not time out.

        The Modal backend reports exactly that when a sandbox is terminated or reaches its
        own lifetime mid-command: an exit status like any other, with the sandbox gone. The
        model has to hear that its files went with it, or it will act on a filesystem that
        no longer exists.
        """
        backend = _RecordingBackend(
            run_result=SandboxExecResult(
                exit_code=128,
                stdout="starting\n",
                stderr="waiting on pid 4: ... failed: EOF\n",
                timed_out=False,
                sandbox_terminated=True,
            )
        )
        ts = SandboxToolset(backend)

        async with ts:
            result = await _call(ts, "run_command", {"command": "x"})

        assert "[exit code: 128]" in result
        assert "sandbox was replaced" in result

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
    async def test_a_recoverable_error_from_create_is_terminal(self):
        # The model has no say in provisioning, so a retry cannot fix a failed
        # create. It must fail the task rather than reach the model as a ModelRetry.
        backend = _RecordingBackend(create_error=SandboxError("image pull timed out"))
        ts = SandboxToolset(backend)

        async with ts:
            with pytest.raises(SandboxTerminalError, match="Could not provision.*image pull timed out"):
                await _call(ts, "run_command", {"command": "x"})

        assert backend.destroyed == [], "nothing was provisioned, so nothing is destroyed"

    @pytest.mark.asyncio
    async def test_a_terminal_error_from_create_propagates_unchanged(self):
        error = SandboxTerminalError("credentials rejected")
        ts = SandboxToolset(_RecordingBackend(create_error=error))

        async with ts:
            with pytest.raises(SandboxTerminalError) as caught:
                await _call(ts, "run_command", {"command": "x"})

        assert caught.value is error

    @pytest.mark.asyncio
    async def test_a_failed_create_is_retried_by_the_next_call(self):
        # A create that raised left no sandbox behind, so the next tool call in
        # the same run must try provisioning again rather than reuse a dead task.
        backend = _RecordingBackend(create_error=SandboxError("transient"))
        ts = SandboxToolset(backend)

        async with ts:
            with pytest.raises(SandboxTerminalError):
                await _call(ts, "run_command", {"command": "x"})
            backend.create_error = None
            assert await _call(ts, "run_command", {"command": "x"})

        assert len(backend.created) == 2
        assert backend.destroyed == ["box-2"]

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

        assert "Failed to destroy sandbox box-1 on backend rec; it may need manual cleanup" in caplog

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

    def test_first_calls_from_threads_with_their_own_loops_share_one_sandbox(self):
        """A native framework may make its first tool calls from several threads at once."""
        both_waiting = threading.Barrier(2)

        class SlowBackend(_RecordingBackend):
            def create(self, *, spec=None):
                time.sleep(0.2)
                return super().create(spec=spec)

        backend = SlowBackend()
        errors: list[BaseException] = []

        def first_call():
            both_waiting.wait()
            try:
                asyncio.run(_call(ts, "run_command", {"command": "x"}))
            except BaseException as e:
                errors.append(e)

        with SandboxToolset(backend) as ts:
            threads = [threading.Thread(target=first_call) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        assert errors == []
        assert [command[0] for command in backend.commands] == ["box-1", "box-1"]
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


class TestExports:
    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"exports": {"": "file:///tmp/x"}}, "neither may be empty"),
            ({"exports": {"out.bin": ""}}, "neither may be empty"),
            ({"export_conn_id": "aws"}, "export_conn_id only applies together with exports"),
        ],
    )
    def test_constructor_refuses_a_shape_that_cannot_work(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            SandboxToolset(_RecordingBackend(), **kwargs)

    def test_an_attached_sandbox_cannot_export(self):
        # The task that owns an attached sandbox reads out what the agent left.
        with pytest.raises(ValueError, match="exports cannot be combined with attach_to"):
            SandboxToolset(_AttachableRecordingBackend(), attach_to="sb-1", exports={"a": "file:///tmp/a"})

    @pytest.mark.asyncio
    async def test_a_finished_run_copies_each_file_out_before_the_sandbox_is_destroyed(self, tmp_path):
        backend = _RecordingBackend()
        backend.files = {"out/report.parquet": b"PAR1...", "chart.png": b"\x89PNG"}
        ts = SandboxToolset(
            backend,
            exports={
                "out/report.parquet": f"file://{tmp_path}/report.parquet",
                "chart.png": f"file://{tmp_path}/chart.png",
            },
            max_export_bytes=1234,
        )

        async with ts:
            await _call(ts, "run_command", {"command": "make report"})

        assert backend.exported == [
            ("box-1", "out/report.parquet", 1234, False),
            ("box-1", "chart.png", 1234, False),
        ]
        assert backend.destroyed == ["box-1"]
        assert (tmp_path / "report.parquet").read_bytes() == b"PAR1..."
        assert (tmp_path / "chart.png").read_bytes() == b"\x89PNG"
        assert sorted(f.name for f in tmp_path.iterdir()) == ["chart.png", "report.parquet"]

    @pytest.mark.asyncio
    async def test_a_failed_run_exports_nothing_and_still_destroys_the_sandbox(self, tmp_path):
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"half done"}
        ts = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})

        async def run_that_fails():
            async with ts:
                await _call(ts, "run_command", {"command": "x"})
                raise RuntimeError("agent blew up")

        with pytest.raises(RuntimeError, match="agent blew up"):
            await run_that_fails()

        assert backend.exported == []
        assert backend.destroyed == ["box-1"]
        assert not (tmp_path / "out.bin").exists()

    @pytest.mark.asyncio
    async def test_a_file_that_cannot_be_exported_fails_the_task(self, tmp_path):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})

        with pytest.raises(SandboxTerminalError, match="Could not export 'out.bin' from sandbox box-1"):
            async with ts:
                await _call(ts, "run_command", {"command": "forgot to write it"})

        assert backend.destroyed == ["box-1"]
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_a_copy_that_fails_partway_leaves_the_destination_as_it_was(self, tmp_path):
        (tmp_path / "out.bin").write_bytes(b"ORIGINAL")
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"new contents"}
        backend.fail_partway = {"out.bin"}
        ts = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})

        with pytest.raises(SandboxTerminalError, match="stopped sending"):
            async with ts:
                await _call(ts, "run_command", {"command": "x"})

        assert (tmp_path / "out.bin").read_bytes() == b"ORIGINAL"
        assert [f.name for f in tmp_path.iterdir()] == ["out.bin"]

    @pytest.mark.asyncio
    async def test_for_run_carries_the_export_connection_across(self):
        base = SandboxToolset(
            _RecordingBackend(), exports={"out.bin": "s3://b/out.bin"}, export_conn_id="reports"
        )

        forked = await base.for_run(_ctx())

        with patch.object(sandbox_module, "ObjectStoragePath", autospec=True) as path_cls:
            forked._export_target("s3://b/out.bin")

        path_cls.assert_called_once_with("s3://b/out.bin", conn_id="reports")

    @pytest.mark.asyncio
    async def test_a_failed_export_changes_none_of_the_destinations(self, tmp_path):
        # Nothing is moved into place until every file has been copied, so a consumer
        # that runs whatever the outcome never finds half of a set.
        (tmp_path / "first.bin").write_bytes(b"ORIGINAL")
        backend = _RecordingBackend()
        backend.files = {"first.bin": b"one"}
        ts = SandboxToolset(
            backend,
            exports={
                "first.bin": f"file://{tmp_path}/first.bin",
                "second.bin": f"file://{tmp_path}/second.bin",
            },
        )

        with pytest.raises(SandboxTerminalError, match="Could not export 'second.bin'"):
            async with ts:
                await _call(ts, "run_command", {"command": "x"})

        assert (tmp_path / "first.bin").read_bytes() == b"ORIGINAL"
        assert [f.name for f in tmp_path.iterdir()] == ["first.bin"]

    @pytest.mark.asyncio
    async def test_a_publish_that_fails_names_what_is_already_in_place(self, tmp_path):
        backend = _RecordingBackend()
        backend.files = {"first.bin": b"one", "second.bin": b"two"}
        ts = SandboxToolset(
            backend,
            exports={
                "first.bin": f"file://{tmp_path}/first.bin",
                "second.bin": f"file://{tmp_path}/second.bin",
            },
        )
        move = ObjectStoragePath.move

        def fail_the_second(self, path, **kwargs):
            if str(path).endswith("second.bin"):
                raise OSError("permission denied")
            return move(self, path, **kwargs)

        with patch.object(ObjectStoragePath, "move", autospec=True, side_effect=fail_the_second):
            with pytest.raises(
                SandboxTerminalError, match=r"second\.bin: permission denied.*Already in place: .*first\.bin"
            ):
                async with ts:
                    await _call(ts, "run_command", {"command": "x"})

        assert sorted(f.name for f in tmp_path.iterdir()) == ["first.bin"]

    @pytest.mark.asyncio
    async def test_a_copy_that_cannot_be_removed_is_named_in_the_error(self, tmp_path, caplog):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})

        with patch.object(ObjectStoragePath, "unlink", autospec=True, side_effect=OSError("denied")):
            with pytest.raises(
                SandboxTerminalError, match=r"Left behind, to delete by hand: .*out\.bin\.\w+\.partial"
            ):
                async with ts:
                    await _call(ts, "run_command", {"command": "x"})

        assert {
            "event": re.compile(r"Could not remove .*out\.bin\.\w+\.partial after a failed export"),
            "logger": "airflow.providers.common.ai.toolsets.sandbox",
        } in caplog

    @pytest.mark.asyncio
    async def test_a_local_destination_gets_its_directories(self, tmp_path):
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"payload"}
        ts = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/run-1/nested/out.bin"})

        async with ts:
            await _call(ts, "run_command", {"command": "x"})

        assert (tmp_path / "run-1" / "nested" / "out.bin").read_bytes() == b"payload"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("rendered", [None, "", "None", "relative/out.bin"])
    async def test_a_destination_that_is_not_a_url_fails_before_the_run(self, rendered):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend, exports={"out.bin": "{{ var.value.dest }}"})
        # What the templater writes after construction.
        ts._exports = {"out.bin": rendered}

        with pytest.raises(SandboxTerminalError, match="not a storage URL"):
            async with ts:
                pass

        assert backend.created == []

    def test_the_connection_is_not_passed_when_none_is_set(self):
        # An explicit conn_id=None discards a connection named in the URL on Airflow 3.0/3.1.
        ts = SandboxToolset(_RecordingBackend(), exports={"out.bin": "s3://conn@bucket/out.bin"})

        with patch.object(sandbox_module, "ObjectStoragePath", autospec=True) as path_cls:
            ts._export_target("s3://conn@bucket/out.bin")

        path_cls.assert_called_once_with("s3://conn@bucket/out.bin")

    @pytest.mark.asyncio
    async def test_a_teardown_failure_after_a_good_export_does_not_fail_the_task(self, tmp_path, caplog):
        backend = _RecordingBackend(destroy_error=RuntimeError("daemon went away"))
        backend.files = {"out.bin": b"payload"}
        ts = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})

        async with ts:
            await _call(ts, "run_command", {"command": "x"})

        assert (tmp_path / "out.bin").read_bytes() == b"payload"
        assert "Failed to destroy sandbox box-1 on backend rec; it may need manual cleanup" in caplog

    @pytest.mark.asyncio
    async def test_a_run_that_never_used_its_sandbox_cannot_deliver_its_files(self, tmp_path):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})

        with pytest.raises(SandboxTerminalError, match="no sandbox standing.*'out.bin'"):
            async with ts:
                pass

        assert backend.created == []

    @pytest.mark.asyncio
    async def test_the_model_is_told_which_files_are_collected(self):
        with_exports = SandboxToolset(_RecordingBackend(), exports={"out/report.csv": "file:///tmp/r.csv"})
        without = SandboxToolset(_RecordingBackend())

        exported = (await with_exports.get_tools(_ctx()))["run_command"].tool_def.description
        plain = (await without.get_tools(_ctx()))["run_command"].tool_def.description

        assert "copied out of the sandbox" in exported
        assert "out/report.csv" in exported
        assert "copied out" not in plain

    @pytest.mark.asyncio
    async def test_for_run_carries_the_exports_across(self, tmp_path):
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"payload"}
        base = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"}, max_export_bytes=99)

        forked = await base.for_run(_ctx())
        async with forked:
            await _call(forked, "run_command", {"command": "x"})

        assert backend.exported == [("box-1", "out.bin", 99, False)]
        assert (tmp_path / "out.bin").read_bytes() == b"payload"

    def test_a_with_block_exports_for_a_native_agent(self, tmp_path):
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"payload"}

        with SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"}) as sandbox:
            run_command = {tool.name: tool for tool in sandbox.airflow_tools()}["run_command"]
            asyncio.run(run_command.call({"command": "ls"}))

        assert (tmp_path / "out.bin").read_bytes() == b"payload"
        assert backend.destroyed == ["box-1"]

    def test_a_native_agent_that_raises_exports_nothing(self, tmp_path):
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"payload"}

        def native_run_that_fails():
            with SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"}) as sandbox:
                run_command = {tool.name: tool for tool in sandbox.airflow_tools()}["run_command"]
                asyncio.run(run_command.call({"command": "ls"}))
                raise RuntimeError("native agent failed")

        with pytest.raises(RuntimeError, match="native agent failed"):
            native_run_that_fails()

        assert backend.exported == []
        assert backend.destroyed == ["box-1"]


class TestAttachMode:
    """
    ``attach_to``: use a sandbox another task provisioned, and leave it standing.

    Outside an Airflow task there is no context to derive an owner from, which is the
    situation these tests run in, so most of them pass ``owner`` explicitly; the ones
    about the defaults patch the context lookup instead.
    """

    @pytest.mark.parametrize(
        ("backend_cls", "kwargs", "match"),
        [
            pytest.param(
                _RecordingBackend,
                {"attach_to": "sb-1"},
                "not an AttachableSandboxBackend",
                id="plain_backend",
            ),
            pytest.param(
                _AttachableRecordingBackend,
                {"attach_to": "sb-1", "spec": SandboxSpec()},
                "spec cannot be combined",
                id="spec",
            ),
            pytest.param(
                _AttachableRecordingBackend, {"owner": "me"}, "owner only applies", id="owner_alone"
            ),
            pytest.param(_AttachableRecordingBackend, {"attach_to": ""}, "empty string", id="empty_handle"),
            pytest.param(
                _AttachableRecordingBackend,
                {"spec": SandboxSpec(owner="me")},
                "SandboxSpec.owner is for",
                id="owner_on_own",
            ),
        ],
    )
    def test_constructor_refuses_a_shape_that_cannot_work(self, backend_cls, kwargs, match):
        with pytest.raises(ValueError, match=match):
            SandboxToolset(backend_cls(), **kwargs)

    @pytest.mark.asyncio
    async def test_uses_the_given_sandbox_and_provisions_nothing(self):
        backend = _AttachableRecordingBackend(tags=_owned())
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        async with ts:
            await _call(ts, "run_command", {"command": "ls"})
            await _call(ts, "write_file", {"path": "a", "content": "b"})

        assert backend.created == []
        assert [c[0] for c in backend.commands] == ["sb-1"]
        assert backend.destroyed == [], "an attached sandbox is never the toolset's to destroy"

    @pytest.mark.asyncio
    async def test_claims_the_sandbox_on_enter_and_releases_it_on_exit(self):
        backend = _AttachableRecordingBackend(tags=_owned())
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        async with ts:
            assert backend.tags["sb-1"][HOLDER_TAG] == "me"

        assert HOLDER_TAG not in backend.tags["sb-1"]
        assert backend.tags["sb-1"][OWNER_TAG] == "me", "releasing must not strip the owner"

    @pytest.mark.asyncio
    async def test_the_wrong_owner_is_refused_before_the_model_spends_anything(self):
        backend = _AttachableRecordingBackend(tags=_owned("someone_else"))
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        with pytest.raises(SandboxTerminalError, match="not owned by 'me'"):
            async with ts:
                raise AssertionError("the body must not run")

        assert backend.commands == []

    @pytest.mark.asyncio
    async def test_outside_a_task_the_owner_has_to_be_given(self):
        ts = SandboxToolset(_AttachableRecordingBackend(tags=_owned()), attach_to="sb-1")

        with pytest.raises(SandboxTerminalError, match="needs an owner"):
            async with ts:
                pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("map_index", "holder"),
        [pytest.param(-1, "d/r/t", id="unmapped"), pytest.param(3, "d/r/t[3]", id="mapped")],
    )
    async def test_inside_a_task_the_dag_run_owns_and_the_task_instance_holds(self, map_index, holder):
        # Owner = the Dag run, so a provisioning task in the same run needs no shared
        # secret; holder = the task instance without its try number, so a retry is the
        # same holder while another Dag run under a shared owner, or another map index,
        # is not.
        backend = _AttachableRecordingBackend(tags=_owned("d/r"))
        ts = SandboxToolset(backend, attach_to="sb-1")

        with patch(
            "airflow.providers.common.ai.toolsets.sandbox.get_current_context",
            autospec=True,
            return_value=_task_context(map_index=map_index),
        ):
            async with ts:
                assert backend.tags["sb-1"][HOLDER_TAG] == holder

    @pytest.mark.asyncio
    async def test_an_explicit_owner_wins_over_the_dag_run(self):
        backend = _AttachableRecordingBackend(tags=_owned("shared-pool"))
        ts = SandboxToolset(backend, attach_to="sb-1", owner="shared-pool")

        with patch(
            "airflow.providers.common.ai.toolsets.sandbox.get_current_context",
            autospec=True,
            return_value=_task_context(),
        ):
            async with ts:
                assert backend.tags["sb-1"][HOLDER_TAG] == "d/r/t"

    @pytest.mark.asyncio
    async def test_a_recoverable_error_while_attaching_is_terminal(self):
        # Nothing the model does can change whether this sandbox can be attached to.
        backend = _AttachableRecordingBackend(tags=_owned(), tags_errors=[SandboxError("blip")])
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        with pytest.raises(SandboxTerminalError, match="Could not attach to sandbox 'sb-1'"):
            async with ts:
                pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [None, "", "None"], ids=["native_none", "empty", "string_none"])
    async def test_a_handle_that_rendered_to_nothing_fails_instead_of_provisioning(self, value):
        # The templater writes the rendered value after construction. A missing XCom
        # renders to None or "None", and neither may turn this into a toolset that
        # quietly provisions a sandbox of its own.
        backend = _AttachableRecordingBackend(tags=_owned())
        ts = SandboxToolset(backend, attach_to="{{ ti.xcom_pull(task_ids='provision') }}", owner="me")
        ts.attach_to = value

        with pytest.raises(SandboxTerminalError, match="attach_to rendered to"):
            async with ts:
                pass
        with pytest.raises(SandboxTerminalError, match="attach_to rendered to"):
            await ts.for_run(_ctx())

        assert backend.created == []

    @pytest.mark.asyncio
    async def test_a_tool_call_before_entering_provisions_nothing(self):
        backend = _AttachableRecordingBackend(tags=_owned())
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        with pytest.raises(SandboxTerminalError, match="not open"):
            await _call(ts, "run_command", {"command": "ls"})

        assert backend.created == []

    @pytest.mark.asyncio
    async def test_a_release_that_keeps_failing_is_logged_with_the_holder_left_behind(
        self, caplog, monkeypatch
    ):
        monkeypatch.setattr(sandbox_module, "_RELEASE_RETRY_DELAY", 0.0)
        backend = _AttachableRecordingBackend(tags=_owned())
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        async with ts:
            backend.tags_errors = [RuntimeError("tags service down")] * 3

        assert (
            "Failed to release sandbox sb-1 on backend attachable after 3 attempts; it stays marked as "
            "held by me, and only that holder, or the task that created the sandbox, can use it"
        ) in caplog
        assert backend.tags["sb-1"][HOLDER_TAG] == "me"

    @pytest.mark.asyncio
    async def test_a_release_blip_is_retried_and_leaves_no_claim(self, caplog, monkeypatch):
        monkeypatch.setattr(sandbox_module, "_RELEASE_RETRY_DELAY", 0.0)
        backend = _AttachableRecordingBackend(tags=_owned())
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        async with ts:
            backend.tags_errors = [RuntimeError("blip"), RuntimeError("blip")]

        assert HOLDER_TAG not in backend.tags["sb-1"]
        assert not any("Failed to release" in e["event"] for e in caplog)

    @pytest.mark.asyncio
    async def test_releasing_a_sandbox_that_has_ended_is_quiet(self, caplog):
        # The sandbox reached its lifetime between the last command and the run's end.
        # Nothing is held any more, so this is information, not a warning with a trace.
        backend = _AttachableRecordingBackend(tags=_owned())
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        async with ts:
            del backend.tags["sb-1"]

        assert not any("Failed to release" in e["event"] for e in caplog)

    @pytest.mark.asyncio
    async def test_the_sandbox_is_released_but_not_destroyed_when_a_call_raises(self):
        backend = _AttachableRecordingBackend(tags=_owned(), run_error=SandboxTerminalError("boom"))
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        with pytest.raises(SandboxTerminalError, match="boom"):
            async with ts:
                await _call(ts, "run_command", {"command": "x"})

        assert backend.destroyed == []
        assert HOLDER_TAG not in backend.tags["sb-1"]

    @pytest.mark.asyncio
    async def test_a_sandbox_that_stops_under_a_command_fails_the_task(self):
        # An owned sandbox would be replaced on the next call. This one was never ours,
        # so the model must not be told its files are gone as if work could continue.
        backend = _AttachableRecordingBackend(
            tags=_owned(),
            run_result=SandboxExecResult(
                exit_code=-1, stdout="", stderr="", timed_out=True, sandbox_terminated=True
            ),
        )
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        with pytest.raises(SandboxTerminalError, match="stopped while running a command"):
            async with ts:
                await _call(ts, "run_command", {"command": "x"})

        assert backend.created == []

    @pytest.mark.asyncio
    async def test_a_call_after_the_sandbox_stopped_does_not_provision_a_replacement(self):
        backend = _AttachableRecordingBackend(
            tags=_owned(),
            run_result=SandboxExecResult(
                exit_code=-1, stdout="", stderr="", timed_out=True, sandbox_terminated=True
            ),
        )
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        async with ts:
            with pytest.raises(SandboxTerminalError, match="stopped while running a command"):
                await _call(ts, "run_command", {"command": "x"})
            with pytest.raises(SandboxTerminalError, match="the sandbox ended"):
                await _call(ts, "run_command", {"command": "y"})

        assert backend.created == []

    @pytest.mark.asyncio
    async def test_the_note_tells_the_model_whose_sandbox_it_is_what_it_reaches_and_how_long_it_has(self):
        # The spec was the provisioning task's, so everything here comes from the tags.
        stamped = encode_network_policy(SandboxSpec(block_network=True))
        tags = _owned(**{EXPIRES_AT_TAG: str(1_000_000 + 30 * 60), NETWORK_TAG: stamped})
        backend = _AttachableRecordingBackend(tags=tags)
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        with time_machine.travel(1_000_000, tick=False):
            async with ts:
                description = (await ts.get_tools(_ctx()))["run_command"].tool_def.description

        assert "set up by an earlier task" in description
        assert "for a later task to collect" in description
        assert "NO network access" in description
        assert "About 30 minutes of its lifetime remained when this run began" in description

    @pytest.mark.asyncio
    async def test_the_note_does_not_change_between_steps(self):
        # Tool definitions are part of the cached prompt prefix; a note that counted
        # down every step would invalidate the cache on every model call.
        tags = _owned(**{EXPIRES_AT_TAG: str(1_000_000 + 30 * 60)})
        ts = SandboxToolset(_AttachableRecordingBackend(tags=tags), attach_to="sb-1", owner="me")

        with time_machine.travel(1_000_000, tick=False) as traveller:
            async with ts:
                first = (await ts.get_tools(_ctx()))["run_command"].tool_def.description
                traveller.shift(600)
                second = (await ts.get_tools(_ctx()))["run_command"].tool_def.description

        assert first == second

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("tags", "expected"),
        [
            pytest.param({}, "How long it has left is not known", id="no_expiry"),
            pytest.param({EXPIRES_AT_TAG: "0"}, "Under a minute", id="expired"),
            pytest.param({EXPIRES_AT_TAG: str(1_000_000 + 59)}, "Under a minute", id="fifty_nine_seconds"),
            pytest.param(
                {EXPIRES_AT_TAG: str(1_000_000 + 75)}, "About 1 minute of", id="seventy_five_seconds"
            ),
            pytest.param({EXPIRES_AT_TAG: str(1_000_000 + 150)}, "About 2 minutes", id="two_and_a_half"),
        ],
    )
    async def test_the_note_is_honest_about_the_clock(self, tags, expected):
        backend = _AttachableRecordingBackend(tags=_owned(**tags))
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me")

        with time_machine.travel(1_000_000, tick=False):
            async with ts:
                description = (await ts.get_tools(_ctx()))["run_command"].tool_def.description

        assert expected in description

    @pytest.mark.asyncio
    async def test_without_a_network_stamp_the_note_admits_it_does_not_know(self):
        ts = SandboxToolset(_AttachableRecordingBackend(tags=_owned()), attach_to="sb-1", owner="me")

        async with ts:
            description = (await ts.get_tools(_ctx()))["run_command"].tool_def.description

        assert "whatever the task that set it up allowed" in description

    @pytest.mark.asyncio
    async def test_commands_are_shortened_to_what_the_creator_left(self, monkeypatch):
        # Whatever backend the sandbox is on: the clamp is the toolset's, so a backend of
        # your own gets it for free.
        monkeypatch.setattr("airflow.providers.common.ai.toolsets.sandbox.time.monotonic", lambda: 50.0)
        tags = _owned(**{EXPIRES_AT_TAG: str(1_000_000 + 30)})
        backend = _AttachableRecordingBackend(tags=tags)
        ts = SandboxToolset(backend, attach_to="sb-1", owner="me", max_command_timeout=600)

        with time_machine.travel(1_000_000, tick=False):
            async with ts:
                await _call(ts, "run_command", {"command": "sleep 300", "timeout_seconds": 300})

        assert backend.commands[0][2] == 30.0

    @pytest.mark.asyncio
    async def test_for_run_carries_the_attachment(self):
        backend = _AttachableRecordingBackend(tags=_owned())
        base = SandboxToolset(backend, attach_to="sb-1", owner="me")

        forked = await base.for_run(_ctx())
        async with forked:
            await forked.call_tool("run_command", {"command": "a"}, ctx=_ctx(), tool=_tool())

        assert forked.attach_to == "sb-1"
        assert [c[0] for c in backend.commands] == ["sb-1"]
        assert backend.created == []

    @pytest.mark.asyncio
    async def test_two_runs_in_sequence_find_the_same_sandbox(self):
        # HITL regeneration is a second run, entered on a fresh for_run copy of the
        # toolset, exactly as pydantic-ai does it. Both find the same sandbox.
        backend = _AttachableRecordingBackend(tags=_owned())
        base = SandboxToolset(backend, attach_to="sb-1", owner="me")

        for _ in range(2):
            run = await base.for_run(_ctx())
            async with run:
                await run.call_tool("run_command", {"command": "ls"}, ctx=_ctx(), tool=_tool())

        assert [c[0] for c in backend.commands] == ["sb-1", "sb-1"]
        assert backend.created == []
        assert backend.destroyed == []
        assert HOLDER_TAG not in backend.tags["sb-1"]


class TestOutsideAnAgentRun:
    """Native frameworks call the tools directly, so the toolset has to own its sandbox's life."""

    def test_a_block_that_ends_while_the_sandbox_is_created_still_destroys_it(self):
        """A task timeout ends the block on the main thread while a framework thread provisions."""
        creating = threading.Event()

        class SlowBackend(_RecordingBackend):
            def create(self, *, spec=None):
                creating.set()
                time.sleep(0.2)
                return super().create(spec=spec)

        backend = SlowBackend()
        errors: list[BaseException] = []

        def first_call():
            try:
                asyncio.run(_call(ts, "run_command", {"command": "x"}))
            except BaseException as e:
                errors.append(e)

        with SandboxToolset(backend) as ts:
            worker = threading.Thread(target=first_call)
            worker.start()
            creating.wait(5)
        worker.join()

        assert backend.destroyed == ["box-1"]
        assert backend.commands == []
        assert [type(e) for e in errors] == [SandboxTerminalError]

    @pytest.mark.asyncio
    async def test_a_call_before_entering_is_refused_and_provisions_nothing(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        with pytest.raises(SandboxTerminalError, match="not open"):
            await _call(ts, "run_command", {"command": "ls"})

        assert backend.created == []

    @pytest.mark.asyncio
    async def test_a_call_after_exiting_is_refused(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)
        async with ts:
            await _call(ts, "run_command", {"command": "ls"})

        with pytest.raises(SandboxTerminalError, match="not open"):
            await _call(ts, "run_command", {"command": "ls"})

        assert backend.created == [SandboxSpec()]

    def test_a_with_block_destroys_the_sandbox_its_tools_provisioned(self):
        backend = _RecordingBackend()

        with SandboxToolset(backend) as sandbox:
            run_command = {tool.name: tool for tool in sandbox.airflow_tools()}["run_command"]
            result = asyncio.run(run_command.call({"command": "ls"}))

        assert not result.is_error
        assert backend.destroyed == ["box-1"]

    def test_a_call_outside_the_block_ends_a_native_agent_run(self):
        run_command = {tool.name: tool for tool in SandboxToolset(_RecordingBackend()).airflow_tools()}[
            "run_command"
        ]

        with pytest.raises(ToolCallError, match="not open"):
            asyncio.run(run_command.call({"command": "ls"}))


class TestInsideAPydanticAIRun:
    @staticmethod
    def _model_calling(tool: str):
        def model(messages, info):
            if any(isinstance(p, ToolReturnPart) for m in messages for p in m.parts):
                return ModelResponse(parts=[TextPart("done")])
            return ModelResponse(parts=[ToolCallPart(tool, {"command": "ls"}, tool_call_id="c")])

        return FunctionModel(model)

    def test_a_successful_run_exports_its_files(self, tmp_path):
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"payload"}
        sandbox = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})

        Agent(self._model_calling("box_run_command"), toolsets=[sandbox.prefixed("box")]).run_sync("go")

        assert backend.exported == [("box-1", "out.bin", 1024**3, False)]
        assert (tmp_path / "out.bin").read_bytes() == b"payload"

    def test_a_run_started_inside_an_except_block_still_exports(self, tmp_path):
        # The exception already being handled when the run began is not the run failing.
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"payload"}
        sandbox = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})
        agent = Agent(self._model_calling("run_command"), toolsets=[sandbox])

        try:
            raise KeyError("cache miss")
        except KeyError:
            result = agent.run_sync("go")

        assert result.output == "done"
        assert (tmp_path / "out.bin").read_bytes() == b"payload"

    def test_an_agent_held_open_exports_from_its_run_and_not_again_at_exit(self, tmp_path):
        # ``async with agent`` enters the toolset the author built; runs use copies of it.
        backend = _RecordingBackend()
        backend.files = {"out.bin": b"payload"}
        sandbox = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})
        agent = Agent(self._model_calling("run_command"), toolsets=[sandbox])

        async def held_open():
            async with agent:
                await agent.run("go")

        asyncio.run(held_open())

        assert backend.exported == [("box-1", "out.bin", 1024**3, False)]
        assert (tmp_path / "out.bin").read_bytes() == b"payload"

    def test_a_failed_run_exports_nothing(self, tmp_path):
        # pydantic-ai closes its toolsets through an exit stack that passes them no
        # exception, so the toolset has to find the failure itself.
        backend = _RecordingBackend(run_error=SandboxTerminalError("credentials rejected"))
        backend.files = {"out.bin": b"payload"}
        sandbox = SandboxToolset(backend, exports={"out.bin": f"file://{tmp_path}/out.bin"})

        with pytest.raises(SandboxTerminalError, match="credentials rejected"):
            Agent(self._model_calling("run_command"), toolsets=[sandbox]).run_sync("go")

        assert backend.exported == []
        assert backend.destroyed == ["box-1"]
        assert not (tmp_path / "out.bin").exists()

    def test_the_run_opens_the_sandbox_and_destroys_it_when_it_ends(self):
        backend = _RecordingBackend()

        def model(messages, info):
            if any(isinstance(p, ToolReturnPart) for m in messages for p in m.parts):
                return ModelResponse(parts=[TextPart("done")])
            return ModelResponse(parts=[ToolCallPart("run_command", {"command": "ls"}, tool_call_id="c")])

        Agent(FunctionModel(model), toolsets=[SandboxToolset(backend)]).run_sync("go")

        assert backend.created == [SandboxSpec()]
        assert backend.destroyed == ["box-1"]
