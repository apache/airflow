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
import threading
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.sandbox.base import SandboxBackend, SandboxResult
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.ai.utils.tool_definition import _SUPPORTS_RETURN_SCHEMA


class _RecordingBackend(SandboxBackend):
    """In-test backend recording every lifecycle call; each create hands out a fresh handle."""

    name = "fake"

    def __init__(
        self,
        result: SandboxResult | None = None,
        run_error: Exception | None = None,
        destroy_error: Exception | None = None,
    ) -> None:
        self.result = result or SandboxResult(exit_code=0, stdout="hello", stderr="")
        self.run_error = run_error
        self.destroy_error = destroy_error
        self.created: list[str] = []
        self.runs: list[tuple[str, list[str], float]] = []
        self.destroyed: list[str] = []

    def create(self) -> str:
        handle = f"sbx-{len(self.created) + 1}"
        self.created.append(handle)
        return handle

    def run(self, sandbox: str, command: list[str], *, timeout: float) -> SandboxResult:
        if self.run_error is not None:
            raise self.run_error
        self.runs.append((sandbox, list(command), timeout))
        return self.result

    def destroy(self, sandbox: str) -> None:
        self.destroyed.append(sandbox)
        if self.destroy_error is not None:
            raise self.destroy_error


def _call_run_code(ts: SandboxToolset, code: str = "print('hi')"):
    return asyncio.run(ts.call_tool("run_code", {"code": code}, ctx=MagicMock(), tool=MagicMock()))


class TestSandboxToolsetInit:
    def test_id_includes_backend_name(self):
        ts = SandboxToolset(_RecordingBackend())
        assert ts.id == "sandbox-fake"

    @pytest.mark.parametrize("timeout", [0, -1, float("nan")])
    def test_rejects_invalid_timeout(self, timeout):
        with pytest.raises(ValueError, match="timeout"):
            SandboxToolset(_RecordingBackend(), timeout=timeout)

    def test_rejects_empty_python_command(self):
        with pytest.raises(ValueError, match="python_command"):
            SandboxToolset(_RecordingBackend(), python_command="")


class TestSandboxToolsetGetTools:
    def test_returns_run_code_tool(self):
        ts = SandboxToolset(_RecordingBackend())
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert set(tools.keys()) == {"run_code"}

    def test_tool_definition_shape(self):
        ts = SandboxToolset(_RecordingBackend())
        tool = asyncio.run(ts.get_tools(ctx=MagicMock()))["run_code"]
        assert tool.tool_def.description
        assert tool.tool_def.sequential is True
        assert tool.tool_def.parameters_json_schema["required"] == ["code"]
        assert tool.tool_def.parameters_json_schema["properties"]["code"]["type"] == "string"
        assert tool.max_retries == 2

    @pytest.mark.skipif(
        not _SUPPORTS_RETURN_SCHEMA, reason="pydantic-ai too old for ToolDefinition.return_schema"
    )
    def test_tool_declares_string_return_schema(self):
        # run_code returns a JSON-encoded string, so code mode should see `-> str`.
        ts = SandboxToolset(_RecordingBackend())
        tool = asyncio.run(ts.get_tools(ctx=MagicMock()))["run_code"]
        assert tool.tool_def.return_schema == {"type": "string"}


class TestSandboxToolsetCallTool:
    def test_creates_sandbox_lazily_on_first_call(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)
        assert backend.created == []

        _call_run_code(ts)

        assert backend.created == ["sbx-1"]

    def test_reuses_one_sandbox_across_calls(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        _call_run_code(ts, "a = 1")
        _call_run_code(ts, "print(a)")

        assert backend.created == ["sbx-1"]
        assert [run[0] for run in backend.runs] == ["sbx-1", "sbx-1"]

    def test_runs_code_with_python_command_and_timeout(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend, timeout=10.0, python_command="python3.12")

        _call_run_code(ts, "print('hi')")

        assert backend.runs == [("sbx-1", ["python3.12", "-c", "print('hi')"], 10.0)]

    def test_returns_json_payload(self):
        backend = _RecordingBackend(result=SandboxResult(exit_code=0, stdout="hello", stderr=""))
        ts = SandboxToolset(backend)

        payload = json.loads(_call_run_code(ts))

        assert payload == {"exit_code": 0, "stdout": "hello", "stderr": "", "timed_out": False}

    def test_truncated_key_only_present_when_set(self):
        backend = _RecordingBackend(result=SandboxResult(exit_code=0, stdout="x", stderr="", truncated=True))
        ts = SandboxToolset(backend)

        payload = json.loads(_call_run_code(ts))

        assert payload["truncated"] is True

    def test_nonzero_exit_is_returned_not_raised(self):
        """A failed user-code run is valid tool output — the model reads stderr and fixes its code."""
        backend = _RecordingBackend(
            result=SandboxResult(exit_code=1, stdout="", stderr="NameError: name 'x' is not defined")
        )
        ts = SandboxToolset(backend)

        payload = json.loads(_call_run_code(ts))

        assert payload["exit_code"] == 1
        assert "NameError" in payload["stderr"]

    def test_timeout_is_returned_not_raised(self):
        backend = _RecordingBackend(result=SandboxResult(exit_code=-1, stdout="", stderr="", timed_out=True))
        ts = SandboxToolset(backend)

        payload = json.loads(_call_run_code(ts))

        assert payload["timed_out"] is True

    def test_terminated_sandbox_is_reported_and_recreated(self):
        backend = _RecordingBackend(
            result=SandboxResult(
                exit_code=-1,
                stdout="",
                stderr="",
                timed_out=True,
                sandbox_terminated=True,
            )
        )
        ts = SandboxToolset(backend)

        first_payload = json.loads(_call_run_code(ts))
        _call_run_code(ts)

        assert first_payload["sandbox_terminated"] is True
        assert backend.created == ["sbx-1", "sbx-2"]

    def test_backend_exception_propagates(self):
        """Infrastructure failures are not tool output; they fail the task."""
        backend = _RecordingBackend(run_error=RuntimeError("daemon unreachable"))
        ts = SandboxToolset(backend)

        with pytest.raises(RuntimeError, match="daemon unreachable"):
            _call_run_code(ts)

    def test_unknown_tool_raises(self):
        ts = SandboxToolset(_RecordingBackend())

        with pytest.raises(ValueError, match="Unknown tool"):
            asyncio.run(ts.call_tool("other_tool", {}, ctx=MagicMock(), tool=MagicMock()))


class TestSandboxToolsetLifecycle:
    def test_destroys_sandbox_on_exit(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        async def scenario():
            async with ts:
                await ts.call_tool("run_code", {"code": "1"}, ctx=MagicMock(), tool=MagicMock())

        asyncio.run(scenario())

        assert backend.destroyed == ["sbx-1"]

    def test_exit_without_use_destroys_nothing(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        async def scenario():
            async with ts:
                pass

        asyncio.run(scenario())

        assert backend.created == []
        assert backend.destroyed == []

    def test_destroys_sandbox_even_when_run_raises(self):
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        async def scenario():
            async with ts:
                backend.run_error = RuntimeError("boom")
                await ts.call_tool("run_code", {"code": "1"}, ctx=MagicMock(), tool=MagicMock())

        with pytest.raises(RuntimeError, match="boom"):
            asyncio.run(scenario())

        assert backend.destroyed == ["sbx-1"]

    def test_reentry_creates_fresh_sandbox(self):
        """HITL regenerate_with_feedback re-enters the same instance; it must not reuse a destroyed sandbox."""
        backend = _RecordingBackend()
        ts = SandboxToolset(backend)

        async def one_run():
            async with ts:
                await ts.call_tool("run_code", {"code": "1"}, ctx=MagicMock(), tool=MagicMock())

        asyncio.run(one_run())
        asyncio.run(one_run())

        assert backend.created == ["sbx-1", "sbx-2"]
        assert backend.destroyed == ["sbx-1", "sbx-2"]

    def test_reentry_creates_fresh_sandbox_after_destroy_error(self):
        backend = _RecordingBackend(destroy_error=RuntimeError("delete failed"))
        ts = SandboxToolset(backend)

        async def one_run():
            async with ts:
                await ts.call_tool("run_code", {"code": "1"}, ctx=MagicMock(), tool=MagicMock())

        with pytest.raises(RuntimeError, match="delete failed"):
            asyncio.run(one_run())

        backend.destroy_error = None
        asyncio.run(one_run())

        assert backend.created == ["sbx-1", "sbx-2"]
        assert backend.destroyed == ["sbx-1", "sbx-2"]

    def test_cancellation_during_create_still_destroys_sandbox(self):
        create_started = threading.Event()
        allow_create = threading.Event()

        class BlockingCreateBackend(_RecordingBackend):
            def create(self) -> str:
                create_started.set()
                if not allow_create.wait(timeout=5):
                    raise RuntimeError("test timed out waiting to release create")
                return super().create()

        backend = BlockingCreateBackend()
        ts = SandboxToolset(backend)

        async def scenario():
            async with ts:
                call = asyncio.create_task(
                    ts.call_tool("run_code", {"code": "1"}, ctx=MagicMock(), tool=MagicMock())
                )
                assert await asyncio.to_thread(create_started.wait, 1)
                call.cancel()
                allow_create.set()
                with pytest.raises(asyncio.CancelledError):
                    await call

        asyncio.run(scenario())

        assert backend.created == ["sbx-1"]
        assert backend.runs == []
        assert backend.destroyed == ["sbx-1"]
