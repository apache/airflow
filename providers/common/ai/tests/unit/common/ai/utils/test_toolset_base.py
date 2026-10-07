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
import threading
import time
import traceback
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic_ai import Agent
from pydantic_ai._run_context import RunContext
from pydantic_ai.exceptions import ApprovalRequired, ModelRetry, ToolFailed
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturn, ToolReturnPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.toolsets.abstract import ToolsetTool
from pydantic_ai.toolsets.function import FunctionToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.utils.toolset_base import AirflowToolset, MaskingToolset, ensure_masked


class _ScriptedToolset(AirflowToolset):
    """Returns or raises whatever the test hands it."""

    def __init__(self, outcome: Any) -> None:
        self._outcome = outcome

    @property
    def id(self) -> str:
        return "scripted"

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        return {}

    async def execute_tool(self, name, tool_args, *, ctx, tool) -> Any:
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        return self._outcome


def _call(toolset: AirflowToolset) -> Any:
    return asyncio.run(
        toolset.call_tool("t", {}, ctx=MagicMock(spec=RunContext), tool=MagicMock(spec=ToolsetTool))
    )


@pytest.mark.enable_redact
class TestMasking:
    def test_masks_a_secret_in_a_text_result(self, registered_secret):
        assert _call(_ScriptedToolset(f"password is {registered_secret}")) == "password is ***"

    def test_masks_a_secret_nested_deep_in_a_structured_result(self, registered_secret):
        deep: Any = registered_secret
        for _ in range(10):
            deep = {"level": [deep]}

        masked = _call(_ScriptedToolset(deep))

        for _ in range(10):
            masked = masked["level"][0]
        assert masked == "***"

    def test_masks_the_text_of_a_model_retry_and_drops_its_unmasked_cause(self, registered_secret):
        """Tracing records a failed call's traceback, cause included."""
        cause = ConnectionError(f"login failed for {registered_secret}")
        retry = ModelRetry(f"query failed: {cause}")
        retry.__cause__ = cause

        with pytest.raises(ModelRetry) as caught:
            _call(_ScriptedToolset(retry))

        assert caught.value.message == "query failed: login failed for ***"
        assert str(caught.value) == "query failed: login failed for ***"
        assert registered_secret not in "".join(traceback.format_exception(caught.value))

    def test_masks_the_text_of_a_tool_failure(self, registered_secret):
        with pytest.raises(ToolFailed) as caught:
            _call(_ScriptedToolset(ToolFailed(f"no such bucket for key {registered_secret}")))

        assert caught.value.message == "no such bucket for key ***"

    def test_another_exception_keeps_its_type_with_its_message_masked(self, registered_secret):
        """Retry policies match on the type; tracing records the message."""
        cause = OSError(f"socket closed by {registered_secret}")
        error = PermissionError(f"denied: {registered_secret}")
        error.__cause__ = cause

        with pytest.raises(PermissionError) as caught:
            _call(_ScriptedToolset(error))

        assert str(caught.value) == "denied: ***"
        assert registered_secret not in "".join(traceback.format_exception(caught.value))

    def test_masks_an_os_error_whose_message_comes_from_its_attributes(self, registered_secret):
        """``OSError`` prints ``strerror`` and ``filename``, which masking ``args`` alone leaves as they were."""
        error = PermissionError(13, f"denied for {registered_secret}", f"/keys/{registered_secret}")

        with pytest.raises(PermissionError) as caught:
            _call(_ScriptedToolset(error))

        assert str(caught.value) == "[Errno 13] denied for ***: '/keys/***'"

    def test_masks_the_attributes_a_custom_message_is_built_from(self, registered_secret):
        class LoginError(Exception):
            def __init__(self, user: str, password: str) -> None:
                super().__init__(user)
                self.password = password

            def __str__(self) -> str:
                return f"{self.args[0]} could not log in with {self.password}"

        with pytest.raises(LoginError) as caught:
            _call(_ScriptedToolset(LoginError("admin", registered_secret)))

        assert str(caught.value) == "admin could not log in with ***"

    def test_an_exception_whose_message_cannot_be_masked_is_replaced(self, registered_secret):
        class OpaqueError(Exception):
            def __str__(self) -> str:
                return f"handshake failed: {registered_secret}"

        with pytest.raises(RuntimeError) as caught:
            _call(_ScriptedToolset(OpaqueError()))

        assert str(caught.value) == "OpaqueError: handshake failed: ***"
        assert registered_secret not in "".join(traceback.format_exception(caught.value))

    def test_masks_a_tool_return_and_keeps_it_one(self, registered_secret):
        result = _call(
            _ScriptedToolset(
                ToolReturn(return_value=f"key={registered_secret}", content=f"for {registered_secret}")
            )
        )

        assert result == ToolReturn(return_value="key=***", content="for ***")

    def test_an_approval_request_keeps_its_type_and_is_not_logged_as_a_failure(
        self, registered_secret, caplog
    ):
        with pytest.raises(ApprovalRequired) as caught:
            _call(_ScriptedToolset(ApprovalRequired(metadata={"reason": registered_secret})))

        assert caught.value.metadata == {"reason": "***"}
        assert not any("failed" in e["event"] for e in caplog)

    def test_masks_the_exceptions_inside_an_exception_group(self, registered_secret):
        cause = ConnectionError(f"socket closed by {registered_secret}")
        inner = ValueError(f"login failed for {registered_secret}")
        inner.__cause__ = cause
        group = ExceptionGroup("tool calls failed", [inner])

        with pytest.raises(ExceptionGroup) as caught:
            _call(_ScriptedToolset(group))

        assert [str(e) for e in caught.value.exceptions] == ["login failed for ***"]
        assert registered_secret not in "".join(traceback.format_exception(caught.value))

    def test_an_exception_that_cannot_be_masked_is_withheld(self):
        class Unprintable(Exception):
            def __str__(self) -> str:
                raise RuntimeError("cannot render")

        with pytest.raises(RuntimeError, match="Unprintable: details withheld"):
            _call(_ScriptedToolset(Unprintable()))


class TestRunBlocking:
    def test_runs_off_the_event_loop_thread(self):
        async def call() -> tuple[int, int]:
            loop_thread = threading.get_ident()
            return loop_thread, await AirflowToolset.run_blocking(threading.get_ident)

        loop_thread, call_thread = asyncio.run(call())

        assert call_thread != loop_thread

    def test_blocking_calls_from_different_toolsets_never_overlap(self):
        active = 0
        overlapped = False

        def blocking_call() -> None:
            nonlocal active, overlapped
            active += 1
            overlapped = overlapped or active > 1
            time.sleep(0.05)
            active -= 1

        async def call_concurrently() -> None:
            first, second = _ScriptedToolset(None), _ScriptedToolset(None)
            await asyncio.gather(*(ts.run_blocking(blocking_call) for ts in (first, second, first)))

        asyncio.run(call_concurrently())

        assert not overlapped


@pytest.mark.enable_redact
class TestMaskingToolset:
    def test_masks_a_toolset_that_does_not_mask_itself(self, registered_secret):
        def read_setting() -> str:
            return f"api key: {registered_secret}"

        masked = MaskingToolset(wrapped=FunctionToolset([read_setting]))

        async def call() -> Any:
            ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
            tools = await masked.get_tools(ctx)
            return await masked.call_tool("read_setting", {}, ctx, tools["read_setting"])

        assert asyncio.run(call()) == "api key: ***"


class _ApiToolset(FunctionToolset):
    """A Dag author's toolset whose one tool returns a connection string."""

    def __init__(self, secret: str) -> None:
        def connection_string() -> str:
            """Return the connection string."""
            return f"postgres://svc:{secret}@db"

        super().__init__([connection_string])


def _run_agent(toolset) -> str:
    def model(messages, info):
        returns = [p for m in messages for p in m.parts if isinstance(p, ToolReturnPart)]
        if returns:
            return ModelResponse(parts=[TextPart(str(returns[0].content))])
        return ModelResponse(parts=[ToolCallPart("connection_string", {}, tool_call_id="c")])

    return Agent(FunctionModel(model), toolsets=[toolset]).run_sync("go").output


@pytest.mark.enable_redact
class TestWithMasking:
    def test_a_function_that_builds_a_toolset_per_run_is_masked_and_still_runs(self, registered_secret):
        def per_run(ctx: RunContext[Any]) -> FunctionToolset:
            return _ApiToolset(registered_secret)

        assert _run_agent(ensure_masked(per_run)) == "postgres://svc:***@db"

    def test_an_airflow_toolset_that_overrides_call_tool_is_wrapped(self, registered_secret):
        class Overriding(_ScriptedToolset):
            async def call_tool(self, name, tool_args, ctx, tool) -> Any:
                return f"token {registered_secret}"

        masked = ensure_masked(Overriding("unused"))

        assert isinstance(masked, MaskingToolset)
        assert _call(masked) == "token ***"

    def test_an_airflow_toolset_that_masks_itself_is_not_wrapped(self):
        toolset = _ScriptedToolset("ok")

        assert ensure_masked(toolset) is toolset

    def test_a_failure_masked_by_two_layers_is_logged_once(self, caplog):
        with pytest.raises(ValueError, match="boom"):
            _call(MaskingToolset(wrapped=_ScriptedToolset(ValueError("boom"))))

        assert [e["event"] for e in caplog].count("Tool t failed") == 1
