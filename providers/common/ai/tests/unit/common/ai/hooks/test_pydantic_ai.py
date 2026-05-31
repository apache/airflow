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

import functools
import json
import sys
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models import Model
from pydantic_ai.models.test import TestModel
from pydantic_ai.run import AgentRunResult as PydanticAgentRunResult
from pydantic_ai.usage import RunUsage, UsageLimits

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.base import AgentRunRequest, AgentRunResult, BaseAIHook, ToolSpec
from airflow.providers.common.ai.hooks.pydantic_ai import (
    PydanticAIAzureHook,
    PydanticAIBedrockHook,
    PydanticAIHook,
    PydanticAIVertexHook,
)


def _test_agent() -> Agent[None, str]:
    return Agent(TestModel())


def _pydantic_run_result(
    output: str,
    *,
    model_name: str = "test-model",
    message_history: list | None = None,
    requests: int = 1,
    tool_calls: int = 0,
    input_tokens: int = 0,
    output_tokens: int = 0,
) -> MagicMock:
    mock_result = MagicMock(spec=PydanticAgentRunResult)
    mock_result.output = output
    mock_result.usage = RunUsage(
        requests=requests,
        tool_calls=tool_calls,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    mock_result.response = ModelResponse(
        parts=[TextPart(content=str(output))],
        model_name=model_name,
    )
    mock_result.all_messages.return_value = message_history or []
    return mock_result


def _noop_override_context() -> MagicMock:
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=None)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


class _PydanticAIHookWithTestModel(PydanticAIHook):
    """Concrete hook that uses a real TestModel without patching Agent construction."""

    def __init__(self, model: TestModel):
        super().__init__(llm_conn_id="test_conn", model_id="test-model")
        self._test_model = model

    def get_model(self) -> TestModel:
        return self._test_model


class TestPydanticAIHookBaseContract:
    def test_is_base_hook(self):
        assert issubclass(PydanticAIHook, BaseAIHook)

    def test_capability_flags(self):
        assert PydanticAIHook.supports_toolsets is True
        assert PydanticAIHook.supports_durable is True
        assert PydanticAIHook.supports_usage_limits is True


class TestPydanticAIHookInit:
    def test_default_conn_id(self):
        hook = PydanticAIHook()
        assert hook.llm_conn_id == "pydanticai_default"
        assert hook.model_id is None

    def test_custom_conn_id(self):
        hook = PydanticAIHook(llm_conn_id="my_llm", model_id="openai:gpt-5.3")
        assert hook.llm_conn_id == "my_llm"
        assert hook.model_id == "openai:gpt-5.3"

    def test_azure_hook_uses_own_default_conn_name(self):
        """Subclass default_conn_name is used, not the base class value."""
        hook = PydanticAIAzureHook()
        assert hook.llm_conn_id == "pydanticai_azure_default"

    def test_bedrock_hook_uses_own_default_conn_name(self):
        hook = PydanticAIBedrockHook()
        assert hook.llm_conn_id == "pydanticai_bedrock_default"

    def test_vertex_hook_uses_own_default_conn_name(self):
        hook = PydanticAIVertexHook()
        assert hook.llm_conn_id == "pydanticai_vertex_default"

    def test_durable_state_not_stored_on_hook_instance(self):
        hook = PydanticAIHook()
        assert not hasattr(hook, "_durable_storage")
        assert not hasattr(hook, "_durable_counter")


class TestPydanticAIHookGetModel:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_model_with_api_key_and_base_url(self, mock_infer_provider_class, mock_infer_model):
        """Credentials are injected via provider_factory, not as direct kwargs."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model
        mock_provider = MagicMock()
        mock_infer_provider_class.return_value = MagicMock(return_value=mock_provider)

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            host="https://api.openai.com/v1",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_model()

        assert result is mock_model
        mock_infer_model.assert_called_once()
        call_args = mock_infer_model.call_args
        assert call_args[0][0] == "openai:gpt-5.3"
        assert "provider_factory" in call_args[1]

        factory = call_args[1]["provider_factory"]
        factory("openai")
        mock_infer_provider_class.assert_called_with("openai")
        mock_infer_provider_class.return_value.assert_called_with(
            api_key="sk-test-key", base_url="https://api.openai.com/v1"
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_model_with_model_from_extra(self, mock_infer_provider_class, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model
        mock_infer_provider_class.return_value = MagicMock(return_value=MagicMock())

        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            extra='{"model": "anthropic:claude-opus-4-6"}',
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_model()

        assert result is mock_model
        assert mock_infer_model.call_args[0][0] == "anthropic:claude-opus-4-6"

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_model_id_param_overrides_extra(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_provider_class.return_value = MagicMock(return_value=MagicMock())

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            extra='{"model": "anthropic:claude-opus-4-6"}',
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        assert mock_infer_model.call_args[0][0] == "openai:gpt-5.3"

    def test_get_model_raises_when_no_model(self):
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="No model specified"):
                hook.get_model()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_model_without_credentials_uses_default_provider(self, mock_infer_model):
        """No api_key or base_url means env-based auth (Bedrock, Vertex, etc.)."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="bedrock:us.anthropic.claude-v2")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        mock_infer_model.assert_called_once_with("bedrock:us.anthropic.claude-v2")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_model_with_base_url_only(self, mock_infer_provider_class, mock_infer_model):
        """Ollama / vLLM: base_url but no API key."""
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_provider_class.return_value = MagicMock(return_value=MagicMock())

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:llama3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            host="http://localhost:11434/v1",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("openai")
        mock_infer_provider_class.return_value.assert_called_with(base_url="http://localhost:11434/v1")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_model_caches_result(self, mock_infer_model):
        """get_model() should resolve the model once and cache it."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_model()
            second = hook.get_model()

        assert first is second
        mock_infer_model.assert_called_once()

    def test_get_conn_delegates_to_get_model(self):
        """get_conn() is a compatibility shim that calls get_model()."""
        hook = PydanticAIHook()
        mock_model = MagicMock()
        with patch.object(hook, "get_model", return_value=mock_model):
            result = hook.get_conn()
        assert result is mock_model


class TestPydanticAIHookCreateAgent:
    def test_create_agent_runs_callable_object_tool_with_real_schema(self):
        """Callable objects should produce a real pydantic-ai function tool and execute successfully."""
        model = TestModel(call_tools="all")
        hook = _PydanticAIHookWithTestModel(model)
        calls: list[str] = []

        class CustomerLookup:
            def __call__(self, customer_id: str) -> dict[str, str]:
                calls.append(customer_id)
                return {"customer_id": customer_id}

        request = AgentRunRequest(
            prompt="Look up a customer",
            toolsets=[CustomerLookup()],
            enable_tool_logging=True,
        )

        agent = hook.create_agent(request)
        run_result = hook.run_agent(agent, request)

        assert run_result.usage is not None
        assert run_result.usage.tool_calls == 1
        assert len(calls) == 1
        assert isinstance(calls[0], str)

        [tool_def] = model.last_model_request_parameters.function_tools
        assert tool_def.name == "CustomerLookup"
        assert set(tool_def.parameters_json_schema["properties"]) == {"customer_id"}
        assert "environment" not in tool_def.parameters_json_schema["properties"]

    def test_create_agent_runs_partial_tool_with_bound_argument_removed_from_schema(self):
        """functools.partial should expose only remaining parameters and preserve bound args at runtime."""
        model = TestModel(call_tools="all")
        hook = _PydanticAIHookWithTestModel(model)
        calls: list[tuple[str, str]] = []

        def fetch_metric(environment: str, metric_name: str) -> float:
            calls.append((environment, metric_name))
            return 1.0

        request = AgentRunRequest(
            prompt="Fetch a metric",
            toolsets=[functools.partial(fetch_metric, "prod")],
            enable_tool_logging=True,
        )

        agent = hook.create_agent(request)
        run_result = hook.run_agent(agent, request)

        assert run_result.usage is not None
        assert run_result.usage.tool_calls == 1
        assert len(calls) == 1
        assert calls[0][0] == "prod"
        assert isinstance(calls[0][1], str)

        [tool_def] = model.last_model_request_parameters.function_tools
        assert tool_def.name == "fetch_metric"
        assert set(tool_def.parameters_json_schema["properties"]) == {"metric_name"}
        assert "environment" not in tool_def.parameters_json_schema["properties"]

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_defaults(self, mock_agent_cls, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(prompt="hi", instructions="You are a helpful assistant.")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(request)

        mock_agent_cls.assert_called_once_with(
            mock_model,
            output_type=str,
            instructions="You are a helpful assistant.",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_with_agent_params(self, mock_agent_cls, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(
            prompt="hi",
            output_type=dict,
            instructions="Be helpful.",
            agent_params={"retries": 3},
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(request)

        mock_agent_cls.assert_called_once_with(
            mock_model,
            output_type=dict,
            instructions="Be helpful.",
            retries=3,
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_rejects_tools_in_agent_params_with_toolsets(self, mock_agent_cls, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(
            prompt="hi",
            toolsets=[lambda: "ok"],
            agent_params={"tools": [MagicMock()]},
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="agent_params must not include 'tools'"):
                hook.create_agent(request)

        mock_agent_cls.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_rejects_toolsets_in_agent_params_with_toolsets(
        self, mock_agent_cls, mock_infer_model
    ):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(
            prompt="hi",
            toolsets=[lambda: "ok"],
            agent_params={"toolsets": [MagicMock()]},
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="agent_params must not include 'toolsets'"):
                hook.create_agent(request)

        mock_agent_cls.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_create_agent_inits_durable_when_context_set(self, mock_infer_model):
        from airflow.providers.common.ai.hooks.base import DurableContext

        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        ctx = DurableContext(dag_id="d", task_id="t", run_id="r")
        request = AgentRunRequest(prompt="hi", durable_context=ctx)

        mock_storage = MagicMock()
        mock_counter = MagicMock()
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with (
            patch.object(hook, "get_connection", return_value=conn),
            patch.object(hook, "_init_durable", return_value=(mock_storage, mock_counter)),
        ):
            agent = hook.create_agent(request)

        assert BaseAIHook._pop_agent_durable(agent) == (mock_storage, mock_counter)

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_create_agent_does_not_bind_durable_when_no_context(self, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")

        request = AgentRunRequest(prompt="hi")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            agent = hook.create_agent(request)

        assert BaseAIHook._pop_agent_durable(agent) is None

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_passes_native_tools_through_directly(self, mock_agent_cls, mock_infer_model):
        """Native pydantic-ai Tool objects bypass Airflow callable wrappers."""
        from pydantic_ai.tools import Tool

        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        native_tool = MagicMock(spec=Tool)
        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(prompt="hi", toolsets=[native_tool])
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(request)

        call_kwargs = mock_agent_cls.call_args[1]
        assert any(t is native_tool for t in call_kwargs["tools"])

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_mixes_base_toolset_and_native_tool(self, mock_agent_cls, mock_infer_model):
        """BaseToolset items are expanded; native Tool objects are passed through unchanged."""
        from pydantic_ai.tools import Tool

        from airflow.providers.common.ai.hooks.base import BaseToolset

        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        def first_fn() -> str:
            return "first"

        def second_fn() -> str:
            return "second"

        class MyToolset(BaseToolset):
            def as_tools(self):
                return [
                    ToolSpec(name="first_fn", description="desc", parameters={}, fn=first_fn),
                    ToolSpec(name="second_fn", description="desc", parameters={}, fn=second_fn),
                ]

        native_tool = MagicMock(spec=Tool)

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(prompt="hi", toolsets=[MyToolset(), native_tool], enable_tool_logging=False)
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(request)

        call_kwargs = mock_agent_cls.call_args[1]
        tools = call_kwargs["tools"]
        assert len(tools) == 3
        assert tools[2] is native_tool
        assert [tool.name for tool in tools[:2]] == ["first_fn", "second_fn"]

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_routes_abstract_toolset_to_toolsets_kwarg(self, mock_agent_cls, mock_infer_model):
        """AbstractToolset items must go in Agent(toolsets=[...]), not Agent(tools=[...])."""
        from pydantic_ai.toolsets.abstract import AbstractToolset

        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        abstract_ts = MagicMock(spec=AbstractToolset)

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(prompt="hi", toolsets=[abstract_ts], enable_tool_logging=False)
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(request)

        call_kwargs = mock_agent_cls.call_args[1]
        assert "tools" not in call_kwargs
        assert "toolsets" in call_kwargs
        assert any(ts is abstract_ts for ts in call_kwargs["toolsets"])

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_wraps_abstract_toolset_with_logging(self, mock_agent_cls, mock_infer_model):
        """AbstractToolset items are wrapped with LoggingToolset when enable_tool_logging=True."""
        from pydantic_ai.toolsets.abstract import AbstractToolset

        from airflow.providers.common.ai.toolsets.logging import LoggingToolset

        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        abstract_ts = MagicMock(spec=AbstractToolset)

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(prompt="hi", toolsets=[abstract_ts], enable_tool_logging=True)
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(request)

        call_kwargs = mock_agent_cls.call_args[1]
        toolsets = call_kwargs["toolsets"]
        assert len(toolsets) == 1
        assert isinstance(toolsets[0], LoggingToolset)
        assert toolsets[0].wrapped is abstract_ts

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_wraps_abstract_toolset_with_caching_when_durable(
        self, mock_agent_cls, mock_infer_model
    ):
        """AbstractToolset items are wrapped with CachingToolset (inner) and LoggingToolset (outer) for durable runs."""
        from pydantic_ai.toolsets.abstract import AbstractToolset

        from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
        from airflow.providers.common.ai.hooks.base import DurableContext
        from airflow.providers.common.ai.toolsets.logging import LoggingToolset

        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        abstract_ts = MagicMock(spec=AbstractToolset)
        mock_storage = MagicMock()
        mock_counter = MagicMock()

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        ctx = DurableContext(dag_id="d", task_id="t", run_id="r")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        request = AgentRunRequest(
            prompt="hi", toolsets=[abstract_ts], durable_context=ctx, enable_tool_logging=True
        )
        with (
            patch.object(hook, "get_connection", return_value=conn),
            patch.object(hook, "_init_durable", return_value=(mock_storage, mock_counter)),
        ):
            hook.create_agent(request)

        call_kwargs = mock_agent_cls.call_args[1]
        toolsets = call_kwargs["toolsets"]
        assert len(toolsets) == 1
        outer = toolsets[0]
        assert isinstance(outer, LoggingToolset)
        assert isinstance(outer.wrapped, CachingToolset)
        assert outer.wrapped.wrapped is abstract_ts

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_create_agent_binds_durable_per_agent_not_on_hook(self, mock_infer_model):
        """Second create_agent must not overwrite durable state for the first agent."""
        from airflow.providers.common.ai.hooks.base import DurableContext

        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        ctx_a = DurableContext(dag_id="d", task_id="t", run_id="r1")
        ctx_b = DurableContext(dag_id="d", task_id="t", run_id="r2")
        storage_a, counter_a = MagicMock(), MagicMock()
        storage_b, counter_b = MagicMock(), MagicMock()
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            with patch.object(
                hook, "_init_durable", side_effect=[(storage_a, counter_a), (storage_b, counter_b)]
            ):
                agent_a = hook.create_agent(AgentRunRequest(prompt="a", durable_context=ctx_a))
                agent_b = hook.create_agent(AgentRunRequest(prompt="b", durable_context=ctx_b))

        assert agent_a is not agent_b
        assert BaseAIHook._pop_agent_durable(agent_a) == (storage_a, counter_a)
        assert BaseAIHook._pop_agent_durable(agent_b) == (storage_b, counter_b)


class TestPydanticAIHookRunAgent:
    def test_run_agent_returns_agent_run_result(self):
        hook = PydanticAIHook()
        agent = _test_agent()
        mock_result = _pydantic_run_result(
            "done",
            model_name="openai:gpt-5",
            input_tokens=5,
            output_tokens=10,
        )

        request = AgentRunRequest(prompt="hello")
        with patch.object(agent, "run_sync", return_value=mock_result) as mock_run_sync:
            run_result = hook.run_agent(agent, request)

        assert isinstance(run_result, AgentRunResult)
        assert run_result.output == "done"
        assert run_result.model_name == "openai:gpt-5"
        assert run_result.usage.total_tokens == 15
        mock_run_sync.assert_called_once_with("hello")

    def test_create_agent_rejects_unsupported_usage_limits(self):
        hook = PydanticAIHook()
        hook.supports_usage_limits = False
        with pytest.raises(ValueError, match="usage_limits are not supported"):
            hook.create_agent(AgentRunRequest(prompt="hi", usage_limits=UsageLimits()))

    def test_run_agent_forwards_message_history_and_usage_limits(self):
        hook = PydanticAIHook()
        agent = _test_agent()
        mock_result = _pydantic_run_result("ok", model_name="m", message_history=["history"])
        limits = UsageLimits()
        history = ["prior"]

        request = AgentRunRequest(prompt="more", message_history=history, usage_limits=limits)
        with patch.object(agent, "run_sync", return_value=mock_result) as mock_run_sync:
            hook.run_agent(agent, request)

        mock_run_sync.assert_called_once_with("more", message_history=history, usage_limits=limits)

    @patch.object(Agent, "override")
    @patch.object(Agent, "run_sync")
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.CachingModel")
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", side_effect=lambda m: m)
    def test_run_agent_durable_applies_caching_model(
        self,
        mock_infer_model,
        mock_caching_model_cls,
        mock_run_sync,
        mock_override,
    ):
        """When durable state is set, run_agent wraps model with CachingModel."""
        hook = PydanticAIHook()
        agent = _test_agent()
        mock_run_sync.return_value = _pydantic_run_result("ok", model_name="m")
        mock_override.return_value = _noop_override_context()
        mock_caching_model_cls.return_value = MagicMock()

        mock_storage = MagicMock()
        mock_counter = MagicMock()
        mock_counter.replayed_model = 1
        mock_counter.replayed_tool = 0
        mock_counter.cached_model = 0
        mock_counter.cached_tool = 0
        BaseAIHook._bind_agent_durable(agent, mock_storage, mock_counter)

        request = AgentRunRequest(prompt="hi")
        run_result = hook.run_agent(agent, request)

        mock_caching_model_cls.assert_called_once()
        mock_override.assert_called_once()
        mock_run_sync.assert_called_once_with("hi")
        assert run_result.durable_stats is not None
        assert BaseAIHook._pop_agent_durable(agent) is None
        mock_storage.cleanup.assert_called_once()

    @patch.object(Agent, "override")
    @patch.object(Agent, "run_sync", side_effect=RuntimeError("boom"))
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.CachingModel")
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", side_effect=lambda m: m)
    def test_run_agent_preserves_durable_cache_on_exception(
        self,
        mock_infer_model,
        mock_caching_model_cls,
        mock_run_sync,
        mock_override,
    ):
        hook = PydanticAIHook()
        agent = _test_agent()
        mock_override.return_value = _noop_override_context()

        mock_storage = MagicMock()
        mock_counter = MagicMock()
        BaseAIHook._bind_agent_durable(agent, mock_storage, mock_counter)

        with pytest.raises(RuntimeError, match="boom"):
            hook.run_agent(agent, AgentRunRequest(prompt="hi"))

        mock_storage.cleanup.assert_not_called()
        assert BaseAIHook._pop_agent_durable(agent) is None

    def test_tool_spec_to_native_tools_called(self):
        hook = PydanticAIHook()

        def fn(customer_id: int) -> str:
            """test function"""
            return "ok"

        with patch("airflow.providers.common.ai.hooks.pydantic_ai.Tool") as mock_tool_cls:
            hook._resolve_tools(toolsets=[fn], enable_logging=False, storage=None, counter=None)
        mock_tool_cls.from_schema.assert_called_once_with(
            fn,
            name="fn",
            description="test function",
            sequential=False,
            json_schema={
                "properties": {"customer_id": {"type": "integer"}},
                "required": ["customer_id"],
                "type": "object",
            },
        )


class TestPydanticAIHookTestConnection:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_successful_connection(self, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is True
        assert message == "Model resolved successfully."

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_failed_connection(self, mock_infer_model):
        mock_infer_model.side_effect = ValueError("Unknown provider 'badprovider'")

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="badprovider:model")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "Unknown provider" in message

    def test_failed_connection_no_model(self):
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "No model specified" in message


# ---------------------------------------------------------------------------
# Subclass hook tests
# ---------------------------------------------------------------------------


class TestPydanticAIAzureHook:
    """Tests for PydanticAIAzureHook."""

    def test_conn_type(self):
        assert PydanticAIAzureHook.conn_type == "pydanticai-azure"

    def test_hook_name(self):
        assert "Azure" in PydanticAIAzureHook.hook_name

    def test_ui_field_behaviour_relabels_host(self):
        behaviour = PydanticAIAzureHook.get_ui_field_behaviour()
        assert behaviour["relabeling"].get("host") == "Azure Endpoint"

    def test_get_provider_kwargs_maps_azure_endpoint(self):
        hook = PydanticAIAzureHook.__new__(PydanticAIAzureHook)
        result = hook._get_provider_kwargs(
            "my-key",
            "https://myresource.openai.azure.com",
            {"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"},
        )
        assert result["azure_endpoint"] == "https://myresource.openai.azure.com"
        assert result["api_key"] == "my-key"
        assert result["api_version"] == "2024-07-01-preview"
        assert "base_url" not in result

    def test_get_provider_kwargs_omits_none_api_key(self):
        hook = PydanticAIAzureHook.__new__(PydanticAIAzureHook)
        result = hook._get_provider_kwargs(
            None,
            "https://myresource.openai.azure.com",
            {"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"},
        )
        assert "api_key" not in result
        assert result["azure_endpoint"] == "https://myresource.openai.azure.com"

    def test_get_provider_kwargs_omits_azure_endpoint_when_no_host(self):
        hook = PydanticAIAzureHook.__new__(PydanticAIAzureHook)
        result = hook._get_provider_kwargs(
            "my-key",
            None,
            {"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"},
        )
        assert "azure_endpoint" not in result
        assert result["api_key"] == "my-key"

    def test_get_provider_kwargs_empty_without_api_version(self):
        hook = PydanticAIAzureHook.__new__(PydanticAIAzureHook)
        result = hook._get_provider_kwargs(
            "my-key",
            "https://myresource.openai.azure.com",
            {"model": "azure:gpt-4o"},
        )
        assert "api_version" not in result

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_model_uses_azure_endpoint(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_provider_cls = MagicMock(return_value=MagicMock())
        mock_infer_provider_class.return_value = mock_provider_cls

        hook = PydanticAIAzureHook(llm_conn_id="azure_test")
        conn = Connection(
            conn_id="azure_test",
            conn_type="pydanticai-azure",
            password="azure-key",
            host="https://myresource.openai.azure.com",
            extra=json.dumps({"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("azure")
        mock_provider_cls.assert_called_with(
            api_key="azure-key",
            azure_endpoint="https://myresource.openai.azure.com",
            api_version="2024-07-01-preview",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_model_falls_back_to_env_auth_when_no_kwargs(self, mock_infer_model):
        """No host + no password → env-var auth path (empty _get_provider_kwargs)."""
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIAzureHook(llm_conn_id="azure_test")
        conn = Connection(
            conn_id="azure_test",
            conn_type="pydanticai-azure",
            extra=json.dumps({"model": "azure:gpt-4o"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        mock_infer_model.assert_called_once_with("azure:gpt-4o")


class TestPydanticAIBedrockHook:
    """Tests for PydanticAIBedrockHook."""

    def test_conn_type(self):
        assert PydanticAIBedrockHook.conn_type == "pydanticai-bedrock"

    def test_hook_name(self):
        assert "Bedrock" in PydanticAIBedrockHook.hook_name

    def test_ui_hides_host_and_password(self):
        behaviour = PydanticAIBedrockHook.get_ui_field_behaviour()
        assert "host" in behaviour["hidden_fields"]
        assert "password" in behaviour["hidden_fields"]

    def test_get_provider_kwargs_maps_bedrock_fields(self):
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "region_name": "us-east-1",
                "aws_access_key_id": "AKIA123",
                "aws_secret_access_key": "secret",
            },
        )
        assert result["region_name"] == "us-east-1"
        assert result["aws_access_key_id"] == "AKIA123"
        assert result["aws_secret_access_key"] == "secret"
        assert "model" not in result
        assert "api_key" not in result

    def test_get_provider_kwargs_returns_empty_for_env_auth(self):
        """When no keys are in extra, return {} so env-auth path is taken."""
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(None, None, {"model": "bedrock:us.anthropic.claude-opus-4-5"})
        assert result == {}

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_model_falls_back_to_env_auth(self, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIBedrockHook(llm_conn_id="bedrock_test")
        conn = Connection(
            conn_id="bedrock_test",
            conn_type="pydanticai-bedrock",
            extra=json.dumps({"model": "bedrock:us.anthropic.claude-opus-4-5"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        mock_infer_model.assert_called_once_with("bedrock:us.anthropic.claude-opus-4-5")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_model_uses_explicit_keys(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_provider_cls = MagicMock(return_value=MagicMock())
        mock_infer_provider_class.return_value = mock_provider_cls

        hook = PydanticAIBedrockHook(llm_conn_id="bedrock_test")
        conn = Connection(
            conn_id="bedrock_test",
            conn_type="pydanticai-bedrock",
            extra=json.dumps(
                {
                    "model": "bedrock:us.anthropic.claude-opus-4-5",
                    "region_name": "eu-west-1",
                    "aws_access_key_id": "AKIA123",
                    "aws_secret_access_key": "secret",
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("bedrock")
        mock_provider_cls.assert_called_with(
            region_name="eu-west-1",
            aws_access_key_id="AKIA123",
            aws_secret_access_key="secret",
        )

    def test_get_provider_kwargs_bearer_token(self):
        """api_key in extra maps to BedrockProvider's bearer-token param."""
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "api_key": "bearer-token-value",
                "region_name": "us-east-1",
            },
        )
        assert result["api_key"] == "bearer-token-value"
        assert result["region_name"] == "us-east-1"
        assert "aws_access_key_id" not in result

    def test_get_provider_kwargs_base_url(self):
        """base_url in extra is forwarded to BedrockProvider."""
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "base_url": "https://custom-bedrock.example.com",
            },
        )
        assert result["base_url"] == "https://custom-bedrock.example.com"

    def test_get_provider_kwargs_float_timeouts(self):
        """Timeout values are coerced to float (JSON delivers them as int)."""
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "aws_read_timeout": 60,
                "aws_connect_timeout": 10.5,
            },
        )
        assert result["aws_read_timeout"] == 60.0
        assert isinstance(result["aws_read_timeout"], float)
        assert result["aws_connect_timeout"] == 10.5
        assert isinstance(result["aws_connect_timeout"], float)


class TestPydanticAIVertexHook:
    """Tests for PydanticAIVertexHook."""

    def test_conn_type(self):
        assert PydanticAIVertexHook.conn_type == "pydanticai-vertex"

    def test_hook_name(self):
        assert "Vertex" in PydanticAIVertexHook.hook_name

    def test_ui_hides_host_and_password(self):
        behaviour = PydanticAIVertexHook.get_ui_field_behaviour()
        assert "host" in behaviour["hidden_fields"]
        assert "password" in behaviour["hidden_fields"]

    def test_get_provider_kwargs_maps_vertex_fields(self):
        """project and location are passed directly; api_key absent when not in extra."""
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "google-vertex:gemini-2.0-flash",
                "project": "my-project",
                "location": "us-central1",
            },
        )
        assert result["project"] == "my-project"
        assert result["location"] == "us-central1"
        assert "model" not in result
        assert "api_key" not in result
        assert "project_id" not in result

    def test_get_provider_kwargs_api_key_gla_mode(self):
        """api_key in extra is forwarded for Generative Language API mode."""
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {"model": "google-gla:gemini-2.0-flash", "api_key": "gla-key"},
        )
        assert result["api_key"] == "gla-key"

    def test_get_provider_kwargs_vertexai_flag(self):
        """vertexai bool is forwarded and coerced to bool."""
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {"model": "google-vertex:gemini-2.0-flash", "api_key": "key", "vertexai": True},
        )
        assert result["vertexai"] is True

    def test_get_provider_kwargs_service_account_info_loads_credentials(self):
        """service_account_info dict is loaded into a Credentials object."""
        mock_sa = MagicMock()
        mock_creds = MagicMock()
        mock_sa.Credentials.from_service_account_info.return_value = mock_creds

        mock_google_oauth2 = MagicMock()
        mock_google_oauth2.service_account = mock_sa

        sa_info_dict = {"type": "service_account", "project_id": "my-project", "private_key": "..."}
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        with patch.dict(
            sys.modules,
            {
                "google": MagicMock(),
                "google.oauth2": mock_google_oauth2,
                "google.oauth2.service_account": mock_sa,
            },
        ):
            result = hook._get_provider_kwargs(
                None,
                None,
                {
                    "model": "google-vertex:gemini-2.0-flash",
                    "service_account_info": sa_info_dict,
                },
            )

        mock_sa.Credentials.from_service_account_info.assert_called_once_with(
            sa_info_dict,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        assert result["credentials"] is mock_creds
        assert "service_account_info" not in result

    def test_get_provider_kwargs_returns_empty_for_adc(self):
        """When no keys are in extra, return {} so ADC path is taken."""
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_provider_kwargs(None, None, {"model": "google-vertex:gemini-2.0-flash"})
        assert result == {}

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_model_falls_back_to_adc(self, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIVertexHook(llm_conn_id="vertex_test")
        conn = Connection(
            conn_id="vertex_test",
            conn_type="pydanticai-vertex",
            extra=json.dumps({"model": "google-vertex:gemini-2.0-flash"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        mock_infer_model.assert_called_once_with("google-vertex:gemini-2.0-flash")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_model_uses_explicit_project(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_provider_cls = MagicMock(return_value=MagicMock())
        mock_infer_provider_class.return_value = mock_provider_cls

        hook = PydanticAIVertexHook(llm_conn_id="vertex_test")
        conn = Connection(
            conn_id="vertex_test",
            conn_type="pydanticai-vertex",
            extra=json.dumps(
                {
                    "model": "google-vertex:gemini-2.0-flash",
                    "project": "my-project",
                    "location": "europe-west4",
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("google-vertex")
        mock_provider_cls.assert_called_with(project="my-project", location="europe-west4")
