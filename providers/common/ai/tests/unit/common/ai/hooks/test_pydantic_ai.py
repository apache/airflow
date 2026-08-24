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

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai.models import Model
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.test import TestModel

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.pydantic_ai import (
    PydanticAIAzureHook,
    PydanticAIBedrockHook,
    PydanticAIHook,
    PydanticAIVertexHook,
)


class TestPydanticAIHookInit:
    def test_default_conn_id(self):
        hook = PydanticAIHook()
        assert hook.llm_conn_id == "pydanticai_default"
        assert hook.model_id is None

    def test_custom_conn_id(self):
        hook = PydanticAIHook(llm_conn_id="my_llm", model_id="openai:gpt-5.6-sol")
        assert hook.llm_conn_id == "my_llm"
        assert hook.model_id == "openai:gpt-5.6-sol"

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


class TestPydanticAIHookGetConn:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_with_api_key_and_base_url(self, mock_infer_provider_class, mock_infer_model):
        """Credentials are injected via provider_factory, not as direct kwargs."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model
        mock_provider = MagicMock()
        mock_infer_provider_class.return_value = MagicMock(return_value=mock_provider)

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            host="https://api.openai.com/v1",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        assert result is mock_model
        mock_infer_model.assert_called_once()
        call_args = mock_infer_model.call_args
        assert call_args[0][0] == "openai:gpt-5.6-sol"
        # provider_factory should be passed as keyword arg
        assert "provider_factory" in call_args[1]

        # Call the factory to verify it creates the provider with credentials
        factory = call_args[1]["provider_factory"]
        factory("openai")
        mock_infer_provider_class.assert_called_with("openai")
        mock_infer_provider_class.return_value.assert_called_with(
            api_key="sk-test-key", base_url="https://api.openai.com/v1"
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_with_model_from_extra(self, mock_infer_provider_class, mock_infer_model):
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
            result = hook.get_conn()

        assert result is mock_model
        assert mock_infer_model.call_args[0][0] == "anthropic:claude-opus-4-6"

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_model_id_param_overrides_extra(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_provider_class.return_value = MagicMock(return_value=MagicMock())

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            extra='{"model": "anthropic:claude-opus-4-6"}',
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        # model_id param takes priority over extra
        assert mock_infer_model.call_args[0][0] == "openai:gpt-5.6-sol"

    def test_get_conn_raises_when_no_model(self):
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="No model specified"):
                hook.get_conn()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_conn_without_credentials_uses_default_provider(self, mock_infer_model):
        """No api_key or base_url means env-based auth (Bedrock, Vertex, etc.)."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="bedrock:us.anthropic.claude-v2")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        # No provider_factory — uses default infer_provider which reads env vars
        mock_infer_model.assert_called_once_with("bedrock:us.anthropic.claude-v2")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_with_base_url_only(self, mock_infer_provider_class, mock_infer_model):
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
            hook.get_conn()

        # provider_factory should be used since base_url is set
        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("openai")
        mock_infer_provider_class.return_value.assert_called_with(base_url="http://localhost:11434/v1")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_conn_caches_model(self, mock_infer_model):
        """get_conn() should resolve the model once and cache it."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_conn()
            second = hook.get_conn()

        assert first is second
        mock_infer_model.assert_called_once()


class _ConnRegistry:
    """
    In-memory stand-in for connection and hook lookup.

    ``_resolve_fallback_models`` goes through ``BaseHook.get_hook``, which needs both the
    metadata DB and provider discovery; this resolves both from a dict instead.
    """

    def __init__(self) -> None:
        self.conns: dict[str, Connection] = {}
        self.hook_classes: dict[str, type[PydanticAIHook]] = {}

    def add(
        self,
        conn_id: str,
        *,
        conn_type: str = "pydanticai",
        hook_class: type[PydanticAIHook] = PydanticAIHook,
        password: str | None = None,
        extra: dict | None = None,
    ) -> None:
        self.conns[conn_id] = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            password=password,
            extra=json.dumps(extra) if extra else None,
        )
        self.hook_classes[conn_id] = hook_class

    def get_connection(self, conn_id: str) -> Connection:
        return self.conns[conn_id]

    def get_hook(self, conn_id: str, hook_params: dict | None = None):
        hook_class = self.hook_classes[conn_id]
        return hook_class(llm_conn_id=conn_id, **(hook_params or {}))


@pytest.fixture
def registry():
    """Patch connection and hook lookup onto a registry the test populates."""
    reg = _ConnRegistry()
    with (
        patch.object(PydanticAIHook, "get_connection", side_effect=reg.get_connection),
        patch.object(PydanticAIHook, "get_hook", side_effect=reg.get_hook),
    ):
        yield reg


class _InferModelStub:
    """Resolve every model string to its own recognisable model, and record how it was built."""

    def __init__(self, mock: MagicMock) -> None:
        self.mock = mock
        self.models: dict[str, MagicMock] = {}

    def __call__(self, model_name: str, **kwargs) -> MagicMock:
        return self.models.setdefault(model_name, MagicMock(spec=Model, name=model_name))

    def provider_kwargs_for(self, model_name: str, infer_provider_class: MagicMock) -> dict:
        """Return the kwargs the provider for *model_name* would be constructed with."""
        factory = next(
            call.kwargs["provider_factory"] for call in self.mock.call_args_list if call.args[0] == model_name
        )
        infer_provider_class.return_value.reset_mock()
        factory(model_name.split(":")[0])
        return infer_provider_class.return_value.call_args.kwargs


@pytest.fixture
def infer_model_stub():
    """Patch ``infer_model`` so tests can tell the models of a chain apart."""
    with patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model") as mock:
        stub = _InferModelStub(mock)
        mock.side_effect = stub
        yield stub


class TestPydanticAIHookFallback:
    def test_no_fallback_returns_the_bare_model(self, registry, infer_model_stub):
        """Without a chain the resolved model is not wrapped at all."""
        registry.add("primary", extra={"model": "openai:gpt-5.6-sol"})
        hook = PydanticAIHook(llm_conn_id="primary")

        assert hook.get_conn() is infer_model_stub.models["openai:gpt-5.6-sol"]

    def test_param_builds_chain_in_order(self, registry, infer_model_stub):
        registry.add("primary", extra={"model": "openai:gpt-5.6-sol"})
        registry.add("second", extra={"model": "anthropic:claude-opus-4-6"})
        registry.add("third", extra={"model": "groq:llama-4"})

        hook = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=["second", "third"])
        model = hook.get_conn()

        assert isinstance(model, FallbackModel)
        assert model.models == [
            infer_model_stub.models["openai:gpt-5.6-sol"],
            infer_model_stub.models["anthropic:claude-opus-4-6"],
            infer_model_stub.models["groq:llama-4"],
        ]

    def test_chain_from_connection_extra(self, registry, infer_model_stub):
        """A deployment manager can configure failover without touching Dag code."""
        registry.add(
            "primary",
            extra={"model": "openai:gpt-5.6-sol", "fallback_conn_ids": ["second"]},
        )
        registry.add("second", extra={"model": "anthropic:claude-opus-4-6"})

        model = PydanticAIHook(llm_conn_id="primary").get_conn()

        assert isinstance(model, FallbackModel)
        assert model.models == [
            infer_model_stub.models["openai:gpt-5.6-sol"],
            infer_model_stub.models["anthropic:claude-opus-4-6"],
        ]

    def test_param_overrides_extra(self, registry, infer_model_stub):
        registry.add(
            "primary",
            extra={"model": "openai:gpt-5.6-sol", "fallback_conn_ids": ["ignored"]},
        )
        registry.add("ignored", extra={"model": "groq:llama-4"})
        registry.add("second", extra={"model": "anthropic:claude-opus-4-6"})

        model = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=["second"]).get_conn()

        assert isinstance(model, FallbackModel)
        assert model.models[1] is infer_model_stub.models["anthropic:claude-opus-4-6"]

    def test_empty_list_param_disables_the_extra_chain(self, registry, infer_model_stub):
        """``[]`` is an explicit opt-out, distinct from ``None`` meaning "read the extra"."""
        registry.add(
            "primary",
            extra={"model": "openai:gpt-5.6-sol", "fallback_conn_ids": ["second"]},
        )
        registry.add("second", extra={"model": "anthropic:claude-opus-4-6"})

        model = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=[]).get_conn()

        assert model is infer_model_stub.models["openai:gpt-5.6-sol"]

    def test_chain_can_span_providers(self, registry, infer_model_stub):
        """Each connection resolves through its own hook class, so credentials differ per hop."""
        registry.add("primary", password="sk-openai", extra={"model": "openai:gpt-5.6-sol"})
        registry.add(
            "bedrock_dr",
            conn_type="pydanticai-bedrock",
            hook_class=PydanticAIBedrockHook,
            extra={
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "region_name": "us-east-1",
                "aws_access_key_id": "AKIA-test",
                "aws_secret_access_key": "secret",
            },
        )

        with patch(
            "airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True
        ) as mock_infer_provider_class:
            mock_infer_provider_class.return_value = MagicMock(return_value=MagicMock())
            hook = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=["bedrock_dr"])
            model = hook.get_conn()

            assert isinstance(model, FallbackModel)
            assert model.models == [
                infer_model_stub.models["openai:gpt-5.6-sol"],
                infer_model_stub.models["bedrock:us.anthropic.claude-opus-4-5"],
            ]

            # Each hop is built by its own hook's field mapping: the primary from
            # password/host, the Bedrock hop from its extra.
            assert infer_model_stub.provider_kwargs_for("openai:gpt-5.6-sol", mock_infer_provider_class) == {
                "api_key": "sk-openai"
            }
            assert infer_model_stub.provider_kwargs_for(
                "bedrock:us.anthropic.claude-opus-4-5", mock_infer_provider_class
            ) == {
                "region_name": "us-east-1",
                "aws_access_key_id": "AKIA-test",
                "aws_secret_access_key": "secret",
            }

    def test_model_id_is_not_forwarded_to_fallbacks(self, registry, infer_model_stub):
        """``model_id`` names a model of the primary's provider; each fallback uses its own."""
        registry.add("primary", extra={"model": "openai:gpt-4o-mini"})
        registry.add("second", extra={"model": "anthropic:claude-opus-4-6"})

        hook = PydanticAIHook(
            llm_conn_id="primary",
            model_id="openai:gpt-5.6-sol",
            fallback_conn_ids=["second"],
        )
        model = hook.get_conn()

        assert isinstance(model, FallbackModel)
        assert model.models == [
            infer_model_stub.models["openai:gpt-5.6-sol"],
            infer_model_stub.models["anthropic:claude-opus-4-6"],
        ]

    def test_non_pydanticai_fallback_raises(self, registry, infer_model_stub):
        """``BaseHook.get_hook`` dispatches on conn_type alone and can return anything."""
        registry.add("primary", extra={"model": "openai:gpt-5.6-sol"})
        registry.add("wrong_type", conn_type="langchain")
        registry.hook_classes["wrong_type"] = MagicMock  # type: ignore[assignment]

        hook = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=["wrong_type"])
        with pytest.raises(ValueError, match="not a PydanticAIHook"):
            hook.get_conn()

    def test_nested_chain_raises(self, registry, infer_model_stub):
        registry.add("primary", extra={"model": "openai:gpt-5.6-sol"})
        registry.add(
            "second",
            extra={"model": "anthropic:claude-opus-4-6", "fallback_conn_ids": ["third"]},
        )
        registry.add("third", extra={"model": "groq:llama-4"})

        hook = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=["second"])
        with pytest.raises(ValueError, match="not resolved recursively"):
            hook.get_conn()

    @pytest.mark.parametrize(
        "fallback_conn_ids",
        [
            pytest.param(["second", "second"], id="repeated-fallback"),
            pytest.param(["primary"], id="primary-repeated"),
        ],
    )
    def test_duplicate_conn_id_raises(self, registry, infer_model_stub, fallback_conn_ids):
        registry.add("primary", extra={"model": "openai:gpt-5.6-sol"})
        registry.add("second", extra={"model": "anthropic:claude-opus-4-6"})

        hook = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=fallback_conn_ids)
        with pytest.raises(ValueError, match="more than once"):
            hook.get_conn()

    @pytest.mark.parametrize(
        "fallback_conn_ids",
        [
            pytest.param("second,third", id="comma-separated-string"),
            pytest.param(["second", ""], id="empty-entry"),
            pytest.param(["second", 3], id="non-string-entry"),
        ],
    )
    def test_malformed_fallback_conn_ids_raises(self, registry, infer_model_stub, fallback_conn_ids):
        registry.add("primary", extra={"model": "openai:gpt-5.6-sol"})

        hook = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=fallback_conn_ids)
        with pytest.raises(ValueError, match="must be a list of non-empty connection IDs"):
            hook.get_conn()

    def test_fallback_without_a_model_names_the_connection(self, registry, infer_model_stub):
        """The error has to say which hop is misconfigured, not just that one is."""
        registry.add("primary", extra={"model": "openai:gpt-5.6-sol"})
        registry.add("second")

        hook = PydanticAIHook(llm_conn_id="primary", fallback_conn_ids=["second"])
        with pytest.raises(ValueError, match="No model specified for connection 'second'"):
            hook.get_conn()

    def test_test_connection_validates_the_whole_chain(self, registry, infer_model_stub):
        registry.add("primary", extra={"model": "openai:gpt-5.6-sol"})
        registry.add("second")

        success, message = PydanticAIHook(
            llm_conn_id="primary", fallback_conn_ids=["second"]
        ).test_connection()

        assert success is False
        assert "second" in message


class TestPydanticAIHookCreateAgent:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_defaults(self, mock_agent_cls, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(instructions="You are a helpful assistant.")

        mock_agent_cls.assert_called_once_with(
            mock_model,
            output_type=str,
            instructions="You are a helpful assistant.",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_with_params(self, mock_agent_cls, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                output_type=dict,
                instructions="Be helpful.",
                retries=3,
            )

        mock_agent_cls.assert_called_once_with(
            mock_model,
            output_type=dict,
            instructions="Be helpful.",
            retries=3,
        )

    def test_create_agent_without_instructions_or_spec_file_raises(self):
        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        with pytest.raises(ValueError, match="instructions is required"):
            hook.create_agent()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_calls_from_file(self, mock_agent_cls, mock_infer_model):
        """spec_file routes to Agent.from_file with the hook model when configured."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(spec_file="/path/to/agent.yaml")

        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            model=mock_model,
            output_type=str,
        )
        mock_agent_cls.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_uses_file_model_when_hook_model_not_configured(
        self, mock_agent_cls, mock_infer_model
    ):
        """spec_file model is used when neither model_id nor connection model is configured."""
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(spec_file="/path/to/agent.yaml")

        mock_infer_model.assert_not_called()
        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            output_type=str,
        )
        mock_agent_cls.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_path_object(self, mock_agent_cls, mock_infer_model):
        """spec_file accepts a pathlib.Path object."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        spec_path = Path("/path/to/agent.yaml")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(spec_file=spec_path)

        mock_agent_cls.from_file.assert_called_once_with(
            spec_path,
            model=mock_model,
            output_type=str,
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_merges_additional_instructions(
        self, mock_agent_cls, mock_infer_model
    ):
        """Explicit instructions are forwarded so pydantic-ai merges them with the spec."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                spec_file="/path/to/agent.yaml",
                instructions="Override instructions.",
            )

        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            model=mock_model,
            output_type=str,
            instructions="Override instructions.",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_custom_output_type(self, mock_agent_cls, mock_infer_model):
        """output_type is forwarded to Agent.from_file."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(output_type=dict, spec_file="/path/to/agent.yaml")

        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            model=mock_model,
            output_type=dict,
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_forwards_agent_kwargs(self, mock_agent_cls, mock_infer_model):
        """Extra agent_kwargs are forwarded to Agent.from_file."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                spec_file="/path/to/agent.yaml",
                retries=5,
                end_strategy="early",
            )

        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            model=mock_model,
            output_type=str,
            retries=5,
            end_strategy="early",
        )


class TestPydanticAIHookCreateAgentInstrumentation:
    """create_agent() wires OpenTelemetry instrumentation from observability."""

    @staticmethod
    def _hook() -> PydanticAIHook:
        return PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    def test_instrument_set_when_settings_returned(self, mock_settings):
        sentinel = MagicMock(name="InstrumentationSettings")
        mock_settings.return_value = sentinel
        hook = self._hook()
        with patch.object(hook, "get_conn", return_value=TestModel()):
            agent = hook.create_agent(instructions="hi")

        assert agent.instrument is sentinel

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    def test_no_instrument_when_settings_none(self, mock_settings):
        mock_settings.return_value = None
        hook = self._hook()
        with patch.object(hook, "get_conn", return_value=TestModel()):
            agent = hook.create_agent(instructions="hi")

        mock_settings.assert_called_once()
        assert agent.instrument is None

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_caller_instrument_short_circuits(self, mock_infer_model, mock_settings, mock_agent_cls):
        """A caller that passes its own ``instrument`` wins; we don't override it."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model
        hook = self._hook()
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            agent = hook.create_agent(instructions="hi", instrument=False)

        # ``instrument`` is not an Agent() constructor kwarg in pydantic-ai 2.x:
        # it must be stripped from the constructor call and applied through the
        # ``agent.instrument`` property instead, and the provider's own
        # auto-instrumentation must not override the caller's value.
        mock_agent_cls.assert_called_once_with(
            mock_model,
            output_type=str,
            instructions="hi",
        )
        assert agent.instrument is False
        mock_settings.assert_not_called()


class TestPydanticAIHookTestConnection:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_successful_connection(self, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
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
        # api_version should not appear if not in extra
        assert "api_version" not in result

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_uses_azure_endpoint(self, mock_infer_provider_class, mock_infer_model):
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
            hook.get_conn()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("azure")
        mock_provider_cls.assert_called_with(
            api_key="azure-key",
            azure_endpoint="https://myresource.openai.azure.com",
            api_version="2024-07-01-preview",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_conn_falls_back_to_env_auth_when_no_kwargs(self, mock_infer_model):
        """No host + no password → env-var auth path (empty _get_provider_kwargs)."""
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIAzureHook(llm_conn_id="azure_test")
        conn = Connection(
            conn_id="azure_test",
            conn_type="pydanticai-azure",
            extra=json.dumps({"model": "azure:gpt-4o"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

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
    def test_get_conn_falls_back_to_env_auth(self, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIBedrockHook(llm_conn_id="bedrock_test")
        conn = Connection(
            conn_id="bedrock_test",
            conn_type="pydanticai-bedrock",
            extra=json.dumps({"model": "bedrock:us.anthropic.claude-opus-4-5"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_infer_model.assert_called_once_with("bedrock:us.anthropic.claude-opus-4-5")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_uses_explicit_keys(self, mock_infer_provider_class, mock_infer_model):
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
            hook.get_conn()

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
                "aws_read_timeout": 60,  # int from JSON
                "aws_connect_timeout": 10.5,  # float already
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

    @pytest.mark.parametrize("vertexai_value", [True, False])
    def test_get_provider_kwargs_vertexai_flag_is_not_forwarded(self, vertexai_value):
        """The ``vertexai`` extra field must never reach the provider constructor.

        Neither ``GoogleProvider`` nor ``GoogleCloudProvider`` in current pydantic-ai
        accept a ``vertexai`` kwarg (pydantic/pydantic-ai#5336 hardcoded it inside
        ``GoogleCloudProvider`` instead). Forwarding it raises ``TypeError``, which the
        base hook's fallback then swallows by dropping every other kwarg -- silently
        re-resolving credentials from the environment. Regression test for that bug.
        """
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "google-vertex:gemini-2.0-flash",
                "project": "my-project",
                "location": "us-central1",
                "vertexai": vertexai_value,
            },
        )
        assert "vertexai" not in result
        # The other credential kwargs must still go through untouched.
        assert result["project"] == "my-project"
        assert result["location"] == "us-central1"

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
    def test_get_conn_falls_back_to_adc(self, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIVertexHook(llm_conn_id="vertex_test")
        conn = Connection(
            conn_id="vertex_test",
            conn_type="pydanticai-vertex",
            extra=json.dumps({"model": "google-vertex:gemini-2.0-flash"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_infer_model.assert_called_once_with("google-vertex:gemini-2.0-flash")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_uses_explicit_project(self, mock_infer_provider_class, mock_infer_model):
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
            hook.get_conn()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("google-vertex")
        mock_provider_cls.assert_called_with(project="my-project", location="europe-west4")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_vertexai_flag_does_not_trigger_typeerror_fallback(
        self, mock_infer_provider_class, mock_infer_provider, mock_infer_model
    ):
        """Setting ``vertexai`` must not push ``get_conn`` onto the ``except TypeError``
        fallback path, which would silently discard project/location/credentials.

        The stand-in below has the exact keyword-only signature of the real
        ``GoogleCloudProvider.__init__`` (verified against the installed pydantic-ai) so
        it raises ``TypeError`` on an unexpected ``vertexai`` kwarg exactly like the real
        class would -- the real class itself needs the optional ``google-genai``
        dependency, which isn't part of this provider's test environment.
        """

        class FakeGoogleCloudProvider:
            def __init__(
                self,
                *,
                api_key=None,
                credentials=None,
                project=None,
                location=None,
                client=None,
                http_client=None,
                base_url=None,
                retry_options=None,
            ):
                self.kwargs = {
                    "api_key": api_key,
                    "credentials": credentials,
                    "project": project,
                    "location": location,
                }

        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_provider_class.return_value = FakeGoogleCloudProvider

        hook = PydanticAIVertexHook(llm_conn_id="vertex_test")
        conn = Connection(
            conn_id="vertex_test",
            conn_type="pydanticai-vertex",
            extra=json.dumps(
                {
                    "model": "google-cloud:gemini-2.0-flash",
                    "project": "my-project",
                    "location": "us-central1",
                    "vertexai": True,
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        provider = factory("google-cloud")

        assert isinstance(provider, FakeGoogleCloudProvider)
        assert provider.kwargs["project"] == "my-project"
        assert provider.kwargs["location"] == "us-central1"
        # The TypeError fallback must never have been reached.
        mock_infer_provider.assert_not_called()
