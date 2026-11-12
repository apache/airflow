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

import subprocess
import sys
from importlib.metadata import EntryPoint
from unittest import mock

import pytest

from airflow.providers.common.ai.batch import dispatch
from airflow.providers.common.ai.batch.base import BatchAdapter
from airflow.providers.common.ai.batch.pydantic_ai_backend import (
    AnthropicBatchAdapter,
    GoogleBatchAdapter,
    OpenAIBatchAdapter,
)
from airflow.providers.common.ai.exceptions import (
    BatchProviderNotYetSupportedError,
    UnsupportedBatchProviderError,
)
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.compat.sdk import Connection

from unit.common.ai.batch.legacy_adapter import LegacyAdapter


class TestSplitModelId:
    @pytest.mark.parametrize(
        ("model_id", "expected"),
        [
            ("openai:gpt-5", ("openai", "gpt-5")),
            ("anthropic:claude-sonnet-4-5", ("anthropic", "claude-sonnet-4-5")),
            (" openai:gpt-5 ", ("openai", "gpt-5")),
            ("openai:org/model:tag", ("openai", "org/model:tag")),
        ],
    )
    def test_splits_prefix_and_model(self, model_id, expected):
        assert dispatch.split_model_id(model_id) == expected

    @pytest.mark.parametrize("model_id", [None, "", "gpt-5", "openai:", ":gpt-5", ":"])
    def test_rejects_missing_or_empty_halves(self, model_id):
        with pytest.raises(
            UnsupportedBatchProviderError, match="provider prefix and a model name|<provider>:<model>"
        ):
            dispatch.split_model_id(model_id)


class TestGetAdapterClass:
    @pytest.mark.parametrize(
        ("model_id", "expected_cls"),
        [
            ("openai:gpt-5", OpenAIBatchAdapter),
            ("anthropic:claude-sonnet-4-5", AnthropicBatchAdapter),
            ("google:gemini-2.5-flash", GoogleBatchAdapter),
        ],
    )
    def test_built_in_adapters_resolve_for_pydanticai_connections(self, model_id, expected_cls):
        adapter_cls = dispatch.get_adapter_class("pydanticai", model_id)

        assert adapter_cls is expected_cls
        assert issubclass(adapter_cls, BatchAdapter)
        assert adapter_cls.name == model_id.partition(":")[0]

    @pytest.mark.parametrize(
        "conn_type", ["pydanticai_azure", "pydanticai_bedrock", "pydanticai_vertex", "openai"]
    )
    def test_connection_type_the_adapter_cannot_authenticate_is_rejected(self, conn_type):
        with pytest.raises(
            BatchProviderNotYetSupportedError, match=f"does not support connection type '{conn_type}'"
        ):
            dispatch.get_adapter_class(conn_type, "openai:gpt-5")

    def test_unknown_prefix_is_rejected(self):
        with pytest.raises(UnsupportedBatchProviderError, match="'groq' has no batch adapter"):
            dispatch.get_adapter_class("pydanticai", "groq:llama")

    def test_openai_chat_prefix_selects_the_openai_adapter(self):
        """``openai-chat`` is served by the OpenAI adapter, which runs the batch on the Responses API."""
        assert dispatch.get_adapter_class("pydanticai", "openai-chat:gpt-5") is OpenAIBatchAdapter

    @pytest.mark.parametrize("model_id", ["google-cloud:gemini-2.5-flash", "google-vertex:gemini-2.5-flash"])
    def test_vertex_is_refused_with_the_fix(self, model_id):
        with pytest.raises(BatchProviderNotYetSupportedError, match="google:<model>"):
            dispatch.get_adapter_class("pydanticai", model_id)

    def test_resolve_adapter_name_returns_the_prefix(self):
        assert dispatch.resolve_adapter_name("pydanticai", "openai:gpt-5") == "openai"


class _RegisteredAdapter(OpenAIBatchAdapter):
    name = "acme"
    conn_types = frozenset({"pydanticai", "acme"})


class TestRegistration:
    @pytest.fixture(autouse=True)
    def _clean_registry(self):
        saved = dict(dispatch._REGISTERED_ADAPTERS)
        yield
        dispatch._REGISTERED_ADAPTERS.clear()
        dispatch._REGISTERED_ADAPTERS.update(saved)

    def test_register_adapter_makes_a_new_prefix_dispatchable(self):
        dispatch.register_adapter(_RegisteredAdapter)

        assert dispatch.get_adapter_class("acme", "acme:model-1") is _RegisteredAdapter

    def test_entry_point_adapter_is_loaded_by_prefix(self):
        ep = mock.Mock(spec=EntryPoint)
        ep.name = "acme"
        ep.load.return_value = _RegisteredAdapter
        with mock.patch.object(dispatch, "entry_points", autospec=True, return_value=[ep]) as entry_points:
            assert dispatch.import_adapter_class("acme") is _RegisteredAdapter

        entry_points.assert_called_once_with(group=dispatch.ENTRY_POINT_GROUP)


class TestBuildAdapter:
    def test_openai_adapter_is_built_from_the_connection_hook(self):
        conn = Connection(conn_id="my_openai", conn_type="pydanticai", password="sk-test")
        hook = mock.create_autospec(PydanticAIHook, instance=True)
        hook._get_fallback_conn_ids.return_value = []
        with mock.patch.object(PydanticAIHook, "get_hook", autospec=True, return_value=hook) as get_hook:
            adapter = dispatch.build_adapter_from_connection(
                "openai", conn, model_id="openai:gpt-5-mini", batch_handle='{"id": "b"}'
            )

        get_hook.assert_called_once_with("my_openai", {"model_id": "openai:gpt-5-mini"})
        assert isinstance(adapter, OpenAIBatchAdapter)
        assert adapter._hook is hook
        assert adapter._handle == '{"id": "b"}'

    def test_build_adapter_fetches_the_connection_by_id(self):
        conn = Connection(conn_id="c", conn_type="pydanticai", password="sk-test", host=None)
        hook = mock.create_autospec(PydanticAIHook, instance=True)
        hook._get_fallback_conn_ids.return_value = []
        with (
            mock.patch.object(
                dispatch.BaseHook, "get_connection", autospec=True, return_value=conn
            ) as get_conn,
            mock.patch.object(PydanticAIHook, "get_hook", autospec=True, return_value=hook),
        ):
            adapter = dispatch.build_adapter("openai", llm_conn_id="my_openai", model_id="openai:gpt-5")

        get_conn.assert_called_once_with("my_openai")
        assert isinstance(adapter, OpenAIBatchAdapter)

    def test_pydantic_ai_adapter_is_built_from_the_connection_hook(self):
        conn = Connection(conn_id="my_anthropic", conn_type="pydanticai", password="sk-ant")
        hook = mock.create_autospec(PydanticAIHook, instance=True)
        hook._get_fallback_conn_ids.return_value = []
        with (
            mock.patch.object(dispatch.BaseHook, "get_connection", autospec=True, return_value=conn),
            mock.patch.object(PydanticAIHook, "get_hook", autospec=True, return_value=hook) as get_hook,
        ):
            adapter = dispatch.build_adapter(
                "anthropic",
                llm_conn_id="my_anthropic",
                model_id="anthropic:claude-sonnet-4-5",
                batch_handle='{"id": "b"}',
            )

        get_hook.assert_called_once_with("my_anthropic", {"model_id": "anthropic:claude-sonnet-4-5"})
        assert isinstance(adapter, AnthropicBatchAdapter)
        assert adapter._hook is hook
        assert adapter._handle == '{"id": "b"}'


class TestNoSdkImportAtDispatchTime:
    def test_importing_dispatch_and_trigger_does_not_import_a_provider_sdk(self):
        code = (
            "import sys\n"
            "import airflow.providers.common.ai.batch.dispatch\n"
            "import airflow.providers.common.ai.triggers.llm_batch\n"
            "import airflow.providers.common.ai.operators.llm_batch\n"
            "import airflow.providers.common.ai.batch.pydantic_ai_backend\n"
            "loaded = [m for m in ('openai', 'anthropic', 'google.genai') if m in sys.modules]\n"
            "print('LOADED_SDKS=' + ','.join(loaded))\n"
        )
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)

        marker = [line for line in result.stdout.splitlines() if line.startswith("LOADED_SDKS=")]
        assert marker == ["LOADED_SDKS="]


class TestThirdPartyAdaptersStillDispatch:
    @pytest.fixture(autouse=True)
    def _clean_registry(self):
        saved = dict(dispatch._REGISTERED_ADAPTERS)
        yield
        dispatch._REGISTERED_ADAPTERS.clear()
        dispatch._REGISTERED_ADAPTERS.update(saved)

    def test_an_entry_point_adapter_is_built_the_same_way(self):
        ep = mock.Mock(spec=EntryPoint)
        ep.name = "acme"
        ep.load.return_value = LegacyAdapter
        conn = Connection(conn_id="c", conn_type="pydanticai", password="sk-acme")
        with mock.patch.object(dispatch, "entry_points", autospec=True, return_value=[ep]):
            adapter = dispatch.build_adapter_from_connection("acme", conn)

        assert isinstance(adapter, LegacyAdapter)

    def test_an_entry_point_adapter_overrides_a_refused_prefix(self):
        class _VertexAdapter(LegacyAdapter):
            name = "google-vertex"

        ep = mock.Mock(spec=EntryPoint)
        ep.name = "google-vertex"
        ep.load.return_value = _VertexAdapter
        with mock.patch.object(dispatch, "entry_points", autospec=True, return_value=[ep]):
            assert dispatch.get_adapter_class("pydanticai", "google-vertex:gemini") is _VertexAdapter

    def test_a_registered_adapter_overrides_a_refused_prefix(self):
        class _VertexAdapter(LegacyAdapter):
            name = "google-cloud"

        dispatch.register_adapter(_VertexAdapter)

        assert dispatch.get_adapter_class("pydanticai", "google-cloud:gemini") is _VertexAdapter
