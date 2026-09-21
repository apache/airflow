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
from airflow.providers.common.ai.batch.anthropic import AnthropicBatchAdapter
from airflow.providers.common.ai.batch.base import BatchAdapter
from airflow.providers.common.ai.batch.openai import OpenAIBatchAdapter
from airflow.providers.common.ai.exceptions import (
    BatchProviderNotYetSupportedError,
    UnsupportedBatchProviderError,
)
from airflow.sdk import Connection


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
        [("openai:gpt-5", OpenAIBatchAdapter), ("anthropic:claude-sonnet-4-5", AnthropicBatchAdapter)],
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
    def test_build_adapter_from_connection_maps_password_and_host(self):
        pytest.importorskip("openai")
        conn = Connection(
            conn_id="c", conn_type="pydanticai", password="sk-test", host="https://gateway.example"
        )

        adapter = dispatch.build_adapter_from_connection("openai", conn)

        assert isinstance(adapter, OpenAIBatchAdapter)
        assert adapter._client.api_key == "sk-test"
        assert str(adapter._client.base_url).rstrip("/") == "https://gateway.example"
        adapter.close()

    def test_build_adapter_fetches_the_connection_by_id(self):
        pytest.importorskip("anthropic")
        conn = Connection(conn_id="c", conn_type="pydanticai", password="sk-ant", host=None)
        with mock.patch.object(
            dispatch.BaseHook, "get_connection", autospec=True, return_value=conn
        ) as get_conn:
            adapter = dispatch.build_adapter("anthropic", llm_conn_id="my_anthropic")

        get_conn.assert_called_once_with("my_anthropic")
        assert isinstance(adapter, AnthropicBatchAdapter)
        assert adapter._client.api_key == "sk-ant"
        adapter.close()


class TestNoSdkImportAtDispatchTime:
    def test_importing_dispatch_and_trigger_does_not_import_a_provider_sdk(self):
        code = (
            "import sys\n"
            "import airflow.providers.common.ai.batch.dispatch\n"
            "import airflow.providers.common.ai.triggers.llm_batch\n"
            "import airflow.providers.common.ai.operators.llm_batch\n"
            "loaded = [m for m in ('openai', 'anthropic') if m in sys.modules]\n"
            "print('LOADED_SDKS=' + ','.join(loaded))\n"
        )
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)

        marker = [line for line in result.stdout.splitlines() if line.startswith("LOADED_SDKS=")]
        assert marker == ["LOADED_SDKS="]
