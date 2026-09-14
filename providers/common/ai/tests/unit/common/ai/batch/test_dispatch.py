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

import pytest

from airflow.providers.common.ai.batch.dispatch import (
    _ADAPTER_MODULES,
    _NOT_YET_SUPPORTED,
    resolve_adapter_name,
)
from airflow.providers.common.ai.exceptions import (
    BatchProviderNotYetSupportedError,
    UnsupportedBatchProviderError,
)


class TestResolveAdapterNameSupportedRows:
    """The two ``✅ 支援`` rows of the §3 dispatch table."""

    def test_openai_prefix_resolves_to_openai(self):
        assert resolve_adapter_name("pydanticai", "openai:gpt-5") == "openai"

    def test_anthropic_prefix_resolves_to_anthropic(self):
        assert resolve_adapter_name("pydanticai", "anthropic:claude-sonnet-5") == "anthropic"


class TestResolveAdapterNameUnsupportedRows:
    """The ``❌ 拒絕`` rows: recognized shape, but no batch API -- ``UnsupportedBatchProviderError``."""

    @pytest.mark.parametrize("model_id", ["groq:llama-3", "mistral:mistral-large", "ollama:llama3"])
    def test_provider_prefix_with_no_batch_api_is_rejected(self, model_id):
        with pytest.raises(UnsupportedBatchProviderError):
            resolve_adapter_name("pydanticai", model_id)

    @pytest.mark.parametrize("model_id", [None, "gpt-5", ""])
    def test_missing_prefix_is_rejected(self, model_id):
        with pytest.raises(UnsupportedBatchProviderError, match="provider>:<model>"):
            resolve_adapter_name("pydanticai", model_id)

    def test_unrecognized_conn_type_is_rejected(self):
        with pytest.raises(UnsupportedBatchProviderError, match="does not recognize connection type"):
            resolve_adapter_name("http_default", "openai:gpt-5")


class TestResolveAdapterNameNotYetSupportedRows:
    """
    The three explicit ``已知、尚未支援`` rows.

    Must raise ``BatchProviderNotYetSupportedError`` specifically, never the
    catch-all ``UnsupportedBatchProviderError`` -- that distinction is the
    entire point of this branch existing (§3), and it is easy to regress back
    into a single catch-all during a refactor.
    """

    @pytest.mark.parametrize(
        ("conn_type", "expected_snippet"),
        [
            ("pydanticai_azure", "Azure OpenAI batch is not supported yet"),
            ("pydanticai_bedrock", "Amazon Bedrock batch is not supported yet"),
            ("pydanticai_vertex", "Vertex AI batch is not supported yet"),
        ],
    )
    def test_raises_not_yet_supported_with_specific_message(self, conn_type, expected_snippet):
        with pytest.raises(BatchProviderNotYetSupportedError, match=expected_snippet):
            resolve_adapter_name(conn_type, "irrelevant:model")

    @pytest.mark.parametrize("conn_type", ["pydanticai_azure", "pydanticai_bedrock", "pydanticai_vertex"])
    def test_not_yet_supported_is_not_the_catch_all_type(self, conn_type):
        """A not-yet-supported conn_type must never surface as the generic unsupported-provider error."""
        with pytest.raises(BatchProviderNotYetSupportedError) as exc_info:
            resolve_adapter_name(conn_type, "irrelevant:model")
        assert not isinstance(exc_info.value, UnsupportedBatchProviderError)

    def test_not_yet_supported_branch_ignores_model_id(self):
        """§3: these three conn_types are rejected regardless of model_id ("任意")."""
        with pytest.raises(BatchProviderNotYetSupportedError):
            resolve_adapter_name("pydanticai_azure", None)


class TestAdapterModuleRegistry:
    """
    Structural checks on the lazy-import registry, without importing the adapter modules themselves.

    The concrete ``openai``/``anthropic`` adapter modules are built in a later
    step; this only proves dispatch.py's own routing table is correctly
    shaped, so dispatch can be fully tested before those modules exist.
    """

    def test_registry_covers_both_supported_adapters(self):
        assert set(_ADAPTER_MODULES) == {"openai", "anthropic"}

    def test_registry_entries_point_at_the_expected_module_and_class_names(self):
        assert _ADAPTER_MODULES["openai"] == (
            "airflow.providers.common.ai.batch.openai",
            "OpenAIBatchAdapter",
        )
        assert _ADAPTER_MODULES["anthropic"] == (
            "airflow.providers.common.ai.batch.anthropic",
            "AnthropicBatchAdapter",
        )

    def test_not_yet_supported_table_covers_exactly_the_three_known_conn_types(self):
        assert set(_NOT_YET_SUPPORTED) == {"pydanticai_azure", "pydanticai_bedrock", "pydanticai_vertex"}


class TestDispatchModuleHasNoSdkImport:
    def test_module_source_does_not_import_a_provider_sdk(self):
        """Static guard against a top-level ``import openai``/``import anthropic`` creeping back in."""
        import inspect

        from airflow.providers.common.ai.batch import dispatch

        source = inspect.getsource(dispatch)
        assert "import openai" not in source
        assert "import anthropic" not in source
