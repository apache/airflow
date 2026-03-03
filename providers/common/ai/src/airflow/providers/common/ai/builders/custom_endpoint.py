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

from typing import TYPE_CHECKING, Any

from pydantic_ai.models import infer_model
from pydantic_ai.providers import Provider, infer_provider, infer_provider_class

if TYPE_CHECKING:
    from pydantic_ai.models import KnownModelName, Model

from airflow.providers.common.ai.builders.base import ProviderBuilder


class CustomEndpointBuilder(ProviderBuilder):
    """
    Builds a pydantic-ai Model with explicitly overridden credentials or endpoints.

    Used for general connections (OpenAI, Gemini, Groq, Ollama) where password or host is set.
    """

    def supports(self, extra: dict[str, Any], api_key: str | None, base_url: str | None) -> bool:
        """Return True if api_key or base_url is explicitly provided."""
        return bool(api_key or base_url)

    def build(
        self,
        model_name: str | KnownModelName,
        extra: dict[str, Any],
        api_key: str | None,
        base_url: str | None,
    ) -> Model:
        return infer_model(model_name, provider_factory=self._make_provider_factory(api_key, base_url))

    @staticmethod
    def _make_provider_factory(api_key: str | None, base_url: str | None):
        """
        Return a provider factory closure for non-Azure custom endpoints.

        Falls back to default provider resolution when the provider's
        constructor does not accept `api_key` / `base_url`
        """

        def _factory(provider_name: str) -> Provider[Any]:
            provider_cls = infer_provider_class(provider_name)
            kwargs: dict[str, Any] = {}
            if api_key:
                kwargs["api_key"] = api_key
            if base_url:
                kwargs["base_url"] = base_url
            try:
                return provider_cls(**kwargs)
            except TypeError:
                return infer_provider(provider_name)

        return _factory


class DefaultBuilder(ProviderBuilder):
    """
    Fallback builder.

    Delegates to pydantic-ai's infer_model with environment variables
    (e.g., standard OPENAI_API_KEY, AWS_PROFILE, etc.) without explicitly passing kwargs.
    """

    def supports(self, extra: dict[str, Any], api_key: str | None, base_url: str | None) -> bool:
        return True

    def build(
        self,
        model_name: str | KnownModelName,
        extra: dict[str, Any],
        api_key: str | None,
        base_url: str | None,
    ) -> Model:
        return infer_model(model_name)
