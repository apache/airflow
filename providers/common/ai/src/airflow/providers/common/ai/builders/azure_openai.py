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

if TYPE_CHECKING:
    from pydantic_ai.models import KnownModelName, Model

from airflow.providers.common.ai.builders.base import ProviderBuilder


class AzureOpenAIBuilder(ProviderBuilder):
    """Builds a pydantic-ai Model backed by an AsyncAzureOpenAI client."""

    def supports(self, extra: dict[str, Any], api_key: str | None, base_url: str | None) -> bool:
        """Return True when the connection extra contains 'api_version' for Azure."""
        return bool(extra.get("api_version"))

    def build(
        self,
        model_name: str | KnownModelName,
        extra: dict[str, Any],
        api_key: str | None,
        base_url: str | None,
    ) -> Model:
        try:
            from openai import AsyncAzureOpenAI
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider
        except ImportError as exc:
            raise ImportError(
                "The 'openai' and 'pydantic-ai[openai]' packages are required for Azure OpenAI connections. "
                "Install them with: pip install 'pydantic-ai[openai]'"
            ) from exc

        api_version: str | None = extra.get("api_version")
        if not api_version:
            raise ValueError(
                "Connection extra must contain 'api_version' for Azure OpenAI. "
                'Example: {"api_version": "2024-07-01-preview"}'
            )

        if not base_url:
            raise ValueError(
                "Connection 'host' must be set to the Azure endpoint. "
                "Example: https://<resource>.openai.azure.com"
            )

        client_kwargs: dict[str, Any] = {
            "api_version": api_version,
            "azure_endpoint": base_url,
        }
        if api_key:
            client_kwargs["api_key"] = api_key

        azure_deployment: str | None = extra.get("azure_deployment")
        if azure_deployment:
            client_kwargs["azure_deployment"] = azure_deployment

        azure_ad_token: str | None = extra.get("azure_ad_token")
        if azure_ad_token:
            client_kwargs["azure_ad_token"] = azure_ad_token

        azure_ad_token_provider_path: str | None = extra.get("azure_ad_token_provider")
        if azure_ad_token_provider_path:
            client_kwargs["azure_ad_token_provider"] = self._import_callable(azure_ad_token_provider_path)

        azure_client = AsyncAzureOpenAI(**client_kwargs)

        # Strip provider prefix if present ("openai:gpt-5.2" → "gpt-5.2")
        slug = model_name.split(":", 1)[-1] if ":" in model_name else model_name
        return OpenAIChatModel(slug, provider=OpenAIProvider(openai_client=azure_client))

    @staticmethod
    def _import_callable(dotted_path: str) -> Any:
        module_path, _, attr = dotted_path.rpartition(".")
        if not module_path:
            raise ValueError(
                f"'azure_ad_token_provider' must be a fully-qualified dotted path, got: {dotted_path!r}"
            )
        import importlib

        module = importlib.import_module(module_path)
        return getattr(module, attr)
