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

from airflow.providers.common.ai.llm_providers.base import ModelProvider

if TYPE_CHECKING:
    from pydantic_ai import ModelSettings
    from pydantic_ai.models import Model


class OpenAIModelProvider(ModelProvider):
    """Model provider for OpenAI models."""

    @property
    def provider_name(self) -> str:
        """Return the name of the provider."""
        return "openai"

    def get_model_settings(self, model_settings: dict[str, Any] | None = None) -> ModelSettings | None:
        """Get model settings for OpenAI models."""
        from pydantic_ai.models.openai import OpenAIChatModelSettings

        if model_settings is None:
            return None

        self.log.info("Model settings %s initialized for %s", model_settings, self.provider_name)

        return OpenAIChatModelSettings(**model_settings)

    def build_model(self, model_name: str, api_key: str, **kwargs) -> Model:
        """Build and returns OpenAIChatModel."""
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider

        model_settings = self.get_model_settings(kwargs.get("model_settings"))

        return OpenAIChatModel(model_name, provider=OpenAIProvider(api_key=api_key), settings=model_settings)


class AnthropicModelProvider(ModelProvider):
    """Model provider for Anthropic models."""

    @property
    def provider_name(self) -> str:
        """Return the name of the provider."""
        return "anthropic"

    def get_model_settings(self, model_settings: dict[str, Any] | None = None) -> ModelSettings | None:
        """Get model settings for Anthropic models."""
        from pydantic_ai.models.anthropic import AnthropicModelSettings

        if model_settings is None:
            return None

        self.log.info("Model settings %s initialized for %s", model_settings, self.provider_name)

        return AnthropicModelSettings(**model_settings)

    def build_model(self, model_name: str, api_key: str, **kwargs) -> Model:
        """Build and returns AnthropicModel."""
        from pydantic_ai.models.anthropic import AnthropicModel
        from pydantic_ai.providers.anthropic import AnthropicProvider

        model_settings = self.get_model_settings(kwargs.get("model_settings"))

        model = AnthropicModel(
            model_name, provider=AnthropicProvider(api_key=api_key), settings=model_settings
        )
        self.log.info("Model %s initialized for provider %s", model_name, self.provider_name)
        return model


class GoogleModelProvider(ModelProvider):
    """Model provider for Google models."""

    @property
    def provider_name(self) -> str:
        """Return the name of the provider."""
        return "google"

    def get_model_settings(self, model_settings: dict[str, Any] | None = None) -> ModelSettings | None:
        """Get model settings for Google models."""
        from pydantic_ai.models.google import GoogleModelSettings

        if model_settings is None:
            return None

        self.log.info("Model settings %s initialized for %s", model_settings, self.provider_name)
        return GoogleModelSettings(**model_settings)

    def build_model(self, model_name: str, api_key: str, **kwargs) -> Model:
        """Build and returns GoogleModel."""
        from pydantic_ai.models.google import GoogleModel
        from pydantic_ai.providers.google import GoogleProvider

        model_settings = self.get_model_settings(kwargs.get("model_settings"))
        model = GoogleModel(model_name, provider=GoogleProvider(api_key=api_key), settings=model_settings)

        self.log.info("Model %s initialized for provider %s", model_name, self.provider_name)
        return model


def _build_open_ai_based_model(model_name, provider, **kwargs) -> Model:
    """
    Create a model instance based on the provided model name and parameters.

    There are models that are compatible with OpenAI compatible modes, https://ai.pydantic.dev/models/openai/#openai-compatible-models
    This function builds those models.
    """
    from pydantic_ai.models.openai import OpenAIChatModel, OpenAIChatModelSettings

    settings = kwargs.get("model_settings")
    if settings:
        settings = OpenAIChatModelSettings(**settings)

    return OpenAIChatModel(model_name, provider=provider, settings=settings)


class GithubModelProvider(ModelProvider):
    """Model provider for GitHub models."""

    @property
    def provider_name(self) -> str:
        """Return the name of the provider."""
        return "github"

    def build_model(self, model_name: str, api_key: str, **kwargs) -> Model:
        """Build and returns GitHubModel."""
        from pydantic_ai.providers.github import GitHubProvider

        model = _build_open_ai_based_model(model_name, GitHubProvider(api_key=api_key), **kwargs)

        self.log.info("Model %s initialized for provider %s", model_name, self.provider_name)
        return model


class ModelProviderFactory:
    """Factory class for model providers."""

    model_providers: dict[str, ModelProvider] = {}
    _initialized: bool = False

    def __init__(self):
        self._initialize_default_model_providers()

    @classmethod
    def _initialize_default_model_providers(cls):
        if cls._initialized:
            return
        defaults = [
            AnthropicModelProvider(),
            GithubModelProvider(),
            GoogleModelProvider(),
            OpenAIModelProvider(),
        ]
        for provider in defaults:
            cls.register_model_provider(provider)
        cls._initialized = True

    @classmethod
    def register_model_provider(cls, provider: ModelProvider) -> None:
        """Register a model provider."""
        cls.model_providers[provider.provider_name] = provider

    def get_model_provider(self, provider_name: str) -> ModelProvider:
        """
        Get the model provider for the given provider name.

        eg: provider names are: openai or google or claude
        """
        if provider_name and provider_name not in self.model_providers:
            raise ValueError(f"Model provider {provider_name} is not registered.")

        return self.model_providers[provider_name]

    @staticmethod
    def parse_model_provider_name(provider_model_name: str) -> str | tuple[str, str]:
        """Return the provider name and model name from the model name."""
        if ":" not in provider_model_name:
            raise ValueError(
                f"Invalid model name {provider_model_name}. Model name must be in the format provider:model_name, e.g. github:openai/gpt-4o-mini"
            )

        model_parts = provider_model_name.split(":", 1)
        provider_name = model_parts[0]
        model_name = model_parts[1]

        if provider_name.startswith("google"):
            return "google", model_name

        if provider_name.startswith("claude"):
            return "anthropic", model_name

        return provider_name, model_name
