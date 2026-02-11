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

from typing import TYPE_CHECKING

import pytest
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.models.openai import OpenAIChatModel

from airflow.providers.common.ai.llm_providers.base import ModelProvider
from airflow.providers.common.ai.llm_providers.model_providers import (
    AnthropicModelProvider,
    GithubModelProvider,
    GoogleModelProvider,
    ModelProviderFactory,
    OpenAIModelProvider,
)

if TYPE_CHECKING:
    from pydantic_ai.models import Model


class TestOpenAIModelProvider:
    open_ai_model_provider = OpenAIModelProvider()

    def test_provider_name(self):
        assert self.open_ai_model_provider.provider_name == "openai"

    def test_model_settings(self):
        model_settings = self.open_ai_model_provider.get_model_settings(
            {"max_tokens": 100, "temperature": 0.5}
        )

        assert isinstance(model_settings, dict)
        assert "max_tokens" in model_settings
        assert "temperature" in model_settings

    def test_model_settings_returns_none(self):
        model_settings = self.open_ai_model_provider.get_model_settings()
        assert model_settings is None

    def test_build_model(self):
        model = self.open_ai_model_provider.build_model("openai/gpt-5-mini", "api_key")
        assert isinstance(model, OpenAIChatModel)
        assert model.model_name == "openai/gpt-5-mini"


class TestAnthropicModelProvider:
    anthropic_model_provider = AnthropicModelProvider()

    def test_provider_name(self):
        assert self.anthropic_model_provider.provider_name == "anthropic"

    def test_model_settings(self):
        model_settings = self.anthropic_model_provider.get_model_settings(
            {"max_tokens": 100, "temperature": 0.5}
        )
        assert isinstance(model_settings, dict)
        assert "max_tokens" in model_settings
        assert "temperature" in model_settings

    def test_model_settings_returns_none(self):
        model_settings = self.anthropic_model_provider.get_model_settings()
        assert model_settings is None

    def test_build_model(self):
        model = self.anthropic_model_provider.build_model("anthropic/claude-3-haiku", "api_key")
        assert isinstance(model, AnthropicModel)
        assert model.model_name == "anthropic/claude-3-haiku"


class TestGoogleModelProvider:
    google_model_provider = GoogleModelProvider()

    def test_provider_name(self):
        assert self.google_model_provider.provider_name == "google"

    def test_model_settings(self):
        model_settings = self.google_model_provider.get_model_settings({"max_tokens": 100})
        assert isinstance(model_settings, dict)
        assert "max_tokens" in model_settings

    def test_model_settings_returns_none(self):
        model_settings = self.google_model_provider.get_model_settings()
        assert model_settings is None

    def test_build_model(self):
        model = self.google_model_provider.build_model("gemini-3-pro-preview", "api_key")
        assert isinstance(model, GoogleModel)
        assert model.model_name == "gemini-3-pro-preview"


class TestGithubModelProvider:
    github_model_provider = GithubModelProvider()

    def test_provider_name(self):
        assert self.github_model_provider.provider_name == "github"

    def test_model_settings_returns_none(self):
        model_settings = self.github_model_provider.get_model_settings()
        assert model_settings is None

    def test_build_model(self):
        model = self.github_model_provider.build_model(
            "openai/gpt-5-mini", "api_key", model_settings={"max_tokens": 100}
        )

        # GitHub provider uses OpenAIChatModel
        assert isinstance(model, OpenAIChatModel)
        assert model.model_name == "openai/gpt-5-mini"


class TestModelProviderFactory:
    model_provider_factory = ModelProviderFactory()

    def test_init(self):
        assert self.model_provider_factory.model_providers is not None
        assert self.model_provider_factory._initialized is True
        # Default providers registered count
        assert len(self.model_provider_factory.model_providers) > 3

    def test_register_model_provider(self):

        class CustomModelProvider(ModelProvider):
            @property
            def provider_name(self) -> str:
                return "custom"

            def build_model(self, model_name: str, api_key: str, **kwargs) -> Model: ...

        provider_factory = ModelProviderFactory()
        provider_factory.register_model_provider(CustomModelProvider())
        assert isinstance(provider_factory.get_model_provider("custom"), CustomModelProvider)

    def test_get_model_provider(self):
        assert self.model_provider_factory.get_model_provider("openai")

    def test_get_model_provider_raises_error(self):
        with pytest.raises(ValueError, match="Model provider invalid_provider is not registered."):
            self.model_provider_factory.get_model_provider("invalid_provider")

    @pytest.mark.parametrize(
        ("model_provider", "expected_provider_name", "expected_model_name"),
        [
            ("github:openai/gpt-3.5-turbo", "github", "openai/gpt-3.5-turbo"),
            ("google-gla:gemini-3-pro-preview", "google", "gemini-3-pro-preview"),
            ("anthropic:claude-sonnet-4-5", "anthropic", "claude-sonnet-4-5"),
        ],
    )
    def test_parse_model_provider_name(self, model_provider, expected_provider_name, expected_model_name):
        provider_name, model_name = self.model_provider_factory.parse_model_provider_name(model_provider)
        assert (provider_name, model_name) == (expected_provider_name, expected_model_name)
