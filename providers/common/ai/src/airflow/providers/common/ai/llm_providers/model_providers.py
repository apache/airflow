from typing import Any

from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.openai import OpenAIChatModel

from airflow.providers.common.ai.llm_providers.base import ModelProvider


class OpenAIModelProvider(ModelProvider):
    """
    Model provider for OpenAI models.
    """

    @property
    def provider_name(self) -> str:
        return "openai"

    def build_model(self, model_name: str, api_key: str, **kwargs) -> Any:
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider

        return OpenAIChatModel(model_name, provider=OpenAIProvider(api_key=api_key))


class AnthropicModelProvider(ModelProvider):
    def provider_name(self) -> str:
        return "anthropic"

    def build_model(self, model_name: str, api_key: str, **kwargs) -> Any:
        from pydantic_ai.providers.anthropic import AnthropicProvider

        model = AnthropicModel(model_name, provider=AnthropicProvider(api_key=api_key))
        return model


def _build_open_ai_based_model(model_name, provider) -> Any:
    return OpenAIChatModel(model_name, provider=provider)


class GithubModelProvider(ModelProvider):
    """
    Model provider for GitHub models.
    """

    @property
    def provider_name(self) -> str:
        return "github"

    def build_model(self, model_name: str, api_key: str, **kwargs) -> object:
        from pydantic_ai.providers.github import GitHubProvider

        return _build_open_ai_based_model(model_name, GitHubProvider(api_key=api_key))


class ModelProviderFactory:
    model_providers: dict[str, ModelProvider] = {}

    def __init__(self):
        self.model_providers: dict[str, ModelProvider] = {}
        self.register_default_model_providers()

    def register_default_model_providers(self):
        self.register_model_provider(OpenAIModelProvider())
        self.register_model_provider(AnthropicModelProvider())
        self.register_model_provider(GithubModelProvider())

    @classmethod
    def register_model_provider(cls, provider: ModelProvider):
        cls.model_providers[provider.provider_name] = provider

    def get_model_provider(self, provider_name: str) -> ModelProvider:
        if provider_name not in self.model_providers:
            raise ValueError(f"Model provider {provider_name} is not registered.")
        return self.model_providers[provider_name]
