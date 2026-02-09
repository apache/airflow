from functools import cached_property
from typing import Any

from airflow.providers.common.ai.llm_providers.base import ModelProvider
from airflow.providers.common.ai.llm_providers.model_providers import ModelProviderFactory
from airflow.sdk import BaseHook
from pydantic_ai import Agent


class PydanticAIHook(BaseHook):
    _model_provider_factory = None

    conn_name_attr = "pydantic_ai_conn_id"
    default_conn_name = "pydantic_ai_default"
    conn_type = "pydantic_ai"
    hook_name = "pydantic_ai"

    def __init__(
        self, pydantic_ai_conn_id: str = default_conn_name, model_name: str | None = None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.model_name = model_name
        self.pydantic_ai_conn_id = pydantic_ai_conn_id

    def get_conn(self):
        pass

    def get_model_name_from_conn(self):
        pass

    @cached_property
    def get_api_key_from_conn(self):
        return self.get_conn().extra_dejson.get("api_key")

    @classmethod
    def get_provider_model_factory(cls):
        if cls._model_provider_factory is None:
            cls._model_provider_factory = ModelProviderFactory()
        return cls._model_provider_factory

    @classmethod
    def register_model_provider(cls, provider: ModelProvider):
        cls.get_provider_model_factory().register_model_provider(provider)

    def get_model(self, **kwargs):
        model_name = self.model_name or self.get_model_name_from_conn()
        return (
            self.get_provider_model_factory()
            .get_model_provider(model_name)
            .build_model(model_name, api_key=self.get_api_key_from_conn, **kwargs)
        )

    @staticmethod
    def _get_db_hook(conn_id: str):
        """Get the given connection's database hook."""
        connection = BaseHook.get_connection(conn_id)
        return connection.get_hook()
