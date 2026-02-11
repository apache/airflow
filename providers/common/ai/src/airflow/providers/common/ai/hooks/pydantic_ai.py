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

import json
from functools import cached_property
from typing import Any

from pydantic_ai.models import Model

from airflow.providers.common.ai.exceptions import ModelCreationError
from airflow.providers.common.ai.llm_providers.base import ModelProvider
from airflow.providers.common.ai.llm_providers.model_providers import ModelProviderFactory
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sdk import BaseHook, Connection


class PydanticAIHook(BaseHook):
    _model_provider_factory: ModelProviderFactory | None = None

    conn_name_attr = "pydantic_ai_conn_id"
    default_conn_name = "pydantic_ai_default"
    conn_type = "pydantic_ai"
    hook_name = "PydanticAI"

    def __init__(
        self, pydantic_ai_conn_id: str = default_conn_name, provider_model: str | None = None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.provider_model = provider_model
        self.pydantic_ai_conn_id = pydantic_ai_conn_id
        self._api_key: str | None = None
        self.connection: Connection | None = None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "password": "API Key",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "provider_model": "",
                        "model_settings": {},
                    }
                )
            },
        }

    def get_conn(self) -> Connection:
        if self.connection is None:
            self.connection = self.get_connection(self.pydantic_ai_conn_id)
        return self.connection

    def get_provider_model_name_from_conn(self):
        return self.get_conn().extra_dejson.get("provider_model")

    @cached_property
    def get_api_key_from_conn(self):
        return self.get_conn().password

    @classmethod
    def get_provider_model_factory(cls):
        if cls._model_provider_factory is None:
            cls._model_provider_factory = ModelProviderFactory()
        return cls._model_provider_factory

    @classmethod
    def register_model_provider(cls, provider: ModelProvider):
        cls.get_provider_model_factory().register_model_provider(provider)

    def get_model(self, **kwargs) -> Model:
        try:
            provider_model_name = self.provider_model or self.get_provider_model_name_from_conn()
            if not provider_model_name:
                raise ValueError("No provider model name provided")
            provider_name, model_name = self.get_provider_model_factory().parse_model_provider_name(provider_model_name)

            settings = self.get_conn().extra_dejson.get("model_settings")
            if settings:
                kwargs["model_settings"] = settings
            return (
                self._model_provider_factory
                .get_model_provider(provider_name)
                .build_model(model_name, api_key=self.get_api_key_from_conn, **kwargs)
            )
        except Exception as e:
            raise ModelCreationError(f"Error building model: {e}")

    @staticmethod
    def _get_db_api_hook(conn_id: str) -> DbApiHook:
        """Get the given connection's database hook."""
        connection = BaseHook.get_connection(conn_id)
        return connection.get_hook()
