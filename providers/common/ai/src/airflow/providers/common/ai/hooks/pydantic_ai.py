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

import json
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic_ai.models import infer_model
from pydantic_ai.providers import infer_provider_class

from airflow.providers.common.ai.exceptions import ModelCreationError
from airflow.sdk import BaseHook, Connection

if TYPE_CHECKING:
    from pydantic_ai.models import Model


class PydanticAIHook(BaseHook):
    """Hook for Pydantic AI."""

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
        """Get the pydantic AI connection."""
        if self.connection is None:
            self.connection = self.get_connection(self.pydantic_ai_conn_id)
        return self.connection

    def get_provider_model_name_from_conn(self):
        """Get the provider model name from the connection."""
        return self.get_conn().extra_dejson.get("provider_model")

    @cached_property
    def _api_key_from_conn(self):
        """Get the API key from the connection."""
        return self.get_conn().password

    def _provider_factory(self, provider_name: str):
        """Get a provider class from the pydantic module."""
        provider_cls = infer_provider_class(provider_name)
        return provider_cls(api_key=self._api_key_from_conn)

    @property
    def _model_settings(self) -> dict[str, Any] | None:
        """Get model settings from the connection."""
        return self.get_conn().extra_dejson.get("model_settings")

    def get_model(self) -> Model:
        """Build provider model."""
        try:
            provider_model_name = self.provider_model or self.get_provider_model_name_from_conn()
            if not provider_model_name:
                raise ValueError("No provider model name provided")

            model = infer_model(provider_model_name, provider_factory=self._provider_factory)

            model._settings = self._model_settings
            return model
        except Exception as e:
            raise ModelCreationError(f"Error building model: {e}")
