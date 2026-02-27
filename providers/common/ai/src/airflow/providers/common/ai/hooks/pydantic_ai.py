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

from typing import TYPE_CHECKING, Any, TypeVar, overload

from pydantic_ai import Agent
from pydantic_ai.models import Model, infer_model
from pydantic_ai.providers import Provider, infer_provider, infer_provider_class

from airflow.providers.common.compat.sdk import BaseHook

OutputT = TypeVar("OutputT")

if TYPE_CHECKING:
    from pydantic_ai.models import KnownModelName


class PydanticAIHook(BaseHook):
    """
    Hook for LLM access via pydantic-ai.

    Manages connection credentials and model creation. Uses pydantic-ai's
    model inference to support any provider (OpenAI, Anthropic, Google,
    Bedrock, Ollama, vLLM, etc.).

    Connection fields:
        - **password**: API key (OpenAI, Anthropic, Groq, Mistral, etc.)
        - **host**: Base URL (optional — for custom endpoints like Ollama, vLLM, Azure)
        - **extra** JSON: ``{"model": "openai:gpt-5.3"}``

    Cloud providers (Bedrock, Vertex) that use native auth chains should leave
    password empty and configure environment-based auth (``AWS_PROFILE``,
    ``GOOGLE_APPLICATION_CREDENTIALS``).

    :param llm_conn_id: Airflow connection ID for the LLM provider.
    :param model_id: Model identifier in ``provider:model`` format (e.g. ``"openai:gpt-5.3"``).
        Overrides the model stored in the connection's extra field.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "pydantic_ai_default"
    conn_type = "pydantic_ai"
    hook_name = "Pydantic AI"

    def __init__(
        self,
        llm_conn_id: str = default_conn_name,
        model_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self._model: Model | None = None

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {
                "host": "https://api.openai.com/v1 (optional, for custom endpoints)",
                "extra": '{"model": "openai:gpt-5.3"}',
            },
        }

    def get_conn(self) -> Model:
        """
        Return a configured pydantic-ai Model.

        Reads API key from connection password, model from connection extra
        or ``model_id`` parameter, and base_url from connection host.
        The result is cached for the lifetime of this hook instance.
        """
        if self._model is not None:
            return self._model

        conn = self.get_connection(self.llm_conn_id)
        model_name: str | KnownModelName = self.model_id or conn.extra_dejson.get("model", "")
        if not model_name:
            raise ValueError(
                "No model specified. Set model_id on the hook or 'model' in the connection's extra JSON."
            )
        api_key = conn.password
        base_url = conn.host or None

        if not api_key and not base_url:
            # No credentials to inject — use default provider resolution
            # (picks up env vars like OPENAI_API_KEY, AWS_PROFILE, etc.)
            self._model = infer_model(model_name)
            return self._model

        def _provider_factory(provider_name: str) -> Provider[Any]:
            """
            Create a provider with credentials from the Airflow connection.

            Falls back to default provider resolution if the provider's constructor
            doesn't accept api_key/base_url (e.g. Google Vertex, Bedrock).
            """
            provider_cls = infer_provider_class(provider_name)
            kwargs: dict[str, Any] = {}
            if api_key:
                kwargs["api_key"] = api_key
            if base_url:
                kwargs["base_url"] = base_url
            try:
                return provider_cls(**kwargs)
            except TypeError:
                # Provider doesn't accept these kwargs (e.g. Google Vertex/GLA
                # use ADC, Bedrock uses boto session). Fall back to default
                # provider resolution which reads credentials from the environment.
                return infer_provider(provider_name)

        self._model = infer_model(model_name, provider_factory=_provider_factory)
        return self._model

    @overload
    def create_agent(
        self, output_type: type[OutputT], *, instructions: str, **agent_kwargs
    ) -> Agent[None, OutputT]: ...

    @overload
    def create_agent(self, *, instructions: str, **agent_kwargs) -> Agent[None, str]: ...

    def create_agent(
        self, output_type: type[Any] = str, *, instructions: str, **agent_kwargs
    ) -> Agent[None, Any]:
        """
        Create a pydantic-ai Agent configured with this hook's model.

        :param output_type: The expected output type from the agent (default: ``str``).
        :param instructions: System-level instructions for the agent.
        :param agent_kwargs: Additional keyword arguments passed to the Agent constructor.
        """
        return Agent(self.get_conn(), output_type=output_type, instructions=instructions, **agent_kwargs)

    def test_connection(self) -> tuple[bool, str]:
        """
        Test connection by resolving the model.

        Validates that the model string is valid, the provider package is
        installed, and the provider class can be instantiated. Does NOT make an
        LLM API call — that would be expensive, flaky, and fail for reasons
        unrelated to connectivity (quotas, billing, rate limits).
        """
        try:
            self.get_conn()
            return True, "Model resolved successfully."
        except Exception as e:
            return False, str(e)
