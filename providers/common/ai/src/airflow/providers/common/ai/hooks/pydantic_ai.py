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

from airflow.providers.common.compat.sdk import BaseHook

OutputT = TypeVar("OutputT")

if TYPE_CHECKING:
    from pydantic_ai.models import KnownModelName, Model


class PydanticAIHook(BaseHook):
    """
    Hook for LLM access via pydantic-ai.

    Manages connection credentials and model creation. Uses pydantic-ai's
    model inference to support any provider (OpenAI, Anthropic, Google,
    Bedrock, Ollama, vLLM, Azure OpenAI, etc.).

    Connection fields:
        - **password**: API key
        - **host**: Base URL or Azure endpoint
          (e.g. ``https://<resource>.openai.azure.com``)
        - **extra** JSON::

            {"model": "openai:gpt-5.3"}

          For Azure OpenAI, add ``api_version`` and optionally
          ``azure_deployment``, ``azure_ad_token``,
          ``azure_ad_token_provider``::

            {
              "model": "openai:gpt-5.2",
              "api_version": "2024-07-01-preview",
              "azure_deployment": "my-gpt4o-deployment"
            }

          When ``api_version`` is present the hook switches to the
          Azure OpenAI client path automatically.

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
                "host": (
                    "https://api.openai.com/v1  — or Azure endpoint: https://<resource>.openai.azure.com"
                ),
                "extra": ('{"model": "openai:gpt-5.3"}  — Azure: also add "api_version", "azure_deployment"'),
            },
        }

    # ------------------------------------------------------------------
    # Core connection / agent API
    # ------------------------------------------------------------------

    def get_conn(self) -> Model:
        """
        Return a configured pydantic-ai Model.

        Resolution is delegated to builders based on connection characteristics:

        1. **Azure OpenAI** — when ``api_version`` is present in connection extra.
        2. **Custom endpoint** — when ``password`` or ``host`` are set.
        3. **Default resolution** — delegates to pydantic-ai ``infer_model``.

        The resolved model is cached for the lifetime of this hook instance.
        """
        if self._model is not None:
            return self._model

        from airflow.providers.common.ai.builders import (
            AzureOpenAIBuilder,
            CustomEndpointBuilder,
            DefaultBuilder,
            ProviderBuilder,
        )

        conn = self.get_connection(self.llm_conn_id)
        extra: dict[str, Any] = conn.extra_dejson
        model_name: str | KnownModelName = self.model_id or extra.get("model", "")
        if not model_name:
            raise ValueError(
                "No model specified. Set model_id on the hook or 'model' in the connection's extra JSON."
            )

        api_key: str | None = conn.password or None
        base_url: str | None = conn.host or None

        builders: list[ProviderBuilder] = [
            AzureOpenAIBuilder(),
            CustomEndpointBuilder(),
            DefaultBuilder(),
        ]

        for builder in builders:
            if builder.supports(extra, api_key, base_url):
                self._model = builder.build(model_name, extra, api_key, base_url)
                return self._model

        raise RuntimeError("No suitable ProviderBuilder found to construct the model.")

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
        installed, and the provider class can be instantiated.  For Azure
        connections this also validates that all required extra fields
        (``api_version``, ``host``) are present.  Does NOT make an LLM API
        call — that would be expensive, flaky, and fail for reasons unrelated
        to connectivity (quotas, billing, rate limits).
        """
        try:
            self.get_conn()
            return True, "Model resolved successfully."
        except Exception as e:
            return False, str(e)
