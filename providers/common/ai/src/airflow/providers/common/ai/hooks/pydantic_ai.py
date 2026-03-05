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
from pydantic_ai.models import infer_model
from pydantic_ai.providers import infer_provider, infer_provider_class

from airflow.providers.common.compat.sdk import BaseHook

OutputT = TypeVar("OutputT")

if TYPE_CHECKING:
    from pydantic_ai.models import KnownModelName, Model


class PydanticAIHook(BaseHook):
    """
    Hook for LLM access via pydantic-ai.

    Covers providers that use a standard ``api_key`` + optional ``base_url``
    (OpenAI, Anthropic, Groq, Mistral, DeepSeek, Ollama, vLLM, …).

    For cloud providers with non-standard auth use the dedicated subclasses:
    :class:`PydanticAIAzureHook`, :class:`PydanticAIBedrockHook`,
    :class:`PydanticAIVertexHook`.

    Connection fields:
        - **password**: API key
        - **host**: Base URL (optional, e.g. ``https://api.openai.com/v1``)
        - **extra** JSON: ``{"model": "openai:gpt-5.3"}``

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
                "host": "https://api.openai.com/v1  (optional, for custom endpoints / Ollama)",
                "extra": '{"model": "openai:gpt-5.3"}',
            },
        }

    # ------------------------------------------------------------------
    # Core connection / agent API
    # ------------------------------------------------------------------

    def _get_provider_kwargs(
        self,
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Return the kwargs to pass to the provider constructor.

        Subclasses override this method to map their connection fields to the
        parameters expected by their specific provider class.  The base
        implementation handles the common ``api_key`` / ``base_url`` pattern
        used by OpenAI, Anthropic, Groq, Mistral, Ollama, and most other
        providers.

        :param api_key: Value of ``conn.password``.
        :param base_url: Value of ``conn.host``.
        :param extra: Deserialized ``conn.extra`` JSON.
        :return: Kwargs forwarded to ``provider_cls(**kwargs)``.  Empty dict
            signals that no explicit credentials are available and the hook
            should fall back to environment-variable–based auth.
        """
        kwargs: dict[str, Any] = {}
        if api_key:
            kwargs["api_key"] = api_key
        if base_url:
            kwargs["base_url"] = base_url
        return kwargs

    def get_conn(self) -> Model:
        """
        Return a configured pydantic-ai ``Model``.

        Resolution order:

        1. **Explicit credentials** — when :meth:`_get_provider_kwargs` returns
           a non-empty dict the provider class is instantiated with those kwargs
           and wrapped in a ``provider_factory``.
        2. **Default resolution** — delegates to pydantic-ai ``infer_model``
           which reads standard env vars (``OPENAI_API_KEY``, ``AWS_PROFILE``, …).

        The resolved model is cached for the lifetime of this hook instance.
        """
        if self._model is not None:
            return self._model

        conn = self.get_connection(self.llm_conn_id)

        extra: dict[str, Any] = conn.extra_dejson
        model_name: str | KnownModelName = self.model_id or extra.get("model", "")
        if not model_name:
            raise ValueError(
                "No model specified. Set model_id on the hook or 'model' in the connection's extra JSON."
            )

        api_key: str | None = conn.password or None
        base_url: str | None = conn.host or None

        # Auto-dispatch: if using base hook with a subclass connection type, borrow
        # that subclass's _get_provider_kwargs so the correct field mapping is used.
        if type(self) is PydanticAIHook:
            hook_cls = _CONN_TYPE_TO_HOOK.get(conn.conn_type or "", PydanticAIHook)
            provider_kwargs = hook_cls._get_provider_kwargs(self, api_key, base_url, extra)
        else:
            provider_kwargs = self._get_provider_kwargs(api_key, base_url, extra)
        if provider_kwargs:
            _kwargs = provider_kwargs  # capture for closure
            self.log.info(
                "Using explicit credentials for provider with model '%s': %s",
                model_name,
                list(provider_kwargs),
            )

            def _provider_factory(pname: str) -> Any:
                try:
                    return infer_provider_class(pname)(**_kwargs)
                except TypeError:
                    self.log.warning(
                        "Provider '%s' does not accept the supplied kwargs %s; "
                        "falling back to environment-variable–based auth.",
                        pname,
                        list(_kwargs),
                    )
                    return infer_provider(pname)

            self._model = infer_model(model_name, provider_factory=_provider_factory)
            return self._model

        self._model = infer_model(model_name)
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

        Validates that the model string is valid and the provider class can be
        instantiated with the supplied credentials.  Does NOT make an LLM API
        call — that would be expensive and fail for reasons unrelated to
        connectivity (quotas, billing, rate limits).
        """
        try:
            self.get_conn()
            return True, "Model resolved successfully."
        except Exception as e:
            return False, str(e)

    @classmethod
    def for_connection(cls, conn_id: str, model_id: str | None = None) -> PydanticAIHook:
        """
        Return the correct :class:`PydanticAIHook` subclass for *conn_id*.

        Looks up the connection's ``conn_type`` in the registered hook map and
        instantiates the matching subclass.  Falls back to
        :class:`PydanticAIHook` for unknown types.

        :param conn_id: Airflow connection ID.
        :param model_id: Optional model override forwarded to the hook.
        """
        conn = cls.get_connection(conn_id)
        hook_cls = _CONN_TYPE_TO_HOOK.get(conn.conn_type or "", cls)
        return hook_cls(llm_conn_id=conn_id, model_id=model_id)


class PydanticAIAzureHook(PydanticAIHook):
    """
    Hook for Azure OpenAI via pydantic-ai.

    Connection fields:
        - **password**: Azure API key
        - **host**: Azure endpoint (e.g. ``https://<resource>.openai.azure.com``)
        - **extra** JSON::

            {"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"}

    :param llm_conn_id: Airflow connection ID.
    :param model_id: Model identifier, e.g. ``"azure:gpt-4o"``.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "pydantic_ai_azure_default"
    conn_type = "pydanticai_azure"
    hook_name = "Pydantic AI (Azure OpenAI)"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key", "host": "Azure Endpoint"},
            "placeholders": {
                "host": "https://<resource>.openai.azure.com",
                "extra": '{"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"}',
            },
        }

    def _get_provider_kwargs(
        self,
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if api_key:
            kwargs["api_key"] = api_key
        if base_url:
            kwargs["azure_endpoint"] = base_url
        if extra.get("api_version"):
            kwargs["api_version"] = extra["api_version"]

        self.log.info(
            "Using explicit credentials for Azure provider with model '%s': %s",
            extra.get("model", ""),
            list(kwargs),
        )
        return kwargs


class PydanticAIBedrockHook(PydanticAIHook):
    """
    Hook for AWS Bedrock via pydantic-ai.

    Credentials are resolved in order:

    1. Explicit keys in ``extra`` (``aws_access_key_id``,
       ``aws_secret_access_key``, ``aws_session_token``).
    2. Environment-variable / instance-role chain (``AWS_PROFILE``,
       IAM role, …) when no explicit keys are provided.

    Connection fields:
        - **extra** JSON::

            {
              "model": "bedrock:us.anthropic.claude-opus-4-5",
              "region_name": "us-east-1",
              "aws_access_key_id": "AKIA...",
              "aws_secret_access_key": "..."
            }

          Leave ``aws_access_key_id`` / ``aws_secret_access_key`` empty to use
          the default AWS credential chain.

    :param llm_conn_id: Airflow connection ID.
    :param model_id: Model identifier, e.g. ``"bedrock:us.anthropic.claude-opus-4-5"``.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "pydantic_ai_bedrock_default"
    conn_type = "pydanticai_bedrock"
    hook_name = "Pydantic AI (AWS Bedrock)"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login", "host", "password"],
            "relabeling": {},
            "placeholders": {
                "extra": (
                    '{"model": "bedrock:us.anthropic.claude-opus-4-5", '
                    '"region_name": "us-east-1"}'
                    "  — leave aws_access_key_id empty for IAM role / env-var auth"
                ),
            },
        }

    def _get_provider_kwargs(
        self,
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        _bedrock_keys = (
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "region_name",
            "profile_name",
        )
        return {k: extra[k] for k in _bedrock_keys if extra.get(k) is not None}


class PydanticAIVertexHook(PydanticAIHook):
    """
    Hook for Google Vertex AI via pydantic-ai.

    Credentials are resolved in order:

    1. ``service_account_file`` / ``service_account_info`` in ``extra``.
    2. Application Default Credentials (``GOOGLE_APPLICATION_CREDENTIALS``,
       ``gcloud auth application-default login``, …) when no explicit keys
       are provided.

    Connection fields:
        - **extra** JSON::

            {
                "model": "google-vertex:gemini-2.0-flash",
                "project_id": "my-gcp-project",
                "location": "us-central1",
                "service_account_file": "/path/to/sa.json",
            }

    :param llm_conn_id: Airflow connection ID.
    :param model_id: Model identifier, e.g. ``"google-vertex:gemini-2.0-flash"``.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "pydantic_ai_vertex_default"
    conn_type = "pydanticai_vertex"
    hook_name = "Pydantic AI (Google Vertex AI)"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login", "host", "password"],
            "relabeling": {},
            "placeholders": {
                "extra": (
                    '{"model": "google-vertex:gemini-2.0-flash", '
                    '"project_id": "my-project", "location": "us-central1"}'
                    "  — leave service_account_file empty for ADC auth"
                ),
            },
        }

    def _get_provider_kwargs(
        self,
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        _vertex_keys = ("project_id", "location", "service_account_file", "service_account_info")
        return {k: extra[k] for k in _vertex_keys if extra.get(k) is not None}


# ---------------------------------------------------------------------------
# Hook registry — maps conn_type → hook class for use by for_connection()
# ---------------------------------------------------------------------------
_CONN_TYPE_TO_HOOK: dict[str, type[PydanticAIHook]] = {
    PydanticAIHook.conn_type: PydanticAIHook,
    PydanticAIAzureHook.conn_type: PydanticAIAzureHook,
    PydanticAIBedrockHook.conn_type: PydanticAIBedrockHook,
    PydanticAIVertexHook.conn_type: PydanticAIVertexHook,
}
