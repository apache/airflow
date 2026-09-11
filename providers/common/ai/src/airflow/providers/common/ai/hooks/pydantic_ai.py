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

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar, overload

from pydantic_ai import Agent, Embedder
from pydantic_ai.embeddings import infer_embedding_model
from pydantic_ai.models import infer_model, parse_model_id
from pydantic_ai.providers import infer_provider_class

from airflow.providers.common.ai.observability import genai_instrumentation_settings
from airflow.providers.common.compat.sdk import AirflowNotFoundException, BaseHook

OutputT = TypeVar("OutputT")


@dataclass(frozen=True)
class _ProviderConnectionConfig:
    get_kwargs: Callable[[str | None, str | None, dict[str, Any]], dict[str, Any]]
    replacement_fields: tuple[str, ...] = ()


# Sentinel distinguishing "caller did not pass ``instrument``" from an explicit
# ``instrument=None`` / ``instrument=False`` (which mean "do not instrument, and
# do not auto-enable it either").
_UNSET: Any = object()

if TYPE_CHECKING:
    from pydantic_ai.models import KnownModelName, Model

    from airflow.providers.common.compat.sdk import Connection


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
        - **extra** JSON: ``{"model": "openai:gpt-5.6-sol", "embed_model": "openai:text-embedding-3-small"}``

    :param llm_conn_id: Airflow connection ID for the LLM provider.
    :param model_id: Model identifier in ``provider:model`` format (e.g. ``"openai:gpt-5.6-sol"``).
        Overrides the model stored in the connection's extra field.
    :param embed_conn_id: Optional separate Airflow connection ID for the embedding provider.
        Falls back to ``llm_conn_id`` when not provided.
    :param embed_model_id: Embedding model identifier in ``provider:model`` format.
            Overrides the embedding model stored in the connection's extra field.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "pydanticai_default"
    conn_type = "pydanticai"
    hook_name = "Pydantic AI"

    def __init__(
        self,
        llm_conn_id: str | None = None,
        model_id: str | None = None,
        embed_conn_id: str | None = None,
        embed_model_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        # Resolve at runtime so each subclass uses its own default_conn_name.
        # A bare `llm_conn_id: str = default_conn_name` would bind the *base*
        # class value for all subclasses because Python evaluates default
        # argument values at class-definition time.
        self.llm_conn_id = llm_conn_id if llm_conn_id is not None else self.default_conn_name
        self.model_id = model_id
        self.embed_conn_id = embed_conn_id if embed_conn_id is not None else self.llm_conn_id
        self.embed_model_id = embed_model_id
        self._model: Model | None = None
        self._embedder: Embedder | None = None
        self._embedder_kwargs: dict[str, Any] | None = None
        self._connections: dict[str, Connection] = {}

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {
                "host": "https://api.openai.com/v1 (optional, for custom endpoints / Ollama)",
                "extra": '{"model": "openai:gpt-5.6-sol", "embed_model": "openai:text-embedding-3-small"}',
            },
        }

    # ------------------------------------------------------------------
    # Core connection / agent API
    # ------------------------------------------------------------------

    @staticmethod
    def _get_provider_kwargs(
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Return the kwargs to pass to the provider constructor.

        The model prefix selects this mapper or a provider-specific mapper. The
        base implementation handles the common ``api_key`` / ``base_url``
        pattern used by OpenAI, Anthropic, Groq, Mistral, Ollama, and most
        other providers.

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

    def _get_cached_connection(self, conn_id: str) -> Connection:
        if conn_id not in self._connections:
            self._connections[conn_id] = self.get_connection(conn_id)
        return self._connections[conn_id]

    def _warn_if_vertexai_field_ignored(self, extra: dict[str, Any]) -> None:
        if extra.get("vertexai") is not None:
            self.log.warning(
                "The 'vertexai' connection field is ignored; Vertex AI vs. Generative Language "
                "API mode is now selected via the model prefix ('google-cloud:' vs. 'google:')."
            )

    def _get_provider_kwargs_for_model(self, conn: Connection, model_name: str) -> dict[str, Any]:
        provider_name, _ = parse_model_id(model_name)
        provider_config = _PROVIDER_CONNECTION_CONFIGS.get(provider_name)
        extra = conn.extra_dejson
        self._warn_if_vertexai_field_ignored(extra)
        if provider_config is None:
            return PydanticAIHook._get_provider_kwargs(conn.password, conn.host, extra)
        if provider_config.replacement_fields:
            self._warn_if_generic_fields_ignored(conn, provider_name, provider_config.replacement_fields)
        return provider_config.get_kwargs(conn.password, conn.host, extra)

    def _warn_if_generic_fields_ignored(
        self, conn: Connection, provider_name: str | None, replacement_fields: tuple[str, ...]
    ) -> None:
        ignored_fields = [
            field for field, value in (("password", conn.password), ("host", conn.host)) if value
        ]
        if ignored_fields:
            self.log.warning(
                "Connection fields are ignored for provider; configure provider-specific values in extra",
                conn_id=conn.conn_id,
                provider=provider_name,
                ignored_fields=ignored_fields,
                replacement_fields=list(replacement_fields),
            )

    def _get_provider_factory_for_model(
        self, conn: Connection, model_name: str
    ) -> Callable[[str], Any] | None:
        provider_name, _ = parse_model_id(model_name)
        if provider_name == "sentence-transformers":
            return None

        provider_kwargs = self._get_provider_kwargs_for_model(conn, model_name)
        if not provider_kwargs:
            return None

        self.log.info(
            "Using explicit connection credentials for model '%s': %s",
            model_name,
            list(provider_kwargs),
        )

        def create_provider(provider: str) -> Any:
            try:
                return infer_provider_class(provider)(**provider_kwargs)
            except TypeError as e:
                raise TypeError(
                    f"Provider {provider!r} rejected connection {conn.conn_id!r} fields "
                    f"mapped to kwargs {sorted(provider_kwargs)}"
                ) from e

        return create_provider

    def _validate_embedding_connection_provider(self, conn: Connection, embed_model_name: str) -> None:
        if self.embed_conn_id != self.llm_conn_id:
            return

        llm_model_name = self.model_id or conn.extra_dejson.get("model", "")
        if not llm_model_name:
            return

        llm_provider, _ = parse_model_id(llm_model_name)
        embed_provider, _ = parse_model_id(embed_model_name)
        if embed_provider == "sentence-transformers":
            return
        if llm_provider != embed_provider:
            raise ValueError(
                f"Connection {self.embed_conn_id!r} configures different LLM and embedding providers "
                f"({llm_provider!r} and {embed_provider!r}). Set embed_conn_id to a separate connection "
                "for the embedding provider."
            )

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

        conn = self._get_cached_connection(self.llm_conn_id)
        extra = conn.extra_dejson

        model_name: str | KnownModelName = self.model_id or extra.get("model", "")
        if not model_name:
            raise ValueError(
                "No model specified. Set model_id on the hook or the Model field on the connection."
            )

        provider_factory = self._get_provider_factory_for_model(conn, model_name)
        if provider_factory is None:
            self._model = infer_model(model_name)
        else:
            self._model = infer_model(model_name, provider_factory=provider_factory)
        return self._model

    def get_embedder(self, **embedder_kwargs: Any) -> Embedder:
        """
        Return a pydantic-ai ``Embedder`` using this connection's credentials.

        :param embedder_kwargs: Additional keyword arguments passed to ``Embedder``.
            Caller-supplied ``instrument`` takes precedence over Airflow's automatic
            instrumentation. Repeated calls with the same arguments return the cached
            instance; different arguments replace it with a newly configured instance.
        """
        if self._embedder is not None and embedder_kwargs == self._embedder_kwargs:
            return self._embedder

        conn = self._get_cached_connection(self.embed_conn_id)
        extra: dict[str, Any] = conn.extra_dejson

        embed_model_name: str = self.embed_model_id or extra.get("embed_model", "")
        if not embed_model_name:
            raise ValueError(
                "No embedding model specified. Set embed_model_id on the hook or the embed_model field "
                "on the connection."
            )

        self._validate_embedding_connection_provider(conn, embed_model_name)
        provider_factory = self._get_provider_factory_for_model(conn, embed_model_name)
        if provider_factory is None:
            embedding_model = infer_embedding_model(embed_model_name)
        else:
            embedding_model = infer_embedding_model(embed_model_name, provider_factory=provider_factory)

        constructor_kwargs = dict(embedder_kwargs)
        if "instrument" not in constructor_kwargs:
            instrumentation_settings = genai_instrumentation_settings()
            if instrumentation_settings is not None:
                constructor_kwargs["instrument"] = instrumentation_settings

        embedder = Embedder(embedding_model, **constructor_kwargs)
        self._embedder = embedder
        self._embedder_kwargs = embedder_kwargs
        return embedder

    def _get_conn_if_model_configured(self) -> Model | None:
        """Return the hook model only when the hook or connection explicitly configures one."""
        if self.model_id:
            return self.get_conn()

        try:
            conn = self._get_cached_connection(self.llm_conn_id)
        except AirflowNotFoundException:
            if self.llm_conn_id == self.default_conn_name and self.embed_conn_id != self.llm_conn_id:
                return None
            raise
        if conn.extra_dejson.get("model"):
            return self.get_conn()

        return None

    def _get_embedder_if_model_configured(self) -> Embedder | None:
        """Return the embedder only when the hook or connection explicitly configures one."""
        if self.embed_model_id:
            return self.get_embedder()

        conn = self._get_cached_connection(self.embed_conn_id)
        if conn.extra_dejson.get("embed_model"):
            return self.get_embedder()

        return None

    @overload
    def create_agent(
        self, output_type: type[OutputT], *, instructions: str, **agent_kwargs
    ) -> Agent[object, OutputT]: ...

    @overload
    def create_agent(self, *, instructions: str, **agent_kwargs) -> Agent[object, str]: ...

    @overload
    def create_agent(
        self,
        output_type: type[OutputT],
        *,
        spec_file: str | Path,
        instructions: str | None = ...,
        **agent_kwargs,
    ) -> Agent[object, OutputT]: ...

    @overload
    def create_agent(
        self,
        *,
        spec_file: str | Path,
        instructions: str | None = ...,
        **agent_kwargs,
    ) -> Agent[object, str]: ...

    def create_agent(
        self,
        output_type: type[Any] = str,
        *,
        instructions: str | None = None,
        spec_file: str | Path | None = None,
        **agent_kwargs,
    ) -> Agent[object, Any]:
        """
        Create a pydantic-ai Agent configured with this hook's model.

        When ``[common.ai] otel_export_enabled`` is set and the worker has an
        OpenTelemetry exporter configured, the agent is instrumented to emit
        GenAI spans through Airflow's tracing pipeline. See
        :mod:`airflow.providers.common.ai.observability`.

        :param output_type: The expected output type from the agent (default: ``str``).
        :param instructions: System-level instructions for the agent.
            Required when *spec_file* is not given. When *spec_file* is given,
            this value is merged with the instructions in the file; omit it to
            use only the file value.
        :param spec_file: Path to a YAML or JSON ``AgentSpec`` file.  When supplied,
            delegates to ``Agent.from_file``. If ``model_id`` or the connection's
            ``model`` extra is set, that model is passed to pydantic-ai; otherwise
            the spec file's ``model`` is used.
        :param agent_kwargs: Additional keyword arguments passed to the Agent constructor.
        """
        # ``instrument`` is no longer an ``Agent()`` / ``Agent.from_file()``
        # constructor argument in pydantic-ai 2.x; it is configured through the
        # ``agent.instrument`` property (which is unchanged across the 2.x line).
        # Pop any caller-supplied value out of the constructor kwargs and apply
        # it after construction so a caller that passes its own ``instrument``
        # still wins over the provider's auto-instrumentation.
        caller_instrument = agent_kwargs.pop("instrument", _UNSET)

        if spec_file is not None:
            from_file_kwargs = dict(agent_kwargs)
            model = self._get_conn_if_model_configured()
            if model is not None:
                from_file_kwargs["model"] = model
            if instructions is not None:
                from_file_kwargs["instructions"] = instructions

            agent = Agent.from_file(
                spec_file,
                output_type=output_type,
                **from_file_kwargs,
            )
        else:
            if instructions is None:
                raise ValueError("instructions is required when spec_file is not provided.")
            agent = Agent(self.get_conn(), output_type=output_type, instructions=instructions, **agent_kwargs)

        if caller_instrument is not _UNSET:
            agent.instrument = caller_instrument
        else:
            settings = genai_instrumentation_settings()
            if settings is not None:
                agent.instrument = settings
        return agent

    def test_connection(self) -> tuple[bool, str]:
        """
        Test connection by resolving the configured model.

        Validates that the LLM or embedding model string is valid and the provider
        class can be instantiated with the supplied credentials. Does NOT make an
        API call — that would be expensive and fail for reasons unrelated to
        connectivity (quotas, billing, rate limits).
        """
        try:
            model = self._get_conn_if_model_configured()
            embedder = self._get_embedder_if_model_configured()
            if model is not None and embedder is not None:
                return True, "Model and embedding model resolved successfully."
            if model is not None:
                return True, "Model resolved successfully."
            if embedder is not None:
                return True, "Embedding model resolved successfully."
            return False, (
                "No model or embedding model specified. Set model_id or embed_model_id on the hook, "
                "or the model or embed_model field on the connection."
            )
        except Exception as e:
            return False, str(e)


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

    conn_type = "pydanticai_azure"
    default_conn_name = "pydanticai_azure_default"
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

    @staticmethod
    def _get_provider_kwargs(
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
        return kwargs


class PydanticAIBedrockHook(PydanticAIHook):
    """
    Hook for AWS Bedrock via pydantic-ai.

    Credentials are resolved in order:

    1. Bearer token in ``extra`` (``api_key``, maps to env ``AWS_BEARER_TOKEN_BEDROCK``).
       Takes precedence over IAM keys if both are set.
    2. IAM keys from ``extra`` (``aws_access_key_id`` + ``aws_secret_access_key``,
       optionally ``aws_session_token``).
    3. Environment-variable / instance-role chain (``AWS_PROFILE``, IAM role, …)
       when no explicit keys are provided.

    Connection fields:
        - **extra** JSON::

            {
              "model": "bedrock:us.anthropic.claude-opus-4-5",
              "region_name": "us-east-1",
              "aws_access_key_id": "AKIA...",
              "aws_secret_access_key": "...",
              "aws_session_token": "...",
              "profile_name": "my-aws-profile",
              "api_key": "bearer-token",
              "base_url": "https://custom-bedrock-endpoint",
              "aws_read_timeout": 60.0,
              "aws_connect_timeout": 10.0
            }

          Leave ``aws_access_key_id`` / ``aws_secret_access_key`` and ``api_key``
          empty to use the default AWS credential chain.

    :param llm_conn_id: Airflow connection ID.
    :param model_id: Model identifier, e.g. ``"bedrock:us.anthropic.claude-opus-4-5"``.
    """

    conn_type = "pydanticai_bedrock"
    default_conn_name = "pydanticai_bedrock_default"
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

    @staticmethod
    def _get_provider_kwargs(
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Return kwargs for ``BedrockProvider``.

        .. note::
            The ``api_key`` and ``base_url`` parameters (sourced from
            ``conn.password`` and ``conn.host``) are intentionally ignored here.
            Bedrock connections hide those fields in the UI; all config is
            stored in ``extra`` instead.  The ``api_key`` and ``base_url``
            keys below refer to *extra* fields, not the method parameters.
        """
        _str_keys = (
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "region_name",
            "profile_name",
            # Bearer-token auth (alternative to IAM key/secret).
            # Maps to AWS_BEARER_TOKEN_BEDROCK env var.
            "api_key",
            # Custom Bedrock runtime endpoint.
            "base_url",
        )
        kwargs: dict[str, Any] = {k: extra[k] for k in _str_keys if extra.get(k)}
        # BedrockProvider expects float for timeout values; JSON integers must be coerced.
        for _timeout_key in ("aws_read_timeout", "aws_connect_timeout"):
            if extra.get(_timeout_key):
                kwargs[_timeout_key] = float(extra[_timeout_key])
        return kwargs


class PydanticAIVertexHook(PydanticAIHook):
    """
    Hook for Google Vertex AI (or Generative Language API) via pydantic-ai.

    For ``google-cloud:`` models, credentials are resolved in order:

    1. ``service_account_info`` (JSON object) in ``extra``
       — loaded into a ``google.auth.credentials.Credentials``
       object and passed as ``credentials`` to ``GoogleCloudProvider``.
    2. ``api_key`` in ``extra`` — for Vertex API-key auth.
    3. Application Default Credentials (``GOOGLE_APPLICATION_CREDENTIALS``,
       ``gcloud auth application-default login``, Workload Identity, …) when
       no explicit credentials are provided.

    For ``google:`` models, only ``api_key`` and ``base_url`` from ``extra``
    are forwarded to ``GoogleProvider``.

    Connection fields:
        - **extra** JSON::

            {
                "model": "google-cloud:gemini-2.0-flash",
                "project": "my-gcp-project",
                "location": "us-central1",
                "service_account_info": {...},
            }

        Use ``"service_account_info"`` to embed the service-account JSON directly
        (as an object, not a string path).

        ``"vertexai"`` is accepted for backward compatibility but has no effect:
        pydantic-ai now selects Vertex AI vs. the Generative Language API from the
        model prefix (``google-cloud:`` vs. ``google:``) rather than a
        constructor flag, so there is nothing left for this field to control.

    :param llm_conn_id: Airflow connection ID.
    :param model_id: Model identifier, e.g. ``"google-cloud:gemini-2.0-flash"``.
    """

    conn_type = "pydanticai_vertex"
    default_conn_name = "pydanticai_vertex_default"
    hook_name = "Pydantic AI (Google Vertex AI)"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login", "host", "password"],
            "relabeling": {},
            "placeholders": {
                "extra": (
                    '{"model": "google-cloud:gemini-2.0-flash", '
                    '"project": "my-project", "location": "us-central1"}'
                    "  — add service_account_info (object) for SA auth;"
                    " omit both to use Application Default Credentials"
                ),
            },
        }

    @staticmethod
    def _get_google_provider_kwargs(
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        """Return kwargs accepted by the Generative Language API provider."""
        return {key: extra[key] for key in ("api_key", "base_url") if extra.get(key)}

    @staticmethod
    def _get_google_cloud_provider_kwargs(
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        sa_info = extra.get("service_account_info")
        kwargs: dict[str, Any] = {}

        # Direct GoogleCloudProvider scalar kwargs.
        for _key in ("api_key", "project", "location", "base_url"):
            if extra.get(_key):
                kwargs[_key] = extra[_key]

        # Service-account credentials — loaded lazily to avoid importing
        # google-auth on non-Vertex code paths (optional heavy dependency).
        if sa_info:
            from google.oauth2 import service_account  # lazy: optional dep

            kwargs["credentials"] = service_account.Credentials.from_service_account_info(
                sa_info,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

        return kwargs


_PROVIDER_CONNECTION_CONFIGS: dict[str | None, _ProviderConnectionConfig] = {
    "azure": _ProviderConnectionConfig(PydanticAIAzureHook._get_provider_kwargs),
    "azure-responses": _ProviderConnectionConfig(PydanticAIAzureHook._get_provider_kwargs),
    "bedrock": _ProviderConnectionConfig(
        PydanticAIBedrockHook._get_provider_kwargs,
        ("api_key", "base_url", "region_name"),
    ),
    "google": _ProviderConnectionConfig(
        PydanticAIVertexHook._get_google_provider_kwargs,
        ("api_key", "base_url"),
    ),
    "google-cloud": _ProviderConnectionConfig(
        PydanticAIVertexHook._get_google_cloud_provider_kwargs,
        ("api_key", "base_url", "project", "location"),
    ),
}
