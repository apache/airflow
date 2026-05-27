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

from pydantic_ai import Agent
from pydantic_ai.messages import ToolCallPart
from pydantic_ai.models import infer_model
from pydantic_ai.providers import infer_provider, infer_provider_class
from pydantic_ai.tools import Tool
from pydantic_ai.toolsets.abstract import AbstractToolset

from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
from airflow.providers.common.ai.hooks.base_ai import (
    AgentRunRequest,
    AgentRunResult,
    AgentUsage,
    BaseAIHook,
    DurableStats,
    ToolSpec,
)
from airflow.providers.common.ai.toolsets.logging import LoggingToolset

if TYPE_CHECKING:
    from pydantic_ai.models import KnownModelName, Model


class PydanticAIHook(BaseAIHook):
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
    default_conn_name = "pydanticai_default"
    conn_type = "pydanticai"
    hook_name = "Pydantic AI"

    supports_toolsets = True
    supports_durable = True
    supports_usage_limits = True

    def __init__(
        self,
        llm_conn_id: str | None = None,
        model_id: str | None = None,
        **kwargs,
    ) -> None:
        # Resolve at runtime so each subclass uses its own default_conn_name.
        # A bare `llm_conn_id: str = default_conn_name` would bind the *base*
        # class value for all subclasses because Python evaluates default
        # argument values at class-definition time.
        resolved_conn_id = llm_conn_id if llm_conn_id is not None else self.default_conn_name
        super().__init__(llm_conn_id=resolved_conn_id, model_id=model_id, **kwargs)
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

    def get_model(self) -> Model:
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

        conn = self.get_connection(self.llm_conn_id or self.default_conn_name)

        extra: dict[str, Any] = conn.extra_dejson
        model_name: str | KnownModelName = self.model_id or extra.get("model", "")
        if not model_name:
            raise ValueError(
                "No model specified. Set model_id on the hook or the Model field on the connection."
            )

        api_key: str | None = conn.password or None
        base_url: str | None = conn.host or None

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
                        "Provider '%s' rejected kwargs %s; falling back to env-var auth",
                        pname,
                        list(_kwargs),
                    )
                    return infer_provider(pname)

            self._model = infer_model(model_name, provider_factory=_provider_factory)
            return self._model

        self._model = infer_model(model_name)
        return self._model

    # ------------------------------------------------------------------
    # BaseAIHook abstract interface
    # ------------------------------------------------------------------

    def _tool_spec_to_native(self, spec: ToolSpec) -> Any:
        """Convert a :class:`~airflow.providers.common.ai.hooks.base_ai.ToolSpec` to a pydantic-ai ``Tool``."""
        return Tool(
            spec.fn,
            name=spec.name,
            description=spec.description,
            sequential=spec.sequential,
        )

    def create_agent(self, request: AgentRunRequest) -> Agent[None, Any]:
        """
        Build a pydantic-ai ``Agent`` from *request*.

        When :attr:`~AgentRunRequest.durable_context` is set, initialises durable
        storage and step counter and binds them to the returned agent for use by
        :meth:`run_agent`.

        :param request: Agent configuration — output type, instructions, toolsets, extra params.
        """
        self.validate_run_request(request)

        storage = counter = None
        if request.durable_context is not None:
            storage, counter = self._init_durable(request.durable_context)

        extra_kwargs = dict(request.agent_params or {})
        if request.toolsets:
            if "tools" in extra_kwargs:
                raise ValueError(
                    "agent_params must not include 'tools' when toolsets= is set on AgentRunRequest."
                )
            if "toolsets" in extra_kwargs:
                raise ValueError(
                    "agent_params must not include 'toolsets' when toolsets= is set on AgentRunRequest."
                )

            abstract_items = [ts for ts in request.toolsets if isinstance(ts, AbstractToolset)]
            pipeline_items = [ts for ts in request.toolsets if not isinstance(ts, AbstractToolset)]

            if pipeline_items:
                resolved = self._resolve_tools(
                    pipeline_items,
                    request.enable_tool_logging,
                    storage,
                    counter,
                )
                extra_kwargs["tools"] = resolved

            if abstract_items:
                processed: list[Any] = list(abstract_items)
                if storage is not None and counter is not None:
                    processed = [
                        CachingToolset(
                            wrapped=ts,
                            storage=storage,
                            counter=counter,
                        )
                        for ts in processed
                    ]
                if request.enable_tool_logging:
                    processed = [LoggingToolset(wrapped=ts, logger=self.log) for ts in processed]
                extra_kwargs["toolsets"] = processed

        agent = Agent(
            self.get_model(),
            output_type=request.output_type,
            instructions=request.instructions,
            **extra_kwargs,
        )
        if storage is not None and counter is not None:
            self._bind_agent_durable(agent, storage, counter)
        return agent

    def run_agent(self, agent: Agent[None, Any], request: AgentRunRequest) -> AgentRunResult:
        """Run *agent* synchronously for *request* and return a normalized :class:`~airflow.providers.common.ai.hooks.base_ai.AgentRunResult`."""
        run_kwargs: dict[str, Any] = {}
        if request.message_history is not None:
            run_kwargs["message_history"] = request.message_history
        if request.usage_limits is not None:
            run_kwargs["usage_limits"] = request.usage_limits

        durable = self._pop_agent_durable(agent)
        storage, counter = durable if durable else (None, None)

        if storage is not None and counter is not None:
            if agent.model is None:
                raise ValueError("Agent model must be set when durable=True")
            model = agent.model
            resolved_model = infer_model(model) if isinstance(model, str) else model
            caching_model = CachingModel(
                resolved_model,
                storage=storage,
                counter=counter,
            )
            with agent.override(model=caching_model):
                result = agent.run_sync(request.prompt, **run_kwargs)
        else:
            result = agent.run_sync(request.prompt, **run_kwargs)

        usage = result.usage
        tool_names: list[str] = []
        for message in result.all_messages():
            for part in getattr(message, "parts", []):
                if isinstance(part, ToolCallPart):
                    tool_names.append(part.tool_name)

        run_result = AgentRunResult(
            output=result.output,
            message_history=result.all_messages(),
            model_name=getattr(result.response, "model_name", None),
            usage=AgentUsage(
                requests=usage.requests,
                tool_calls=usage.tool_calls,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                total_tokens=usage.total_tokens,
            ),
            tool_names=tool_names or None,
        )

        if counter is not None:
            run_result.durable_stats = DurableStats(
                replayed_model=counter.replayed_model,
                replayed_tool=counter.replayed_tool,
                cached_model=counter.cached_model,
                cached_tool=counter.cached_tool,
            )

        if storage is not None:
            storage.cleanup()
        return run_result

    def test_connection(self) -> tuple[bool, str]:
        """
        Test connection by resolving the model.

        Validates that the model string is valid and the provider class can be
        instantiated with the supplied credentials.  Does NOT make an LLM API
        call — that would be expensive and fail for reasons unrelated to
        connectivity (quotas, billing, rate limits).
        """
        try:
            self.get_model()
            return True, "Model resolved successfully."
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

    conn_type = "pydanticai-azure"
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
        return kwargs


class PydanticAIBedrockHook(PydanticAIHook):
    """
    Hook for AWS Bedrock via pydantic-ai.

    Credentials are resolved in order:

    1. IAM keys from ``extra`` (``aws_access_key_id`` + ``aws_secret_access_key``,
       optionally ``aws_session_token``).
    2. Bearer token in ``extra`` (``api_key``, maps to env ``AWS_BEARER_TOKEN_BEDROCK``).
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

    conn_type = "pydanticai-bedrock"
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

    def _get_provider_kwargs(
        self,
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

    Credentials are resolved in order:

    1. ``service_account_info`` (JSON object) in ``extra``
       — loaded into a ``google.auth.credentials.Credentials``
       object and passed as ``credentials`` to ``GoogleProvider``.
    2. ``api_key`` in ``extra`` — for Generative Language API (non-Vertex) or
       Vertex API-key auth.
    3. Application Default Credentials (``GOOGLE_APPLICATION_CREDENTIALS``,
       ``gcloud auth application-default login``, Workload Identity, …) when
       no explicit credentials are provided.

    Connection fields:
        - **extra** JSON::

            {
                "model": "google-vertex:gemini-2.0-flash",
                "project": "my-gcp-project",
                "location": "us-central1",
                "service_account_info": {...},
                "vertexai": true,
            }

        Use ``"service_account_info"`` to embed the service-account JSON directly
        (as an object, not a string path).

        Set ``"vertexai": true`` to force Vertex AI mode when only ``api_key`` is
        provided.  Omit ``vertexai`` for the Generative Language API (GLA).

    :param llm_conn_id: Airflow connection ID.
    :param model_id: Model identifier, e.g. ``"google-vertex:gemini-2.0-flash"``.
    """

    conn_type = "pydanticai-vertex"
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
                    '{"model": "google-vertex:gemini-2.0-flash", '
                    '"project": "my-project", "location": "us-central1", "vertexai": true}'
                    "  — add service_account_info (object) for SA auth;"
                    " omit both to use Application Default Credentials"
                ),
            },
        }

    def _get_provider_kwargs(
        self,
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        sa_info = extra.get("service_account_info")
        kwargs: dict[str, Any] = {}

        # Direct GoogleProvider scalar kwargs.
        for _key in ("api_key", "project", "location", "base_url"):
            if extra.get(_key):
                kwargs[_key] = extra[_key]

        # Optional vertexai bool flag (force Vertex AI mode for API-key auth).
        _vertexai = extra.get("vertexai")
        if _vertexai is not None:
            kwargs["vertexai"] = bool(_vertexai)

        # Service-account credentials — loaded lazily to avoid importing
        # google-auth on non-Vertex code paths (optional heavy dependency).
        if sa_info:
            from google.oauth2 import service_account  # lazy: optional dep

            kwargs["credentials"] = service_account.Credentials.from_service_account_info(
                sa_info,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

        return kwargs
