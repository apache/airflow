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
"""Hook for CrewAI integration with Airflow connections."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from crewai import LLM


# LiteLLM prefixes whose auth surface needs more than ``api_key`` + ``base_url``.
# AWS Bedrock requires ``aws_region_name`` (and optional IAM creds), Google
# Vertex AI requires ``vertex_project`` / ``vertex_location``, and Azure OpenAI
# requires a separate ``endpoint`` plus ``api_version``. This hook handles only
# the OpenAI-compatible credential shape; per-vendor subclasses are deferred.
_UNSUPPORTED_LITELLM_PREFIXES = ("bedrock/", "vertex_ai/", "azure/")


class CrewAIHook(BaseHook):
    """
    Bridge an Airflow connection to a CrewAI :class:`~crewai.LLM`.

    The hook reads credentials (API key, optional base URL) from the Airflow
    connection and returns a ``crewai.LLM`` instance that can be passed to any
    ``crewai.Agent``. CrewAI's ``LLM`` is a LiteLLM wrapper, so model
    identifiers use LiteLLM's ``provider/name`` form (e.g.
    ``"openai/gpt-4o"``, ``"anthropic/claude-3-5-sonnet"``).

    Only OpenAI-compatible providers (OpenAI itself, Anthropic, Groq, Mistral
    AI, Ollama, DeepSeek, Gemini's public API, ...) work with this hook's
    ``api_key`` + optional ``base_url`` credential surface. Cloud providers
    with bespoke auth (AWS Bedrock, Google Vertex AI, Azure OpenAI) require
    region / project / endpoint kwargs that this hook does not forward; the
    hook raises ``NotImplementedError`` if such a model is requested, pointing
    users at the per-vendor subclass story (mirroring the pydantic-ai pattern).

    Connection fields:

    * **password**: API key passed as ``api_key=`` to ``crewai.LLM``.
    * **host**: Optional base URL passed as ``base_url=`` (custom endpoints, Ollama).
    * **extra** JSON: ``{"model": "openai/gpt-4o"}`` -- default model identifier.

    :param llm_conn_id: Airflow connection ID for the LLM provider. Falls back
        to :attr:`default_conn_name` (``"crewai_default"``) if not provided.
    :param llm_model: Model identifier in LiteLLM ``provider/name`` format
        (e.g. ``"openai/gpt-4o"``, ``"anthropic/claude-3-5-sonnet"``).
        Overrides ``extra["model"]`` on the connection.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "crewai_default"
    conn_type = "crewai"
    hook_name = "CrewAI"

    def __init__(
        self,
        llm_conn_id: str | None = None,
        llm_model: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        # Resolve at runtime so a future subclass with its own
        # `default_conn_name` is honoured.
        self.llm_conn_id = llm_conn_id if llm_conn_id is not None else self.default_conn_name
        self.llm_model = llm_model

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {
                "host": "https://api.openai.com/v1 (optional, for custom endpoints / Ollama)",
                "extra": '{"model": "openai/gpt-4o"}',
            },
        }

    @staticmethod
    def _resolve_model_id(conn_extra: dict[str, Any], constructor_value: str | None) -> str:
        """Resolve the model identifier from constructor arg or connection extra."""
        model_id = constructor_value or conn_extra.get("model")
        if not model_id:
            raise ValueError(
                "No model identifier set. Pass llm_model= to the hook constructor "
                'or set extra={"model": "provider/name"} on the connection. '
                "CrewAI uses LiteLLM model strings (slash format, e.g. openai/gpt-4o)."
            )
        return model_id

    @staticmethod
    def _check_provider_supported(model_id: str) -> None:
        """Raise NotImplementedError for LiteLLM prefixes this hook can't auth."""
        prefix = next(
            (p for p in _UNSUPPORTED_LITELLM_PREFIXES if model_id.startswith(p)),
            None,
        )
        if prefix:
            raise NotImplementedError(
                f"CrewAIHook does not yet support {prefix.rstrip('/')!r} models. "
                "Their auth surface needs provider-specific kwargs (AWS region/IAM, "
                "GCP project/location, Azure endpoint+api_version) that this hook "
                "does not forward. Per-vendor subclasses mirroring the pydantic-ai "
                "pattern will land in a follow-up."
            )

    @staticmethod
    def _connection_kwargs(conn: Any) -> dict[str, Any]:
        """Return shared LLM(...) kwargs (api_key, base_url) derived from the connection."""
        kwargs: dict[str, Any] = {}
        if conn.password:
            kwargs["api_key"] = conn.password
        if conn.host:
            kwargs["base_url"] = conn.host
        return kwargs

    def get_llm(self) -> LLM:
        """
        Return a CrewAI ``LLM`` configured from the Airflow connection.

        Raises ``NotImplementedError`` for Bedrock / Vertex AI / Azure model
        prefixes whose auth this hook cannot construct.
        """
        # Lazy: crewai is an optional extra; importing at module level would
        # break common.ai for users who haven't installed `[crewai]`.
        from crewai import LLM

        conn = self.get_connection(self.llm_conn_id)
        model_id = self._resolve_model_id(conn.extra_dejson, self.llm_model)
        self._check_provider_supported(model_id)
        return LLM(model=model_id, **self._connection_kwargs(conn))
