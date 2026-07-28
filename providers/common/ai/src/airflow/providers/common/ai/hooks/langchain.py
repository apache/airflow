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
"""Hook for LangChain integration with Airflow connections."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from langchain_core.embeddings import Embeddings
    from langchain_core.language_models.chat_models import BaseChatModel


class LangChainHook(BaseHook):
    """
    Bridge an Airflow connection to LangChain chat and embedding models.

    The hook resolves credentials (API key, optional base URL) from the Airflow
    connection and returns LangChain model objects via two universal entry-point
    functions:

    * :func:`langchain.chat_models.init_chat_model` dispatches to the right
      chat-model vendor based on the model identifier.
    * :func:`langchain.embeddings.init_embeddings` dispatches to the right
      embedding-model vendor based on the model identifier.

    Both identifiers use the ``provider:name`` format
    (e.g. ``"openai:gpt-4o"``, ``"openai:text-embedding-3-small"``). Only
    OpenAI-compatible providers (OpenAI itself, Anthropic, Groq, Mistral AI
    chat, Ollama, DeepSeek, ...) work with this hook's ``api_key`` + optional
    ``base_url`` credential surface. Providers with bespoke auth (AWS Bedrock,
    Google Vertex AI / GenAI, Azure OpenAI, Cohere, HuggingFace) reject these
    kwargs; per-vendor subclasses can be added later mirroring the pydantic-ai
    pattern.

    Connection fields:

    * **password**: API key passed as ``api_key=`` to the model constructor.
    * **host**: Optional base URL passed as ``base_url=`` (custom endpoints, Ollama, vLLM).
    * **extra** JSON: ``{"model": "openai:gpt-4o", "embed_model": "openai:text-embedding-3-small"}``
      -- default chat and embedding model identifiers.

    :param llm_conn_id: Airflow connection ID for the LLM provider. Falls back
        to :attr:`default_conn_name` (``"langchain_default"``) if not provided.
    :param embed_conn_id: Optional separate Airflow connection ID for the
        embedding provider. Falls back to ``llm_conn_id`` when not provided --
        the common case of one provider for both chat and embeddings stays a
        single hook instance.
    :param llm_model: Chat model identifier in ``provider:name`` format
        (e.g. ``"openai:gpt-4o"``, ``"anthropic:claude-3-7-sonnet"``).
        Overrides ``extra["model"]`` on the connection.
    :param embed_model: Embedding model identifier in ``provider:name`` format
        (e.g. ``"openai:text-embedding-3-small"``). Overrides
        ``extra["embed_model"]`` on the connection.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "langchain_default"
    conn_type = "langchain"
    hook_name = "LangChain"

    def __init__(
        self,
        llm_conn_id: str | None = None,
        embed_conn_id: str | None = None,
        llm_model: str | None = None,
        embed_model: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        # Resolve at runtime so a future subclass with its own
        # `default_conn_name` is honoured. A bare `llm_conn_id: str =
        # default_conn_name` would bind the *base* class value because Python
        # evaluates default argument values at class-definition time.
        self.llm_conn_id = llm_conn_id if llm_conn_id is not None else self.default_conn_name
        self.embed_conn_id = embed_conn_id if embed_conn_id is not None else self.llm_conn_id
        self.llm_model = llm_model
        self.embed_model = embed_model

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {
                "host": "https://api.openai.com/v1 (optional, for custom endpoints / Ollama)",
                "extra": '{"model": "openai:gpt-4o", "embed_model": "openai:text-embedding-3-small"}',
            },
        }

    @staticmethod
    def _resolve_model_id(
        conn_extra: dict[str, Any],
        *,
        constructor_value: str | None,
        extra_key: str,
        kind: str,
    ) -> str:
        """Resolve a model identifier from constructor arg or connection extra."""
        model_id = constructor_value or conn_extra.get(extra_key)
        if not model_id:
            raise ValueError(
                f"No {kind} model identifier set. Pass {extra_key}= to the hook "
                f'constructor or set extra={{"{extra_key}": "provider:name"}} on '
                "the connection."
            )
        return model_id

    @staticmethod
    def _connection_kwargs(conn: Any) -> dict[str, Any]:
        """Return shared init_* kwargs (api_key, base_url) derived from the connection."""
        kwargs: dict[str, Any] = {}
        if conn.password:
            kwargs["api_key"] = conn.password
        if conn.host:
            kwargs["base_url"] = conn.host
        return kwargs

    def get_chat_model(self) -> BaseChatModel:
        """
        Return a LangChain chat model configured from the Airflow connection.

        Dispatch is delegated to ``init_chat_model``, which picks the right
        vendor class based on the ``provider:name`` prefix in the model id.
        """
        # Lazy: langchain is an optional extra; importing at module level would
        # break common.ai for users who haven't installed `[langchain]`.
        from langchain.chat_models import init_chat_model

        conn = self.get_connection(self.llm_conn_id)
        model_id = self._resolve_model_id(
            conn.extra_dejson,
            constructor_value=self.llm_model,
            extra_key="model",
            kind="chat",
        )
        return init_chat_model(model_id, **self._connection_kwargs(conn))

    def get_embedding_model(self) -> Embeddings:
        """
        Return a LangChain embedding model configured from the Airflow connection.

        Dispatch is delegated to ``init_embeddings``, which picks the right
        vendor class based on the ``provider:name`` prefix in the model id.
        Uses ``embed_conn_id`` if set (falls back to ``llm_conn_id``).
        """
        from langchain.embeddings import init_embeddings

        conn = self.get_connection(self.embed_conn_id)
        model_id = self._resolve_model_id(
            conn.extra_dejson,
            constructor_value=self.embed_model,
            extra_key="embed_model",
            kind="embedding",
        )
        return init_embeddings(model_id, **self._connection_kwargs(conn))
