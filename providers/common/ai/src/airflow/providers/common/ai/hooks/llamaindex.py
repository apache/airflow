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
"""Hook for LlamaIndex integration with Airflow connections."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseHook,
)

if TYPE_CHECKING:
    from llama_index.core.base.embeddings.base import BaseEmbedding
    from llama_index.core.llms.llm import LLM


class LlamaIndexHook(BaseHook):
    """
    Bridge an Airflow connection to LlamaIndex chat and embedding models.

    The hook resolves credentials (API key, optional API base URL) from the
    Airflow connection and returns native LlamaIndex objects ready to pass
    to ``VectorStoreIndex(..., embed_model=...)``,
    ``load_index_from_storage(..., embed_model=...)``, or
    ``index.as_retriever(..., llm=...)``.

    LlamaIndex does not ship a universal ``init_chat_model`` /
    ``init_embedding_model`` equivalent (each vendor is a separate package
    under ``llama-index-llms-*`` / ``llama-index-embeddings-*`` with its own
    constructor kwargs). The hook therefore covers the OpenAI-compatible
    surface that matches LlamaIndex's own ``resolve_embed_model("default")``
    behaviour. For other vendors (Cohere, Bedrock, Vertex, HuggingFace, ...)
    instantiate the LlamaIndex class directly in your ``@task`` and pass it
    to the operator's ``embed_model=`` / ``llm=`` parameter -- both
    ``LlamaIndexEmbeddingOperator`` and ``LlamaIndexRetrievalOperator`` accept a pre-built
    ``BaseEmbedding`` / ``LLM`` instance and bypass the hook in that case.

    .. note::

        The hook deliberately does **not** mutate LlamaIndex's global
        ``Settings`` singleton. Operators pass the resolved model directly
        to LlamaIndex constructors so concurrent tasks in the same worker
        don't race on shared state.

    Connection fields:

    * **password**: API key passed as ``api_key=``.
    * **host**: Optional base URL passed as ``api_base=`` (custom endpoints,
      Ollama, vLLM).
    * **extra** JSON: ``{"embed_model": "text-embedding-3-small",
      "llm_model": "gpt-4o"}`` -- default model identifiers stored on the
      connection.

    :param llm_conn_id: Airflow connection ID for the LLM provider. Falls
        back to :attr:`default_conn_name` (``"llamaindex_default"``) when
        not provided.
    :param embed_conn_id: Optional separate Airflow connection ID for the
        embedding provider. Falls back to ``llm_conn_id`` when not set.
    :param embed_model: Embedding model name (e.g.
        ``"text-embedding-3-small"``). Overrides ``extra["embed_model"]``
        on the connection.
    :param llm_model: LLM model name (e.g. ``"gpt-4o"``). Overrides
        ``extra["llm_model"]`` on the connection. Required when calling
        :meth:`get_llm`.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "llamaindex_default"
    conn_type = "llamaindex"
    hook_name = "LlamaIndex"

    def __init__(
        self,
        llm_conn_id: str | None = None,
        embed_conn_id: str | None = None,
        embed_model: str | None = None,
        llm_model: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        # Resolve at runtime so a future per-vendor subclass with its own
        # ``default_conn_name`` is honoured.
        self.llm_conn_id = llm_conn_id if llm_conn_id is not None else self.default_conn_name
        self.embed_conn_id = embed_conn_id if embed_conn_id is not None else self.llm_conn_id
        self.embed_model = embed_model
        self.llm_model = llm_model

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {
                "host": "https://api.openai.com/v1 (optional, for custom endpoints / Ollama)",
                "extra": '{"embed_model": "text-embedding-3-small", "llm_model": "gpt-4o"}',
            },
        }

    @staticmethod
    def _resolve_model(
        conn_extra: dict[str, Any],
        *,
        constructor_value: str | None,
        extra_key: str,
        kind: str,
    ) -> str:
        """Resolve a model identifier from the constructor arg or connection extra."""
        model_id = constructor_value or conn_extra.get(extra_key)
        if not model_id:
            raise ValueError(
                f"No {kind} model identifier set. Pass {extra_key}= to the hook "
                f'constructor or set extra={{"{extra_key}": "model-name"}} on '
                "the connection."
            )
        return model_id

    @staticmethod
    def _connection_kwargs(conn: Any) -> dict[str, Any]:
        """Return shared OpenAI-style kwargs (api_key, api_base) from the connection."""
        kwargs: dict[str, Any] = {}
        if conn.password:
            kwargs["api_key"] = conn.password
        if conn.host:
            kwargs["api_base"] = conn.host
        return kwargs

    def get_embedding_model(self) -> BaseEmbedding:
        """
        Return a LlamaIndex embedding model configured from the Airflow connection.

        Uses ``embed_conn_id`` (falls back to ``llm_conn_id``) for credentials.
        Returns an ``OpenAIEmbedding`` instance; for other vendors,
        instantiate the LlamaIndex class directly and pass it to the
        operator's ``embed_model=`` parameter.
        """
        # Lazy: llama-index is an optional extra; importing at module level
        # would break common.ai for users who haven't installed ``[llamaindex]``.
        try:
            from llama_index.embeddings.openai import OpenAIEmbedding
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        conn = self.get_connection(self.embed_conn_id)
        model_id = self._resolve_model(
            conn.extra_dejson,
            constructor_value=self.embed_model,
            extra_key="embed_model",
            kind="embedding",
        )
        return OpenAIEmbedding(model=model_id, **self._connection_kwargs(conn))

    def get_llm(self) -> LLM:
        """
        Return a LlamaIndex LLM configured from the Airflow connection.

        Returns an ``OpenAI`` LLM instance; for other vendors, instantiate
        the LlamaIndex class directly and pass it to the operator's ``llm=``
        parameter.
        """
        try:
            from llama_index.llms.openai import OpenAI
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        conn = self.get_connection(self.llm_conn_id)
        model_id = self._resolve_model(
            conn.extra_dejson,
            constructor_value=self.llm_model,
            extra_key="llm_model",
            kind="llm",
        )
        return OpenAI(model=model_id, **self._connection_kwargs(conn))
