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

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from llama_index.core.base.embeddings.base import BaseEmbedding
    from llama_index.core.llms.llm import LLM


class LlamaIndexHook(BaseHook):
    """
    Bridge Airflow connections to LlamaIndex's Settings singleton.

    Reuses the ``pydanticai`` connection type so users configure a single
    connection for both pydantic-ai operators and LlamaIndex operators.

    :param llm_conn_id: Airflow connection ID for the LLM/embedding provider.
    :param embed_conn_id: Separate connection for embeddings. Defaults to
        ``llm_conn_id`` when not provided.
    :param embed_model: Embedding model name (e.g. ``text-embedding-3-small``).
    :param llm_model: LLM model name (e.g. ``gpt-4o``). Only needed when
        configuring ``Settings.llm``.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "pydanticai_default"
    conn_type = "pydanticai"
    hook_name = "LlamaIndex"

    def __init__(
        self,
        llm_conn_id: str = "pydanticai_default",
        embed_conn_id: str | None = None,
        embed_model: str = "text-embedding-3-small",
        llm_model: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id
        self.embed_conn_id = embed_conn_id or llm_conn_id
        self.embed_model = embed_model
        self.llm_model = llm_model

    def _resolve_connection_kwargs(self, conn_id: str) -> dict[str, Any]:
        """Extract API key and base URL from an Airflow connection."""
        conn = self.get_connection(conn_id)
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
        """
        from llama_index.embeddings.openai import OpenAIEmbedding

        conn_kwargs = self._resolve_connection_kwargs(self.embed_conn_id)
        return OpenAIEmbedding(model=self.embed_model, **conn_kwargs)

    def get_llm(self) -> LLM:
        """
        Return a LlamaIndex LLM configured from the Airflow connection.

        Requires ``llm_model`` to be set on the hook.
        """
        if not self.llm_model:
            raise ValueError("llm_model must be set to use get_llm()")

        from llama_index.llms.openai import OpenAI

        conn_kwargs = self._resolve_connection_kwargs(self.llm_conn_id)
        return OpenAI(model=self.llm_model, **conn_kwargs)

    def configure_settings(self) -> None:
        """
        Configure LlamaIndex's global Settings with models from Airflow connections.

        Sets ``Settings.embed_model`` always, and ``Settings.llm`` when
        ``llm_model`` is provided.
        """
        from llama_index.core import Settings

        Settings.embed_model = self.get_embedding_model()
        if self.llm_model:
            Settings.llm = self.get_llm()
