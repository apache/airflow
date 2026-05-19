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


class CrewAIHook(BaseHook):
    """
    Bridge Airflow connections to CrewAI model constructors.

    Reuses the ``pydanticai`` connection type so users configure a single
    connection for PydanticAI operators, LlamaIndex tasks, LangChain tasks,
    and CrewAI tasks.

    :param llm_conn_id: Airflow connection ID for the LLM provider.
    :param llm_model: Model name in LiteLLM format (e.g. ``openai/gpt-4o``,
        ``anthropic/claude-sonnet-4-20250514``).  Required for :meth:`get_llm`.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "pydanticai_default"
    conn_type = "pydanticai"
    hook_name = "CrewAI"

    def __init__(
        self,
        llm_conn_id: str = "pydanticai_default",
        llm_model: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id
        self.llm_model = llm_model

    def _resolve_connection_kwargs(self, conn_id: str) -> dict[str, Any]:
        """Extract API key and base URL from an Airflow connection."""
        conn = self.get_connection(conn_id)
        kwargs: dict[str, Any] = {}
        if conn.password:
            kwargs["api_key"] = conn.password
        if conn.host:
            kwargs["base_url"] = conn.host
        return kwargs

    def get_llm(self) -> LLM:
        """
        Return a CrewAI LLM configured from the Airflow connection.

        Requires ``llm_model`` to be set on the hook.
        """
        if not self.llm_model:
            raise ValueError("llm_model must be set to use get_llm()")

        from crewai import LLM

        conn_kwargs = self._resolve_connection_kwargs(self.llm_conn_id)
        return LLM(model=self.llm_model, **conn_kwargs)
