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

import asyncio
import os
from typing import Any

from google.adk.agents import Agent as ADKAgent
from google.adk.runners import Runner as ADKRunner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types

from airflow.providers.common.compat.sdk import BaseHook


class AdkHook(BaseHook):
    """
    Hook for LLM access via Google Agent Development Kit (ADK).

    Manages connection credentials and agent creation/execution.  Uses
    Google's ADK to orchestrate multi-turn agent interactions with Gemini
    models.

    Connection fields:
        - **password**: Google API key (for Gemini API access)
        - **host**: Custom endpoint URL (optional — for custom/proxy endpoints)

    Cloud authentication via Application Default Credentials (ADC) is used
    when no API key is provided.  Configure ``GOOGLE_APPLICATION_CREDENTIALS``
    or run ``gcloud auth application-default login``.

    :param llm_conn_id: Airflow connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"gemini-2.5-flash"``).
        Overrides the model stored in the connection's extra field.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "adk_default"
    conn_type = "adk"
    hook_name = "Google ADK"

    def __init__(
        self,
        llm_conn_id: str = default_conn_name,
        model_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self._configured: bool = False

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {
                "host": "(optional — for custom/proxy endpoints)",
            },
        }

    def get_conn(self) -> str:
        """
        Configure credentials and return the model identifier.

        Reads API key from connection password and model from (in priority
        order):

        1. ``model_id`` parameter on the hook
        2. ``extra["model"]`` on the connection

        When an API key is found in the connection, it is set as the
        ``GOOGLE_API_KEY`` environment variable so the google-genai SDK
        picks it up automatically.

        The result is cached for the lifetime of this hook instance.

        :return: The resolved model identifier string.
        """
        if self._configured:
            return self.model_id

        if self.llm_conn_id:
            try:
                conn = self.get_connection(self.llm_conn_id)
                api_key = conn.password
                if api_key:
                    os.environ.setdefault("GOOGLE_API_KEY", api_key)
                model_name = self.model_id or conn.extra_dejson.get("model", "")
                if model_name:
                    self.model_id = model_name
            except Exception:
                # Connection not found — fall back to env-based auth
                pass

        if not self.model_id:
            raise ValueError(
                "No model specified. Set model_id on the hook or the Model field on the connection."
            )

        self._configured = True
        return self.model_id

    def create_agent(
        self,
        *,
        name: str = "airflow_agent",
        instruction: str = "",
        tools: list | None = None,
        **agent_kwargs: Any,
    ) -> ADKAgent:
        """
        Create an ADK Agent configured with this hook's model.

        :param name: Agent name (default: ``"airflow_agent"``).
        :param instruction: System-level instructions for the agent.
        :param tools: List of callables exposed as tools to the LLM.
        :param agent_kwargs: Additional keyword arguments passed to the
            ADK ``Agent`` constructor.
        :return: A configured ``google.adk.agents.Agent`` instance.
        """
        self.get_conn()
        return ADKAgent(
            name=name,
            model=self.model_id,
            instruction=instruction,
            tools=tools or [],
            **agent_kwargs,
        )

    def run_agent_sync(self, *, agent: ADKAgent, prompt: str) -> str:
        """
        Run an ADK agent synchronously and return the final response text.

        Creates an in-memory session, submits the prompt, iterates through
        the async event stream, and collects the final response.

        :param agent: A configured ADK Agent instance.
        :param prompt: The user prompt to send to the agent.
        :return: The concatenated text of the agent's final response.
        """
        session_service = InMemorySessionService()
        runner = ADKRunner(
            agent=agent,
            app_name="airflow",
            session_service=session_service,
        )

        async def _run() -> str:
            session = await session_service.create_session(app_name="airflow", user_id="airflow")
            content = genai_types.Content(
                role="user",
                parts=[genai_types.Part(text=prompt)],
            )
            final_text = ""
            async for event in runner.run_async(
                user_id="airflow",
                session_id=session.id,
                new_message=content,
            ):
                if event.is_final_response() and event.content and event.content.parts:
                    for part in event.content.parts:
                        if part.text:
                            final_text += part.text
            return final_text

        return asyncio.run(_run())

    def test_connection(self) -> tuple[bool, str]:
        """
        Test connection by resolving the model.

        Validates that the model string is set and the google-adk package
        is installed.  Does NOT make an LLM API call — that would be
        expensive, flaky, and fail for reasons unrelated to connectivity.
        """
        try:
            model = self.get_conn()
            return True, f"Model resolved successfully: {model}"
        except Exception as e:
            return False, str(e)
