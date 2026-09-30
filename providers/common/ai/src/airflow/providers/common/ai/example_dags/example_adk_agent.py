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
"""
Run a Google ADK agent in an Airflow task, with Airflow's SQL toolset as its tools.

The agent is plain ADK: you build the model, the ``LlmAgent`` and the runner
yourself. Airflow supplies two things. The model's API key comes from an Airflow
connection, and :class:`~airflow.providers.common.ai.tools.adk.AirflowTools` gives
the agent a ``SQLToolset``'s tools, with read-only SQL validation and bounded
results, and with Airflow's secret masker applied to every tool result.

Before running:

1. Install ADK: ``pip install "google-adk>=2.9.1"``, without Airflow's constraints file.
   ADK caps OpenTelemetry at 1.42.1, below the version that file pins; Airflow itself
   accepts it. The Anthropic model below also needs ``anthropic``, which the
   ``anthropic`` extra of this provider installs.
2. Create a connection (``LLM_CONN_ID``, default ``anthropic_default``) whose
   password is an Anthropic API key. Set its host only to route through a gateway.
3. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose
   hook is a ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
"""

from __future__ import annotations

import asyncio
import os

from airflow.providers.common.compat.sdk import BaseHook, dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "anthropic_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "claude-sonnet-5")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_adk_agent]
@dag(tags=["example"])
def example_adk_agent():
    """Answer a question about a database with a Google ADK agent."""

    @task
    def run_adk_agent(question: str = DEFAULT_QUESTION) -> str:
        from anthropic import AsyncAnthropic
        from google.adk.agents import LlmAgent
        from google.adk.models.anthropic_llm import AnthropicLlm
        from google.adk.runners import InMemoryRunner
        from google.genai import types

        from airflow.providers.common.ai.tools.adk import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        llm = BaseHook.get_connection(LLM_CONN_ID)
        agent = LlmAgent(
            name="analyst",
            model=AnthropicLlm(
                model=LLM_MODEL,
                client=AsyncAnthropic(api_key=llm.password, base_url=llm.host or None),
            ),
            instruction=(
                "You are a SQL analyst. Use list_tables and get_schema to explore "
                "the database, then run read-only queries to answer the question."
            ),
            tools=[AirflowTools(SQLToolset(db_conn_id=DB_CONN_ID))],
        )

        async def ask() -> str:
            runner = InMemoryRunner(agent=agent, app_name="airflow")
            session = await runner.session_service.create_session(app_name="airflow", user_id="airflow")
            message = types.Content(role="user", parts=[types.Part(text=question)])
            answer = ""
            async for event in runner.run_async(
                user_id="airflow", session_id=session.id, new_message=message
            ):
                if event.is_final_response() and event.content and event.content.parts:
                    answer = "".join(part.text or "" for part in event.content.parts)
            return answer

        return asyncio.run(ask())

    run_adk_agent()


# [END example_adk_agent]


example_adk_agent()
