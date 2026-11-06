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
Run an OpenAI Agents SDK agent in an Airflow task, with Airflow's SQL toolset as its tools.

The agent is plain OpenAI Agents SDK: you build the model, the ``Agent`` and the run
yourself. Airflow supplies three things. The model's API key comes from an Airflow
connection; :class:`~airflow.providers.common.ai.tools.openai_agents.AirflowTools`
gives the agent a ``SQLToolset``'s tools, with read-only SQL validation and bounded
results, and with Airflow's secret masker applied to every tool result; and
:class:`~airflow.providers.common.ai.tools.openai_agents.AirflowTraceProcessor` records
the SDK's traces as the task's OpenTelemetry spans instead of sending them to OpenAI.

Before running:

1. Install the SDK: ``pip install "openai-agents>=0.20.0"``.
2. Create a connection (``LLM_CONN_ID``, default ``openai_default``) whose password is
   an OpenAI API key. Set its host to the base URL of any other OpenAI-compatible
   endpoint, such as a gateway or a self-hosted server.
3. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose
   hook is a ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
"""

from __future__ import annotations

import os

from airflow.providers.common.compat.sdk import BaseHook, dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "openai_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-5.6-luna")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_openai_agents_agent]
@dag(tags=["example"])
def example_openai_agents_agent():
    """Answer a question about a database with an OpenAI Agents SDK agent."""

    @task
    def run_openai_agent(question: str = DEFAULT_QUESTION) -> str:
        from agents import Agent, OpenAIChatCompletionsModel, Runner, set_trace_processors
        from openai import AsyncOpenAI

        from airflow.providers.common.ai.tools.openai_agents import AirflowTools, AirflowTraceProcessor
        from airflow.providers.common.ai.tools.tracing import agent_framework_tracing
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        # Record the SDK's traces as the task's OpenTelemetry spans instead of sending them to OpenAI.
        set_trace_processors([AirflowTraceProcessor()])

        llm = BaseHook.get_connection(LLM_CONN_ID)
        agent = Agent(
            name="analyst",
            model=OpenAIChatCompletionsModel(
                model=LLM_MODEL,
                openai_client=AsyncOpenAI(api_key=llm.password, base_url=llm.host or None),
            ),
            instructions=(
                "You are a SQL analyst. Use list_tables and get_schema to explore "
                "the database, then run read-only queries to answer the question."
            ),
            tools=AirflowTools(SQLToolset(db_conn_id=DB_CONN_ID)),
        )
        with agent_framework_tracing():
            return Runner.run_sync(agent, question).final_output

    run_openai_agent()


# [END example_openai_agents_agent]


example_openai_agents_agent()
