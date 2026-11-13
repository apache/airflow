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
Run an OpenAI Agents SDK agent whose completed model and tool calls a task retry replays.

The agent is the one in ``example_openai_agents_agent``: a model whose client comes from
an Airflow connection, and a ``SQLToolset``'s tools through
:class:`~airflow.providers.common.ai.tools.openai_agents.AirflowTools`.
:func:`~airflow.providers.common.ai.durable.openai_agents.run_durably` runs it, so
when the task fails and Airflow retries it, the calls the failed try completed return
what they returned then instead of running again.

Before running:

1. Install the SDK and the OpenAI provider:
   ``pip install "openai-agents>=0.20.0" "apache-airflow-providers-openai>=2.2.0"``.
2. Create an OpenAI connection (``LLM_CONN_ID``, default ``openai_default``) whose
   password is an OpenAI API key. Set its host to the base URL of any other
   OpenAI-compatible endpoint, such as a gateway or a self-hosted server.
3. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose
   hook is a ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
4. On Airflow before 3.3, set ``[common.ai] durable_cache_path`` to where the journal
   is kept; from 3.3 on it is kept in the task state store.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.common.compat.sdk import dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "openai_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-5.6-luna")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_openai_agents_durable]
@dag(tags=["example"])
def example_openai_agents_durable():
    """Answer a question about a database with an OpenAI Agents SDK agent that survives task retries."""

    @task(retries=2, retry_delay=timedelta(minutes=1))
    async def run_durable_openai_agent(question: str = DEFAULT_QUESTION) -> str:
        from agents import Agent, OpenAIChatCompletionsModel, RunConfig

        from airflow.providers.common.ai.durable.openai_agents import run_durably
        from airflow.providers.common.ai.tools.openai_agents import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset
        from airflow.providers.openai.hooks.openai import OpenAIHook

        async with await OpenAIHook(LLM_CONN_ID).aget_conn() as client:
            agent = Agent(
                name="analyst",
                model=OpenAIChatCompletionsModel(model=LLM_MODEL, openai_client=client),
                instructions=(
                    "You are a SQL analyst. Use list_tables and get_schema to explore "
                    "the database, then run read-only queries to answer the question."
                ),
                tools=AirflowTools(SQLToolset(db_conn_id=DB_CONN_ID)),
            )
            # The SDK sends traces to OpenAI by default; see the guide's tracing section.
            result = await run_durably(agent, question, run_config=RunConfig(tracing_disabled=True))
        return result.final_output

    run_durable_openai_agent()


# [END example_openai_agents_durable]


example_openai_agents_durable()
