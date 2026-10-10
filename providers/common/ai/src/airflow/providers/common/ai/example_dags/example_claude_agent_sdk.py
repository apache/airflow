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
Answer a question about a database with the Claude Agent SDK's agent loop, in an Airflow task.

The agent loop is the SDK's own ``query()``, which runs the bundled Claude Code CLI as a
subprocess. Airflow supplies the API key, from a connection, and the tools:
:class:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools` serves a
``SQLToolset`` to the CLI through an in-process MCP server, with read-only SQL validation,
bounded results and Airflow's secret masker applied to every result. ``AirflowTools.build_options``
also turns off the CLI's built-in tools and every settings source on the worker, so the only
tools the model can call are Airflow's.

Before running:

1. Install this provider's ``claude-agent-sdk`` extra:
   ``pip install "apache-airflow-providers-common-ai[claude-agent-sdk]"``.
2. Create a connection (``LLM_CONN_ID``, default ``anthropic_default``) whose password is
   an Anthropic API key.
3. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose hook is a
   ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
"""

from __future__ import annotations

import os

from claude_agent_sdk import query

from airflow.providers.common.ai.tools.claude_agent_sdk import AirflowTools
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import BaseHook, dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "anthropic_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "claude-sonnet-5")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_claude_agent_sdk]
@dag(tags=["example"])
def example_claude_agent_sdk():
    """Answer a question about a database with the Claude Agent SDK's agent loop."""

    @task
    async def ask_the_warehouse(question: str = DEFAULT_QUESTION) -> str | None:
        llm = BaseHook.get_connection(LLM_CONN_ID)
        tools = AirflowTools(SQLToolset(db_conn_id=DB_CONN_ID))
        options = tools.build_options(
            model=LLM_MODEL,
            system_prompt=(
                "You are a SQL analyst. Use list_tables and get_schema to explore the "
                "database, then run read-only queries to answer the question."
            ),
            # Without a limit the model can keep asking for tool calls indefinitely if it
            # never settles on an answer; see "When a tool fails" in the guide.
            max_turns=10,
            env={"ANTHROPIC_API_KEY": llm.password},
        )
        result = await tools.run(query(prompt=question, options=options))
        return result.result

    ask_the_warehouse()


# [END example_claude_agent_sdk]


example_claude_agent_sdk()
