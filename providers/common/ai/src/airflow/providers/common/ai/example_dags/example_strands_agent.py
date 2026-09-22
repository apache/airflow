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
Run a Strands Agents agent in an Airflow task, with Airflow's SQL toolset as its tools.

The agent is plain Strands: you build the model and the ``Agent`` yourself and call
it as you would anywhere else. Airflow supplies two things. The model's API key comes
from an Airflow connection, and
:func:`~airflow.providers.common.ai.tools.strands.as_strands_tools` turns a
``SQLToolset`` into Strands tools, with read-only SQL validation and bounded
results, and with Airflow's secret masker applied to every tool result.

Before running:

1. Install Strands: ``pip install "strands-agents>=1.56.0"``. The Anthropic model
   below also needs ``anthropic``, which the ``anthropic`` extra of this provider
   installs.
2. Create a connection (``LLM_CONN_ID``, default ``anthropic_default``) whose
   password is an Anthropic API key. Set its host only to route through a gateway.
3. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose
   hook is a ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
"""

from __future__ import annotations

import os

from airflow.providers.common.compat.sdk import dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "anthropic_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "claude-sonnet-5")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_strands_agent]
@dag(tags=["example"])
def example_strands_agent():
    """Answer a question about a database with a Strands agent."""

    @task
    def run_strands_agent(question: str = DEFAULT_QUESTION) -> str:
        from strands import Agent
        from strands.models.anthropic import AnthropicModel

        from airflow.providers.common.ai.tools.strands import as_strands_tools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset
        from airflow.providers.common.compat.sdk import BaseHook

        llm = BaseHook.get_connection(LLM_CONN_ID)
        model = AnthropicModel(
            client_args={"api_key": llm.password, "base_url": llm.host or None},
            model_id=LLM_MODEL,
            max_tokens=2048,
        )
        agent = Agent(
            model=model,
            tools=as_strands_tools(SQLToolset(db_conn_id=DB_CONN_ID)),
            system_prompt=(
                "You are a SQL analyst. Use list_tables and get_schema to explore "
                "the database, then run read-only queries to answer the question."
            ),
            # Strands streams the reply to stdout by default; the task returns it instead.
            callback_handler=None,
        )
        return str(agent(question))

    run_strands_agent()


# [END example_strands_agent]


example_strands_agent()
