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
Run a Strands Agents agent that a task retry resumes from its last checkpoint.

The agent is the one in ``example_strands_agent``: an Anthropic model whose API key
comes from an Airflow connection, and a ``SQLToolset``'s tools through the
:class:`~airflow.providers.common.ai.tools.strands.AirflowTools` plugin.
:func:`~airflow.providers.common.ai.durable.strands.invoke_durably` runs it, so when
the task fails and Airflow retries it, the agent carries on from the last cycle it
finished instead of starting over.

Before running:

1. Use Airflow 3.3 or later, where the agent's state is kept in the task state store.
2. Install Strands: ``pip install "strands-agents>=1.56.0"``. The Anthropic model
   below also needs ``anthropic``, which the ``anthropic`` extra of this provider
   installs.
3. Create a connection (``LLM_CONN_ID``, default ``anthropic_default``) whose
   password is an Anthropic API key. Set its host only to route through a gateway.
4. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose
   hook is a ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
"""

from __future__ import annotations

import os
from datetime import timedelta
from functools import partial

from airflow.providers.common.compat.sdk import BaseHook, dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "anthropic_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "claude-sonnet-5")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_strands_durable]
@dag(tags=["example"])
def example_strands_durable():
    """Answer a question about a database with a Strands agent that survives task retries."""

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def run_durable_strands_agent(question: str = DEFAULT_QUESTION) -> str:
        from strands import Agent
        from strands.models.anthropic import AnthropicModel

        from airflow.providers.common.ai.durable.strands import invoke_durably
        from airflow.providers.common.ai.tools.strands import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        llm = BaseHook.get_connection(LLM_CONN_ID)
        model = AnthropicModel(
            client_args={"api_key": llm.password, "base_url": llm.host or None},
            model_id=LLM_MODEL,
            max_tokens=2048,
        )
        # invoke_durably builds the agent, adding the session manager and checkpointing.
        agent = partial(
            Agent,
            model=model,
            plugins=[AirflowTools(SQLToolset(db_conn_id=DB_CONN_ID))],
            system_prompt=(
                "You are a SQL analyst. Use list_tables and get_schema to explore "
                "the database, then run read-only queries to answer the question."
            ),
            callback_handler=None,
        )
        return str(invoke_durably(agent, question))

    run_durable_strands_agent()


# [END example_strands_durable]


example_strands_durable()
