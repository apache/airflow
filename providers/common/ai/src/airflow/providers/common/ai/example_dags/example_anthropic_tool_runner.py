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
Answer a question about a database with the Anthropic SDK's tool runner, in an Airflow task.

The agent loop is the Anthropic SDK's own ``client.beta.messages.tool_runner``. Airflow
supplies the API key, from a connection, and the tools:
:class:`~airflow.providers.common.ai.tools.anthropic.AirflowTools` turns a ``SQLToolset``
into runner tools, with read-only SQL validation, bounded results and Airflow's secret
masker applied to every result.

Before running:

1. Install this provider's ``anthropic`` extra:
   ``pip install "apache-airflow-providers-common-ai[anthropic]"``.
2. Create a connection (``LLM_CONN_ID``, default ``anthropic_default``) whose password is
   an Anthropic API key. Set its host only to route through a gateway.
3. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose hook is a
   ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
"""

from __future__ import annotations

import os

from airflow.providers.common.compat.sdk import BaseHook, dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "anthropic_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "claude-opus-5-5")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_anthropic_tool_runner]
@dag(tags=["example"])
def example_anthropic_tool_runner():
    """Answer a question about a database with the Anthropic SDK's tool runner."""

    @task
    def ask_the_warehouse(question: str = DEFAULT_QUESTION) -> str:
        from anthropic import Anthropic

        from airflow.providers.common.ai.tools.anthropic import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        llm = BaseHook.get_connection(LLM_CONN_ID)
        client = Anthropic(api_key=llm.password, base_url=llm.host or None)
        tools = AirflowTools(SQLToolset(db_conn_id=DB_CONN_ID))
        runner = client.beta.messages.tool_runner(
            model=LLM_MODEL,
            max_tokens=16000,
            system="You are a SQL analyst. Explore the schema before you query, and only read.",
            tools=tools.tools,
            messages=[{"role": "user", "content": question}],
            # The runner has no limit of its own on model requests.
            max_iterations=10,
        )
        message = tools.run(runner)
        return "".join(block.text for block in message.content if block.type == "text")

    ask_the_warehouse()


# [END example_anthropic_tool_runner]


example_anthropic_tool_runner()
