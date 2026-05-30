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
Expose an Airflow toolset to a LangChain agent (the reverse bridge).

common.ai's curated toolsets (``SQLToolset``, ``HookToolset``, ``MCPToolset``)
are pydantic-ai toolsets, so ``AgentOperator`` uses them natively. This example
shows the *reverse* direction: convert a ``SQLToolset`` into LangChain
``StructuredTool`` objects with
:func:`~airflow.providers.common.ai.toolsets.langchain_bridge.airflow_toolset_to_langchain_tools`
and hand them to a LangChain ReAct agent built with ``create_agent``. The agent
gets Airflow's managed connections and read-only SQL validation for free.

**Forward direction** (LangChain tools -> ``AgentOperator``): no Airflow code
is needed. pydantic-ai ships ``pydantic_ai.ext.langchain.LangChainToolset``
upstream, so ``LangChainToolset([my_langchain_tool])`` drops straight into
``AgentOperator(toolsets=[...])``. See https://ai.pydantic.dev for details.

Before running:

1. Install LangChain: ``pip install "apache-airflow-providers-common-ai[langchain]" langchain-openai``
2. Create a ``langchain`` connection named ``langchain_default`` (set
   ``password`` to your API key) for the model.
3. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose
   hook is a ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
"""

from __future__ import annotations

import os

from airflow.providers.common.compat.sdk import dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "langchain_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "openai:gpt-4o")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_langchain_toolset_bridge]
@dag(tags=["example"])
def example_langchain_toolset_bridge():
    """Run a LangChain SQL agent backed by Airflow's curated ``SQLToolset``."""

    @task
    def run_sql_agent(question: str = DEFAULT_QUESTION) -> str:
        from langchain.agents import create_agent

        from airflow.providers.common.ai.hooks.langchain import LangChainHook
        from airflow.providers.common.ai.toolsets import airflow_toolset_to_langchain_tools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        # Airflow's curated, read-only SQL toolset, exposed as LangChain tools.
        # The bridge carries each tool's name, description, and args schema, and
        # routes calls back through SQLToolset (connection resolution + SQL
        # validation included).
        tools = airflow_toolset_to_langchain_tools(SQLToolset(db_conn_id=DB_CONN_ID))

        model = LangChainHook(llm_conn_id=LLM_CONN_ID, llm_model=LLM_MODEL).get_chat_model()
        agent = create_agent(
            model,
            tools=tools,
            system_prompt=(
                "You are a SQL analyst. Use list_tables and get_schema to explore "
                "the database, then run read-only queries to answer the question."
            ),
        )

        result = agent.invoke({"messages": [{"role": "user", "content": question}]})
        return result["messages"][-1].content

    run_sql_agent()


# [END example_langchain_toolset_bridge]


example_langchain_toolset_bridge()
