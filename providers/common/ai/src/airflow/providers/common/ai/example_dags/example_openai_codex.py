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
Answer a question about a database with the OpenAI Codex SDK's agent loop, in an Airflow task.

The agent loop is Codex's own, run by the ``codex app-server`` subprocess that ``AsyncCodex``
starts. Airflow supplies the API key, from a connection, and the tools:
:class:`~airflow.providers.common.ai.tools.openai_codex.AirflowTools` serves a ``SQLToolset``
to Codex through an MCP server on ``127.0.0.1`` in the task's event loop, with read-only SQL
validation, bounded results and Airflow's secret masker applied to every result.
``AirflowTools.serve`` and ``build_thread_options`` also point Codex at empty temporary
directories instead of the worker's ``~/.codex``, ask Codex to turn off its own shell and the
other built-in tools that do not belong here, and run the thread in a read-only sandbox that
never asks for approval.

This example is experimental and was not run against a model: the tool round trip, that the
built-in tools are gone from the model's tool list, and that the sandbox refuses writes still
need to be checked by hand; see the guide.

Before running:

1. Install this provider's ``openai-codex`` extra:
   ``pip install "apache-airflow-providers-common-ai[openai-codex]"``.
2. Create a connection (``LLM_CONN_ID``, default ``openai_default``) whose password is
   an OpenAI API key.
3. Create a database connection (``DB_CONN_ID``, default ``sql_default``) whose hook is a
   ``DbApiHook`` (e.g. SQLite, Postgres, MySQL).
"""

from __future__ import annotations

import os

from airflow.providers.common.compat.sdk import BaseHook, dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "openai_default")
# Unset, Codex uses its own default model.
LLM_MODEL = os.environ.get("LLM_MODEL")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")

DEFAULT_QUESTION = "Which tables exist, and how many rows does each contain?"


# [START example_openai_codex]
@dag(tags=["example"])
def example_openai_codex():
    """Answer a question about a database with the OpenAI Codex SDK's agent loop."""

    @task
    def ask_the_warehouse(question: str = DEFAULT_QUESTION) -> str | None:
        import asyncio

        from openai_codex import AsyncCodex

        from airflow.providers.common.ai.tools.openai_codex import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        llm = BaseHook.get_connection(LLM_CONN_ID)
        tools = AirflowTools(SQLToolset(db_conn_id=DB_CONN_ID))

        async def ask():
            async with tools.serve() as config, AsyncCodex(config) as codex:
                await codex.login_api_key(llm.password)
                thread = await codex.thread_start(
                    **tools.build_thread_options(
                        model=LLM_MODEL,
                        developer_instructions=(
                            "You are a SQL analyst. Use list_tables and get_schema to explore the "
                            "database, then run read-only queries to answer the question."
                        ),
                    )
                )
                return await tools.run(await thread.turn(question))

        return asyncio.run(ask()).final_response

    ask_the_warehouse()


# [END example_openai_codex]


example_openai_codex()
