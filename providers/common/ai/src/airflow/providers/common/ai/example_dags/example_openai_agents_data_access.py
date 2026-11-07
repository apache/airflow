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
Give an OpenAI Agents SDK agent read access to two warehouse tables and one prefix of a bucket.

Each toolset logs in through an Airflow connection the agent never sees, and limits what
the agent can reach with it: ``SQLToolset`` to two tables, read-only, and
``ObjectStorageToolset`` to one prefix of a bucket.
:class:`~airflow.providers.common.ai.tools.openai_agents.AirflowTools` hands both to the
agent as function tools.

Before running:

1. Install the SDK and the provider the bucket needs:
   ``pip install "openai-agents>=0.20.0" "apache-airflow-providers-amazon[s3fs]"``.
2. Create a connection (``LLM_CONN_ID``, default ``openai_default``) whose password is
   an OpenAI API key. Set its host to the base URL of any other OpenAI-compatible
   endpoint, such as a gateway or a self-hosted server.
3. Create the two data connections: ``warehouse_agent_reader`` (a ``DbApiHook``
   database, such as Postgres) and ``aws_reports_reader`` (AWS). Give each credentials
   that can read only what the agent needs.
"""

from __future__ import annotations

import os

from airflow.providers.common.compat.sdk import BaseHook, dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "openai_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-5.6-luna")

DEFAULT_QUESTION = "Why did EMEA revenue drop in October?"


# [START example_openai_agents_data_access]
@dag(tags=["example"])
def example_openai_agents_data_access():
    """Answer a revenue question from the warehouse and the finance reports."""

    @task
    def explain_revenue(question: str = DEFAULT_QUESTION) -> str:
        # The SDK is an optional dependency, so it is imported when the task runs, not at parse time.
        from agents import Agent, OpenAIChatCompletionsModel, RunConfig, Runner
        from openai import AsyncOpenAI

        from airflow.providers.common.ai.tools.openai_agents import AirflowTools
        from airflow.providers.common.ai.toolsets import ObjectStorageToolset, SQLToolset

        warehouse = SQLToolset(
            # A role granted SELECT on these two tables only.
            db_conn_id="warehouse_agent_reader",
            # Refuse a query that reaches any other table.
            allowed_tables=["orders", "customers"],
            max_rows=100,
        )
        reports = ObjectStorageToolset(
            # Every path the model asks for is relative to this prefix.
            "s3://acme-reports/finance/",
            conn_id="aws_reports_reader",
            max_read_bytes=1024 * 1024,
        )

        llm = BaseHook.get_connection(LLM_CONN_ID)
        agent = Agent(
            name="revenue_analyst",
            model=OpenAIChatCompletionsModel(
                model=LLM_MODEL,
                openai_client=AsyncOpenAI(api_key=llm.password, base_url=llm.host or None),
            ),
            instructions=(
                "Answer revenue questions. Query the warehouse for the numbers, and read "
                "the finance reports for context."
            ),
            tools=AirflowTools(warehouse, reports),
        )
        # The SDK sends traces to OpenAI by default; see the guide's tracing section.
        result = Runner.run_sync(agent, question, run_config=RunConfig(tracing_disabled=True))
        return result.final_output

    explain_revenue()


# [END example_openai_agents_data_access]


example_openai_agents_data_access()
