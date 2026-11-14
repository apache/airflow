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
Run a Pydantic AI agent you build yourself, with Airflow's toolsets as its tools.

``AgentOperator`` builds the agent for you. When you already have a Pydantic AI agent,
keep it and run it in an ordinary ``@task``: ``PydanticAIHook`` gives it a model from an
Airflow connection, and Airflow's toolsets are Pydantic AI toolsets, so they go straight
into ``toolsets=``.
"""

from __future__ import annotations

import os

from pydantic_ai import Agent

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.toolsets import ObjectStorageToolset
from airflow.providers.common.compat.sdk import dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "pydanticai_default")
DB_CONN_ID = os.environ.get("DB_CONN_ID", "sql_default")
FILES = os.environ.get("FILES", "s3://acme-reports/finance/")
FILES_CONN_ID = os.environ.get("FILES_CONN_ID", "aws_default")

DEFAULT_QUESTION = "Does the September report's revenue match the orders table?"


# [START example_pydantic_ai_agent]
@dag(tags=["example"])
def example_pydantic_ai_agent():
    """Answer a question across a database and a reports bucket with your own agent."""

    @task
    def run_pydantic_ai_agent(question: str = DEFAULT_QUESTION) -> str:
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        agent = Agent(
            PydanticAIHook.get_hook(LLM_CONN_ID).get_conn(),
            instructions=(
                "You check reports against the warehouse. Read the report files, query the "
                "orders table, and say whether the numbers agree."
            ),
            toolsets=[
                SQLToolset(db_conn_id=DB_CONN_ID, allowed_tables=["orders"]),
                ObjectStorageToolset(FILES, conn_id=FILES_CONN_ID),
            ],
        )
        return agent.run_sync(question).output

    run_pydantic_ai_agent()


# [END example_pydantic_ai_agent]


example_pydantic_ai_agent()
