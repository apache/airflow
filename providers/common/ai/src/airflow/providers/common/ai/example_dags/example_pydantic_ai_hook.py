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
"""Example DAGs demonstrating PydanticAIHook and direct pydantic-ai Agent usage."""

from __future__ import annotations

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.compat.sdk import dag, task


# [START howto_hook_pydantic_ai_basic]
@dag(schedule=None)
def example_pydantic_ai_hook():
    @task
    def generate_summary(text: str) -> str:
        hook = PydanticAIHook(llm_conn_id="pydanticai_default")
        agent = hook.create_agent(output_type=str, instructions="Summarize concisely.")
        result = agent.run_sync(text)
        return result.output

    generate_summary("Apache Airflow is a platform for programmatically authoring...")


# [END howto_hook_pydantic_ai_basic]

example_pydantic_ai_hook()


# [START howto_hook_pydantic_ai_structured_output]
@dag(schedule=None)
def example_pydantic_ai_structured_output():
    @task
    def generate_sql(prompt: str) -> dict:
        class SQLResult(BaseModel):
            query: str
            explanation: str

        hook = PydanticAIHook(llm_conn_id="pydanticai_default")
        agent = hook.create_agent(
            output_type=SQLResult,
            instructions="Generate a SQL query and explain it.",
        )
        result = agent.run_sync(prompt)
        return result.output.model_dump()

    generate_sql("Find the top 10 customers by revenue")


# [END howto_hook_pydantic_ai_structured_output]

example_pydantic_ai_structured_output()


# [START howto_task_with_toolsets]
@dag(schedule=None)
def example_task_with_toolsets():
    """Use toolsets directly in a @task function without AgentOperator."""

    @task
    def analyze_revenue() -> str:
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        hook = PydanticAIHook(llm_conn_id="pydanticai_default")
        agent = hook.create_agent(
            output_type=str,
            instructions=(
                "You are a sales analytics assistant. "
                "Use the SQL tools to explore the database schema and answer questions."
            ),
            toolsets=[
                SQLToolset(
                    db_conn_id="my_database",
                    allowed_tables=["customers", "orders"],
                    max_rows=20,
                ),
            ],
        )
        result = agent.run_sync("Which customers have spent the most? Show the top 5.")
        return result.output

    analyze_revenue()


# [END howto_task_with_toolsets]

example_task_with_toolsets()
