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
"""Example DAGs demonstrating AgentOperator, @task.agent, and toolsets."""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.hook import HookToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import dag, task

# ---------------------------------------------------------------------------
# 1. SQL Agent: answer a question using database tools
# ---------------------------------------------------------------------------


# [START howto_operator_agent_sql]
@dag
def example_agent_operator_sql():
    AgentOperator(
        task_id="analyst",
        prompt="What are the top 5 customers by order count?",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are a SQL analyst. Use the available tools to explore "
            "the schema and answer the question with data."
        ),
        toolsets=[
            SQLToolset(
                db_conn_id="postgres_default",
                allowed_tables=["customers", "orders"],
                max_rows=20,
            )
        ],
    )


# [END howto_operator_agent_sql]

example_agent_operator_sql()


# ---------------------------------------------------------------------------
# 2. Hook-based tools: wrap an existing hook for the agent
# ---------------------------------------------------------------------------


# [START howto_operator_agent_hook]
@dag
def example_agent_operator_hook():
    from airflow.providers.http.hooks.http import HttpHook

    http_hook = HttpHook(http_conn_id="my_api")

    AgentOperator(
        task_id="api_explorer",
        prompt="What endpoints are available and what does /status return?",
        llm_conn_id="pydanticai_default",
        system_prompt="You are an API explorer. Use the tools to discover and call endpoints.",
        toolsets=[
            HookToolset(
                http_hook,
                allowed_methods=["run"],
                tool_name_prefix="http_",
            )
        ],
    )


# [END howto_operator_agent_hook]

example_agent_operator_hook()


# ---------------------------------------------------------------------------
# 3. @task.agent decorator with dynamic prompt
# ---------------------------------------------------------------------------


# [START howto_decorator_agent]
@dag
def example_agent_decorator():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a data analyst. Use tools to answer questions.",
        toolsets=[
            SQLToolset(
                db_conn_id="postgres_default",
                allowed_tables=["orders"],
            )
        ],
    )
    def analyze(question: str):
        return f"Answer this question about our orders data: {question}"

    analyze("What was our total revenue last month?")


# [END howto_decorator_agent]

example_agent_decorator()


# ---------------------------------------------------------------------------
# 4. Structured output — agent returns a Pydantic model
# ---------------------------------------------------------------------------


# [START howto_decorator_agent_structured]
@dag
def example_agent_structured_output():
    from pydantic import BaseModel

    class Analysis(BaseModel):
        summary: str
        top_items: list[str]
        row_count: int

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a data analyst. Return structured results.",
        output_type=Analysis,
        toolsets=[SQLToolset(db_conn_id="postgres_default")],
    )
    def analyze(question: str):
        return f"Analyze: {question}"

    analyze("What are the trending products this week?")


# [END howto_decorator_agent_structured]

example_agent_structured_output()


# ---------------------------------------------------------------------------
# 5. Chaining: agent output feeds into downstream tasks via XCom
# ---------------------------------------------------------------------------


# [START howto_agent_chain]
@dag
def example_agent_chain():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a SQL analyst.",
        toolsets=[SQLToolset(db_conn_id="postgres_default", allowed_tables=["orders"])],
    )
    def investigate(question: str):
        return f"Investigate: {question}"

    @task
    def send_report(analysis: str):
        """Send the agent's analysis to a downstream system."""
        print(f"Report: {analysis}")
        return analysis

    result = investigate("Summarize order trends for last quarter")
    send_report(result)


# [END howto_agent_chain]

example_agent_chain()


# ---------------------------------------------------------------------------
# 6. HITL Review: human approves/rejects agent output before downstream runs
# ---------------------------------------------------------------------------


# [START howto_operator_agent_hitl_review]
@dag
def example_agent_operator_hitl_review():
    """AgentOperator with HITL review — a human approves output via hitl-review plugin UI."""
    AgentOperator(
        task_id="summarize_with_review",
        prompt="Summarize the Q4 sales report in 3 bullet points.",
        llm_conn_id="pydantic_ai_default",
        system_prompt="You are a concise business analyst.",
        enable_hitl_review=True,
        max_hitl_iterations=5,
        hitl_timeout=timedelta(minutes=30),
        hitl_poll_interval=10.0,
    )


# [END howto_operator_agent_hitl_review]

example_agent_operator_hitl_review()
