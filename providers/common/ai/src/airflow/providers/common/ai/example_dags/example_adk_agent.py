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
"""Example DAGs demonstrating AgentOperator with Google ADK backend.

The same ``AgentOperator`` and ``@task.agent`` decorator used for pydantic-ai
also work with Google ADK — the backend is selected by the **connection type**
(``adk``) rather than a different operator class.
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import dag, task

# ---------------------------------------------------------------------------
# 1. AgentOperator with ADK backend: answer using custom tools
# ---------------------------------------------------------------------------


# [START howto_operator_adk_agent]
@dag
def example_adk_agent_operator():
    def get_customer_count() -> dict:
        """Returns the total number of customers in the database."""
        return {"status": "success", "count": 1234}

    def get_top_customers(limit: int = 5) -> dict:
        """Returns the top customers by order count.

        Args:
            limit: Maximum number of customers to return.

        Returns:
            dict: status and list of top customers.
        """
        return {
            "status": "success",
            "customers": [
                {"name": "Acme Corp", "orders": 150},
                {"name": "Globex", "orders": 120},
            ][:limit],
        }

    # Same AgentOperator — ADK backend is selected by connection type.
    # Pass ADK-specific ``tools`` via ``agent_params``.
    AgentOperator(
        task_id="adk_analyst",
        prompt="What are the top 5 customers by order count?",
        llm_conn_id="adk_default",
        model_id="gemini-2.5-flash",
        system_prompt=(
            "You are a data analyst. Use the available tools to answer questions about customer data."
        ),
        agent_params={"tools": [get_customer_count, get_top_customers]},
    )


# [END howto_operator_adk_agent]

example_adk_agent_operator()


# ---------------------------------------------------------------------------
# 2. @task.agent decorator with ADK backend
# ---------------------------------------------------------------------------


# [START howto_decorator_adk_agent]
@dag
def example_adk_agent_decorator():
    def lookup_order_total(month: str) -> dict:
        """Returns the total revenue for a given month.

        Args:
            month: Month name (e.g. 'January', 'February').

        Returns:
            dict: status and total revenue.
        """
        return {"status": "success", "month": month, "total_revenue": 42000.50}

    @task.agent(
        llm_conn_id="adk_default",
        model_id="gemini-2.5-flash",
        system_prompt="You are a data analyst. Use tools to answer questions.",
        agent_params={"tools": [lookup_order_total]},
    )
    def analyze(question: str):
        return f"Answer this question about our revenue data: {question}"

    analyze("What was our total revenue last month?")


# [END howto_decorator_adk_agent]

example_adk_agent_decorator()
