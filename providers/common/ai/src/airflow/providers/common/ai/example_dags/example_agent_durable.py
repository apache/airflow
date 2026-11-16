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
"""Example Dags demonstrating durable execution with AgentOperator, @task.agent and a plain @task."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic_ai import RunContext, Tool
from pydantic_ai.capabilities import SetToolMetadata
from pydantic_ai.toolsets import FunctionToolset

from airflow.providers.common.ai.durable import AirflowDurability
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import dag, task

try:
    from airflow.providers.common.ai.toolsets.sql import SQLToolset
except Exception:
    SQLToolset = None  # type: ignore[assignment,misc]

# ---------------------------------------------------------------------------
# 1. Durable AgentOperator: a retry replays the steps the failed attempt completed
# ---------------------------------------------------------------------------


# [START howto_operator_agent_durable]
if SQLToolset is not None:

    @dag(default_args={"retries": 3, "retry_delay": timedelta(seconds=30)}, tags=["example"])
    def example_agent_durable_operator():
        """Agent with durable execution: a retry replays the steps the failed attempt completed."""
        AgentOperator(
            task_id="durable_analyst",
            prompt="What are the top 5 customers by order count?",
            llm_conn_id="pydanticai_default",
            system_prompt=(
                "You are a SQL analyst. Use the available tools to explore "
                "the schema and answer the question with data."
            ),
            durable=True,
            toolsets=[
                SQLToolset(
                    db_conn_id="postgres_default",
                    allowed_tables=["customers", "orders"],
                    max_rows=20,
                )
            ],
        )

    # [END howto_operator_agent_durable]

    example_agent_durable_operator()


# ---------------------------------------------------------------------------
# 2. Durable @task.agent decorator
# ---------------------------------------------------------------------------


# [START howto_decorator_agent_durable]
if SQLToolset is not None:

    @dag(default_args={"retries": 3, "retry_delay": timedelta(seconds=30)}, tags=["example"])
    def example_agent_durable_decorator():
        @task.agent(
            llm_conn_id="pydanticai_default",
            system_prompt="You are a data analyst. Use tools to answer questions.",
            durable=True,
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

    # [END howto_decorator_agent_durable]

    example_agent_durable_decorator()


# ---------------------------------------------------------------------------
# 3. A pydantic-ai agent in a plain @task
# ---------------------------------------------------------------------------


# [START howto_capability_airflow_durability]
if SQLToolset is not None:

    @dag(default_args={"retries": 3, "retry_delay": timedelta(seconds=30)}, tags=["example"])
    def example_airflow_durability_capability():
        @task
        def analyze(question: str) -> str:
            agent = PydanticAIHook(llm_conn_id="pydanticai_default").create_agent(
                name="orders_analyst",
                instructions="You are a data analyst. Use tools to answer questions.",
                toolsets=[SQLToolset(db_conn_id="postgres_default", allowed_tables=["orders"])],
                capabilities=[AirflowDurability()],
            )
            return agent.run_sync(question).output

        analyze("What was our total revenue last month?")

    # [END howto_capability_airflow_durability]

    example_airflow_durability_capability()


# ---------------------------------------------------------------------------
# 4. Tools that run on every attempt instead of replaying
# ---------------------------------------------------------------------------


# [START howto_agent_durable_tool_opt_out]
def today() -> str:
    """Return today's date in UTC."""
    return datetime.now(UTC).date().isoformat()


if SQLToolset is not None:

    @dag(default_args={"retries": 3, "retry_delay": timedelta(seconds=30)}, tags=["example"])
    def example_agent_durable_tool_opt_out():
        AgentOperator(
            task_id="durable_freshness_check",
            prompt="Which tables in load_log have not been loaded today?",
            llm_conn_id="pydanticai_default",
            durable=True,
            toolsets=[
                FunctionToolset(tools=[Tool(today, metadata={"airflow": False})], id="calendar"),
                SQLToolset(db_conn_id="postgres_default", allowed_tables=["load_log"]),
            ],
            capabilities=[SetToolMetadata(tools=["query"], airflow=False)],
        )

    # [END howto_agent_durable_tool_opt_out]

    example_agent_durable_tool_opt_out()


# ---------------------------------------------------------------------------
# 5. Toolsets without an id
# ---------------------------------------------------------------------------


# [START howto_agent_durable_toolsets_without_id]
def get_order(order_id: int) -> dict[str, str]:
    """Look up an order's status and carrier."""
    return {"status": "shipped", "carrier": "dhl"}


def track_parcel(carrier: str, order_id: int) -> str:
    """Return where a carrier's parcel for an order is."""
    return f"{carrier} parcel for order {order_id}: out for delivery"


def carrier_tools(ctx: RunContext) -> FunctionToolset:
    """Build the carrier tools for each run."""
    return FunctionToolset(tools=[track_parcel])


@dag(default_args={"retries": 3, "retry_delay": timedelta(seconds=30)}, tags=["example"])
def example_agent_durable_toolsets_without_id():
    AgentOperator(
        task_id="durable_order_status",
        prompt="Where is order 4411?",
        llm_conn_id="pydanticai_default",
        durable=True,
        toolsets=[FunctionToolset(tools=[get_order]), carrier_tools],
    )


# [END howto_agent_durable_toolsets_without_id]

example_agent_durable_toolsets_without_id()
