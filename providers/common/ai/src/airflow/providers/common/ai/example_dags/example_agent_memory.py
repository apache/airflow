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
Give an agent memory that outlives the Dag run, with pydantic-ai-harness ``Memory``.

``Memory`` gives the agent a notebook it reads at the start of a run and writes to with
tools. The notebook lives in a store you choose, so a run tomorrow sees what the agent
wrote today.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

from pydantic_ai import Agent

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import BaseHook, dag, task

try:
    from pydantic_ai_harness import Memory
    from pydantic_ai_harness.memory import PostgresMemoryStore, SqliteMemoryStore
except ImportError:
    Memory = None  # type: ignore[assignment,misc]

try:
    import asyncpg
except ImportError:
    asyncpg = None

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "pydanticai_default")
MEMORY_DB = os.environ.get("AGENT_MEMORY_DB", "/opt/airflow/agent-memory/triage.db")
MEMORY_CONN_ID = os.environ.get("AGENT_MEMORY_CONN_ID", "agent_memory_db")


# [START howto_agent_memory_sqlite]
if Memory is not None:

    @dag(schedule="@daily", catchup=False, tags=["example"])
    def example_agent_memory():
        @task
        def collect_failures() -> list[str]:
            # Replace with a query against your monitoring system.
            return [
                "load_orders: psycopg2.OperationalError: could not connect to server",
                "sync_crm: HTTP 429 Too Many Requests from api.example-crm.com",
            ]

        collect_failures() >> AgentOperator(
            task_id="triage_failures",
            llm_conn_id=LLM_CONN_ID,
            prompt="Triage the failed tasks from {{ ds }}:\n{{ ti.xcom_pull(task_ids='collect_failures') }}",
            system_prompt=(
                "You triage failed data pipeline tasks. Your notes hold the causes you have "
                "seen before. Say which failures match a known cause and which are new. "
                "Record each new cause in your notes with the date you first saw it, and "
                "add the date to a known cause each time it comes back."
            ),
            capabilities=[Memory(SqliteMemoryStore(database=MEMORY_DB))],
        )

    # [END howto_agent_memory_sqlite]

    example_agent_memory()


# [START howto_agent_memory_postgres]
if Memory is not None and asyncpg is not None:

    @dataclass
    class Customer:
        customer_id: str

    @dag(schedule=None, tags=["example"])
    def example_agent_memory_per_customer():
        @task
        def answer_ticket(customer_id: str, question: str) -> str:
            conn = BaseHook.get_connection(MEMORY_CONN_ID)

            async def run() -> str:
                # The pool belongs to this event loop, so it is created and closed in it.
                async with asyncpg.create_pool(
                    host=conn.host,
                    port=conn.port or 5432,
                    user=conn.login,
                    password=conn.password,
                    database=conn.schema,
                ) as pool:
                    agent = Agent(
                        PydanticAIHook.get_hook(LLM_CONN_ID).get_conn(),
                        deps_type=Customer,
                        instructions=(
                            "You answer support tickets. Your notes hold what you have learned "
                            "about this customer's setup. Record anything that will help with "
                            "their next ticket."
                        ),
                        capabilities=[
                            Memory[Customer](
                                PostgresMemoryStore(pool),
                                namespace=lambda ctx: ctx.deps.customer_id,
                            )
                        ],
                    )
                    result = await agent.run(question, deps=Customer(customer_id))
                    return result.output

            return asyncio.run(run())

        answer_ticket.expand_kwargs(
            [
                {"customer_id": "acme", "question": "Our nightly export stopped at 02:00. Why?"},
                {"customer_id": "globex", "question": "Can we move our sync to every 15 minutes?"},
            ]
        )

    # [END howto_agent_memory_postgres]

    example_agent_memory_per_customer()
