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
Example Dag: an agent task that keeps its progress through a provider outage.

``fallback_conn_ids`` moves a failed model request to the next connection inside the
attempt, and that connection continues the run from the same message history.
``durable=True`` keeps what each attempt completed, so when every connection fails, or
the worker running the task dies, the retry replays those steps and continues the run
on whichever connection answers.

Prerequisites:
  - Connection ``llm_primary`` with ``conn_type='pydanticai'``, ``password=<API key>``
    and ``extra='{"model": "openai:gpt-5-mini"}'``
  - Connection ``llm_standby`` with ``conn_type='pydanticai'``, ``password=<API key>``
    and ``extra='{"model": "anthropic:claude-sonnet-5"}'``
  - Connection ``warehouse`` to a database with an ``orders`` table
  - The provider installed with the ``anthropic``, ``openai`` and ``sql`` extras
  - On Airflow before 3.3, ``[common.ai] durable_cache_path`` set to shared storage
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import dag

try:
    from airflow.providers.common.ai.toolsets.sql import SQLToolset
except Exception:
    SQLToolset = None  # type: ignore[assignment,misc]


# [START howto_agent_recovery]
if SQLToolset is not None:

    @dag(
        default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
        catchup=False,
        tags=["example", "fallback", "durable"],
    )
    def example_agent_recovery():
        AgentOperator(
            task_id="investigate_orders",
            prompt="Which region placed the most orders, and what share of all orders is that?",
            llm_conn_id="llm_primary",
            fallback_conn_ids=["llm_standby"],
            durable=True,
            system_prompt="You are a SQL analyst. Use the tools to answer with data.",
            toolsets=[SQLToolset(db_conn_id="warehouse", allowed_tables=["orders"], max_rows=20)],
        )

    # [END howto_agent_recovery]

    example_agent_recovery()
