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
"""Example DAGs demonstrating pydantic-ai capabilities via ``agent_params``.

Capabilities (https://ai.pydantic.dev/capabilities/) are pydantic-ai's
composable units for thinking, web search, image generation, MCP, and more.
``AgentOperator`` forwards anything in ``agent_params`` to the underlying
``Agent(...)`` constructor, so capabilities work today without operator-level
support. A first-class ``capabilities=`` kwarg is on the roadmap.
"""

from __future__ import annotations

from pydantic_ai.capabilities import Thinking, WebSearch

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import dag

# ---------------------------------------------------------------------------
# 1. Thinking capability: enable model reasoning at a configurable effort level
# ---------------------------------------------------------------------------


# [START howto_operator_agent_capabilities_thinking]
@dag(tags=["example"])
def example_agent_capabilities_thinking():
    AgentOperator(
        task_id="reasoner",
        prompt="Walk through the steps to compute the 10th Fibonacci number, then give the answer.",
        llm_conn_id="pydanticai_default",
        system_prompt="You are a careful mathematician. Think before answering.",
        agent_params={
            "capabilities": [Thinking(effort="high")],
        },
    )


# [END howto_operator_agent_capabilities_thinking]

example_agent_capabilities_thinking()


# ---------------------------------------------------------------------------
# 2. WebSearch capability: provider-adaptive web search as a single declaration
# ---------------------------------------------------------------------------


# [START howto_operator_agent_capabilities_web_search]
@dag(tags=["example"])
def example_agent_capabilities_web_search():
    AgentOperator(
        task_id="researcher",
        prompt="Summarize the latest Apache Airflow 3.x release notes from airflow.apache.org.",
        llm_conn_id="pydanticai_default",
        system_prompt="You are a release-notes summarizer. Cite the source URL.",
        agent_params={
            "capabilities": [WebSearch()],
        },
    )


# [END howto_operator_agent_capabilities_web_search]

example_agent_capabilities_web_search()


# ---------------------------------------------------------------------------
# 3. Composing capabilities with toolsets
# ---------------------------------------------------------------------------
# Capabilities and toolsets compose: pydantic-ai merges tools from both.


# [START howto_operator_agent_capabilities_composed]
@dag(tags=["example"])
def example_agent_capabilities_composed():
    AgentOperator(
        task_id="analyst",
        prompt="Cross-reference our top customers with their recent public news. Think first.",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are a sales analyst. Query the database for customers, then search the web "
            "for recent news. Reason carefully about which leads to surface."
        ),
        toolsets=[
            SQLToolset(
                db_conn_id="postgres_default",
                allowed_tables=["customers", "orders"],
                max_rows=20,
            ),
        ],
        agent_params={
            "capabilities": [Thinking(effort="medium"), WebSearch()],
        },
    )


# [END howto_operator_agent_capabilities_composed]

example_agent_capabilities_composed()
