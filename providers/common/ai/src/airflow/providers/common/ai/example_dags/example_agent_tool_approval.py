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
"""Example Dag: an agent that looks orders up freely but needs a person before it refunds one."""

from __future__ import annotations

from datetime import timedelta

from pydantic_ai.toolsets.function import FunctionToolset

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import dag


# [START howto_agent_tool_approval]
def lookup_order(order_id: int) -> str:
    """Look up an order."""
    return f"order {order_id}: $42, delivered"


def refund_order(order_id: int) -> str:
    """Refund an order. Irreversible."""
    return f"refunded order {order_id}"


shop = FunctionToolset(tools=[lookup_order, refund_order]).approval_required(
    lambda ctx, tool_def, args: tool_def.name == "refund_order"
)


@dag(tags=["example"])
def example_agent_tool_approval():
    AgentOperator(
        task_id="handle_ticket",
        llm_conn_id="pydanticai_default",
        prompt="The customer says order 7 never arrived. Resolve it.",
        toolsets=[shop],
        tool_approval_timeout=timedelta(hours=4),
    )


# [END howto_agent_tool_approval]

example_agent_tool_approval()
