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
Have a person approve each refund an OpenAI Agents SDK support agent wants to issue.

The agent looks orders up on its own, but ``issue_refund`` sets ``needs_approval=True``, so
:func:`~airflow.providers.common.ai.durable.openai_agents.run_durably` pauses the task
in Required Actions for every refund and resumes it with the reviewer's answer.

Before running:

1. Install the SDK and the OpenAI provider:
   ``pip install "openai-agents>=0.20.0" "apache-airflow-providers-openai>=2.2.0"``.
2. Create an OpenAI connection (``LLM_CONN_ID``, default ``openai_default``) whose
   password is an OpenAI API key. Set its host to the base URL of any other
   OpenAI-compatible endpoint, such as a gateway or a self-hosted server.
3. Use Airflow 3.4 or later, with a standard provider that has
   :func:`~airflow.providers.standard.operators.hitl.request_input`.
"""

from __future__ import annotations

import os

from airflow.providers.common.compat.sdk import dag, task

LLM_CONN_ID = os.environ.get("LLM_CONN_ID", "openai_default")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-5.6-luna")

ORDERS = {
    "A-1001": {"total": 42.00, "status": "delivered damaged"},
    "A-1002": {"total": 18.50, "status": "never delivered"},
}
DEFAULT_TICKET = "Order A-1001 arrived broken and A-1002 never came. Please refund both in full."


# [START example_openai_agents_approval]
@dag(tags=["example"])
def example_openai_agents_approval():
    """Resolve a support ticket with an agent whose refunds a person approves in Required Actions."""

    @task
    async def handle_ticket(ticket: str = DEFAULT_TICKET) -> str:
        # The SDK is an optional dependency, so it is imported when the task runs, not at parse time.
        from agents import Agent, OpenAIChatCompletionsModel, RunConfig, function_tool

        from airflow.providers.common.ai.durable.openai_agents import run_durably
        from airflow.providers.openai.hooks.openai import OpenAIHook

        @function_tool
        def get_order(order_id: str) -> dict:
            """Look an order up by its id."""
            return ORDERS.get(order_id, {"error": f"No order {order_id}"})

        @function_tool(needs_approval=True)
        def issue_refund(order_id: str, amount: float, reason: str) -> str:
            """Refund part or all of an order."""
            return f"Refunded {amount:.2f} on {order_id}"

        async with await OpenAIHook(LLM_CONN_ID).aget_conn() as client:
            agent = Agent(
                name="support",
                model=OpenAIChatCompletionsModel(model=LLM_MODEL, openai_client=client),
                instructions=(
                    "You resolve support tickets. Check each order with get_order, then refund "
                    "only what the order supports with issue_refund, one call per order."
                ),
                tools=[get_order, issue_refund],
            )
            # The SDK sends traces to OpenAI by default; see the guide's tracing section.
            result = await run_durably(agent, ticket, run_config=RunConfig(tracing_disabled=True))
        return result.final_output

    handle_ticket()


# [END example_openai_agents_approval]


example_openai_agents_approval()
