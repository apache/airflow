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
Example Dag demonstrating provider failover, and how to drill it.

The Dag never names a fallback provider: ``llm_primary_down`` carries the chain in
its extra, so the failover topology belongs to whoever administers the connections.

Prerequisites:
  - Connection ``llm_primary_down`` with ``conn_type='pydanticai'``,
    ``host='http://127.0.0.1:9/v1'`` (a port nothing listens on, standing in for an
    outage), ``password=<any value>``, and
    ``extra='{"model": "openai:gpt-4o-mini", "fallback_conn_ids": ["llm_fallback"]}'``
  - Connection ``llm_fallback`` with ``conn_type='pydanticai'``,
    ``password=<API key>``, ``extra='{"model": "anthropic:claude-haiku-4-5-20251001"}'``
  - ``pip install apache-airflow-providers-common-ai[anthropic]``

Run it as a drill: the task succeeds, and its log reports ``model=claude-haiku-...``
rather than the primary's model. Point ``llm_primary_down`` at a working endpoint
and the same log names the primary instead -- that difference is the evidence the
chain is live, and it is the check to repeat whenever the topology changes.
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import dag, task

# [START howto_llm_fallback_connection_driven]


@dag(catchup=False, tags=["example", "fallback", "llm"])
def example_llm_fallback():
    LLMOperator(
        task_id="summarize_through_the_chain",
        prompt="Summarize the key findings from the Q4 earnings report.",
        llm_conn_id="llm_primary_down",
        system_prompt="You are a financial analyst. Be concise.",
    )


example_llm_fallback()

# [END howto_llm_fallback_connection_driven]


# [START howto_llm_fallback_hook_argument]
@dag(catchup=False, tags=["example", "fallback", "llm"])
def example_llm_fallback_explicit_chain():
    @task
    def classify_with_an_explicit_chain() -> str:
        """Build the chain in code, for a task that owns its own failover order."""
        from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook

        hook = PydanticAIHook(
            llm_conn_id="llm_primary_down",
            fallback_conn_ids=["llm_fallback"],
        )
        agent = hook.create_agent(instructions="Reply with a single word.")
        return agent.run_sync("Is a raven a bird?").output

    classify_with_an_explicit_chain()


example_llm_fallback_explicit_chain()
# [END howto_llm_fallback_hook_argument]
