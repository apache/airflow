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
from __future__ import annotations

from airflow.sdk import dag, task

ANTHROPIC_CONN_ID = "anthropic_default"


@dag(schedule=None, catchup=False)
def anthropic_managed_agent():
    @task
    def setup_agent_and_environment() -> dict[str, str]:
        # One-time setup helper — in production run this once and store the IDs, do not
        # create a fresh agent every Dag run. Here we create them and pass the IDs via XCom.
        from airflow.providers.anthropic.hooks.anthropic import AnthropicHook

        hook = AnthropicHook(conn_id=ANTHROPIC_CONN_ID)
        agent = hook.create_agent(
            name="airflow-research-agent",
            system="You are a concise research assistant.",
            tools=[{"type": "agent_toolset_20260401"}],
        )
        environment = hook.create_environment(name="airflow-agent-env")
        return {"agent_id": agent.id, "environment_id": environment.id}

    setup = setup_agent_and_environment()

    # [START howto_operator_anthropic_agent_session]
    from airflow.providers.anthropic.operators.agent import AnthropicAgentSessionOperator

    run_agent = AnthropicAgentSessionOperator(
        task_id="run_agent",
        conn_id=ANTHROPIC_CONN_ID,
        agent_id=setup["agent_id"],
        environment_id=setup["environment_id"],
        message="Summarize the latest stable Apache Airflow release in two sentences.",
        deferrable=True,
    )
    # [END howto_operator_anthropic_agent_session]

    run_agent


anthropic_managed_agent()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
