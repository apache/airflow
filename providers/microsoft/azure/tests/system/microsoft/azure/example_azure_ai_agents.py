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

import os
from datetime import datetime

from airflow.providers.microsoft.azure.operators.ai_agents import (
    CreateAzureAIAgentOperator,
    DeleteAzureAIAgentOperator,
    RunAzureAIAgentOperator,
    UpdateAzureAIAgentOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain
else:
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "example_azure_ai_agents"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
MODEL = os.environ.get("AZURE_AI_AGENTS_MODEL", "gpt-4o")
AGENT_NAME = f"airflow-ai-agent-{ENV_ID}"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_operator_azure_ai_agent_create]
    create_agent = CreateAzureAIAgentOperator(
        task_id="create_agent",
        model=MODEL,
        config={
            "name": AGENT_NAME,
            "instructions": "You are a concise assistant that answers Airflow orchestration questions.",
        },
    )
    # [END howto_operator_azure_ai_agent_create]

    # [START howto_operator_azure_ai_agent_update]
    update_agent = UpdateAzureAIAgentOperator(
        task_id="update_agent",
        agent_id="{{ ti.xcom_pull(task_ids='create_agent')['id'] }}",
        config={
            "instructions": "You are a concise assistant that explains Airflow task failures.",
        },
    )
    # [END howto_operator_azure_ai_agent_update]

    # [START howto_operator_azure_ai_agent_run]
    run_agent = RunAzureAIAgentOperator(
        task_id="run_agent",
        agent_id="{{ ti.xcom_pull(task_ids='create_agent')['id'] }}",
        config={
            "thread": {
                "messages": [
                    {
                        "role": "user",
                        "content": "Summarize why retrying transient task failures can be useful.",
                    }
                ],
            },
        },
    )
    # [END howto_operator_azure_ai_agent_run]

    # [START howto_operator_azure_ai_agent_run_deferrable]
    run_agent_deferrable = RunAzureAIAgentOperator(
        task_id="run_agent_deferrable",
        agent_id="{{ ti.xcom_pull(task_ids='create_agent')['id'] }}",
        config={
            "thread": {
                "messages": [
                    {
                        "role": "user",
                        "content": "Give one short recommendation for investigating failed Dags.",
                    }
                ],
            },
        },
        deferrable=True,
    )
    # [END howto_operator_azure_ai_agent_run_deferrable]

    # [START howto_operator_azure_ai_agent_delete]
    delete_agent = DeleteAzureAIAgentOperator(
        task_id="delete_agent",
        agent_id="{{ ti.xcom_pull(task_ids='create_agent')['id'] }}",
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )
    # [END howto_operator_azure_ai_agent_delete]

    chain(create_agent, update_agent, [run_agent, run_agent_deferrable], delete_agent)

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
