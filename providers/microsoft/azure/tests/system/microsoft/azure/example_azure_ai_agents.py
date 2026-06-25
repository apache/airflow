#
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
from typing import Any

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
MODEL_DEPLOYMENT_NAME = os.environ.get("AZURE_AI_AGENTS_MODEL_DEPLOYMENT_NAME", "gpt-4o")
CONTAINER_IMAGE = os.environ.get(
    "AZURE_AI_AGENTS_CONTAINER_IMAGE",
    "myregistry.azurecr.io/airflow-hosted-agent:latest",
)
AGENT_NAME = f"airflow-ai-agent-{ENV_ID}"

HOSTED_AGENT_DEFINITION: dict[str, Any] = {
    "kind": "hosted",
    "container_configuration": {
        "image": CONTAINER_IMAGE,
    },
    "cpu": "1",
    "memory": "2Gi",
    "protocol_versions": [
        {"protocol": "responses", "version": "1.0.0"},
        {"protocol": "invocations", "version": "1.0.0"},
    ],
    "environment_variables": {
        "MODEL_DEPLOYMENT_NAME": MODEL_DEPLOYMENT_NAME,
    },
}

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_operator_azure_ai_agent_create]
    create_agent = CreateAzureAIAgentOperator(
        task_id="create_agent",
        agent_name=AGENT_NAME,
        definition=HOSTED_AGENT_DEFINITION,
        deferrable=True,
    )
    # [END howto_operator_azure_ai_agent_create]

    # [START howto_operator_azure_ai_agent_update]
    update_agent = UpdateAzureAIAgentOperator(
        task_id="update_agent",
        agent_name=AGENT_NAME,
        definition={
            **HOSTED_AGENT_DEFINITION,
            "environment_variables": {
                **HOSTED_AGENT_DEFINITION["environment_variables"],
                "AGENT_BEHAVIOR": "explain-airflow-task-failures",
            },
        },
        deferrable=True,
    )
    # [END howto_operator_azure_ai_agent_update]

    # [START howto_operator_azure_ai_agent_run]
    run_agent = RunAzureAIAgentOperator(
        task_id="run_agent",
        agent_name=AGENT_NAME,
        protocol="responses",
        input_data={
            "input": "Summarize why retrying transient task failures can be useful.",
            "store": True,
        },
    )
    # [END howto_operator_azure_ai_agent_run]

    # [START howto_operator_azure_ai_agent_run_invocations]
    run_agent_invocations = RunAzureAIAgentOperator(
        task_id="run_agent_invocations",
        agent_name=AGENT_NAME,
        protocol="invocations",
        input_data={"message": "Give one short recommendation for investigating failed Dags."},
    )
    # [END howto_operator_azure_ai_agent_run_invocations]

    # [START howto_operator_azure_ai_agent_delete]
    delete_agent = DeleteAzureAIAgentOperator(
        task_id="delete_agent",
        agent_name=AGENT_NAME,
        force=True,
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )
    # [END howto_operator_azure_ai_agent_delete]

    chain(create_agent, update_agent, [run_agent, run_agent_invocations], delete_agent)

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
