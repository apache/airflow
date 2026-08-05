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
"""
System test for Azure AI Foundry Hosted agent operators.

Requires a real Azure AI Foundry project and a Hosted agent container image pushed to
Azure Container Registry:

* ``AIRFLOW_CONN_AZURE_AI_AGENTS_DEFAULT``: Azure AI Agents connection URI.
* ``AZURE_AI_AGENTS_ENDPOINT``: Azure AI Foundry project endpoint.
* ``AZURE_AI_AGENTS_CONTAINER_IMAGE``: Hosted agent container image URI.
* ``AZURE_AI_AGENTS_MODEL_DEPLOYMENT_NAME``: Model deployment available in the project.
* ``AZURE_AI_AGENTS_RUN_PROTOCOL``: Optional runtime protocol to invoke (``responses`` or ``invocations``).
* ``AZURE_AI_AGENTS_USE_MODEL``: Optional, defaults to ``false`` for a deterministic smoke test.
* ``AZURE_AI_AGENTS_USE_MOCKS``: Optional, defaults to ``true`` so the sample agent does not need
  external service credentials.
"""

from __future__ import annotations

import atexit
import json
import os
import tempfile
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
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID") or "default"


def _get_env(name: str, default: str = "") -> str:
    return os.environ.get(name) or os.environ.get(f"AIRFLOW_VAR_{name}", default)


AZURE_AI_AGENTS_CONN_ID = _get_env("AZURE_AI_AGENTS_CONN_ID", "azure_ai_agents_default")
AZURE_AI_AGENTS_CONN_URI = _get_env("AIRFLOW_CONN_AZURE_AI_AGENTS_DEFAULT")
ENDPOINT = _get_env("AZURE_AI_AGENTS_ENDPOINT")
MODEL_DEPLOYMENT_NAME = _get_env("AZURE_AI_AGENTS_MODEL_DEPLOYMENT_NAME", "gpt-4o")
CONTAINER_IMAGE = _get_env(
    "AZURE_AI_AGENTS_CONTAINER_IMAGE", "myregistry.azurecr.io/airflow-hosted-agent:latest"
)
AGENT_NAME = _get_env("AZURE_AI_AGENT_NAME", f"airflow-ai-agent-{ENV_ID}")
RUN_AGENT_PROTOCOL = _get_env("AZURE_AI_AGENTS_RUN_PROTOCOL").lower()
if RUN_AGENT_PROTOCOL not in {"", "responses", "invocations"}:
    raise RuntimeError("AZURE_AI_AGENTS_RUN_PROTOCOL must be either 'responses' or 'invocations'.")
HOSTED_AGENT_PROTOCOL = RUN_AGENT_PROTOCOL or "responses"
run_agent: RunAzureAIAgentOperator | None

HOSTED_AGENT_DEFINITION: dict[str, Any] = {
    "kind": "hosted",
    "container_configuration": {
        "image": CONTAINER_IMAGE,
    },
    "cpu": "1",
    "memory": "2Gi",
    "protocol_versions": [
        {"protocol": HOSTED_AGENT_PROTOCOL, "version": "1.0.0"},
    ],
    "environment_variables": {
        "AZURE_AI_MODEL_DEPLOYMENT_NAME": MODEL_DEPLOYMENT_NAME,
        "AIRFLOW_AGENT_FOUNDRY_PROJECT_ENDPOINT": ENDPOINT,
        "AIRFLOW_AGENT_USE_MODEL": _get_env("AZURE_AI_AGENTS_USE_MODEL", "false"),
        "AIRFLOW_AGENT_USE_MOCKS": _get_env("AZURE_AI_AGENTS_USE_MOCKS", "true"),
    },
}

UPDATED_AGENT_DEFINITION: dict[str, Any] = {
    **HOSTED_AGENT_DEFINITION,
    "environment_variables": {
        **HOSTED_AGENT_DEFINITION["environment_variables"],
        "AIRFLOW_AGENT_BEHAVIOR": "troubleshoot-airflow-task-failures",
    },
}

SMOKE_FAILURE_CONTEXT = json.dumps(
    {
        "dag_id": "azure_ai_agents_demo_failing_etl",
        "run_id": "manual__azure_ai_agents_smoke",
        "dag_file": "azure_ai_agents/demo_failing_etl.py",
        "failed_task": {
            "task_id": "transform",
            "state": "failed",
            "try_number": 1,
        },
        "log_excerpt": "KeyError: 'rowz'",
    }
)

RUN_AGENT_PROMPT = (
    "Analyze this Airflow failure context and return JSON with summary, "
    "root_cause, and suggested_fix. Do not claim PR or Slack actions unless "
    "the hosted agent tools actually performed them.\n\n"
    f"{SMOKE_FAILURE_CONTEXT}"
)

RUN_AGENT_INPUT = {"input" if RUN_AGENT_PROTOCOL == "responses" else "message": RUN_AGENT_PROMPT}


def create_connection_file(conn_id_name: str, conn_uri: str) -> str | None:
    if not conn_uri:
        return None
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as conn_file:
        conn_file.write(f"{conn_id_name}={conn_uri}\n")
        atexit.register(os.unlink, conn_file.name)
        return conn_file.name


CONN_FILE_PATH = create_connection_file(AZURE_AI_AGENTS_CONN_ID, AZURE_AI_AGENTS_CONN_URI)


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
        poll_interval=10,
        timeout=900,
        deferrable=False,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    # [END howto_operator_azure_ai_agent_create]

    # [START howto_operator_azure_ai_agent_update]
    update_agent = UpdateAzureAIAgentOperator(
        task_id="update_agent",
        agent_name=AGENT_NAME,
        definition=UPDATED_AGENT_DEFINITION,
        poll_interval=10,
        timeout=900,
        deferrable=False,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    # [END howto_operator_azure_ai_agent_update]

    # [START howto_operator_azure_ai_agent_update_deferrable]
    update_agent_deferrable = UpdateAzureAIAgentOperator(
        task_id="update_agent_deferrable",
        agent_name=AGENT_NAME,
        definition=UPDATED_AGENT_DEFINITION,
        poll_interval=10,
        timeout=900,
        deferrable=True,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    # [END howto_operator_azure_ai_agent_update_deferrable]

    if RUN_AGENT_PROTOCOL:
        # [START howto_operator_azure_ai_agent_run]
        run_agent = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_name=AGENT_NAME,
            protocol=RUN_AGENT_PROTOCOL,
            input_data=RUN_AGENT_INPUT,
            azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
            endpoint=ENDPOINT,
        )
        # [END howto_operator_azure_ai_agent_run]
    else:
        run_agent = None

    # [START howto_operator_azure_ai_agent_delete]
    delete_agent = DeleteAzureAIAgentOperator(
        task_id="delete_agent",
        agent_name=AGENT_NAME,
        force=True,
        poll_interval=10,
        timeout=900,
        trigger_rule=TriggerRule.ALL_DONE,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    # [END howto_operator_azure_ai_agent_delete]

    if run_agent is None:
        chain(create_agent, update_agent, update_agent_deferrable, delete_agent)
    else:
        chain(create_agent, update_agent, update_agent_deferrable, run_agent, delete_agent)

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag, conn_file_path=CONN_FILE_PATH)
