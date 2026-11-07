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
Invoke the deterministic agent in resources/agent_server on Databricks Apps.

Deploy the bundled app and configure DATABRICKS_AGENT_APP_URL and the
databricks_oauth connection before running this system test.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.ai.managed_agents import ManagedAgentRequest
from airflow.providers.common.compat.sdk import DAG, task
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook
from airflow.providers.databricks.operators.agent import DatabricksAgentInvokeOperator

DAG_ID = "example_databricks_agent"
APP_URL = os.environ.get("DATABRICKS_AGENT_APP_URL", "https://my-agent.databricksapps.com")
CONN_ID = os.environ.get("DATABRICKS_AGENT_CONN_ID", "databricks_oauth")
INPUT = {"messages": [{"role": "user", "content": "Hello from Airflow"}]}
SESSION_ID = "{{ dag.dag_id }}-{{ run_id }}"


# [START howto_databricks_managed_agent]
@task
def invoke_managed_agent(app_url: str, conn_id: str, session_id: str) -> dict:
    agent = DatabricksAgentHook(databricks_conn_id=conn_id).agent(app_url)
    result = agent.invoke(
        ManagedAgentRequest(
            messages=INPUT["messages"],
            session_id=session_id,
            timeout=60,
        )
    )
    return result.raw


# [END howto_databricks_managed_agent]


@task
def verify_result(result: dict, session_id: str) -> None:
    """Verify that the real agent server completed and preserved the input."""
    assert result["status"] == "completed"
    assert result["output"]["message"] == "Hello from Databricks"
    assert result["output"]["received"] == INPUT
    assert result["output"]["session_id"] == session_id


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_databricks_agent_invoke]
    invoke = DatabricksAgentInvokeOperator(
        task_id="invoke_agent",
        app_url=APP_URL,
        databricks_conn_id=CONN_ID,
        session_id=SESSION_ID,
        input=INPUT,
        deferrable=False,
        timeout=3600,
    )
    # [END howto_operator_databricks_agent_invoke]

    verify_sync = verify_result.override(task_id="verify_sync")(invoke.output, SESSION_ID)
    invoke_managed = invoke_managed_agent(APP_URL, CONN_ID, SESSION_ID)
    verify_managed = verify_result.override(task_id="verify_managed")(invoke_managed, SESSION_ID)
    verify_sync >> invoke_managed

    # [START howto_operator_databricks_agent_invoke_deferrable]
    invoke_deferrable = DatabricksAgentInvokeOperator(
        task_id="invoke_agent_deferrable",
        app_url=APP_URL,
        databricks_conn_id=CONN_ID,
        session_id=SESSION_ID,
        input=INPUT,
        deferrable=True,
        timeout=3600,
    )
    # [END howto_operator_databricks_agent_invoke_deferrable]

    verify_deferred = verify_result.override(task_id="verify_deferred")(invoke_deferrable.output, SESSION_ID)
    verify_managed >> invoke_deferrable >> verify_deferred

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag, conn_file_path=os.environ.get("DATABRICKS_AGENT_CONN_FILE"))
