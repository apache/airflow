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
Example Dag giving an agent the tools of a Unity Gateway MCP Service.

Needs a Databricks connection whose identity has EXECUTE on the MCP Service and USE CATALOG and
USE SCHEMA on its catalog and schema, an LLM connection for the agent, and these environment
variables:

- ``UNITY_MCP_SERVICE``: the service's three-level name, ``catalog.schema.service``.
- ``UNITY_MCP_READ_ONLY_TOOL``: the name of one of its tools that only reads, which the agent calls
  once.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import DAG
from airflow.providers.databricks.toolsets.unity_mcp import DatabricksUnityMCPToolset

DAG_ID = "example_databricks_unity_mcp"
UNITY_MCP_SERVICE = os.environ.get("UNITY_MCP_SERVICE", "main.default.my_mcp_service")
UNITY_MCP_READ_ONLY_TOOL = os.environ.get("UNITY_MCP_READ_ONLY_TOOL", "my_read_only_tool")

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_toolset_databricks_unity_mcp]
    ask_unity_mcp_service = AgentOperator(
        task_id="ask_unity_mcp_service",
        prompt=(
            "List the names of the tools you have. Then call the "
            f"{UNITY_MCP_READ_ONLY_TOOL} tool once and summarize what it returned."
        ),
        llm_conn_id="pydanticai_default",
        toolsets=[
            DatabricksUnityMCPToolset(UNITY_MCP_SERVICE, databricks_conn_id="databricks_default"),
        ],
    )
    # [END howto_toolset_databricks_unity_mcp]

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
