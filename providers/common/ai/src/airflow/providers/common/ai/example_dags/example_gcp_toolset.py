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
"""Example Dag: agent with allow-listed Google API access via GoogleCloudToolset."""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.google import GoogleCloudToolset
from airflow.providers.common.compat.sdk import dag


# [START howto_operator_agent_gcp]
@dag(tags=["example"])
def example_agent_gcp_toolset():
    AgentOperator(
        task_id="gcs_auditor",
        prompt="Which buckets exist, and roughly how many objects are in 'data-lake-raw'?",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are a Google Cloud operations assistant. Discover what you "
            "are allowed to call, check parameter shapes before calling, and "
            "answer with concrete numbers."
        ),
        toolsets=[
            GoogleCloudToolset(
                gcp_conn_id="google_cloud_default",
                allowed_methods=[
                    "storage/v1:buckets.list",
                    "storage/v1:buckets.get",
                    "storage/v1:objects.list",
                ],
            )
        ],
    )


# [END howto_operator_agent_gcp]

example_agent_gcp_toolset()
