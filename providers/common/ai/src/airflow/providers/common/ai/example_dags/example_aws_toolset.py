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
"""Example Dag: agent with allow-listed AWS access via AWSToolset."""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.aws import AWSToolset
from airflow.providers.common.compat.sdk import dag


# [START howto_operator_agent_aws]
@dag(tags=["example"])
def example_agent_aws_toolset():
    AgentOperator(
        task_id="s3_auditor",
        prompt="Which buckets exist, and roughly how much data is in 'data-lake-raw'?",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are an AWS operations assistant. Discover what you are allowed "
            "to call, check parameter shapes before calling, and answer with "
            "concrete numbers."
        ),
        toolsets=[
            AWSToolset(
                aws_conn_id="aws_default",
                allowed_actions=[
                    "s3:ListBuckets",
                    "s3:ListObjectsV2",
                    "s3:GetBucketLocation",
                ],
                region_name="us-east-1",
            )
        ],
    )


# [END howto_operator_agent_aws]

example_agent_aws_toolset()
