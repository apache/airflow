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
"""Example Dag: an agent that can list and read one S3 bucket through ``S3Hook``, and nothing else."""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets import HookToolset
from airflow.providers.common.compat.sdk import dag

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    S3Hook = None  # type: ignore[assignment,misc]


if S3Hook is not None:
    # [START howto_toolset_hook_restricted]
    @dag(tags=["example"])
    def example_hook_toolset_restricted():
        AgentOperator(
            task_id="read_reports",
            prompt="Summarize the September finance report.",
            llm_conn_id="pydanticai_default",
            toolsets=[
                HookToolset(
                    # Log in with credentials scoped to the reports bucket.
                    S3Hook(aws_conn_id="aws_reports_reader"),
                    # Offer these two methods as tools; no other S3Hook method is reachable.
                    allowed_methods=["list_keys", "read_key"],
                    # Fix the bucket: the model never sees the argument and cannot set it.
                    pinned_arguments={"bucket_name": "acme-reports"},
                    # Name the tools s3_list_keys and s3_read_key.
                    tool_prefix="s3_",
                    # Let the model correct an invalid call up to 2 times.
                    max_retries=2,
                )
            ],
        )

    # [END howto_toolset_hook_restricted]

    example_hook_toolset_restricted()
