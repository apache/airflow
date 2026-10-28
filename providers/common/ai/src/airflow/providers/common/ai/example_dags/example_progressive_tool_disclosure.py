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
"""Example Dags: an agent whose S3 tools stay hidden until the model searches for them."""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets import HookToolset
from airflow.providers.common.compat.sdk import dag

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    S3Hook = None  # type: ignore[assignment,misc]


if S3Hook is not None:
    # [START howto_toolset_defer_loading]
    # Nine read-only S3Hook methods, one tool each.
    REPORT_METHODS = [
        "list_keys",
        "list_prefixes",
        "check_for_key",
        "check_for_prefix",
        "check_for_wildcard_key",
        "check_for_bucket",
        "head_object",
        "read_key",
        "get_bucket_tagging",
    ]

    @dag(tags=["example"])
    def example_defer_loading():
        AgentOperator(
            task_id="answer_from_reports",
            prompt="Summarize the reports under 2026/09/.",
            llm_conn_id="pydanticai_default",
            toolsets=[
                # Hide every tool until the model searches for it.
                HookToolset(
                    S3Hook(aws_conn_id="aws_reports_reader"),
                    allowed_methods=REPORT_METHODS,
                    pinned_arguments={"bucket_name": "acme-reports"},
                    tool_prefix="s3",
                ).defer_loading()
            ],
        )

    # [END howto_toolset_defer_loading]

    example_defer_loading()

    # [START howto_toolset_defer_loading_some]
    @dag(tags=["example"])
    def example_defer_loading_some():
        AgentOperator(
            task_id="answer_from_reports",
            prompt="Summarize the reports under 2026/09/.",
            llm_conn_id="pydanticai_default",
            toolsets=[
                HookToolset(
                    S3Hook(aws_conn_id="aws_reports_reader"),
                    allowed_methods=[
                        "list_keys",
                        "list_prefixes",
                        "check_for_key",
                        "check_for_prefix",
                        "check_for_wildcard_key",
                        "check_for_bucket",
                        "head_object",
                        "read_key",
                        "get_bucket_tagging",
                    ],
                    pinned_arguments={"bucket_name": "acme-reports"},
                    tool_prefix="s3",
                ).defer_loading(
                    # Hide these seven; s3_list_keys and s3_read_key, which most runs use, stay visible.
                    [
                        "s3_list_prefixes",
                        "s3_check_for_key",
                        "s3_check_for_prefix",
                        "s3_check_for_wildcard_key",
                        "s3_check_for_bucket",
                        "s3_head_object",
                        "s3_get_bucket_tagging",
                    ]
                )
            ],
        )

    # [END howto_toolset_defer_loading_some]

    example_defer_loading_some()
