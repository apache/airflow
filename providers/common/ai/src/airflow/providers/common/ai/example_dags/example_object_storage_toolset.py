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
"""Example Dag: an agent that reads the files under one object-storage path."""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets import ObjectStorageToolset
from airflow.providers.common.compat.sdk import dag, task


# [START howto_toolset_object_storage]
@dag(tags=["example"])
def example_object_storage_toolset():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You answer questions about the files in a reports bucket. List directories "
            "to find what exists, then read the files you need. Cite the files you used."
        ),
        toolsets=[
            # Read-only, and every path the model names is resolved under this root.
            ObjectStorageToolset("s3://acme-reports/finance/", conn_id="aws_default"),
        ],
    )
    def summarize_month(month: str) -> str:
        return f"Summarize revenue and the biggest changes for {month}."

    summarize_month("2026-09")


# [END howto_toolset_object_storage]


example_object_storage_toolset()


# [START howto_toolset_object_storage_restricted]
@dag(tags=["example"])
def example_object_storage_toolset_restricted():
    AgentOperator(
        task_id="read_finance_reports",
        prompt="Summarize the September finance report.",
        llm_conn_id="pydanticai_default",
        toolsets=[
            ObjectStorageToolset(
                # Resolve every path the model names under this root. The toolset never writes.
                "s3://acme-reports/finance/",
                # Credentials scoped to that prefix keep the limit if the path check has a gap.
                conn_id="aws_reports_reader",
                # List at most 50 entries per call; the model pages through the rest.
                max_files=50,
                # Refuse a file larger than 1 MiB, measured after decompression.
                max_read_bytes=1024 * 1024,
                # Return at most 16 KiB from one read; the model pages through a longer text file.
                max_output_bytes=16 * 1024,
                # Name the tools reports_list_files, reports_get_file_info and reports_read_file.
                tool_prefix="reports",
                # Let the model correct an invalid call up to 2 times.
                max_retries=2,
            )
        ],
    )


# [END howto_toolset_object_storage_restricted]


example_object_storage_toolset_restricted()
