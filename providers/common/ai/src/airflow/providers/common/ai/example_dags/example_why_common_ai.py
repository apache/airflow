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
Check a report against the warehouse with ``AgentOperator``.

The "Why Common AI" guide shows this Dag next to the same job written as a plain ``@task``.
"""

from __future__ import annotations

# [START example_why_common_ai]
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets import ObjectStorageToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import dag


@dag(tags=["example"])
def example_why_common_ai():
    AgentOperator(
        task_id="check_report",
        prompt="Does the September report's revenue match the orders table?",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You check reports against the warehouse. Read the report files, query the "
            "orders table, and say whether the numbers agree."
        ),
        toolsets=[
            SQLToolset(db_conn_id="warehouse", allowed_tables=["orders"]),
            ObjectStorageToolset("s3://acme-reports/finance/", conn_id="aws_default"),
        ],
        retries=2,
        # A retry replays the model and tool calls that already finished.
        durable=True,
        # On Airflow 3.3+ one total covers every attempt of the task instance.
        usage_limits={"request_limit": 30, "total_tokens_limit": 200_000},
    )


# [END example_why_common_ai]

example_why_common_ai()
