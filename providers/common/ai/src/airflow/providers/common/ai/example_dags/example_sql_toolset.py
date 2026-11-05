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
"""Example Dag: a SQL agent held to two tables, read-only, with bounded results and a call budget."""

from __future__ import annotations

from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import dag

try:
    from airflow.providers.common.ai.toolsets.sql import SQLToolset
except Exception:
    SQLToolset = None  # type: ignore[assignment,misc]


if SQLToolset is not None:
    # [START howto_toolset_sql_restricted]
    @dag(tags=["example"])
    def example_sql_toolset_restricted():
        AgentOperator(
            task_id="revenue_by_region",
            prompt="What was last week's revenue by region?",
            llm_conn_id="pydanticai_default",
            toolsets=[
                SQLToolset(
                    # Log in as a role granted SELECT on orders and customers only.
                    db_conn_id="warehouse_agent_reader",
                    # Refuse any query that reaches another table or a source the parser cannot check.
                    allowed_tables=["orders", "customers"],
                    # Accept this function, which the SQL parser does not recognize.
                    allowed_functions=["json_build_object"],
                    # The default: allow only SELECT-family, DESCRIBE and SHOW statements.
                    allow_writes=False,
                    # Return at most 100 rows and 32 KiB from one query.
                    max_rows=100,
                    max_result_bytes=32 * 1024,
                    # Summarize a table wider than 50 columns instead of listing every column.
                    max_columns=50,
                    # Let the model correct a refused or failed call up to 3 times.
                    max_retries=3,
                )
            ],
            # Fail the run before a 21st successful tool call, counted across retries on Airflow 3.3+.
            usage_limits=UsageLimits(tool_calls_limit=20),
        )

    # [END howto_toolset_sql_restricted]

    example_sql_toolset_restricted()
