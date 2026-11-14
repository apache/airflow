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
"""Example Dag: an agent that runs read-only SQL over two registered tables of files on S3."""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import dag

try:
    from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
    from airflow.providers.common.sql.config import DataSourceConfig
except Exception:
    DataFusionToolset = None  # type: ignore[assignment,misc]


if DataFusionToolset is not None:
    # [START howto_toolset_datafusion_restricted]
    @dag(tags=["example"])
    def example_datafusion_toolset_restricted():
        AgentOperator(
            task_id="returns_by_region",
            prompt="Which region had the highest return rate in September?",
            llm_conn_id="pydanticai_default",
            toolsets=[
                DataFusionToolset(
                    # Register only what the agent may query: there is no other allow-list.
                    datasource_configs=[
                        DataSourceConfig(
                            conn_id="aws_reports_reader",
                            table_name="sales",
                            uri="s3://acme-reports/finance/sales/",
                            format="parquet",
                        ),
                        DataSourceConfig(
                            conn_id="aws_reports_reader",
                            table_name="returns",
                            uri="s3://acme-reports/finance/returns/",
                            format="csv",
                        ),
                    ],
                    # The default: allow only SELECT-family statements.
                    allow_writes=False,
                    # Return at most 100 rows and 32 KiB from one query.
                    max_rows=100,
                    max_result_bytes=32 * 1024,
                    # Summarize a table wider than 50 columns instead of listing every column.
                    max_columns=50,
                    # Let the model correct a refused query, or one naming an unknown table or
                    # column, up to 3 times.
                    max_retries=3,
                )
            ],
        )

    # [END howto_toolset_datafusion_restricted]

    example_datafusion_toolset_restricted()
