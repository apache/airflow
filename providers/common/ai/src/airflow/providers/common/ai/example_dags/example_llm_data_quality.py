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
"""Example DAGs demonstrating LLMDataQualityOperator usage with AWS."""

from __future__ import annotations

from airflow.providers.common.ai.operators.llm_data_quality import LLMDataQualityOperator
from airflow.providers.common.ai.utils.dq_validation import (
    duplicate_pct_check,
    exact_check,
    null_pct_check,
    row_count_check,
)
from airflow.providers.common.compat.sdk import dag
from airflow.providers.common.sql.config import DataSourceConfig


# [START howto_operator_llm_dq_s3_parquet]
@dag
def example_llm_dq_s3_parquet():
    """
    Run data-quality checks on a Parquet dataset stored in Amazon S3.

    DataFusion is used to register the S3 source so the LLM can introspect
    its schema and generate the appropriate SQL.

    Connections required:
    - ``aws_llm``: Pydantic AI connection with Bedrock.
    - ``aws_default``: AWS connection with S3 read permissions.
    """
    datasource_config = DataSourceConfig(
        conn_id="aws_default",
        table_name="sales_events",
        uri="s3://my-data-lake/events/sales/year=2025/",
        format="parquet",
    )

    LLMDataQualityOperator(
        task_id="validate_sales_events",
        llm_conn_id="aws_llm",
        datasource_config=datasource_config,
        prompt_version="v1",
        prompts={
            "null_event_id": "Check the percentage of rows where event_id is NULL",
            "invalid_revenue": "Count rows where revenue is negative or NULL",
            "stale_data": "Count rows where event_timestamp is older than 90 days",
            "duplicate_events": "Calculate the percentage of duplicate event_id values",
            "min_row_count": "Count the total number of rows in the dataset",
        },
        validators={
            "null_event_id": null_pct_check(max_pct=0.0),
            "invalid_revenue": exact_check(expected=0),
            "stale_data": lambda v: int(v) < 1000,
            "duplicate_events": duplicate_pct_check(max_pct=0.0),
            "min_row_count": row_count_check(min_count=1_000_000),
        },
    )


# [END howto_operator_llm_dq_s3_parquet]

example_llm_dq_s3_parquet()
