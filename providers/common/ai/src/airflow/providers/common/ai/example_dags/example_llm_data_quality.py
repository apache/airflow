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
from airflow.providers.standard.operators.hitl import ApprovalOperator


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


_DQ_PROMPTS = {
    "null_order_id": "Check the percentage of rows where order_id is NULL",
    "negative_amount": "Count rows where order_amount is negative or NULL",
    "duplicate_orders": "Calculate the percentage of duplicate order_id values",
    "min_row_count": "Count the total number of rows in the orders table",
}

_DQ_VALIDATORS = {
    "null_order_id": null_pct_check(max_pct=0.0),
    "negative_amount": exact_check(expected=0),
    "duplicate_orders": duplicate_pct_check(max_pct=0.0),
    "min_row_count": row_count_check(min_count=10_000),
}

_DQ_COMMON_KWARGS = dict(
    llm_conn_id="pydanticai_default",
    db_conn_id="postgres_default",
    table_names=["orders"],
    prompt_version="v1",
    prompts=_DQ_PROMPTS,
)


# [START howto_operator_llm_dq_with_human_approval]
@dag
def example_llm_dq_with_approval():
    """
    Generate a DQ plan, let a human review it, then execute the checks.

     Workflow:
     ``preview_dq_plan`` runs with ``dry_run=True`` to generate (and cache) the SQL
     plan via the LLM, but does **not** execute it. It returns the serialised plan
     dict as XCom so the approver can review the generated SQL.
     ``approve_dq_plan`` pauses the DAG and shows the generated SQL to the user.
     Selecting "Approve" proceeds to execution; selecting "Reject" skips it.
     ``run_dq_checks`` executes the cached plan against the database and validates
     each metric. Because the plan was already cached by ``preview_dq_plan``, the
     LLM is **not** called a second time.

    Connections required:
    - ``pydanticai_default``: Pydantic AI connection (any supported LLM provider).
    - ``postgres_default``: PostgreSQL connection for the target database.
    """
    preview_dq_plan = LLMDataQualityOperator(
        task_id="preview_dq_plan",
        dry_run=True,
        **_DQ_COMMON_KWARGS,
    )

    approve_dq_plan = ApprovalOperator(
        task_id="approve_dq_plan",
        subject="Review the generated DQ plan before execution",
        body=(
            "The LLM generated the following SQL plan for the data-quality checks.\n"
            "Please review it and approve or reject.\n\n"
            "Plan:\n"
            "{{ ti.xcom_pull(task_ids='preview_dq_plan') | tojson(indent=2) }}"
        ),
    )

    run_dq_checks = LLMDataQualityOperator(
        task_id="run_dq_checks",
        validators=_DQ_VALIDATORS,
        **_DQ_COMMON_KWARGS,
    )

    preview_dq_plan >> approve_dq_plan >> run_dq_checks


# [END howto_operator_llm_dq_with_human_approval]

example_llm_dq_with_approval()
