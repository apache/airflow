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
"""Example DAGs demonstrating LLMSchemaCompareOperator usage."""

from __future__ import annotations

from airflow.providers.common.ai.operators.llm_schema_compare import LLMSchemaCompareOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.common.sql.config import DataSourceConfig


# [START howto_operator_llm_schema_compare_basic]
@dag
def example_llm_schema_compare_basic():
    LLMSchemaCompareOperator(
        task_id="detect_schema_drift",
        prompt="Identify schema mismatches that would break data loading between systems",
        llm_conn_id="pydanticai_default",
        db_conn_ids=["postgres_default", "snowflake_default"],
        table_names=["customers"],
    )


# [END howto_operator_llm_schema_compare_basic]

example_llm_schema_compare_basic()


# [START howto_operator_llm_schema_compare_full]
@dag
def example_llm_schema_compare_full_context():
    LLMSchemaCompareOperator(
        task_id="detect_schema_drift",
        prompt=(
            "Compare schemas and generate a migration plan. "
            "Flag any differences that would break nightly ETL loads."
        ),
        llm_conn_id="pydanticai_default",
        db_conn_ids=["postgres_source", "snowflake_target"],
        table_names=["customers", "orders"],
        context_strategy="full",
    )


# [END howto_operator_llm_schema_compare_full]

example_llm_schema_compare_full_context()


# [START howto_operator_llm_schema_compare_datasource]
@dag
def example_llm_schema_compare_with_object_storage():
    s3_source = DataSourceConfig(
        conn_id="aws_default",
        table_name="customers",
        uri="s3://data-lake/customers/",
        format="parquet",
    )

    LLMSchemaCompareOperator(
        task_id="compare_s3_vs_db",
        prompt="Compare S3 Parquet schema against the Postgres table and flag breaking changes",
        llm_conn_id="pydanticai_default",
        db_conn_ids=["postgres_default"],
        table_names=["customers"],
        data_sources=[s3_source],
    )


# [END howto_operator_llm_schema_compare_datasource]

example_llm_schema_compare_with_object_storage()


# [START howto_decorator_llm_schema_compare]
@dag
def example_llm_schema_compare_decorator():
    @task.llm_schema_compare(
        llm_conn_id="pydanticai_default",
        db_conn_ids=["postgres_source", "snowflake_target"],
        table_names=["customers"],
    )
    def check_migration_readiness(ds=None):
        return f"Compare schemas as of {ds}. Flag breaking changes and suggest migration actions."

    check_migration_readiness()


# [END howto_decorator_llm_schema_compare]

example_llm_schema_compare_decorator()


# [START howto_operator_llm_schema_compare_conditional]
@dag
def example_llm_schema_compare_conditional():
    @task.llm_schema_compare(
        llm_conn_id="pydanticai_default",
        db_conn_ids=["postgres_source", "snowflake_target"],
        table_names=["customers"],
        context_strategy="full",
    )
    def check_before_etl():
        return (
            "Compare schemas and flag any mismatches that would break data loading. "
            "No migrations allowed — report only."
        )

    @task.branch
    def decide(comparison_result):
        if comparison_result["compatible"]:
            return "run_etl"
        return "notify_team"

    comparison = check_before_etl()
    decision = decide(comparison)

    @task(task_id="run_etl")
    def run_etl():
        return "ETL completed"

    @task(task_id="notify_team")
    def notify_team():
        return "Schema drift detected — team notified"

    decision >> [run_etl(), notify_team()]


# [END howto_operator_llm_schema_compare_conditional]

example_llm_schema_compare_conditional()
