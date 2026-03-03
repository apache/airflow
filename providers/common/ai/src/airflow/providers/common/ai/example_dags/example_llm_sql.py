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
"""Example DAGs demonstrating LLMSQLQueryOperator usage."""

from __future__ import annotations

from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.common.sql.config import DataSourceConfig


# [START howto_operator_llm_sql_basic]
@dag
def example_llm_sql_basic():
    LLMSQLQueryOperator(
        task_id="generate_sql",
        prompt="Find the top 10 customers by total revenue",
        llm_conn_id="pydantic_ai_default",
        schema_context=(
            "Table: customers\n"
            "Columns: id INT, name TEXT, email TEXT\n\n"
            "Table: orders\n"
            "Columns: id INT, customer_id INT, total DECIMAL, created_at TIMESTAMP"
        ),
    )


# [END howto_operator_llm_sql_basic]

example_llm_sql_basic()


# [START howto_operator_llm_sql_schema]
@dag
def example_llm_sql_schema_introspection():
    LLMSQLQueryOperator(
        task_id="generate_sql",
        prompt="Calculate monthly revenue for 2024",
        llm_conn_id="pydantic_ai_default",
        db_conn_id="postgres_default",
        table_names=["orders", "customers"],
        dialect="postgres",
    )


# [END howto_operator_llm_sql_schema]

example_llm_sql_schema_introspection()


# [START howto_decorator_llm_sql]
@dag
def example_llm_sql_decorator():
    @task.llm_sql(
        llm_conn_id="pydantic_ai_default",
        schema_context="Table: users\nColumns: id INT, name TEXT, signup_date DATE",
    )
    def build_churn_query(ds=None):
        return f"Find users who signed up before {ds} and have no orders"

    build_churn_query()


# [END howto_decorator_llm_sql]

example_llm_sql_decorator()


# [START howto_operator_llm_sql_expand]
@dag
def example_llm_sql_expand():
    LLMSQLQueryOperator.partial(
        task_id="generate_sql",
        llm_conn_id="pydantic_ai_default",
        schema_context=(
            "Table: orders\nColumns: id INT, customer_id INT, total DECIMAL, created_at TIMESTAMP"
        ),
    ).expand(
        prompt=[
            "Total revenue by month",
            "Top 10 customers by order count",
            "Average order value by day of week",
        ]
    )


# [END howto_operator_llm_sql_expand]

example_llm_sql_expand()


# [START howto_operator_llm_sql_with_object_storage]
@dag
def example_llm_sql_with_object_storage():
    datasource_config = DataSourceConfig(
        conn_id="aws_default",
        table_name="sales_data",
        uri="s3://my-bucket/data/sales/",
        format="parquet",
    )

    LLMSQLQueryOperator(
        task_id="generate_sql",
        prompt="Find the top 5 products by total sales amount",
        llm_conn_id="pydantic_ai_default",
        datasource_config=datasource_config,
    )


# [END howto_operator_llm_sql_with_object_storage]

example_llm_sql_with_object_storage()
