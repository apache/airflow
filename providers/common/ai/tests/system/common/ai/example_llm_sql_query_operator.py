#
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
from __future__ import annotations

from airflow import DAG
from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator
from airflow.providers.common.ai.utils.config import DataSourceConfig
from airflow.utils import timezone

datasource_config_with_schema = DataSourceConfig(
    conn_id="postgres_default",
    uri="postgres://",
    table_name="customers",
    schema={
        "id": "integer",
        "name": "character varying",
        "time": "timestamp",
        "products": "character varying",
    },
)

datasource_config_without_schema = DataSourceConfig(
    conn_id="postgres_default",
    uri="postgres://",
    table_name="customers",
)


with DAG(
    "example_llm_sql_query",
    description="Example DAG for LLM SQL Query Generator.",
    start_date=timezone.datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # [START howto_operator_llm_sql_query_operator]

    llm_sql_query_operator_with_input_schema = LLMSQLQueryOperator(
        task_id="llm_sql_query_generator_with_input_schema",
        datasource_configs=[datasource_config_with_schema],
        prompts=[
            "Generate distinct customers for last week",
            "Find the customers that purchased the most products",
        ],
        provider_model="google:gemini-3-flash-preview",
    )

    # When there is no schema is provided, the LLM Operator fetches the schema using the conn_id provided from the respective hook.
    # Eg: here in this case it uses a postgres hook to fetch the schema. This feature is only supported for limited datasource's see
    # the supported databases in the operator docs.

    llm_sql_query_operator_without_input_schema = LLMSQLQueryOperator(
        task_id="llm_sql_query_generator_without_input_schema",
        datasource_configs=[datasource_config_without_schema],
        prompts=[
            "Generate distinct customers for last week",
            "Find the customers that purchased the most products",
        ],
        provider_model="google:gemini-3-flash-preview",
    )
    # [END howto_operator_llm_sql_query_operator]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
