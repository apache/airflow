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
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.timezone import datetime

AIRFLOW_DB_METADATA_TABLE = "ab_role"
connection_args = {
    "conn_id": "airflow_db",
    "conn_type": "Postgres",
    "host": "postgres",
    "schema": "postgres",
    "login": "postgres",
    "password": "postgres",
    "port": 5432,
}

with DAG(
    "example_sql_execute_query",
    description="Example DAG for SQLExecuteQueryOperator.",
    default_args=connection_args,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    """
    ### Example SQL execute query DAG

    Runs the SQLExecuteQueryOperator against the Airflow metadata DB.
    """

    # [START howto_operator_sql_execute_query]
    execute_query = SQLExecuteQueryOperator(
        task_id="execute_query",
        sql=f"SELECT 1; SELECT * FROM {AIRFLOW_DB_METADATA_TABLE} LIMIT 1;",
        split_statements=True,
        return_last=False,
    )
    # [END howto_operator_sql_execute_query]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
