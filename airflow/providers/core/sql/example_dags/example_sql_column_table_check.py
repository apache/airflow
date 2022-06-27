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
from airflow import DAG
from airflow.providers.core.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator
from airflow.utils.dates import datetime

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
    "example_sql_column_table_check",
    description="Example DAG for SQLColumnCheckOperator and SQLTableCheckOperator.",
    default_args=connection_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    """
    ### Example SQL Column and Table Check DAG

    Runs the SQLColumnCheckOperator and SQLTableCheckOperator against the Airflow metadata DB.
    """

    # [START howto_operator_sql_column_check]
    column_check = SQLColumnCheckOperator(
        task_id="column_check",
        table=AIRFLOW_DB_METADATA_TABLE,
        column_mapping={
            "id": {
                "null_check": {
                    "equal_to": 0,
                    "tolerance": 0,
                },
                "distinct_check": {
                    "equal_to": 1,
                },
            }
        },
    )
    # [END howto_operator_sql_column_check]

    # [START howto_operator_sql_table_check]
    row_count_check = SQLTableCheckOperator(
        task_id="row_count_check",
        table=AIRFLOW_DB_METADATA_TABLE,
        checks={
            "row_count_check": {
                "check_statement": "COUNT(*) = 1",
            }
        },
    )
    # [END howto_operator_sql_table_check]

    column_check >> row_count_check
