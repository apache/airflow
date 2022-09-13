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
"""
This is an example DAG which uses the DatabricksSubmitRunOperator.
In this example, we create two tasks which execute sequentially.
The first task is to run a notebook at the workspace path "/test"
and the second task is to run a JAR uploaded to DBFS. Both,
tasks use new clusters.

Because we have set a downstream dependency on the notebook task,
the spark jar task will NOT run until the notebook task completes
successfully.

The definition of a successful run is if the run has a result_state of "SUCCESS".
For more information about the state of a run refer to
https://docs.databricks.com/api/latest/jobs.html#runstate
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import (
    DatabricksCopyIntoOperator,
    DatabricksSqlOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_sql_operator"

with DAG(
    dag_id=DAG_ID,
    schedule='@daily',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    connection_id = 'my_connection'
    sql_endpoint_name = "My Endpoint"

    # [START howto_operator_databricks_sql_multiple]
    # Example of using the Databricks SQL Operator to perform multiple operations.
    create = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id='create_and_populate_table',
        sql=[
            "drop table if exists default.my_airflow_table",
            "create table default.my_airflow_table(id int, v string)",
            "insert into default.my_airflow_table values (1, 'test 1'), (2, 'test 2')",
        ],
    )
    # [END howto_operator_databricks_sql_multiple]

    # [START howto_operator_databricks_sql_select]
    # Example of using the Databricks SQL Operator to select data.
    select = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id='select_data',
        sql="select * from default.my_airflow_table",
    )
    # [END howto_operator_databricks_sql_select]

    # [START howto_operator_databricks_sql_select_file]
    # Example of using the Databricks SQL Operator to select data into a file with JSONL format.
    select_into_file = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id='select_data_into_file',
        sql="select * from default.my_airflow_table",
        output_path="/tmp/1.jsonl",
        output_format="jsonl",
    )
    # [END howto_operator_databricks_sql_select_file]

    # [START howto_operator_databricks_sql_multiple_file]
    # Example of using the Databricks SQL Operator to select data.
    # SQL statements should be in the file with name test.sql
    create_file = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id='create_and_populate_from_file',
        sql="test.sql",
    )
    # [END howto_operator_databricks_sql_multiple_file]

    # [START howto_operator_databricks_copy_into]
    # Example of importing data using COPY_INTO SQL command
    import_csv = DatabricksCopyIntoOperator(
        task_id='import_csv',
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        table_name="my_table",
        file_format="CSV",
        file_location="abfss://container@account.dfs.core.windows.net/my-data/csv",
        format_options={'header': 'true'},
        force_copy=True,
    )
    # [END howto_operator_databricks_copy_into]

    (create >> create_file >> import_csv >> select >> select_into_file)

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
