#
# jkbngl
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

"""Example DAG demonstrating the usage of the MsSqlToOracleTransfer Operator."""
from airflow import DAG
from airflow.contrib.operators.mssql_to_oracle_transfer import MsSqlToOracleTransfer
from airflow.utils import dates

args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(2),
}

dag = DAG(dag_id='example_mssql_to_oracle_tranfer', default_args=args, tags=['example'])

cond_true = MsSqlToOracleTransfer(
    task_id='example_mssql_to_oracle_tranfer_task',
	oracle_destination_conn_id="oracle_default",
	destination_table="dual",
	mssql_source_conn_id="mssql_default",
	source_sql="select getdate()",
	rows_chunk=10000,
    dag=dag,
)


