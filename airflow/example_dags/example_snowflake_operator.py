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

"""Example DAG demonstrating the usage of the SnowflakeOperator."""


from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='example_snowflake_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example']
)

# [START howto_operator_snowflake]

create_table_sql_string = "CREATE OR REPLACE TABLE mytable (name VARCHAR(250), id INT);"
sql_insert_statement = "INSERT INTO mytable VALUES ('name', %s)"
sql_list = [sql_insert_statement % n for n in range(0, 10)]

snowflake_op_sql_str = SnowflakeOperator(
    task_id='snowflake_op_sql_str',
    dag=dag,
    snowflake_conn_id="snowflake_conn_id",
    sql=create_table_sql_string,
    warehouse="warehouse_name",
    database="database_name",
    schema="schema_name",
    role="role_name"
)

snowflake_op_with_params = SnowflakeOperator(
    task_id='snowflake_op_with_params',
    dag=dag,
    snowflake_conn_id="snowflake_conn_id",
    sql=sql_insert_statement,
    parameters=(56,),
    warehouse="warehouse_name",
    database="database_name",
    schema="schema_name",
    role="role_name",
)

snowflake_op_sql_list = SnowflakeOperator(
    task_id='snowflake_op_sql_list',
    dag=dag,
    snowflake_conn_id="snowflake_conn_id",
    sql=sql_list,
    warehouse="warehouse_name",
    database="database_name",
    schema="schema_name",
    role="role_name",

)

snowflake_op_template_file = SnowflakeOperator(
    task_id='snowflake_op_template_file',
    dag=dag,
    snowflake_conn_id="snowflake_conn_id",
    sql='/path/to/sql/<filename>.sql',
    warehouse="warehouse_name",
    database="database_name",
    schema="schema_name",
    role="role_name",
)

snowflake_op_sql_str >> snowflake_op_with_params >> snowflake_op_sql_list >> snowflake_op_template_file

# [END howto_operator_snowflake]

