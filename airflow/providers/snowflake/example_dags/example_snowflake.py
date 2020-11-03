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
Example use of Snowflake related operators.
"""
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = 'my_snowflake_conn'
SLACK_CONN_ID = 'my_slack_conn'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_SCHEMA = 'schema_name'
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'warehouse_name'
SNOWFLAKE_DATABASE = 'database_name'
SNOWFLAKE_ROLE = 'role_name'
SNOWFLAKE_SAMPLE_TABLE = 'sample_table'
S3_FILE_PATH = '</path/to/file/sample_file.csv'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SNOWFLAKE_SLACK_SQL = f"SELECT name, id FROM {SNOWFLAKE_SAMPLE_TABLE} LIMIT 10;"
SNOWFLAKE_SLACK_MESSAGE = (
    "Results in an ASCII table:\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```"
)

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'example_snowflake',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['example'],
)

# [START howto_operator_snowflake]

snowflake_op_sql_str = SnowflakeOperator(
    task_id='snowflake_op_sql_str',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=CREATE_TABLE_SQL_STRING,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)

snowflake_op_with_params = SnowflakeOperator(
    task_id='snowflake_op_with_params',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_INSERT_STATEMENT,
    parameters={"id": 56},
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)

snowflake_op_sql_list = SnowflakeOperator(
    task_id='snowflake_op_sql_list', dag=dag, snowflake_conn_id=SNOWFLAKE_CONN_ID, sql=SQL_LIST
)

snowflake_op_template_file = SnowflakeOperator(
    task_id='snowflake_op_template_file',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql='/path/to/sql/<filename>.sql',
)

# [END howto_operator_snowflake]

# [START howto_operator_s3_to_snowflake]

copy_into_table = S3ToSnowflakeOperator(
    task_id='copy_into_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    s3_keys=[S3_FILE_PATH],
    table=SNOWFLAKE_SAMPLE_TABLE,
    schema=SNOWFLAKE_SCHEMA,
    stage=SNOWFLAKE_STAGE,
    file_format="(type = 'CSV'," "field_delimiter = ';')",
    dag=dag,
)

# [END howto_operator_s3_to_snowflake]

# [START howto_operator_snowflake_to_slack]

slack_report = SnowflakeToSlackOperator(
    task_id="slack_report",
    sql=SNOWFLAKE_SLACK_SQL,
    slack_message=SNOWFLAKE_SLACK_MESSAGE,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    slack_conn_id=SLACK_CONN_ID,
    dag=dag,
)

# [END howto_operator_snowflake_to_slack]

snowflake_op_sql_str >> [
    snowflake_op_with_params,
    snowflake_op_sql_list,
    snowflake_op_template_file,
    copy_into_table,
] >> slack_report
