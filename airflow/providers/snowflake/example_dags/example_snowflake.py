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
import os

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = os.environ.get('SNOWFLAKE_CONN_ID', 'snowflake_default')
SLACK_CONN_ID = os.environ.get('SLACK_CONN_ID', 'slack_default')
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'public')
SNOWFLAKE_STAGE = os.environ.get('SNOWFLAKE_STAGE', 'airflow')
SNOWFLAKE_SAMPLE_TABLE = os.environ.get('SNOWFLAKE_SAMPLE_TABLE', 'snowflake_sample_data.tpch_sf001.orders')
SNOWFLAKE_LOAD_TABLE = os.environ.get('SNOWFLAKE_LOAD_TABLE', 'airflow_example')
SNOWFLAKE_LOAD_JSON_PATH = os.environ.get('SNOWFLAKE_LOAD_PATH', 'example.json')

SNOWFLAKE_SELECT_SQL = f"SELECT * FROM {SNOWFLAKE_SAMPLE_TABLE} LIMIT 100;"
SNOWFLAKE_SLACK_SQL = f"SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS FROM {SNOWFLAKE_SAMPLE_TABLE} LIMIT 10;"
SNOWFLAKE_SLACK_MESSAGE = "Results in an ASCII table:\n" \
                          "```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```"
SNOWFLAKE_CREATE_TABLE_SQL = f"CREATE TRANSIENT TABLE IF NOT EXISTS {SNOWFLAKE_LOAD_TABLE}(data VARIANT);"

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'example_snowflake',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['example'],
)

select = SnowflakeOperator(
    task_id='select',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SNOWFLAKE_SELECT_SQL,
    dag=dag,
)

slack_report = SnowflakeToSlackOperator(
    task_id="slack_report",
    sql=SNOWFLAKE_SLACK_SQL,
    slack_message=SNOWFLAKE_SLACK_MESSAGE,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    slack_conn_id=SLACK_CONN_ID,
    dag=dag
)

create_table = SnowflakeOperator(
    task_id='create_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SNOWFLAKE_CREATE_TABLE_SQL,
    schema=SNOWFLAKE_SCHEMA,
    dag=dag,
)

copy_into_table = S3ToSnowflakeOperator(
    task_id='copy_into_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    s3_keys=[SNOWFLAKE_LOAD_JSON_PATH],
    table=SNOWFLAKE_LOAD_TABLE,
    schema=SNOWFLAKE_SCHEMA,
    stage=SNOWFLAKE_STAGE,
    file_format="(type = 'JSON')",
    dag=dag,
)

select >> slack_report
create_table >> copy_into_table
