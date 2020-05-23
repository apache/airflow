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
from airflow.providers.snowflake.operators.s3_to_snowflake import S3ToSnowflakeTransfer
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = os.environ.get('SNOWFLAKE_CONN_ID', 'snowflake_default')
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'public')
SNOWFLAKE_STAGE = os.environ.get('SNOWFLAKE_STAGE', 'airflow')
SNOWFLAKE_SAMPLE_TABLE = os.environ.get('SNOWFLAKE_SAMPLE_TABLE', 'snowflake_sample_data.tpch_sf001.orders')
SNOWFLAKE_LOAD_TABLE = os.environ.get('SNOWFLAKE_LOAD_TABLE', 'airflow_example')
SNOWFLAKE_LOAD_JSON_PATH = os.environ.get('SNOWFLAKE_LOAD_PATH', 'example.json')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    'example_snowflake',
    default_args=default_args,
    tags=['example'],
)

select = SnowflakeOperator(
    task_id='select',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    SELECT *
    FROM {0}
    LIMIT 100;
    """.format(SNOWFLAKE_SAMPLE_TABLE),
    dag=dag,
)

create_table = SnowflakeOperator(
    task_id='create_table',
    snowflake_conn_id='snowflake_conn_id',
    sql="""
    CREATE TRANSIENT TABLE IF NOT EXISTS {0} (
        data VARIANT
    );
    """.format(SNOWFLAKE_LOAD_TABLE),
    schema=SNOWFLAKE_SCHEMA,
    dag=dag,
)

copy_into_table = S3ToSnowflakeTransfer(
    task_id='copy_into_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    s3_keys=[SNOWFLAKE_LOAD_JSON_PATH],
    table=SNOWFLAKE_LOAD_TABLE,
    schema=SNOWFLAKE_SCHEMA,
    stage=SNOWFLAKE_STAGE,
    file_format="(type = 'JSON')",
    dag=dag,
)

create_table >> copy_into_table
