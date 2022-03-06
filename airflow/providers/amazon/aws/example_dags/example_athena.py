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

from datetime import datetime, timedelta
from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor

# [START howto_operator_athena_env_variables]
S3_BUCKET = getenv("S3_BUCKET", "test-bucket")
S3_KEY = getenv("S3_KEY", "key")
ATHENA_TABLE = getenv("ATHENA_TABLE", "test_table")
ATHENA_DATABASE = getenv("ATHENA_DATABASE", "default")
# [END howto_operator_athena_env_variables]

SAMPLE_DATA = """"Alice",20
"Bob",25
"Charlie",30
"""
SAMPLE_FILENAME = 'airflow_sample.csv'


@task(task_id='setup__add_sample_data_to_s3')
def add_sample_data_to_s3():
    s3_hook = S3Hook()
    s3_hook.load_string(SAMPLE_DATA, f'{S3_KEY}/{ATHENA_TABLE}/{SAMPLE_FILENAME}', S3_BUCKET, replace=True)


@task(task_id='teardown__remove_sample_data_from_s3')
def remove_sample_data_from_s3():
    s3_hook = S3Hook()
    if s3_hook.check_for_key(f'{S3_KEY}/{ATHENA_TABLE}/{SAMPLE_FILENAME}', S3_BUCKET):
        s3_hook.delete_objects(S3_BUCKET, f'{S3_KEY}/{ATHENA_TABLE}/{SAMPLE_FILENAME}')


@task(task_id='query__read_results_from_s3')
def read_results_from_s3(query_execution_id):
    s3_hook = S3Hook()
    if s3_hook.check_for_key(f'{S3_KEY}/{query_execution_id}.csv', S3_BUCKET):
        file_obj = s3_hook.get_conn().get_object(Bucket=S3_BUCKET, Key=f'{S3_KEY}/{query_execution_id}.csv')
        file_content = file_obj['Body'].read().decode('utf-8')
        print(file_content)
    else:
        print('Could not find QueryExecutionId:', query_execution_id)


QUERY_CREATE_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE} ( `name` string, `age` int )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ( 'serialization.format' = ',', 'field.delim' = ','
) LOCATION 's3://{S3_BUCKET}/{S3_KEY}/{ATHENA_TABLE}'
TBLPROPERTIES ('has_encrypted_data'='false')
"""

QUERY_READ_TABLE = f"""
SELECT * from {ATHENA_DATABASE}.{ATHENA_TABLE}
"""

QUERY_DROP_TABLE = f"""
DROP TABLE IF EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE}
"""

with DAG(
    dag_id='example_athena',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_athena_operator_and_sensor]

    # Using a task-decorated function to create a CSV file in S3
    add_sample_data_to_s3 = add_sample_data_to_s3()

    create_table = AthenaOperator(
        task_id='setup__create_table',
        query=QUERY_CREATE_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
        sleep_time=30,
        max_tries=None,
    )

    read_table = AthenaOperator(
        task_id='query__read_table',
        query=QUERY_READ_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
        sleep_time=30,
        max_tries=None,
    )

    get_read_state = AthenaSensor(
        task_id='query__get_read_state',
        query_execution_id=read_table.output,
        max_retries=None,
        sleep_time=10,
    )

    # Using a task-decorated function to read the results from S3
    read_results_from_s3 = read_results_from_s3(read_table.output)

    drop_table = AthenaOperator(
        task_id='teardown__drop_table',
        query=QUERY_DROP_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
        sleep_time=30,
        max_tries=None,
    )

    # Using a task-decorated function to delete the S3 file we created earlier
    remove_sample_data_from_s3 = remove_sample_data_from_s3()

    (
        add_sample_data_to_s3  # type: ignore
        >> create_table
        >> read_table
        >> get_read_state
        >> read_results_from_s3
        >> drop_table
        >> remove_sample_data_from_s3
    )
    # [END howto_athena_operator_and_sensor]

    # Task dependencies created via `XComArgs`:
    #   read_table >> get_read_state
    #   read_table >> read_results_from_s3
