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
from datetime import datetime

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from tests.system.providers.amazon.aws.utils import set_env_id

ENV_ID = set_env_id()
DAG_ID = 'example_athena'

S3_BUCKET = f'{ENV_ID}-athena-bucket'
ATHENA_TABLE = f'{ENV_ID}_test_table'
ATHENA_DATABASE = f'{ENV_ID}_default'

SAMPLE_DATA = '''"Alice",20
    "Bob",25
    "Charlie",30
    '''
SAMPLE_FILENAME = 'airflow_sample.csv'

QUERY_CREATE_DATABASE = f'CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE}'
QUERY_CREATE_TABLE = f'''CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE}
    ( `name` string, `age` int )
    ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    WITH SERDEPROPERTIES ( "serialization.format" = ",", "field.delim" = "," )
    LOCATION "s3://{S3_BUCKET}//{ATHENA_TABLE}"
    TBLPROPERTIES ("has_encrypted_data"="false")
    '''
QUERY_READ_TABLE = f'SELECT * from {ATHENA_DATABASE}.{ATHENA_TABLE}'
QUERY_DROP_TABLE = f'DROP TABLE IF EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE}'
QUERY_DROP_DATABASE = f'DROP DATABASE IF EXISTS {ATHENA_DATABASE}'


@task
def await_bucket():
    # Avoid a race condition after creating the S3 Bucket.
    client = boto3.client('s3')
    waiter = client.get_waiter('bucket_exists')
    waiter.wait(Bucket=S3_BUCKET)


@task
def read_results_from_s3(query_execution_id):
    s3_hook = S3Hook()
    file_obj = s3_hook.get_conn().get_object(Bucket=S3_BUCKET, Key=f'{query_execution_id}.csv')
    file_content = file_obj['Body'].read().decode('utf-8')
    print(file_content)


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    create_s3_bucket = S3CreateBucketOperator(task_id='create_s3_bucket', bucket_name=S3_BUCKET)

    upload_sample_data = S3CreateObjectOperator(
        task_id='upload_sample_data',
        s3_bucket=S3_BUCKET,
        s3_key=f'{ATHENA_TABLE}/{SAMPLE_FILENAME}',
        data=SAMPLE_DATA,
        replace=True,
    )

    create_database = AthenaOperator(
        task_id='create_database',
        query=QUERY_CREATE_DATABASE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
    )

    create_table = AthenaOperator(
        task_id='create_table',
        query=QUERY_CREATE_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
    )

    # [START howto_operator_athena]
    read_table = AthenaOperator(
        task_id='read_table',
        query=QUERY_READ_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
    )
    # [END howto_operator_athena]

    # [START howto_sensor_athena]
    await_query = AthenaSensor(
        task_id='await_query',
        query_execution_id=read_table.output,
    )
    # [END howto_sensor_athena]

    drop_table = AthenaOperator(
        task_id='drop_table',
        query=QUERY_DROP_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
        trigger_rule="all_done",
    )

    drop_database = AthenaOperator(
        task_id='drop_database',
        query=QUERY_DROP_DATABASE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
        trigger_rule="all_done",
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id='delete_s3_bucket',
        bucket_name=S3_BUCKET,
        force_delete=True,
        trigger_rule="all_done",
    )

    chain(
        # TEST SETUP
        create_s3_bucket,
        await_bucket(),
        upload_sample_data,
        create_database,
        # TEST BODY
        create_table,
        read_table,
        await_query,
        read_results_from_s3(read_table.output),
        # TEST TEARDOWN
        drop_table,
        drop_database,
        delete_s3_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
