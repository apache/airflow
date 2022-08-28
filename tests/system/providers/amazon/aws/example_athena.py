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
from typing import cast

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
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = 'example_athena'

SAMPLE_DATA = '''"Alice",20
    "Bob",25
    "Charlie",30
    '''
SAMPLE_FILENAME = 'airflow_sample.csv'


@task
def await_bucket(bucket_name):
    # Avoid a race condition after creating the S3 Bucket.
    client = boto3.client('s3')
    waiter = client.get_waiter('bucket_exists')
    waiter.wait(Bucket=bucket_name)


@task
def read_results_from_s3(bucket_name, query_execution_id):
    s3_hook = S3Hook()
    file_obj = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=f'{query_execution_id}.csv')
    file_content = file_obj['Body'].read().decode('utf-8')
    print(file_content)


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context['ENV_ID']

    s3_bucket = f'{env_id}-athena-bucket'
    athena_table = f'{env_id}_test_table'
    athena_database = f'{env_id}_default'

    query_create_database = f'CREATE DATABASE IF NOT EXISTS {athena_database}'
    query_create_table = f'''CREATE EXTERNAL TABLE IF NOT EXISTS {athena_database}.{athena_table}
        ( `name` string, `age` int )
        ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        WITH SERDEPROPERTIES ( "serialization.format" = ",", "field.delim" = "," )
        LOCATION "s3://{s3_bucket}//{athena_table}"
        TBLPROPERTIES ("has_encrypted_data"="false")
        '''
    query_read_table = f'SELECT * from {athena_database}.{athena_table}'
    query_drop_table = f'DROP TABLE IF EXISTS {athena_database}.{athena_table}'
    query_drop_database = f'DROP DATABASE IF EXISTS {athena_database}'

    create_s3_bucket = S3CreateBucketOperator(task_id='create_s3_bucket', bucket_name=s3_bucket)

    upload_sample_data = S3CreateObjectOperator(
        task_id='upload_sample_data',
        s3_bucket=s3_bucket,
        s3_key=f'{athena_table}/{SAMPLE_FILENAME}',
        data=SAMPLE_DATA,
        replace=True,
    )

    create_database = AthenaOperator(
        task_id='create_database',
        query=query_create_database,
        database=athena_database,
        output_location=f's3://{s3_bucket}/',
    )

    create_table = AthenaOperator(
        task_id='create_table',
        query=query_create_table,
        database=athena_database,
        output_location=f's3://{s3_bucket}/',
    )

    # [START howto_operator_athena]
    read_table = AthenaOperator(
        task_id='read_table',
        query=query_read_table,
        database=athena_database,
        output_location=f's3://{s3_bucket}/',
    )
    # [END howto_operator_athena]

    # [START howto_sensor_athena]
    await_query = AthenaSensor(
        task_id='await_query',
        query_execution_id=cast(str, read_table.output),
    )
    # [END howto_sensor_athena]

    drop_table = AthenaOperator(
        task_id='drop_table',
        query=query_drop_table,
        database=athena_database,
        output_location=f's3://{s3_bucket}/',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    drop_database = AthenaOperator(
        task_id='drop_database',
        query=query_drop_database,
        database=athena_database,
        output_location=f's3://{s3_bucket}/',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id='delete_s3_bucket',
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        await_bucket(s3_bucket),
        upload_sample_data,
        create_database,
        # TEST BODY
        create_table,
        read_table,
        await_query,
        read_results_from_s3(s3_bucket, read_table.output),
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
