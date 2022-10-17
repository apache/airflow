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
from __future__ import annotations

from datetime import datetime

import boto3

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.dynamodb_to_s3 import DynamoDBToS3Operator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = 'example_dynamodb_to_s3'

sys_test_context_task = SystemTestContextBuilder().build()

TABLE_ATTRIBUTES = [
    {'AttributeName': 'ID', 'AttributeType': 'S'},
    {'AttributeName': 'Value', 'AttributeType': 'S'},
]
TABLE_KEY_SCHEMA = [
    {'AttributeName': 'ID', 'KeyType': 'HASH'},
    {'AttributeName': 'Value', 'KeyType': 'RANGE'},
]
TABLE_THROUGHPUT = {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
S3_KEY_PREFIX = 'dynamodb-segmented-file'


@task
def set_up_table(table_name: str):
    dynamo_resource = boto3.resource('dynamodb')
    table = dynamo_resource.create_table(
        AttributeDefinitions=TABLE_ATTRIBUTES,
        TableName=table_name,
        KeySchema=TABLE_KEY_SCHEMA,
        ProvisionedThroughput=TABLE_THROUGHPUT,
    )
    boto3.client('dynamodb').get_waiter('table_exists').wait(
        TableName=table_name, WaiterConfig={'Delay': 10, 'MaxAttempts': 10}
    )
    table.put_item(Item={'ID': '123', 'Value': 'Testing'})


@task
def wait_for_bucket(s3_bucket_name):
    waiter = boto3.client('s3').get_waiter('bucket_exists')
    waiter.wait(Bucket=s3_bucket_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_dynamodb_table(table_name: str):
    boto3.resource('dynamodb').Table(table_name).delete()
    boto3.client('dynamodb').get_waiter('table_not_exists').wait(
        TableName=table_name, WaiterConfig={'Delay': 10, 'MaxAttempts': 10}
    )


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    table_name = f'{env_id}-dynamodb-table'
    bucket_name = f'{env_id}-dynamodb-bucket'

    create_table = set_up_table(table_name=table_name)

    create_bucket = S3CreateBucketOperator(task_id='create_bucket', bucket_name=bucket_name)

    # [START howto_transfer_dynamodb_to_s3]
    backup_db = DynamoDBToS3Operator(
        task_id='backup_db',
        dynamodb_table_name=table_name,
        s3_bucket_name=bucket_name,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=20,
    )
    # [END howto_transfer_dynamodb_to_s3]

    # [START howto_transfer_dynamodb_to_s3_segmented]
    # Segmenting allows the transfer to be parallelized into {segment} number of parallel tasks.
    backup_db_segment_1 = DynamoDBToS3Operator(
        task_id='backup_db_segment_1',
        dynamodb_table_name=table_name,
        s3_bucket_name=bucket_name,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=1000,
        s3_key_prefix=f'{S3_KEY_PREFIX}-1-',
        dynamodb_scan_kwargs={
            "TotalSegments": 2,
            "Segment": 0,
        },
    )

    backup_db_segment_2 = DynamoDBToS3Operator(
        task_id="backup_db_segment_2",
        dynamodb_table_name=table_name,
        s3_bucket_name=bucket_name,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=1000,
        s3_key_prefix=f'{S3_KEY_PREFIX}-2-',
        dynamodb_scan_kwargs={
            "TotalSegments": 2,
            "Segment": 1,
        },
    )
    # [END howto_transfer_dynamodb_to_s3_segmented]
    delete_table = delete_dynamodb_table(table_name=table_name)

    delete_bucket = S3DeleteBucketOperator(
        task_id='delete_bucket',
        bucket_name=bucket_name,
        trigger_rule=TriggerRule.ALL_DONE,
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        create_table,
        create_bucket,
        wait_for_bucket(s3_bucket_name=bucket_name),
        # TEST BODY
        backup_db,
        backup_db_segment_1,
        backup_db_segment_2,
        # TEST TEARDOWN
        delete_table,
        delete_bucket,
    )
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
