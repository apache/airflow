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
from os import environ

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.transfers.dynamodb_to_s3 import DynamoDBToS3Operator

TABLE_NAME = environ.get('DYNAMO_TABLE_NAME', 'ExistingDynamoDbTableName')
BUCKET_NAME = environ.get('S3_BUCKET_NAME', 'ExistingS3BucketName')


with DAG(
    dag_id='example_dynamodb_to_s3',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_transfer_dynamodb_to_s3]
    backup_db = DynamoDBToS3Operator(
        task_id='backup_db',
        dynamodb_table_name=TABLE_NAME,
        s3_bucket_name=BUCKET_NAME,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=1000,
    )
    # [END howto_transfer_dynamodb_to_s3]

    # [START howto_transfer_dynamodb_to_s3_segmented]
    # Segmenting allows the transfer to be parallelized into {segment} number of parallel tasks.
    backup_db_segment_1 = DynamoDBToS3Operator(
        task_id='backup-1',
        dynamodb_table_name=TABLE_NAME,
        s3_bucket_name=BUCKET_NAME,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=1000,
        dynamodb_scan_kwargs={
            "TotalSegments": 2,
            "Segment": 0,
        },
    )

    backup_db_segment_2 = DynamoDBToS3Operator(
        task_id="backup-2",
        dynamodb_table_name=TABLE_NAME,
        s3_bucket_name=BUCKET_NAME,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=1000,
        dynamodb_scan_kwargs={
            "TotalSegments": 2,
            "Segment": 1,
        },
    )
    # [END howto_transfer_dynamodb_to_s3_segmented]

    chain(backup_db, [backup_db_segment_1, backup_db_segment_2])
