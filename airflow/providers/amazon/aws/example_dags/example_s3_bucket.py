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

import os
from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
    S3DeleteBucketTaggingOperator,
    S3GetBucketTaggingOperator,
    S3PutBucketTaggingOperator,
)

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'test-airflow-12345')
TAG_KEY = os.environ.get('TAG_KEY', 'test-s3-bucket-tagging-key')
TAG_VALUE = os.environ.get('TAG_VALUE', 'test-s3-bucket-tagging-value')

with DAG(
    dag_id='example_s3_bucket',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_s3_create_bucket]
    create_bucket = S3CreateBucketOperator(
        task_id='s3_create_bucket',
        bucket_name=BUCKET_NAME,
    )
    # [END howto_operator_s3_create_bucket]

    # [START howto_operator_s3_put_bucket_tagging]
    put_tagging = S3PutBucketTaggingOperator(
        task_id='s3_put_bucket_tagging',
        bucket_name=BUCKET_NAME,
        key=TAG_KEY,
        value=TAG_VALUE,
    )
    # [END howto_operator_s3_put_bucket_tagging]

    # [START howto_operator_s3_get_bucket_tagging]
    get_tagging = S3GetBucketTaggingOperator(
        task_id='s3_get_bucket_tagging',
        bucket_name=BUCKET_NAME,
    )
    # [END howto_operator_s3_get_bucket_tagging]

    # [START howto_operator_s3_delete_bucket_tagging]
    delete_tagging = S3DeleteBucketTaggingOperator(
        task_id='s3_delete_bucket_tagging',
        bucket_name=BUCKET_NAME,
    )
    # [END howto_operator_s3_delete_bucket_tagging]

    # [START howto_operator_s3_delete_bucket]
    delete_bucket = S3DeleteBucketOperator(
        task_id='s3_delete_bucket', bucket_name=BUCKET_NAME, force_delete=True
    )
    # [END howto_operator_s3_delete_bucket]

    chain(create_bucket, put_tagging, get_tagging, delete_tagging, delete_bucket)
