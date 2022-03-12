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

# Ignore missing args provided by default_args
# type: ignore[call-arg]

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
    S3DeleteBucketTaggingOperator,
    S3GetBucketTaggingOperator,
    S3PutBucketTaggingOperator,
)

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'test-s3-bucket-tagging')
TAG_KEY = os.environ.get('TAG_KEY', 'test-s3-bucket-tagging-key')
TAG_VALUE = os.environ.get('TAG_VALUE', 'test-s3-bucket-tagging-value')


# [START howto_operator_s3_bucket_tagging]
with DAG(
    dag_id='s3_bucket_tagging_dag',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={"bucket_name": BUCKET_NAME},
    max_active_runs=1,
    tags=['example'],
) as dag:

    create_bucket = S3CreateBucketOperator(task_id='s3_bucket_tagging_dag_create', region_name='us-east-1')

    delete_bucket = S3DeleteBucketOperator(task_id='s3_bucket_tagging_dag_delete', force_delete=True)

    get_tagging = S3GetBucketTaggingOperator(task_id='s3_bucket_tagging_dag_get_tagging')

    put_tagging = S3PutBucketTaggingOperator(
        task_id='s3_bucket_tagging_dag_put_tagging', key=TAG_KEY, value=TAG_VALUE
    )

    delete_tagging = S3DeleteBucketTaggingOperator(task_id='s3_bucket_tagging_dag_delete_tagging')

    create_bucket >> put_tagging >> get_tagging >> delete_tagging >> delete_bucket
    # [END howto_operator_s3_bucket_tagging]
