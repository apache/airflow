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

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.utils.dates import days_ago

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'gcp-project-id')
S3BUCKET_NAME = os.environ.get('S3BUCKET_NAME', 'example-s3bucket-name')
GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET', 'example-gcsbucket-name')
UPLOAD_FILE = '/tmp/example-file.txt'
PREFIX = 'TESTS'


def upload_file():
    """A callable to upload file to AWS bucket"""
    s3_hook = S3Hook()
    s3_hook.load_file(filename=UPLOAD_FILE, key=PREFIX, bucket_name=S3BUCKET_NAME)


with models.DAG(
    'example_s3_to_gcs',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=S3BUCKET_NAME, region_name='us-east-1'
    )

    upload_to_s3 = PythonOperator(task_id='upload_file_to_s3', python_callable=upload_file)

    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=GCS_BUCKET,
        project_id=GCP_PROJECT_ID,
    )
    # [START howto_transfer_s3togcs_operator]
    transfer_to_gcs = S3ToGCSOperator(
        task_id='s3_to_gcs_task', bucket=S3BUCKET_NAME, prefix=PREFIX, dest_gcs="gs://" + GCS_BUCKET
    )
    # [END howto_transfer_s3togcs_operator]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id='delete_s3_bucket', bucket_name=S3BUCKET_NAME, force_delete=True
    )

    delete_gcs_bucket = GCSDeleteBucketOperator(task_id='delete_gcs_bucket', bucket_name=GCS_BUCKET)

    (
        create_s3_bucket
        >> upload_to_s3
        >> create_gcs_bucket
        >> transfer_to_gcs
        >> delete_s3_bucket
        >> delete_gcs_bucket
    )
