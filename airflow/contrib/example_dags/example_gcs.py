# -*- coding: utf-8 -*-
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
Example Airflow DAG for Google Cloud Storage operators.
"""

import os
import airflow
from airflow import models
from airflow.contrib.operators.gcs_operator import (
    GoogleCloudStorageCreateBucketOperator,
)
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.contrib.operators.gcs_download_operator import (
    GoogleCloudStorageDownloadOperator,
)
from airflow.contrib.operators.gcs_delete_operator import (
    GoogleCloudStorageDeleteOperator,
)
from airflow.operators.local_to_gcs import FileToGoogleCloudStorageOperator

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

BUCKET = os.environ.get("GCP_GCS_BUCKET", "test-gcs-23e-5v5143")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")

PATH_TO_FILE = os.environ.get("GCS_UPLOAD_FILE_PATH", "test_n9c8347r.txt")
PATH_TO_SAVED_FILE = os.environ.get("PATH_TO_SAVED_FILE", "test_download_n9c8347r.txt")

BUCKET_FILE_LOCATION = PATH_TO_FILE.rpartition("/")[-1]


with models.DAG(
    "example_gcs", default_args=default_args, schedule_interval=None
) as dag:
    create_bucket = GoogleCloudStorageCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET, project_id=PROJECT_ID
    )

    list_buckets = GoogleCloudStorageListOperator(task_id="list_buckets", bucket=BUCKET)

    upload_file = FileToGoogleCloudStorageOperator(
        task_id="upload_file", src=PATH_TO_FILE, dst=BUCKET_FILE_LOCATION, bucket=BUCKET
    )

    download_file = GoogleCloudStorageDownloadOperator(
        task_id="download_file",
        object=BUCKET_FILE_LOCATION,
        bucket=BUCKET,
        filename=PATH_TO_SAVED_FILE,
    )

    delete_files = GoogleCloudStorageDeleteOperator(
        task_id="delete_files", bucket_name=BUCKET, prefix=""
    )

    create_bucket >> list_buckets >> upload_file >> download_file >> delete_files
