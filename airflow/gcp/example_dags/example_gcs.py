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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.local_to_gcs import FileToGoogleCloudStorageOperator
from airflow.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.gcp.operators.gcs import (
    GoogleCloudStorageBucketCreateAclEntryOperator,
    GoogleCloudStorageObjectCreateAclEntryOperator,
    GoogleCloudStorageListOperator,
    GoogleCloudStorageDeleteOperator,
    GoogleCloudStorageDownloadOperator,
    GoogleCloudStorageCreateBucketOperator
)

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

# [START howto_operator_gcs_acl_args_common]
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-id")
BUCKET_1 = os.environ.get("GCP_GCS_BUCKET_1", "test-gcs-example-bucket")
GCS_ACL_ENTITY = os.environ.get("GCS_ACL_ENTITY", "allUsers")
GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"
# [END howto_operator_gcs_acl_args_common]

BUCKET_2 = os.environ.get("GCP_GCS_BUCKET_1", "test-gcs-example-bucket-2")

PATH_TO_UPLOAD_FILE = os.environ.get(
    "GCP_GCS_PATH_TO_UPLOAD_FILE", "test-gcs-example.txt"
)
PATH_TO_SAVED_FILE = os.environ.get(
    "GCP_GCS_PATH_TO_SAVED_FILE", "test-gcs-example-download.txt"
)

BUCKET_FILE_LOCATION = PATH_TO_UPLOAD_FILE.rpartition("/")[-1]


with models.DAG(
    "example_gcs", default_args=default_args, schedule_interval=None
) as dag:
    create_bucket1 = GoogleCloudStorageCreateBucketOperator(
        task_id="create_bucket1", bucket_name=BUCKET_1, project_id=PROJECT_ID
    )

    create_bucket2 = GoogleCloudStorageCreateBucketOperator(
        task_id="create_bucket2", bucket_name=BUCKET_2, project_id=PROJECT_ID
    )

    list_buckets = GoogleCloudStorageListOperator(
        task_id="list_buckets", bucket=BUCKET_1
    )

    list_buckets_result = BashOperator(
        task_id="list_buckets_result",
        bash_command="echo \"{{ task_instance.xcom_pull('list_buckets') }}\"",
    )

    upload_file = FileToGoogleCloudStorageOperator(
        task_id="upload_file",
        src=PATH_TO_UPLOAD_FILE,
        dst=BUCKET_FILE_LOCATION,
        bucket=BUCKET_1,
    )

    # [START howto_operator_gcs_bucket_create_acl_entry_task]
    gcs_bucket_create_acl_entry_task = GoogleCloudStorageBucketCreateAclEntryOperator(
        bucket=BUCKET_1,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_BUCKET_ROLE,
        task_id="gcs_bucket_create_acl_entry_task",
    )
    # [END howto_operator_gcs_bucket_create_acl_entry_task]

    # [START howto_operator_gcs_object_create_acl_entry_task]
    gcs_object_create_acl_entry_task = GoogleCloudStorageObjectCreateAclEntryOperator(
        bucket=BUCKET_1,
        object_name=BUCKET_FILE_LOCATION,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_OBJECT_ROLE,
        task_id="gcs_object_create_acl_entry_task",
    )
    # [END howto_operator_gcs_object_create_acl_entry_task]

    download_file = GoogleCloudStorageDownloadOperator(
        task_id="download_file",
        object_name=BUCKET_FILE_LOCATION,
        bucket=BUCKET_1,
        filename=PATH_TO_SAVED_FILE,
    )

    copy_file = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id="copy_file",
        source_bucket=BUCKET_1,
        source_object=BUCKET_FILE_LOCATION,
        destination_bucket=BUCKET_2,
        destination_object=BUCKET_FILE_LOCATION,
    )

    delete_files = GoogleCloudStorageDeleteOperator(
        task_id="delete_files", bucket_name=BUCKET_1, prefix=""
    )

    [create_bucket1, create_bucket2] >> list_buckets >> list_buckets_result
    [create_bucket1, create_bucket2] >> upload_file
    upload_file >> [download_file, copy_file]
    upload_file >> gcs_bucket_create_acl_entry_task >> gcs_object_create_acl_entry_task >> delete_files
