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
Example Airflow DAG for Google Cloud Storage to Google Cloud Storage transfer operators.
"""

import os

from airflow import models
from airflow.providers.google.cloud.operators.gcs_to_gcs import GCSSynchronizeBuckets, GCSToGCSOperator
from airflow.utils.dates import days_ago

default_args = {"start_date": days_ago(1)}

BUCKET_1_SRC = os.environ.get("GCP_GCS_BUCKET_1_SRC", "test-gcs-sync-1-src")
BUCKET_1_DST = os.environ.get("GCP_GCS_BUCKET_1_DST", "test-gcs-sync-1-dst")

BUCKET_2_SRC = os.environ.get("GCP_GCS_BUCKET_2_SRC", "test-gcs-sync-2-src")
BUCKET_2_DST = os.environ.get("GCP_GCS_BUCKET_2_DST", "test-gcs-sync-2-dst")

BUCKET_3_SRC = os.environ.get("GCP_GCS_BUCKET_3_SRC", "test-gcs-sync-3-src")
BUCKET_3_DST = os.environ.get("GCP_GCS_BUCKET_3_DST", "test-gcs-sync-3-dst")

OBJECT_1 = os.environ.get("GCP_GCS_OBJECT_1", "test-gcs-to-gcs-1")
OBJECT_2 = os.environ.get("GCP_GCS_OBJECT_2", "test-gcs-to-gcs-2")

with models.DAG(
    "example_gcs_to_gcs", default_args=default_args, schedule_interval=None, tags=['example']
) as dag:
    # [START howto_synch_bucket]
    sync_bucket = GCSSynchronizeBuckets(
        task_id="sync_bucket",
        source_bucket=BUCKET_1_SRC,
        destination_bucket=BUCKET_1_DST
    )
    # [END howto_synch_bucket]

    # [START howto_synch_full_bucket]
    sync_full_bucket = GCSSynchronizeBuckets(
        task_id="sync_full_bucket",
        source_bucket=BUCKET_1_SRC,
        destination_bucket=BUCKET_1_DST,
        delete_extra_files=True,
        allow_overwrite=True
    )
    # [END howto_synch_full_bucket]

    # [START howto_synch_to_subdir]
    sync_to_subdirectory = GCSSynchronizeBuckets(
        task_id="sync_to_subdirectory",
        source_bucket=BUCKET_1_SRC,
        destination_bucket=BUCKET_1_DST,
        destination_object="subdir/"
    )
    # [END howto_synch_to_subdir]

    # [START howto_sync_from_subdir]
    sync_from_subdirectory = GCSSynchronizeBuckets(
        task_id="sync_from_subdirectory",
        source_bucket=BUCKET_1_SRC,
        source_object="subdir/",
        destination_bucket=BUCKET_1_DST
    )
    # [END howto_sync_from_subdir]

    # [START howto_operator_gcs_to_gcs_single_file]
    copy_single_file = GCSToGCSOperator(
        task_id="copy_single_gcs_file",
        source_bucket=BUCKET_1_SRC,
        source_object=OBJECT_1,
        destination_bucket=BUCKET_1_DST,  # If not supplied the source_bucket value will be used
        destination_object="backup_" + OBJECT_1  # If not supplied the source_object value will be used
    )
    # [END howto_operator_gcs_to_gcs_single_file]

    # [START howto_operator_gcs_to_gcs_wildcard]
    copy_files_with_wildcard = GCSToGCSOperator(
        task_id="copy_files_with_wildcard",
        source_bucket=BUCKET_1_SRC,
        source_object="data/*.txt",
        destination_bucket=BUCKET_1_DST,
        destination_object="backup/"
    )
    # [END howto_operator_gcs_to_gcs_wildcard]

    # [START howto_operator_gcs_to_gcs_delimiter]
    copy_files_with_delimiter = GCSToGCSOperator(
        task_id="copy_files_with_delimiter",
        source_bucket=BUCKET_1_SRC,
        source_object="data/",
        destination_bucket=BUCKET_1_DST,
        destination_object="backup/",
        delimiter='.txt'
    )
    # [END howto_operator_gcs_to_gcs_delimiter]

    # [START howto_operator_gcs_to_gcs_list]
    copy_files_with_list = GCSToGCSOperator(
        task_id="copy_files_with_list",
        source_bucket=BUCKET_1_SRC,
        source_objects=[OBJECT_1, OBJECT_2],  # Instead of files each element could be a wildcard expression
        destination_bucket=BUCKET_1_DST,
        destination_object="backup/"
    )
    # [END howto_operator_gcs_to_gcs_list]

    # [START howto_operator_gcs_to_gcs_single_file_move]
    move_single_file = GCSToGCSOperator(
        task_id="move_single_file",
        source_bucket=BUCKET_1_SRC,
        source_object=OBJECT_1,
        destination_bucket=BUCKET_1_DST,
        destination_object="backup_" + OBJECT_1,
        move_object=True
    )
    # [END howto_operator_gcs_to_gcs_single_file_move]

    # [START howto_operator_gcs_to_gcs_list_move]
    move_files_with_list = GCSToGCSOperator(
        task_id="move_files_with_list",
        source_bucket=BUCKET_1_SRC,
        source_objects=[OBJECT_1, OBJECT_2],
        destination_bucket=BUCKET_1_DST,
        destination_object="backup/"
    )
    # [END howto_operator_gcs_to_gcs_list_move]
