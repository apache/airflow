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
This is an example dag for demonstrating usage of GCS sensors.
"""
import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSUploadSessionCompleteSensor, GCSObjectUpdateSensor

TEST_BUCKET = os.getenv("GCP_TEST_BUCKET", "test-gcs-bucket")
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")

# Upload example_test.txt in the <TEST_BUCKET> after triggering the DAG.
BUCKET_FILE_LOCATION = "example_test.txt"

# This is the upload file name prefix the sensor will be waiting for.
PATH_TO_UPLOAD_FILE_PREFIX = "example_"

with DAG(
    "example_gcs_sensors",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    default_args={
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["example", "gcs"],
) as dag:

    # [START howto_sensor_gcs_upload_session_complete_task]
    gcs_upload_session_complete = GCSUploadSessionCompleteSensor(
        bucket=TEST_BUCKET,
        prefix=PATH_TO_UPLOAD_FILE_PREFIX,
        inactivity_period=60,
        min_objects=1,
        allow_delete=True,
        previous_objects=set(),
        task_id="gcs_upload_session_complete_task",
        google_cloud_conn_id=GCP_CONN_ID,
    )
    # [END howto_sensor_gcs_upload_session_complete_task]

    # [START howto_sensor_object_update_exists_task]
    gcs_update_object_exists = GCSObjectUpdateSensor(
        bucket=TEST_BUCKET,
        object=BUCKET_FILE_LOCATION,
        task_id="gcs_object_update_sensor_task",
        google_cloud_conn_id=GCP_CONN_ID,
    )
    # [END howto_sensor_object_update_exists_task]

    gcs_upload_session_complete >> gcs_update_object_exists
