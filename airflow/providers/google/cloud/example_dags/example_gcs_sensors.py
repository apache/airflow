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
Example Airflow DAG for Google Cloud Storage Sensors.
"""

import os
from airflow import models
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWtihPrefixExistenceSensor
)
from airflow.utils.dates import days_ago

BUCKET = os.environ.get("GCP_GCS_BUCKET", "gcs-example-bucket")
FILE_NAME = os.environ.get("GCP_GCS_INCOMING_FILE_NAME", "gcs-example-filename_01012020.txt")
FILE_PREFIX = os.environ.get("GCP_GCS_INCOMING_FILE_PREFIX", "gcs-example-filename_")

with models.DAG(
    "example_gcs_sensor",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
) as dag:    
    # [START howto_sensor_object_exists_task]
    gcs_object_exists_task = GCSObjectExistenceSensor(
        bucket=BUCKET,
        object=FILE_NAME,
        mode='poke',        
        task_id="gcs_object_exists_task",
    )
    # [END howto_sensor_object_exists_task]
    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_with_prefix_exists_task = GCSObjectsWtihPrefixExistenceSensor(
        bucket=BUCKET,
        prefix=FILE_PREFIX, 
        mode='poke',       
        task_id="gcs_object_with_prefix_exists_task",
    )
    # [END howto_sensor_object_with_prefix_exists_task]

  