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
Example Airflow DAG that sense a cloud task queue being empty.

This DAG relies on the following OS environment variables

* GCP_PROJECT_ID - Google Cloud project where the Compute Engine instance exists.
* GCP_ZONE - Google Cloud zone where the cloud task queue exists.
* QUEUE_NAME - Name of the cloud task queue.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.sensors.tasks import TaskQueueEmptySensor

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_ZONE = os.environ.get('GCE_ZONE', 'europe-west1-b')
QUEUE_NAME = os.environ.get('GCP_QUEUE_NAME', 'testqueue')


with models.DAG(
    'example_gcp_cloud_tasks_sensor',
    start_date=datetime(2022, 8, 8),
    catchup=False,
    tags=['example'],
) as dag:
    # [START cloud_tasks_empty_sensor]
    gcp_cloud_tasks_sensor = TaskQueueEmptySensor(
        project_id=GCP_PROJECT_ID,
        location=GCP_ZONE,
        task_id='gcp_sense_cloud_tasks_empty',
        queue_name=QUEUE_NAME,
    )
    # [END cloud_tasks_empty_sensor]
