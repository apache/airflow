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
Example Airflow DAG that displays interactions with Google Cloud Life Sciences.

This DAG relies on the following OS environment variables:

* GCP_PROJECT_ID - Google Cloud Project to use for the Cloud Function.
* GCP_GCS_BUCKET - Google Cloud Storage Bucket to use
* GCP_LOCATION - The Location of the Google Cloud Project
"""
import os

from airflow import models
from airflow.providers.google.cloud.operators.life_sciences import LifeSciencesRunPipelineOperator
from airflow.utils import dates

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project-id")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "example-bucket")
LOCATION = os.environ.get("GCP_LOCATION", 'example-location')

# [START howto_configure_simple_action_pipeline]
SIMPLE_ACTION_PIEPELINE = {
    "pipeline": {
        "actions": [
            {
                "imageUri": "bash",
                "commands": ["-c", "echo Hello, world"]
            },
        ],
        "resources": {
            "regions": ["{}".format(LOCATION)],
            "virtualMachine": {
                "machineType": "n1-standard-1",
            }
        }
    },
}
# [END howto_configure_simple_action_pipeline]
# [START howto_configure_multiple_action_pipeline]
MULTI_ACTION_PIPELINE = {
    "pipeline": {
        "actions": [
            {
                "imageUri": "google/cloud-sdk",
                "commands": ["gsutil", "cp", "gs://{}/input.in".format(BUCKET), "/tmp"]
            },
            {
                "imageUri": "bash",
                "commands": ["-c", "echo Hello, world"]
            },
            {
                "imageUri": "google/cloud-sdk",
                "commands": ["gsutil", "cp", "gs://{}/input.in".format(BUCKET),
                             "gs://{}/output.in".format(BUCKET)]
            },
        ],
        "resources": {
            "regions": ["{}".format(LOCATION)],
            "virtualMachine": {
                "machineType": "n1-standard-1",
            }
        }
    }
}
# [END howto_configure_multiple_action_pipeline]

with models.DAG("example_gcp_life_sciences",
                default_args=dict(start_date=dates.days_ago(1)),
                schedule_interval=None,
                tags=['example'],
                ) as dag:
    # [START howto_run_pipeline]
    simple_life_science_action_pipeline = LifeSciencesRunPipelineOperator(
        task_id='simple-action-pipeline',
        body=SIMPLE_ACTION_PIEPELINE,
        project_id=PROJECT_ID,
        location=LOCATION
    )
    # [END howto_run_pipeline]

    multiple_life_science_action_pipeline = LifeSciencesRunPipelineOperator(
        task_id='multi-action-pipeline',
        body=MULTI_ACTION_PIPELINE,
        project_id=PROJECT_ID,
        location=LOCATION
    )

    simple_life_science_action_pipeline >> multiple_life_science_action_pipeline
