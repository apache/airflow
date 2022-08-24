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

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.life_sciences import LifeSciencesRunPipelineOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_gcp_life_sciences"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project-id")
BUCKET = os.environ.get("GCP_GCS_LIFE_SCIENCES_BUCKET", "INVALID BUCKET NAME")
FILENAME = os.environ.get("GCP_GCS_LIFE_SCIENCES_FILENAME", 'input.in')
LOCATION = os.environ.get("GCP_LIFE_SCIENCES_LOCATION", 'us-central1')


# [START howto_configure_simple_action_pipeline]
SIMPLE_ACTION_PIPELINE = {
    "pipeline": {
        "actions": [
            {"imageUri": "bash", "commands": ["-c", "echo Hello, world"]},
        ],
        "resources": {
            "regions": [f"{LOCATION}"],
            "virtualMachine": {
                "machineType": "n1-standard-1",
            },
        },
    },
}
# [END howto_configure_simple_action_pipeline]

# [START howto_configure_multiple_action_pipeline]
MULTI_ACTION_PIPELINE = {
    "pipeline": {
        "actions": [
            {
                "imageUri": "google/cloud-sdk",
                "commands": ["gsutil", "cp", f"gs://{BUCKET}/{FILENAME}", "/tmp"],
            },
            {"imageUri": "bash", "commands": ["-c", "echo Hello, world"]},
            {
                "imageUri": "google/cloud-sdk",
                "commands": [
                    "gsutil",
                    "cp",
                    f"gs://{BUCKET}/{FILENAME}",
                    f"gs://{BUCKET}/output.in",
                ],
            },
        ],
        "resources": {
            "regions": [f"{LOCATION}"],
            "virtualMachine": {
                "machineType": "n1-standard-1",
            },
        },
    }
}
# [END howto_configure_multiple_action_pipeline]

with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # [START howto_run_pipeline]
    simple_life_science_action_pipeline = LifeSciencesRunPipelineOperator(
        task_id='simple-action-pipeline',
        body=SIMPLE_ACTION_PIPELINE,
        project_id=PROJECT_ID,
        location=LOCATION,
    )
    # [END howto_run_pipeline]

    multiple_life_science_action_pipeline = LifeSciencesRunPipelineOperator(
        task_id='multi-action-pipeline', body=MULTI_ACTION_PIPELINE, project_id=PROJECT_ID, location=LOCATION
    )

    simple_life_science_action_pipeline >> multiple_life_science_action_pipeline


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
