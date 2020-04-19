import os

from airflow import models
from airflow.utils import dates
from airflow.providers.google.cloud.operators.life_sciences import LifeSciencesRunPipelineOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-id")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "test-life-sciences-bucket")
LOCATION = os.environ.get("GCP_LOCATION", 'example-project-location')

SIMPLE_ACTION_PIEPELINE = {
        "pipeline": {
            "actions": [
                {
                    "imageUri": "bash",
                    "commands": ["-c", "echo Hello, world"]
                },
            ],
            "resources": {
                "regions": ["us-central11"],
                "virtualMachine": {
                    "machineType": "n1-standard-1",
                }
            }
        }
}
MULTI_ACTION_PIPELINE = {
        "pipeline": {
            "actions": [
                {
                    "imageUri": "google/cloud-sdk",
                    "commands": ["gsutil", "cp", "gs://{}/input.in".format(BUCKET), "/tmp"]
                },
                {
                    "imageUri": "bash",
                    "commands": ["-c", "sha1sum /tmp/in > /tmp/test.sha1"]
                },
                {
                    "imageUri": "google/cloud-sdk",
                    "commands": ["gsutil", "cp", "/tmp/output.sha1", "gs://{}/output.sha1".format(BUCKET)]
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

with models.DAG("example_gcp_life_sciences",
                default_args=dict(start_date=dates.days_ago(1)),
                schedule_interval=None,
                tags=['example'],
                ) as dag:

    simple_life_science_action_pipeline = LifeSciencesRunPipelineOperator(
        task_id='simple-action-pipeline',
        body=SIMPLE_ACTION_PIEPELINE,
        project_id=PROJECT_ID,
        location=LOCATION
    )

    multiple_life_science_action_pipeline = LifeSciencesRunPipelineOperator(
        task_id='multi-action-pipeline',
        body=MULTI_ACTION_PIPELINE,
        project_id=PROJECT_ID,
        location=LOCATION
    )

    simple_life_science_action_pipeline >> multiple_life_science_action_pipeline
