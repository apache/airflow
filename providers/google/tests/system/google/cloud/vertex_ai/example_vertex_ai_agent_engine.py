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
Example Airflow Dag for Google Vertex AI Agent Engine operations.

The test is self-contained: it builds the agent container image with Cloud Build,
pushing it to an Artifact Registry repository scoped to this test run, and deletes
the repository (and the GCS bucket used for query output) when the test finishes.

One-time setup required in the test project before running the test:

1. Enable the required APIs::

    gcloud services enable cloudbuild.googleapis.com artifactregistry.googleapis.com

2. Grant the Agent Engine service agent access to pull the built image and to
   write the query job output (the service agent is created the first time
   Agent Engine is used in the project)::

    gcloud projects add-iam-policy-binding PROJECT_ID \
        --member="serviceAccount:service-PROJECT_NUMBER@gcp-sa-aiplatform-re.iam.gserviceaccount.com" \
        --role="roles/artifactregistry.reader"
    gcloud projects add-iam-policy-binding PROJECT_ID \
        --member="serviceAccount:service-PROJECT_NUMBER@gcp-sa-aiplatform-re.iam.gserviceaccount.com" \
        --role="roles/storage.objectAdmin"

3. The Cloud Build service account must be able to create and delete Artifact
   Registry repositories (the default one can; otherwise grant it
   ``roles/artifactregistry.admin``).
"""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.providers.google.cloud.operators.cloud_build import CloudBuildCreateBuildOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.vertex_ai.agent_engine import (
    CreateAgentEngineOperator,
    DeleteAgentEngineOperator,
    GetAgentEngineOperator,
    RunQueryJobOperator,
    UpdateAgentEngineOperator,
)

if TYPE_CHECKING:
    from vertexai._genai import types as vertexai_types

try:
    from airflow.sdk import DAG, TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "vertex_ai_agent_engine_operations"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
LOCATION = "us-central1"

REPOSITORY_ID = f"repo_{DAG_ID}_{ENV_ID}".replace("_", "-")
CONTAINER_URI = f"{LOCATION}-docker.pkg.dev/{PROJECT_ID}/{REPOSITORY_ID}/airflow-hello-agent:latest"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
QUERY_OUTPUT_GCS_URI = f"gs://{BUCKET_NAME}/query-output/"
DISPLAY_NAME = f"airflow-agent-engine-{ENV_ID}"
QUERY_STR = json.dumps({"input": "hello from Airflow"})

AGENT_RESOURCES_DIR = Path(__file__).parent / "resources" / "agent_engine"
AGENT_SOURCE_B64 = base64.b64encode((AGENT_RESOURCES_DIR / "hello_agent.py").read_bytes()).decode()
AGENT_DOCKERFILE_B64 = base64.b64encode((AGENT_RESOURCES_DIR / "Dockerfile").read_bytes()).decode()

# The agent sources under resources/agent_engine/ are inlined base64-encoded so the
# build needs no external source (bucket or repository) to exist beforehand.
BUILD_AGENT_IMAGE_BODY = {
    "steps": [
        {
            "name": "gcr.io/google.com/cloudsdktool/cloud-sdk:slim",
            "entrypoint": "bash",
            "args": [
                "-c",
                f"gcloud artifacts repositories describe {REPOSITORY_ID} --location={LOCATION} "
                f"|| gcloud artifacts repositories create {REPOSITORY_ID} "
                f"--repository-format=docker --location={LOCATION}",
            ],
        },
        {
            "name": "gcr.io/cloud-builders/docker",
            "entrypoint": "bash",
            "args": [
                "-c",
                f"echo {AGENT_SOURCE_B64} | base64 -d > hello_agent.py && "
                f"echo {AGENT_DOCKERFILE_B64} | base64 -d > Dockerfile && "
                f"docker build -t {CONTAINER_URI} .",
            ],
        },
    ],
    "images": [CONTAINER_URI],
}

DELETE_AGENT_IMAGE_BODY = {
    "steps": [
        {
            "name": "gcr.io/google.com/cloudsdktool/cloud-sdk:slim",
            "entrypoint": "bash",
            "args": [
                "-c",
                f"gcloud artifacts repositories delete {REPOSITORY_ID} --location={LOCATION} --quiet",
            ],
        },
    ],
}

AGENT_ENGINE_ID = "{{ task_instance.xcom_pull(task_ids='create_agent_engine')['name'].split('/')[-1] }}"

QUERY_CONFIG: vertexai_types.RunQueryJobAgentEngineConfigDict = {
    "query": QUERY_STR,
    "output_gcs_uri": QUERY_OUTPUT_GCS_URI,
}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "agent_engine"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION,
    )

    build_agent_image = CloudBuildCreateBuildOperator(
        task_id="build_agent_image",
        project_id=PROJECT_ID,
        build=BUILD_AGENT_IMAGE_BODY,
    )

    # [START how_to_cloud_vertex_ai_create_agent_engine_operator]
    create_agent_engine = CreateAgentEngineOperator(
        task_id="create_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        config={
            "display_name": DISPLAY_NAME,
            "description": "Airflow system test Agent Engine",
            "agent_framework": "custom",
            "min_instances": 0,
            "max_instances": 1,
            "resource_limits": {"cpu": "1", "memory": "1Gi"},
            "container_spec": {"image_uri": CONTAINER_URI},
            "class_methods": [
                {
                    "name": "query",
                    "api_mode": "",
                },
            ],
        },
    )
    # [END how_to_cloud_vertex_ai_create_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_get_agent_engine_operator]
    get_agent_engine = GetAgentEngineOperator(
        task_id="get_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
    )
    # [END how_to_cloud_vertex_ai_get_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_run_query_job_operator]
    run_query_job = RunQueryJobOperator(
        task_id="run_query_job",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        config=QUERY_CONFIG,
        check_config={"retrieve_result": True},
        poll_interval=10,
        timeout=900,
    )
    # [END how_to_cloud_vertex_ai_run_query_job_operator]

    # [START how_to_cloud_vertex_ai_run_query_job_operator_deferrable]
    run_query_job_deferrable = RunQueryJobOperator(
        task_id="run_query_job_deferrable",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        config=QUERY_CONFIG,
        check_config={"retrieve_result": True},
        poll_interval=10,
        timeout=900,
        deferrable=True,
    )
    # [END how_to_cloud_vertex_ai_run_query_job_operator_deferrable]

    # [START how_to_cloud_vertex_ai_update_agent_engine_operator]
    update_agent_engine = UpdateAgentEngineOperator(
        task_id="update_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        config={
            "display_name": f"{DISPLAY_NAME}-updated",
            "description": "Updated Airflow system test Agent Engine",
        },
    )
    # [END how_to_cloud_vertex_ai_update_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_delete_agent_engine_operator]
    delete_agent_engine = DeleteAgentEngineOperator(
        task_id="delete_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        force=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_agent_engine_operator]

    delete_agent_image = CloudBuildCreateBuildOperator(
        task_id="delete_agent_image",
        project_id=PROJECT_ID,
        build=DELETE_AGENT_IMAGE_BODY,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        [create_bucket, build_agent_image]
        >> create_agent_engine
        >> get_agent_engine
        >> run_query_job
        >> run_query_job_deferrable
        >> update_agent_engine
        >> delete_agent_engine
        >> [delete_agent_image, delete_bucket]
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the Dag
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
