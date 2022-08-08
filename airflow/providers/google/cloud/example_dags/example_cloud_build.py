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
Example Airflow DAG that displays interactions with Google Cloud Build.

This DAG relies on the following OS environment variables:

* GCP_PROJECT_ID - Google Cloud Project to use for the Cloud Function.
* GCP_CLOUD_BUILD_ARCHIVE_URL - Path to the zipped source in Google Cloud Storage.
    This object must be a gzipped archive file (.tar.gz) containing source to build.
* GCP_CLOUD_BUILD_REPOSITORY_NAME - Name of the Cloud Source Repository.

"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
from future.backports.urllib.parse import urlparse

from airflow import models
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCancelBuildOperator,
    CloudBuildCreateBuildOperator,
    CloudBuildCreateBuildTriggerOperator,
    CloudBuildDeleteBuildTriggerOperator,
    CloudBuildGetBuildOperator,
    CloudBuildGetBuildTriggerOperator,
    CloudBuildListBuildsOperator,
    CloudBuildListBuildTriggersOperator,
    CloudBuildRetryBuildOperator,
    CloudBuildRunBuildTriggerOperator,
    CloudBuildUpdateBuildTriggerOperator,
)

START_DATE = datetime(2021, 1, 1)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "aitflow-test-project")

GCP_SOURCE_ARCHIVE_URL = os.environ.get("GCP_CLOUD_BUILD_ARCHIVE_URL", "gs://airflow-test-bucket/file.tar.gz")
GCP_SOURCE_REPOSITORY_NAME = os.environ.get("GCP_CLOUD_BUILD_REPOSITORY_NAME", "airflow-test-repo")

GCP_SOURCE_ARCHIVE_URL_PARTS = urlparse(GCP_SOURCE_ARCHIVE_URL)
GCP_SOURCE_BUCKET_NAME = GCP_SOURCE_ARCHIVE_URL_PARTS.netloc

CURRENT_FOLDER = Path(__file__).parent

# [START howto_operator_gcp_create_build_trigger_body]
create_build_trigger_body = {
    "name": "test-cloud-build-trigger",
    "trigger_template": {
        "project_id": GCP_PROJECT_ID,
        "repo_name": GCP_SOURCE_REPOSITORY_NAME,
        "branch_name": "master",
    },
    "filename": "cloudbuild.yaml",
}
# [END howto_operator_gcp_create_build_trigger_body]

update_build_trigger_body = {
    "name": "test-cloud-build-trigger",
    "trigger_template": {
        "project_id": GCP_PROJECT_ID,
        "repo_name": GCP_SOURCE_REPOSITORY_NAME,
        "branch_name": "dev",
    },
    "filename": "cloudbuild.yaml",
}

# [START howto_operator_gcp_create_build_from_storage_body]
create_build_from_storage_body = {
    "source": {"storage_source": GCP_SOURCE_ARCHIVE_URL},
    "steps": [
        {
            "name": "gcr.io/cloud-builders/docker",
            "args": ["build", "-t", f"gcr.io/$PROJECT_ID/{GCP_SOURCE_BUCKET_NAME}", "."],
        }
    ],
    "images": [f"gcr.io/$PROJECT_ID/{GCP_SOURCE_BUCKET_NAME}"],
}
# [END howto_operator_gcp_create_build_from_storage_body]

# [START howto_operator_create_build_from_repo_body]
create_build_from_repo_body: Dict[str, Any] = {
    "source": {"repo_source": {"repo_name": GCP_SOURCE_REPOSITORY_NAME, "branch_name": "main"}},
    "steps": [
        {
            "name": "gcr.io/cloud-builders/docker",
            "args": ["build", "-t", "gcr.io/$PROJECT_ID/$REPO_NAME", "."],
        }
    ],
    "images": ["gcr.io/$PROJECT_ID/$REPO_NAME"],
}
# [END howto_operator_create_build_from_repo_body]


with models.DAG(
    "example_gcp_cloud_build",
    start_date=START_DATE,
    catchup=False,
    tags=["example"],
) as build_dag:

    # [START howto_operator_create_build_from_storage]
    create_build_from_storage = CloudBuildCreateBuildOperator(
        task_id="create_build_from_storage", project_id=GCP_PROJECT_ID, build=create_build_from_storage_body
    )
    # [END howto_operator_create_build_from_storage]

    # [START howto_operator_create_build_from_storage_result]
    create_build_from_storage_result = BashOperator(
        bash_command=f"echo { create_build_from_storage.output['results'] }",
        task_id="create_build_from_storage_result",
    )
    # [END howto_operator_create_build_from_storage_result]

    # [START howto_operator_create_build_from_repo]
    create_build_from_repo = CloudBuildCreateBuildOperator(
        task_id="create_build_from_repo", project_id=GCP_PROJECT_ID, build=create_build_from_repo_body
    )
    # [END howto_operator_create_build_from_repo]

    # [START howto_operator_create_build_from_repo_result]
    create_build_from_repo_result = BashOperator(
        bash_command=f"echo { create_build_from_repo.output['results'] }",
        task_id="create_build_from_repo_result",
    )
    # [END howto_operator_create_build_from_repo_result]

    # [START howto_operator_list_builds]
    list_builds = CloudBuildListBuildsOperator(
        task_id="list_builds", project_id=GCP_PROJECT_ID, location="global"
    )
    # [END howto_operator_list_builds]

    # [START howto_operator_create_build_without_wait]
    create_build_without_wait = CloudBuildCreateBuildOperator(
        task_id="create_build_without_wait",
        project_id=GCP_PROJECT_ID,
        build=create_build_from_repo_body,
        wait=False,
    )
    # [END howto_operator_create_build_without_wait]

    # [START howto_operator_cancel_build]
    cancel_build = CloudBuildCancelBuildOperator(
        task_id="cancel_build",
        id_=create_build_without_wait.output['id'],
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_cancel_build]

    # [START howto_operator_retry_build]
    retry_build = CloudBuildRetryBuildOperator(
        task_id="retry_build",
        id_=cancel_build.output['id'],
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_retry_build]

    # [START howto_operator_get_build]
    get_build = CloudBuildGetBuildOperator(
        task_id="get_build",
        id_=retry_build.output['id'],
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_get_build]

    # [START howto_operator_gcp_create_build_from_yaml_body]
    create_build_from_file = CloudBuildCreateBuildOperator(
        task_id="create_build_from_file",
        project_id=GCP_PROJECT_ID,
        build=yaml.safe_load((Path(CURRENT_FOLDER) / 'example_cloud_build.yaml').read_text()),
        params={'name': 'Airflow'},
    )
    # [END howto_operator_gcp_create_build_from_yaml_body]

    create_build_from_storage >> create_build_from_storage_result
    create_build_from_storage_result >> list_builds
    create_build_from_repo >> create_build_from_repo_result
    create_build_from_repo_result >> list_builds
    list_builds >> create_build_without_wait >> cancel_build
    cancel_build >> retry_build >> get_build

with models.DAG(
    "example_gcp_cloud_build_trigger",
    start_date=START_DATE,
    catchup=False,
    tags=["example"],
) as build_trigger_dag:

    # [START howto_operator_create_build_trigger]
    create_build_trigger = CloudBuildCreateBuildTriggerOperator(
        task_id="create_build_trigger", project_id=GCP_PROJECT_ID, trigger=create_build_trigger_body
    )
    # [END howto_operator_create_build_trigger]

    # [START howto_operator_run_build_trigger]
    run_build_trigger = CloudBuildRunBuildTriggerOperator(
        task_id="run_build_trigger",
        project_id=GCP_PROJECT_ID,
        trigger_id=create_build_trigger.output['id'],
        source=create_build_from_repo_body['source']['repo_source'],
    )
    # [END howto_operator_run_build_trigger]

    # [START howto_operator_create_build_trigger]
    update_build_trigger = CloudBuildUpdateBuildTriggerOperator(
        task_id="update_build_trigger",
        project_id=GCP_PROJECT_ID,
        trigger_id=create_build_trigger.output['id'],
        trigger=update_build_trigger_body,
    )
    # [END howto_operator_create_build_trigger]

    # [START howto_operator_get_build_trigger]
    get_build_trigger = CloudBuildGetBuildTriggerOperator(
        task_id="get_build_trigger",
        project_id=GCP_PROJECT_ID,
        trigger_id=create_build_trigger.output['id'],
    )
    # [END howto_operator_get_build_trigger]

    # [START howto_operator_delete_build_trigger]
    delete_build_trigger = CloudBuildDeleteBuildTriggerOperator(
        task_id="delete_build_trigger",
        project_id=GCP_PROJECT_ID,
        trigger_id=create_build_trigger.output['id'],
    )
    # [END howto_operator_delete_build_trigger]

    # [START howto_operator_list_build_triggers]
    list_build_triggers = CloudBuildListBuildTriggersOperator(
        task_id="list_build_triggers", project_id=GCP_PROJECT_ID, location="global", page_size=5
    )
    # [END howto_operator_list_build_triggers]

    chain(
        create_build_trigger,
        run_build_trigger,
        update_build_trigger,
        get_build_trigger,
        delete_build_trigger,
        list_build_triggers,
    )
