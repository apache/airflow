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

from future.backports.urllib.parse import urlparse

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_build import CloudBuildCreateOperator
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

GCP_SOURCE_ARCHIVE_URL = os.environ.get("GCP_CLOUD_BUILD_ARCHIVE_URL", "gs://example-bucket/file")
GCP_SOURCE_REPOSITORY_NAME = os.environ.get("GCP_CLOUD_BUILD_REPOSITORY_NAME", "repository-name")

GCP_SOURCE_ARCHIVE_URL_PARTS = urlparse(GCP_SOURCE_ARCHIVE_URL)
GCP_SOURCE_BUCKET_NAME = GCP_SOURCE_ARCHIVE_URL_PARTS.netloc

# [START howto_operator_gcp_create_build_from_storage_body]
create_build_from_storage_body = {
    "source": {"storageSource": GCP_SOURCE_ARCHIVE_URL},
    "steps": [
        {
            "name": "gcr.io/cloud-builders/docker",
            "args": ["build", "-t", "gcr.io/$PROJECT_ID/{}".format(GCP_SOURCE_BUCKET_NAME), "."],
        }
    ],
    "images": ["gcr.io/$PROJECT_ID/{}".format(GCP_SOURCE_BUCKET_NAME)],
}
# [END howto_operator_gcp_create_build_from_storage_body]

# [START howto_operator_create_build_from_repo_body]
create_build_from_repo_body = {
    "source": {"repoSource": {"repoName": GCP_SOURCE_REPOSITORY_NAME, "branchName": "master"}},
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
    default_args=dict(start_date=dates.days_ago(1)),
    schedule_interval=None,
    tags=['example'],
) as dag:
    # [START howto_operator_create_build_from_storage]
    create_build_from_storage = CloudBuildCreateOperator(
        task_id="create_build_from_storage", project_id=GCP_PROJECT_ID, body=create_build_from_storage_body
    )
    # [END howto_operator_create_build_from_storage]

    # [START howto_operator_create_build_from_storage_result]
    create_build_from_storage_result = BashOperator(
        bash_command="echo '{{ task_instance.xcom_pull('create_build_from_storage')['images'][0] }}'",
        task_id="create_build_from_storage_result",
    )
    # [END howto_operator_create_build_from_storage_result]

    create_build_from_repo = CloudBuildCreateOperator(
        task_id="create_build_from_repo", project_id=GCP_PROJECT_ID, body=create_build_from_repo_body
    )

    create_build_from_repo_result = BashOperator(
        bash_command="echo '{{ task_instance.xcom_pull('create_build_from_repo')['images'][0] }}'",
        task_id="create_build_from_repo_result",
    )

    create_build_from_storage >> create_build_from_storage_result  # pylint: disable=pointless-statement

    create_build_from_repo >> create_build_from_repo_result  # pylint: disable=pointless-statement
