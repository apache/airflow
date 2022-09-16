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

* PROJECT_ID - Google Cloud Project to use for the Cloud Function.
* GCP_CLOUD_BUILD_ARCHIVE_URL - Path to the zipped source in Google Cloud Storage.
    This object must be a gzipped archive file (.tar.gz) containing source to build.
* GCP_CLOUD_BUILD_REPOSITORY_NAME - Name of the Cloud Source Repository.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import yaml
from future.backports.urllib.parse import urlparse

from airflow import models
from airflow.models.baseoperator import chain
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCancelBuildOperator,
    CloudBuildCreateBuildOperator,
    CloudBuildGetBuildOperator,
    CloudBuildListBuildsOperator,
    CloudBuildRetryBuildOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_gcp_cloud_build"

BUCKET_NAME_SRC = f"bucket-src-{DAG_ID}-{ENV_ID}"

GCP_SOURCE_ARCHIVE_URL = os.environ.get("GCP_CLOUD_BUILD_ARCHIVE_URL", f"gs://{BUCKET_NAME_SRC}/file.tar.gz")
GCP_SOURCE_REPOSITORY_NAME = "test-cloud-build-repository"

GCP_SOURCE_ARCHIVE_URL_PARTS = urlparse(GCP_SOURCE_ARCHIVE_URL)
GCP_SOURCE_BUCKET_NAME = GCP_SOURCE_ARCHIVE_URL_PARTS.netloc

CURRENT_FOLDER = Path(__file__).parent
FILE_LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources" / "file.tar.gz")

# [START howto_operator_gcp_create_build_from_storage_body]
create_build_from_storage_body = {
    "source": {"storage_source": GCP_SOURCE_ARCHIVE_URL},
    "steps": [{"name": "ubuntu", "args": ['echo', 'Hello world']}],
}
# [END howto_operator_gcp_create_build_from_storage_body]

# [START howto_operator_create_build_from_repo_body]
create_build_from_repo_body: dict[str, Any] = {
    "source": {"repo_source": {"repo_name": GCP_SOURCE_REPOSITORY_NAME, "branch_name": "master"}},
    "steps": [{"name": "ubuntu", "args": ['echo', 'Hello world']}],
}
# [END howto_operator_create_build_from_repo_body]


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    create_bucket_src = GCSCreateBucketOperator(task_id="create_bucket_src", bucket_name=BUCKET_NAME_SRC)

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=FILE_LOCAL_PATH,
        dst="file.tar.gz",
        bucket=BUCKET_NAME_SRC,
    )

    # [START howto_operator_create_build_from_storage]
    create_build_from_storage = CloudBuildCreateBuildOperator(
        task_id="create_build_from_storage", project_id=PROJECT_ID, build=create_build_from_storage_body
    )
    # [END howto_operator_create_build_from_storage]

    # [START howto_operator_create_build_from_storage_result]
    create_build_from_storage_result = BashOperator(
        bash_command=f"echo {cast(str, XComArg(create_build_from_storage, key='results'))}",
        task_id="create_build_from_storage_result",
    )
    # [END howto_operator_create_build_from_storage_result]

    # [START howto_operator_create_build_from_repo]
    create_build_from_repo = CloudBuildCreateBuildOperator(
        task_id="create_build_from_repo", project_id=PROJECT_ID, build=create_build_from_repo_body
    )
    # [END howto_operator_create_build_from_repo]

    # [START howto_operator_create_build_from_repo_result]
    create_build_from_repo_result = BashOperator(
        bash_command=f"echo {cast(str, XComArg(create_build_from_repo, key='results'))}",
        task_id="create_build_from_repo_result",
    )
    # [END howto_operator_create_build_from_repo_result]

    # [START howto_operator_list_builds]
    list_builds = CloudBuildListBuildsOperator(
        task_id="list_builds", project_id=PROJECT_ID, location="global"
    )
    # [END howto_operator_list_builds]

    # [START howto_operator_create_build_without_wait]
    create_build_without_wait = CloudBuildCreateBuildOperator(
        task_id="create_build_without_wait",
        project_id=PROJECT_ID,
        build=create_build_from_repo_body,
        wait=False,
    )
    # [END howto_operator_create_build_without_wait]

    # [START howto_operator_cancel_build]
    cancel_build = CloudBuildCancelBuildOperator(
        task_id="cancel_build",
        id_=cast(str, XComArg(create_build_without_wait, key='id')),
        project_id=PROJECT_ID,
    )
    # [END howto_operator_cancel_build]

    # [START howto_operator_retry_build]
    retry_build = CloudBuildRetryBuildOperator(
        task_id="retry_build",
        id_=cast(str, XComArg(cancel_build, key='id')),
        project_id=PROJECT_ID,
    )
    # [END howto_operator_retry_build]

    # [START howto_operator_get_build]
    get_build = CloudBuildGetBuildOperator(
        task_id="get_build",
        id_=cast(str, XComArg(retry_build, key='id')),
        project_id=PROJECT_ID,
    )
    # [END howto_operator_get_build]

    # [START howto_operator_gcp_create_build_from_yaml_body]
    create_build_from_file = CloudBuildCreateBuildOperator(
        task_id="create_build_from_file",
        project_id=PROJECT_ID,
        build=yaml.safe_load((Path(CURRENT_FOLDER) / 'resources' / 'example_cloud_build.yaml').read_text()),
        params={'name': 'Airflow'},
    )
    # [END howto_operator_gcp_create_build_from_yaml_body]

    delete_bucket_src = GCSDeleteBucketOperator(
        task_id="delete_bucket_src", bucket_name=BUCKET_NAME_SRC, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        create_bucket_src,
        upload_file,
        # TEST BODY
        create_build_from_storage,
        create_build_from_storage_result,
        create_build_from_repo,
        create_build_from_repo_result,
        list_builds,
        create_build_without_wait,
        cancel_build,
        retry_build,
        get_build,
        create_build_from_file,
        # TEST TEARDOWN
        delete_bucket_src,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
