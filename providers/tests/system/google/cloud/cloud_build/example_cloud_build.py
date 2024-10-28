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
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import yaml

from airflow.decorators import task_group
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCancelBuildOperator,
    CloudBuildCreateBuildOperator,
    CloudBuildGetBuildOperator,
    CloudBuildListBuildsOperator,
    CloudBuildRetryBuildOperator,
)
from airflow.providers.standard.operators.bash import BashOperator

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = (
    os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
)

DAG_ID = "gcp_cloud_build"

GCP_SOURCE_ARCHIVE_URL = "gs://airflow-system-tests-resources/cloud-build/file.tar.gz"
# Repository with this name is expected created within the project $SYSTEM_TESTS_GCP_PROJECT
# If you'd like to run this system test locally, please
#   1. Create Cloud Source Repository
#   2. Push into a master branch the following file:
#   providers/tests/system/google/cloud/cloud_build/resources/example_cloud_build.yaml
GCP_SOURCE_REPOSITORY_NAME = "test-cloud-build-repository"

CURRENT_FOLDER = Path(__file__).parent

# [START howto_operator_gcp_create_build_from_storage_body]
CREATE_BUILD_FROM_STORAGE_BODY = {
    "source": {"storage_source": GCP_SOURCE_ARCHIVE_URL},
    "steps": [{"name": "ubuntu", "args": ["echo", "Hello world", "sleep 200"]}],
}
# [END howto_operator_gcp_create_build_from_storage_body]

# [START howto_operator_create_build_from_repo_body]
CREATE_BUILD_FROM_REPO_BODY: dict[str, Any] = {
    "source": {
        "repo_source": {"repo_name": GCP_SOURCE_REPOSITORY_NAME, "branch_name": "master"}
    },
    "steps": [{"name": "ubuntu", "args": ["echo", "Hello world", "sleep 200"]}],
}
# [END howto_operator_create_build_from_repo_body]


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "cloud_build"],
) as dag:

    @task_group(group_id="build_from_storage")
    def build_from_storage():
        # [START howto_operator_create_build_from_storage]
        create_build_from_storage = CloudBuildCreateBuildOperator(
            task_id="create_build_from_storage",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_STORAGE_BODY,
        )
        # [END howto_operator_create_build_from_storage]

        # [START howto_operator_create_build_from_storage_result]
        create_build_from_storage_result = BashOperator(
            bash_command=f"echo {cast(str, XComArg(create_build_from_storage, key='results'))}",
            task_id="create_build_from_storage_result",
        )
        # [END howto_operator_create_build_from_storage_result]

        create_build_from_storage >> create_build_from_storage_result

    @task_group(group_id="build_from_storage_deferrable")
    def build_from_storage_deferrable():
        # [START howto_operator_create_build_from_storage_async]
        create_build_from_storage = CloudBuildCreateBuildOperator(
            task_id="create_build_from_storage",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_STORAGE_BODY,
            deferrable=True,
        )
        # [END howto_operator_create_build_from_storage_async]

        create_build_from_storage_result = BashOperator(
            bash_command=f"echo {cast(str, XComArg(create_build_from_storage, key='results'))}",
            task_id="create_build_from_storage_result",
        )

        create_build_from_storage >> create_build_from_storage_result

    @task_group(group_id="build_from_repo")
    def build_from_repo():
        # [START howto_operator_create_build_from_repo]
        create_build_from_repo = CloudBuildCreateBuildOperator(
            task_id="create_build_from_repo",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_REPO_BODY,
        )
        # [END howto_operator_create_build_from_repo]

        # [START howto_operator_create_build_from_repo_result]
        create_build_from_repo_result = BashOperator(
            bash_command=f"echo {cast(str, XComArg(create_build_from_repo, key='results'))}",
            task_id="create_build_from_repo_result",
        )
        # [END howto_operator_create_build_from_repo_result]

        create_build_from_repo >> create_build_from_repo_result

    @task_group(group_id="build_from_repo_deferrable")
    def build_from_repo_deferrable():
        # [START howto_operator_create_build_from_repo_async]
        create_build_from_repo = CloudBuildCreateBuildOperator(
            task_id="create_build_from_repo",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_REPO_BODY,
            deferrable=True,
        )
        # [END howto_operator_create_build_from_repo_async]

        create_build_from_repo_result = BashOperator(
            bash_command=f"echo {cast(str, XComArg(create_build_from_repo, key='results'))}",
            task_id="create_build_from_repo_result",
        )

        create_build_from_repo >> create_build_from_repo_result

    # [START howto_operator_gcp_create_build_from_yaml_body]
    create_build_from_file = CloudBuildCreateBuildOperator(
        task_id="create_build_from_file",
        project_id=PROJECT_ID,
        build=yaml.safe_load(
            (Path(CURRENT_FOLDER) / "resources" / "example_cloud_build.yaml").read_text()
        ),
        params={"name": "Airflow"},
    )
    # [END howto_operator_gcp_create_build_from_yaml_body]

    # [START howto_operator_gcp_create_build_from_yaml_body_async]
    create_build_from_file_deferrable = CloudBuildCreateBuildOperator(
        task_id="create_build_from_file_deferrable",
        project_id=PROJECT_ID,
        build=yaml.safe_load(
            (Path(CURRENT_FOLDER) / "resources" / "example_cloud_build.yaml").read_text()
        ),
        params={"name": "Airflow"},
        deferrable=True,
    )
    # [END howto_operator_gcp_create_build_from_yaml_body_async]

    # [START howto_operator_list_builds]
    list_builds = CloudBuildListBuildsOperator(
        task_id="list_builds",
        project_id=PROJECT_ID,
        location="global",
    )
    # [END howto_operator_list_builds]

    @task_group(group_id="no_wait_cancel_retry_get")
    def no_wait_cancel_retry_get():
        # [START howto_operator_create_build_without_wait]
        create_build_without_wait = CloudBuildCreateBuildOperator(
            task_id="create_build_without_wait",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_REPO_BODY,
            wait=False,
        )
        # [END howto_operator_create_build_without_wait]

        # [START howto_operator_cancel_build]
        cancel_build = CloudBuildCancelBuildOperator(
            task_id="cancel_build",
            id_=cast(str, XComArg(create_build_without_wait, key="id")),
            project_id=PROJECT_ID,
        )
        # [END howto_operator_cancel_build]

        # [START howto_operator_retry_build]
        retry_build = CloudBuildRetryBuildOperator(
            task_id="retry_build",
            id_=cast(str, XComArg(cancel_build, key="id")),
            project_id=PROJECT_ID,
        )
        # [END howto_operator_retry_build]

        # [START howto_operator_get_build]
        get_build = CloudBuildGetBuildOperator(
            task_id="get_build",
            id_=cast(str, XComArg(retry_build, key="id")),
            project_id=PROJECT_ID,
        )
        # [END howto_operator_get_build]

        create_build_without_wait >> cancel_build >> retry_build >> get_build

    @task_group(group_id="no_wait_cancel_retry_get_deferrable")
    def no_wait_cancel_retry_get_deferrable():
        # [START howto_operator_create_build_without_wait_async]
        create_build_without_wait = CloudBuildCreateBuildOperator(
            task_id="create_build_without_wait",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_REPO_BODY,
            wait=False,
            deferrable=True,
        )
        # [END howto_operator_create_build_without_wait_async]

        cancel_build = CloudBuildCancelBuildOperator(
            task_id="cancel_build",
            id_=cast(str, XComArg(create_build_without_wait, key="id")),
            project_id=PROJECT_ID,
        )

        retry_build = CloudBuildRetryBuildOperator(
            task_id="retry_build",
            id_=cast(str, XComArg(cancel_build, key="id")),
            project_id=PROJECT_ID,
        )

        get_build = CloudBuildGetBuildOperator(
            task_id="get_build",
            id_=cast(str, XComArg(retry_build, key="id")),
            project_id=PROJECT_ID,
        )

        create_build_without_wait >> cancel_build >> retry_build >> get_build

    # TEST BODY
    (
        [
            build_from_storage(),
            build_from_storage_deferrable(),
            build_from_repo(),
            build_from_repo_deferrable(),
            create_build_from_file,
            create_build_from_file_deferrable,
        ]
        >> list_builds
        >> [
            no_wait_cancel_retry_get(),
            no_wait_cancel_retry_get_deferrable(),
        ]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
