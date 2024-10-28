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
from typing import Any, cast

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCreateBuildTriggerOperator,
    CloudBuildDeleteBuildTriggerOperator,
    CloudBuildGetBuildTriggerOperator,
    CloudBuildListBuildTriggersOperator,
    CloudBuildRunBuildTriggerOperator,
    CloudBuildUpdateBuildTriggerOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = (
    os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
)

DAG_ID = "gcp_cloud_build_trigger"

# Repository with this name is expected created within the project $SYSTEM_TESTS_GCP_PROJECT
# If you'd like to run this system test locally, please
#   1. Create Cloud Source Repository
#   2. Push into a master branch the following file:
#   providers/tests/system/google/cloud/cloud_build/resources/example_cloud_build.yaml
GCP_SOURCE_REPOSITORY_NAME = "test-cloud-build-repository"

TRIGGER_NAME = f"cloud-build-trigger-{ENV_ID}".replace("_", "-")
PROJECT_NUMBER = "{{ task_instance.xcom_pull('get_project_number') }}"

# [START howto_operator_gcp_create_build_trigger_body]
create_build_trigger_body = {
    "name": TRIGGER_NAME,
    "trigger_template": {
        "project_id": PROJECT_ID,
        "repo_name": GCP_SOURCE_REPOSITORY_NAME,
        "branch_name": "master",
    },
    "filename": "example_cloud_build.yaml",
    "service_account": f"projects/{PROJECT_ID}/serviceAccounts/{PROJECT_NUMBER}-compute@developer.gserviceaccount.com",
}
# [END howto_operator_gcp_create_build_trigger_body]

# [START howto_operator_gcp_update_build_trigger_body]
update_build_trigger_body = {
    "name": TRIGGER_NAME,
    "trigger_template": {
        "project_id": PROJECT_ID,
        "repo_name": GCP_SOURCE_REPOSITORY_NAME,
        "branch_name": "master",
    },
    "filename": "example_cloud_build.yaml",
    "service_account": f"projects/{PROJECT_ID}/serviceAccounts/{PROJECT_NUMBER}-compute@developer.gserviceaccount.com",
}
# [END START howto_operator_gcp_update_build_trigger_body]

# [START howto_operator_create_build_from_repo_body]
create_build_from_repo_body: dict[str, Any] = {
    "source": {
        "repo_source": {"repo_name": GCP_SOURCE_REPOSITORY_NAME, "branch_name": "master"}
    },
    "steps": [{"name": "ubuntu", "args": ["echo", "Hello world", "sleep 200"]}],
}
# [END howto_operator_create_build_from_repo_body]


@task(task_id="get_project_number")
def get_project_number():
    """Helper function to retrieve the number of the project based on PROJECT_ID"""
    try:
        with build("cloudresourcemanager", "v1") as service:
            response = service.projects().get(projectId=PROJECT_ID).execute()
        return response["projectNumber"]
    except HttpError as exc:
        if exc.status_code == 403:
            raise AirflowException(
                "No project found with specified name, "
                "or caller does not have permissions to read specified project"
            )
        else:
            raise exc


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "cloud_build_trigger"],
) as dag:
    # [START howto_operator_create_build_trigger]
    create_build_trigger = CloudBuildCreateBuildTriggerOperator(
        task_id="create_build_trigger",
        project_id=PROJECT_ID,
        trigger=create_build_trigger_body,
    )
    # [END howto_operator_create_build_trigger]

    build_trigger_id = cast(str, XComArg(create_build_trigger, key="id"))

    # [START howto_operator_run_build_trigger]
    run_build_trigger = CloudBuildRunBuildTriggerOperator(
        task_id="run_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
        source=create_build_from_repo_body["source"]["repo_source"],
    )
    # [END howto_operator_run_build_trigger]

    # [START howto_operator_update_build_trigger]
    update_build_trigger = CloudBuildUpdateBuildTriggerOperator(
        task_id="update_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
        trigger=update_build_trigger_body,
    )
    # [END howto_operator_update_build_trigger]

    # [START howto_operator_get_build_trigger]
    get_build_trigger = CloudBuildGetBuildTriggerOperator(
        task_id="get_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
    )
    # [END howto_operator_get_build_trigger]

    # [START howto_operator_delete_build_trigger]
    delete_build_trigger = CloudBuildDeleteBuildTriggerOperator(
        task_id="delete_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
    )
    # [END howto_operator_delete_build_trigger]
    delete_build_trigger.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_list_build_triggers]
    list_build_triggers = CloudBuildListBuildTriggersOperator(
        task_id="list_build_triggers",
        project_id=PROJECT_ID,
        location="global",
        page_size=5,
    )
    # [END howto_operator_list_build_triggers]

    (
        # TEST SETUP
        get_project_number()
        # TEST BODY
        >> create_build_trigger
        >> run_build_trigger
        >> update_build_trigger
        >> get_build_trigger
        # TEST TEARDOWN
        >> delete_build_trigger
        >> list_build_triggers
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
