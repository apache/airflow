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
"""

import os
from datetime import datetime
from typing import Any, Dict, cast

from airflow import models
from airflow.models.baseoperator import chain
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCreateBuildTriggerOperator,
    CloudBuildDeleteBuildTriggerOperator,
    CloudBuildGetBuildTriggerOperator,
    CloudBuildListBuildTriggersOperator,
    CloudBuildRunBuildTriggerOperator,
    CloudBuildUpdateBuildTriggerOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_gcp_cloud_build_trigger"

GCP_SOURCE_REPOSITORY_NAME = "test-cloud-build-repository"

# [START howto_operator_gcp_create_build_trigger_body]
create_build_trigger_body = {
    "name": "test-cloud-build-trigger",
    "trigger_template": {
        "project_id": PROJECT_ID,
        "repo_name": GCP_SOURCE_REPOSITORY_NAME,
        "branch_name": "main",
    },
    "filename": "example_cloud_build.yaml",
}
# [END howto_operator_gcp_create_build_trigger_body]

# [START howto_operator_gcp_update_build_trigger_body]
update_build_trigger_body = {
    "name": "test-cloud-build-trigger",
    "trigger_template": {
        "project_id": PROJECT_ID,
        "repo_name": GCP_SOURCE_REPOSITORY_NAME,
        "branch_name": "master",
    },
    "filename": "example_cloud_build.yaml",
}
# [END START howto_operator_gcp_update_build_trigger_body]

# [START howto_operator_create_build_from_repo_body]
create_build_from_repo_body: Dict[str, Any] = {
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

    # [START howto_operator_create_build_trigger]
    create_build_trigger = CloudBuildCreateBuildTriggerOperator(
        task_id="create_build_trigger", project_id=PROJECT_ID, trigger=create_build_trigger_body
    )
    # [END howto_operator_create_build_trigger]

    build_trigger_id = cast(str, XComArg(create_build_trigger, key="id"))

    # [START howto_operator_run_build_trigger]
    run_build_trigger = CloudBuildRunBuildTriggerOperator(
        task_id="run_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
        source=create_build_from_repo_body['source']['repo_source'],
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

    # [START howto_operator_list_build_triggers]
    list_build_triggers = CloudBuildListBuildTriggersOperator(
        task_id="list_build_triggers", project_id=PROJECT_ID, location="global", page_size=5
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

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
