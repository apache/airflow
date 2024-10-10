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
Example Airflow DAG that displays interactions with Google Cloud Functions.
It creates a function and then deletes it.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.functions import (
    CloudFunctionDeleteFunctionOperator,
    CloudFunctionDeployFunctionOperator,
    CloudFunctionInvokeFunctionOperator,
)

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "gcp_function"
LOCATION = "europe-west1"

# make sure there are no dashes in function name (!)
SHORT_FUNCTION_NAME = "hello_world"
FUNCTION_NAME = f"projects/{PROJECT_ID}/locations/{LOCATION}/functions/{SHORT_FUNCTION_NAME}"
SOURCE_ARCHIVE_URL = "gs://airflow-system-tests-resources/cloud-functions/main_function.zip"
ENTRYPOINT = "hello_world"
RUNTIME = "python38"

SOURCE_UPLOAD_URL = ""
ZIP_PATH = ""
repo = f"repo-{DAG_ID}-{ENV_ID}".replace("_", "-")
SOURCE_REPOSITORY = ()
VALIDATE_BODY = True

# [START howto_operator_gcf_deploy_body]
body = {"name": FUNCTION_NAME, "entryPoint": ENTRYPOINT, "runtime": RUNTIME, "httpsTrigger": {}}
# [END howto_operator_gcf_deploy_body]

# [START howto_operator_gcf_default_args]
default_args: dict[str, Any] = {"retries": 3}
# [END howto_operator_gcf_default_args]

# [START howto_operator_gcf_deploy_variants]
if SOURCE_ARCHIVE_URL:
    body["sourceArchiveUrl"] = SOURCE_ARCHIVE_URL
elif SOURCE_REPOSITORY:
    body["sourceRepository"] = {"url": SOURCE_REPOSITORY}
elif ZIP_PATH:
    body["sourceUploadUrl"] = ""
    default_args["zip_path"] = ZIP_PATH
elif SOURCE_UPLOAD_URL:
    body["sourceUploadUrl"] = SOURCE_UPLOAD_URL
else:
    raise Exception("Please provide one of the source_code parameters")
# [END howto_operator_gcf_deploy_variants]


with DAG(
    DAG_ID,
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "gcp-functions"],
) as dag:
    # [START howto_operator_gcf_deploy]
    deploy_function = CloudFunctionDeployFunctionOperator(
        task_id="deploy_function",
        project_id=PROJECT_ID,
        location=LOCATION,
        body=body,
        validate_body=VALIDATE_BODY,
    )
    # [END howto_operator_gcf_deploy]

    # [START howto_operator_gcf_deploy_no_project_id]
    deploy_function_no_project = CloudFunctionDeployFunctionOperator(
        task_id="deploy_function_no_project", location=LOCATION, body=body, validate_body=VALIDATE_BODY
    )
    # [END howto_operator_gcf_deploy_no_project_id]

    # [START howto_operator_gcf_invoke_function]
    invoke_function = CloudFunctionInvokeFunctionOperator(
        task_id="invoke_function",
        project_id=PROJECT_ID,
        location=LOCATION,
        input_data={},
        function_id=SHORT_FUNCTION_NAME,
    )
    # [END howto_operator_gcf_invoke_function]

    # [START howto_operator_gcf_delete]
    delete_function = CloudFunctionDeleteFunctionOperator(task_id="delete_function", name=FUNCTION_NAME)
    # [END howto_operator_gcf_delete]

    chain(
        deploy_function,
        deploy_function_no_project,
        invoke_function,
        delete_function,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
