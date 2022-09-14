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

This DAG relies on the following OS environment variables
https://airflow.apache.org/concepts.html#variables

* PROJECT_ID - Google Cloud Project to use for the Cloud Function.
* LOCATION - Google Cloud Functions region where the function should be
  created.
* ENTRYPOINT - Name of the executable function in the source code.
* and one of the below:

    * SOURCE_ARCHIVE_URL - Path to the zipped source in Google Cloud Storage

    * SOURCE_UPLOAD_URL - Generated upload URL for the zipped source and ZIP_PATH - Local path to
      the zipped source archive

    * SOURCE_REPOSITORY - The URL pointing to the hosted repository where the function
      is defined in a supported Cloud Source Repository URL format
      https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#SourceRepository

"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.functions import (
    CloudFunctionDeleteFunctionOperator,
    CloudFunctionDeployFunctionOperator,
    CloudFunctionInvokeFunctionOperator,
)

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_gcp_function"

# make sure there are no dashes in function name (!)
SHORT_FUNCTION_NAME = 'hello'

LOCATION = 'europe-west1'

FUNCTION_NAME = f'projects/{PROJECT_ID}/locations/{LOCATION}/functions/{SHORT_FUNCTION_NAME}'
SOURCE_ARCHIVE_URL = ''
SOURCE_UPLOAD_URL = ''

repo = 'test-repo'
SOURCE_REPOSITORY = (
    f'https://source.developers.google.com/projects/{PROJECT_ID}/repos/{repo}/moveable-aliases/master'
)

ZIP_PATH = ''
ENTRYPOINT = 'helloWorld'
RUNTIME = 'nodejs14'
VALIDATE_BODY = True

# [START howto_operator_gcf_deploy_body]
body = {"name": FUNCTION_NAME, "entryPoint": ENTRYPOINT, "runtime": RUNTIME, "httpsTrigger": {}}
# [END howto_operator_gcf_deploy_body]

# [START howto_operator_gcf_default_args]
default_args: dict[str, Any] = {'retries': 3}
# [END howto_operator_gcf_default_args]

# [START howto_operator_gcf_deploy_variants]
if SOURCE_ARCHIVE_URL:
    body['sourceArchiveUrl'] = SOURCE_ARCHIVE_URL
elif SOURCE_REPOSITORY:
    body['sourceRepository'] = {'url': SOURCE_REPOSITORY}
elif ZIP_PATH:
    body['sourceUploadUrl'] = ''
    default_args['zip_path'] = ZIP_PATH
elif SOURCE_UPLOAD_URL:
    body['sourceUploadUrl'] = SOURCE_UPLOAD_URL
else:
    raise Exception("Please provide one of the source_code parameters")
# [END howto_operator_gcf_deploy_variants]


with models.DAG(
    DAG_ID,
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # [START howto_operator_gcf_deploy]
    deploy_task = CloudFunctionDeployFunctionOperator(
        task_id="gcf_deploy_task",
        project_id=PROJECT_ID,
        location=LOCATION,
        body=body,
        validate_body=VALIDATE_BODY,
    )
    # [END howto_operator_gcf_deploy]

    # [START howto_operator_gcf_deploy_no_project_id]
    deploy2_task = CloudFunctionDeployFunctionOperator(
        task_id="gcf_deploy2_task", location=LOCATION, body=body, validate_body=VALIDATE_BODY
    )
    # [END howto_operator_gcf_deploy_no_project_id]

    # [START howto_operator_gcf_invoke_function]
    invoke_task = CloudFunctionInvokeFunctionOperator(
        task_id="invoke_task",
        project_id=PROJECT_ID,
        location=LOCATION,
        input_data={},
        function_id=SHORT_FUNCTION_NAME,
    )
    # [END howto_operator_gcf_invoke_function]

    # [START howto_operator_gcf_delete]
    delete_task = CloudFunctionDeleteFunctionOperator(task_id="gcf_delete_task", name=FUNCTION_NAME)
    # [END howto_operator_gcf_delete]

    chain(
        deploy_task,
        deploy2_task,
        invoke_task,
        delete_task,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
