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
from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime

import boto3

from airflow import models
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.lambda_function import (
    AwsLambdaInvokeFunctionOperator,
    LambdaCreateFunctionOperator,
)
from airflow.providers.amazon.aws.sensors.lambda_function import LambdaFunctionStateSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, purge_logs

DAG_ID = "example_lambda"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

CODE_CONTENT = """
def test(*args):
    print('Hello')
"""


# Create a zip file containing one file "lambda_function.py" to deploy to the lambda function
def create_zip(content: str):
    zip_output = io.BytesIO()
    with zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED) as zip_file:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o777 << 16
        zip_file.writestr(info, content)
    zip_output.seek(0)
    return zip_output.read()


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_lambda(function_name: str):
    client = boto3.client("lambda")
    client.delete_function(
        FunctionName=function_name,
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_logs(function_name: str) -> None:
    generated_log_groups: list[tuple[str, str | None]] = [
        (f"/aws/lambda/{function_name}", None),
    ]

    purge_logs(test_logs=generated_log_groups, force_delete=True, retry=True)


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    lambda_function_name: str = f"{test_context[ENV_ID_KEY]}-function"
    role_arn = test_context[ROLE_ARN_KEY]

    # [START howto_operator_create_lambda_function]
    create_lambda_function = LambdaCreateFunctionOperator(
        task_id="create_lambda_function",
        function_name=lambda_function_name,
        runtime="python3.9",
        role=role_arn,
        handler="lambda_function.test",
        code={
            "ZipFile": create_zip(CODE_CONTENT),
        },
    )
    # [END howto_operator_create_lambda_function]

    # [START howto_sensor_lambda_function_state]
    wait_lambda_function_state = LambdaFunctionStateSensor(
        task_id="wait_lambda_function_state",
        function_name=lambda_function_name,
    )
    # [END howto_sensor_lambda_function_state]

    # [START howto_operator_invoke_lambda_function]
    invoke_lambda_function = AwsLambdaInvokeFunctionOperator(
        task_id="invoke_lambda_function",
        function_name=lambda_function_name,
        payload=json.dumps({"SampleEvent": {"SampleData": {"Name": "XYZ", "DoB": "1993-01-01"}}}),
    )
    # [END howto_operator_invoke_lambda_function]

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_lambda_function,
        wait_lambda_function_state,
        invoke_lambda_function,
        # TEST TEARDOWN
        delete_lambda(lambda_function_name),
        delete_logs(lambda_function_name),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
