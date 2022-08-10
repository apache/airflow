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
import io
import json
import zipfile
from datetime import datetime
from typing import List, Optional, Tuple

import boto3

from airflow import models
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, purge_logs

DAG_ID = 'example_lambda'

# Externally fetched variables:
ROLE_ARN_KEY = 'ROLE_ARN'

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


@task
def create_lambda(function_name: str, role_arn: str):
    client = boto3.client('lambda')
    client.create_function(
        FunctionName=function_name,
        Runtime='python3.9',
        Role=role_arn,
        Handler='lambda_function.test',
        Code={
            'ZipFile': create_zip(CODE_CONTENT),
        },
        Description='Function used for system tests',
    )


@task
def await_lambda(function_name: str):
    client = boto3.client('lambda')
    waiter = client.get_waiter('function_active_v2')
    waiter.wait(FunctionName=function_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_lambda(function_name: str):
    client = boto3.client('lambda')
    client.delete_function(
        FunctionName=function_name,
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_logs(function_name: str) -> None:
    generated_log_groups: List[Tuple[str, Optional[str]]] = [
        (f'/aws/lambda/{function_name}', None),
    ]

    purge_logs(test_logs=generated_log_groups, force_delete=True, retry=True)


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    lambda_function_name: str = f'{test_context[ENV_ID_KEY]}-function'

    # [START howto_operator_lambda]
    invoke_lambda_function = AwsLambdaInvokeFunctionOperator(
        task_id='invoke_lambda_function',
        function_name=lambda_function_name,
        payload=json.dumps({"SampleEvent": {"SampleData": {"Name": "XYZ", "DoB": "1993-01-01"}}}),
    )
    # [END howto_operator_lambda]

    chain(
        # TEST SETUP
        test_context,
        create_lambda(lambda_function_name, test_context[ROLE_ARN_KEY]),
        await_lambda(lambda_function_name),
        # TEST BODY
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
