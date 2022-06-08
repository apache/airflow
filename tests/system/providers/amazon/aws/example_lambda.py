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

import boto3

from airflow import models
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import fetch_variable, set_env_id

ENV_ID = set_env_id()
DAG_ID = 'example_lambda'

FUNCTION_NAME = f'{ENV_ID}-function'
ROLE_ARN = fetch_variable('ROLE_ARN')

CODE_CONTENT = """
def test(*args):
    print('Hello')
"""


# Create a zip file containing one file "lambda_function.py" to deploy to the lambda function
def create_zip(content):
    zip_output = io.BytesIO()
    with zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED) as zip_file:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o777 << 16
        zip_file.writestr(info, content)
    zip_output.seek(0)
    return zip_output.read()


@task
def create_lambda():
    client = boto3.client('lambda')
    client.create_function(
        FunctionName=FUNCTION_NAME,
        Runtime='python3.9',
        Role=ROLE_ARN,
        Handler='lambda_function.test',
        Code={
            'ZipFile': create_zip(CODE_CONTENT),
        },
        Description='Function used for system tests',
    )


@task
def await_lambda():
    client = boto3.client('lambda')
    waiter = client.get_waiter('function_active_v2')
    waiter.wait(FunctionName=FUNCTION_NAME)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_lambda():
    client = boto3.client('lambda')
    client.delete_function(
        FunctionName=FUNCTION_NAME,
    )


with models.DAG(
    DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_lambda]
    invoke_lambda_function = AwsLambdaInvokeFunctionOperator(
        task_id='invoke_lambda_function',
        function_name=FUNCTION_NAME,
        payload=json.dumps({"SampleEvent": {"SampleData": {"Name": "XYZ", "DoB": "1993-01-01"}}}),
    )
    # [END howto_operator_lambda]

    chain(
        # TEST SETUP
        create_lambda(),
        await_lambda(),
        # TEST BODY
        invoke_lambda_function,
        # TEST TEARDOWN
        delete_lambda(),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
