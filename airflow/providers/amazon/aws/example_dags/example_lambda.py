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

import json
from datetime import datetime, timedelta
from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator

# [START howto_operator_lambda_env_variables]
LAMBDA_FUNCTION_NAME = getenv("LAMBDA_FUNCTION_NAME", "test-function")
# [END howto_operator_lambda_env_variables]

SAMPLE_EVENT = json.dumps({"SampleEvent": {"SampleData": {"Name": "XYZ", "DoB": "1993-01-01"}}})

with DAG(
    dag_id='example_lambda',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_lambda]
    invoke_lambda_function = AwsLambdaInvokeFunctionOperator(
        task_id='setup__invoke_lambda_function',
        function_name=LAMBDA_FUNCTION_NAME,
        payload=SAMPLE_EVENT,
    )
    # [END howto_operator_lambda]
