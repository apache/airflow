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
from datetime import datetime
from json import dumps

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.cloud_formation import (
    CloudFormationCreateStackOperator,
    CloudFormationDeleteStackOperator,
)
from airflow.providers.amazon.aws.sensors.cloud_formation import (
    CloudFormationCreateStackSensor,
    CloudFormationDeleteStackSensor,
)

CLOUDFORMATION_STACK_NAME = 'example-stack-name'
# The CloudFormation template must have at least one resource to be usable, this example uses SQS
# as a free and serverless option.
CLOUDFORMATION_TEMPLATE = {
    'Description': 'Stack from Airflow CloudFormation example DAG',
    'Resources': {
        'ExampleQueue': {
            'Type': 'AWS::SQS::Queue',
        }
    },
}
CLOUDFORMATION_CREATE_PARAMETERS = {
    'StackName': CLOUDFORMATION_STACK_NAME,
    'TemplateBody': dumps(CLOUDFORMATION_TEMPLATE),
    'TimeoutInMinutes': 2,
    'OnFailure': 'DELETE',  # Don't leave stacks behind if creation fails.
}

with DAG(
    dag_id='example_cloudformation',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_cloudformation_create_stack]
    create_stack = CloudFormationCreateStackOperator(
        task_id='create_stack',
        stack_name=CLOUDFORMATION_STACK_NAME,
        cloudformation_parameters=CLOUDFORMATION_CREATE_PARAMETERS,
    )
    # [END howto_operator_cloudformation_create_stack]

    # [START howto_sensor_cloudformation_create_stack]
    wait_for_stack_create = CloudFormationCreateStackSensor(
        task_id='wait_for_stack_creation', stack_name=CLOUDFORMATION_STACK_NAME
    )
    # [END howto_sensor_cloudformation_create_stack]

    # [START howto_operator_cloudformation_delete_stack]
    delete_stack = CloudFormationDeleteStackOperator(
        task_id='delete_stack', stack_name=CLOUDFORMATION_STACK_NAME
    )
    # [END howto_operator_cloudformation_delete_stack]

    # [START howto_sensor_cloudformation_delete_stack]
    wait_for_stack_delete = CloudFormationDeleteStackSensor(
        task_id='wait_for_stack_deletion', trigger_rule='all_done', stack_name=CLOUDFORMATION_STACK_NAME
    )
    # [END howto_sensor_cloudformation_delete_stack]

    chain(create_stack, wait_for_stack_create, delete_stack, wait_for_stack_delete)
