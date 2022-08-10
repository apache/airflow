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

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.batch import BatchCreateComputeEnvironmentOperator, BatchOperator
from airflow.providers.amazon.aws.sensors.batch import (
    BatchComputeEnvironmentSensor,
    BatchJobQueueSensor,
    BatchSensor,
)
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, split_string

DAG_ID = 'example_batch'

# Externally fetched variables:
ROLE_ARN_KEY = 'ROLE_ARN'
SUBNETS_KEY = 'SUBNETS'
SECURITY_GROUPS_KEY = 'SECURITY_GROUPS'

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(ROLE_ARN_KEY)
    .add_variable(SUBNETS_KEY)
    .add_variable(SECURITY_GROUPS_KEY)
    .build()
)

JOB_OVERRIDES: dict = {}


@task
def create_job_definition(role_arn, job_definition_name):
    boto3.client('batch').register_job_definition(
        type='container',
        containerProperties={
            'command': [
                'sleep',
                '2',
            ],
            'executionRoleArn': role_arn,
            'image': 'busybox',
            'resourceRequirements': [
                {'value': '1', 'type': 'VCPU'},
                {'value': '2048', 'type': 'MEMORY'},
            ],
            'networkConfiguration': {
                'assignPublicIp': 'ENABLED',
            },
        },
        jobDefinitionName=job_definition_name,
        platformCapabilities=['FARGATE'],
    )


@task
def create_job_queue(job_compute_environment_name, job_queue_name):
    boto3.client('batch').create_job_queue(
        computeEnvironmentOrder=[
            {
                'computeEnvironment': job_compute_environment_name,
                'order': 1,
            },
        ],
        jobQueueName=job_queue_name,
        priority=1,
        state='ENABLED',
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_job_definition(job_definition_name):
    client = boto3.client('batch')

    response = client.describe_job_definitions(
        jobDefinitionName=job_definition_name,
        status='ACTIVE',
    )

    for job_definition in response['jobDefinitions']:
        client.deregister_job_definition(
            jobDefinition=job_definition['jobDefinitionArn'],
        )


@task(trigger_rule=TriggerRule.ALL_DONE)
def disable_compute_environment(job_compute_environment_name):
    boto3.client('batch').update_compute_environment(
        computeEnvironment=job_compute_environment_name,
        state='DISABLED',
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_compute_environment(job_compute_environment_name):
    boto3.client('batch').delete_compute_environment(
        computeEnvironment=job_compute_environment_name,
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def disable_job_queue(job_queue_name):
    boto3.client('batch').update_job_queue(
        jobQueue=job_queue_name,
        state='DISABLED',
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_job_queue(job_queue_name):
    boto3.client('batch').delete_job_queue(
        jobQueue=job_queue_name,
    )


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    batch_job_name: str = f'{test_context[ENV_ID_KEY]}-test-job'
    batch_job_definition_name: str = f'{test_context[ENV_ID_KEY]}-test-job-definition'
    batch_job_compute_environment_name: str = f'{test_context[ENV_ID_KEY]}-test-job-compute-environment'
    batch_job_queue_name: str = f'{test_context[ENV_ID_KEY]}-test-job-queue'

    security_groups = split_string(test_context[SECURITY_GROUPS_KEY])
    subnets = split_string(test_context[SUBNETS_KEY])

    # [START howto_operator_batch_create_compute_environment]
    create_compute_environment = BatchCreateComputeEnvironmentOperator(
        task_id='create_compute_environment',
        compute_environment_name=batch_job_compute_environment_name,
        environment_type='MANAGED',
        state='ENABLED',
        compute_resources={
            'type': 'FARGATE',
            'maxvCpus': 10,
            'securityGroupIds': security_groups,
            'subnets': subnets,
        },
    )
    # [END howto_operator_batch_create_compute_environment]

    # [START howto_sensor_batch_compute_environment]
    wait_for_compute_environment_valid = BatchComputeEnvironmentSensor(
        task_id='wait_for_compute_environment_valid',
        compute_environment=batch_job_compute_environment_name,
    )
    # [END howto_sensor_batch_compute_environment]

    # [START howto_sensor_batch_job_queue]
    wait_for_job_queue_valid = BatchJobQueueSensor(
        task_id='wait_for_job_queue_valid',
        job_queue=batch_job_queue_name,
    )
    # [END howto_sensor_batch_job_queue]

    # [START howto_operator_batch]
    submit_batch_job = BatchOperator(
        task_id='submit_batch_job',
        job_name=batch_job_name,
        job_queue=batch_job_queue_name,
        job_definition=batch_job_definition_name,
        overrides=JOB_OVERRIDES,
        # Set this flag to False, so we can test the sensor below
        wait_for_completion=False,
    )
    # [END howto_operator_batch]

    # [START howto_sensor_batch]
    wait_for_batch_job = BatchSensor(
        task_id='wait_for_batch_job',
        job_id=submit_batch_job.output,
    )
    # [END howto_sensor_batch]

    wait_for_compute_environment_disabled = BatchComputeEnvironmentSensor(
        task_id='wait_for_compute_environment_disabled',
        compute_environment=batch_job_compute_environment_name,
    )

    wait_for_job_queue_modified = BatchJobQueueSensor(
        task_id='wait_for_job_queue_modified',
        job_queue=batch_job_queue_name,
    )

    wait_for_job_queue_deleted = BatchJobQueueSensor(
        task_id='wait_for_job_queue_deleted',
        job_queue=batch_job_queue_name,
        treat_non_existing_as_deleted=True,
    )

    chain(
        # TEST SETUP
        test_context,
        security_groups,
        subnets,
        create_job_definition(test_context[ROLE_ARN_KEY], batch_job_definition_name),
        # TEST BODY
        create_compute_environment,
        wait_for_compute_environment_valid,
        # ``create_job_queue`` is part of test setup but need the compute-environment to be created before
        create_job_queue(batch_job_compute_environment_name, batch_job_queue_name),
        wait_for_job_queue_valid,
        submit_batch_job,
        wait_for_batch_job,
        # TEST TEARDOWN
        disable_job_queue(batch_job_queue_name),
        wait_for_job_queue_modified,
        delete_job_queue(batch_job_queue_name),
        wait_for_job_queue_deleted,
        disable_compute_environment(batch_job_compute_environment_name),
        wait_for_compute_environment_disabled,
        delete_compute_environment(batch_job_compute_environment_name),
        delete_job_definition(batch_job_definition_name),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
