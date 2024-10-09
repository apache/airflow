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

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import boto3

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_emr"
CONFIG_NAME = "EMR Runtime Role Security Configuration"
EXECUTION_ROLE_ARN_KEY = "EXECUTION_ROLE_ARN"
BUCKET_NAME_KEY = "BUCKET_NAME"

SECURITY_CONFIGURATION = {
    "AuthorizationConfiguration": {
        "IAMConfiguration": {
            "EnableApplicationScopedIAMRole": True,
        },
    },
    # Use IMDSv2 for greater security, see the following doc for more details:
    # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-create-security-configuration.html
    "InstanceMetadataServiceConfiguration": {
        "MinimumInstanceMetadataServiceVersion": 2,
        "HttpPutResponseHopLimit": 2,
    },
}

# [START howto_operator_emr_steps_config]
SPARK_STEPS = [
    {
        "Name": "calculate_pi",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        },
    }
]

JOB_FLOW_OVERRIDES: dict[str, Any] = {
    "Name": "PiCalc",
    "ReleaseLabel": "emr-7.1.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        # If the EMR steps complete too quickly the cluster will be torn down before the other system test
        # tasks have a chance to run (such as the modify cluster step, the addition of more EMR steps, etc).
        # Set KeepJobFlowAliveWhenNoSteps to False to avoid the cluster from being torn down prematurely.
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}
# [END howto_operator_emr_steps_config]


@task
def configure_security_config(config_name: str):
    boto3.client("emr").create_security_configuration(
        Name=config_name,
        SecurityConfiguration=json.dumps(SECURITY_CONFIGURATION),
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_security_config(config_name: str):
    boto3.client("emr").delete_security_configuration(
        Name=config_name,
    )


@task
def get_step_id(step_ids: list):
    return step_ids[0]


sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(EXECUTION_ROLE_ARN_KEY)
    .add_variable(BUCKET_NAME_KEY, optional=True)
    .build()
)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()

    env_id = test_context[ENV_ID_KEY]
    config_name = f"{CONFIG_NAME}-{env_id}"
    execution_role_arn = test_context[EXECUTION_ROLE_ARN_KEY]
    s3_bucket = test_context[BUCKET_NAME_KEY] or f"{env_id}-emr-bucket"

    JOB_FLOW_OVERRIDES["LogUri"] = f"s3://{s3_bucket}/"
    JOB_FLOW_OVERRIDES["SecurityConfiguration"] = config_name

    create_security_configuration = configure_security_config(config_name)

    # [START howto_operator_emr_create_job_flow]
    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )
    # [END howto_operator_emr_create_job_flow]

    # [START howto_operator_emr_modify_cluster]
    modify_cluster = EmrModifyClusterOperator(
        task_id="modify_cluster", cluster_id=create_job_flow.output, step_concurrency_level=1
    )
    # [END howto_operator_emr_modify_cluster]

    # [START howto_operator_emr_add_steps]
    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=create_job_flow.output,
        steps=SPARK_STEPS,
        execution_role_arn=execution_role_arn,
    )
    # [END howto_operator_emr_add_steps]
    add_steps.wait_for_completion = True

    # [START howto_sensor_emr_step]
    wait_for_step = EmrStepSensor(
        task_id="wait_for_step",
        job_flow_id=create_job_flow.output,
        step_id=get_step_id(add_steps.output),
    )
    # [END howto_sensor_emr_step]

    # [START howto_operator_emr_terminate_job_flow]
    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=create_job_flow.output,
    )
    # [END howto_operator_emr_terminate_job_flow]
    remove_cluster.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_sensor_emr_job_flow]
    check_job_flow = EmrJobFlowSensor(task_id="check_job_flow", job_flow_id=create_job_flow.output)
    # [END howto_sensor_emr_job_flow]
    check_job_flow.poke_interval = 10

    delete_security_configuration = delete_security_config(config_name)

    # There are two options:
    # - Pass the bucket name as an argument to the system test. This bucket will then be used to store
    #   the EMR-related logs.
    # - The test itself creates and delete the S3 bucket needed for this test.
    create_s3_bucket: S3CreateBucketOperator | None = None
    delete_s3_bucket: S3DeleteBucketOperator | None = None
    if not test_context[BUCKET_NAME_KEY]:
        create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)
        delete_s3_bucket = S3DeleteBucketOperator(
            task_id="delete_s3_bucket",
            bucket_name=s3_bucket,
            force_delete=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

    chain(
        *[
            task
            for task in [
                # TEST SETUP
                test_context,
                create_s3_bucket,
                create_security_configuration,
                # TEST BODY
                create_job_flow,
                modify_cluster,
                add_steps,
                wait_for_step,
                # TEST TEARDOWN
                remove_cluster,
                check_job_flow,
                delete_security_configuration,
                delete_s3_bucket,
            ]
            if task is not None
        ]
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
