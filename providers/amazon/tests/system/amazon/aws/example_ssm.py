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

import datetime
import time

import boto3

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.ec2 import EC2CreateInstanceOperator, EC2TerminateInstanceOperator
from airflow.providers.amazon.aws.operators.ssm import SsmRunCommandOperator
from airflow.providers.amazon.aws.sensors.ssm import SsmRunCommandCompletedSensor
from airflow.utils.trigger_rule import TriggerRule

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from system.amazon.aws.utils.ec2 import get_latest_ami_id

ROLE_ARN_KEY = "ROLE_ARN"
INSTANCE_PROFILE_NAME_KEY = "INSTANCE_PROFILE_NAME"

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).add_variable(INSTANCE_PROFILE_NAME_KEY).build()
)

DAG_ID = "example_ssm"
USER_DATA = """
    #!/bin/bash
    set -e

    # Update the system
    if command -v yum &> /dev/null; then
        PACKAGE_MANAGER="yum"
    elif command -v dnf &> /dev/null; then
        PACKAGE_MANAGER="dnf"
    else
        exit 1
    fi

    # Install SSM agent if it's not installed
    if ! command -v amazon-ssm-agent &> /dev/null; then
        if [[ "$PACKAGE_MANAGER" == "yum" || "$PACKAGE_MANAGER" == "dnf" ]]; then
            $PACKAGE_MANAGER install -y amazon-ssm-agent
        else
            exit 1
        fi
    fi

    # Enable and start the SSM agent
    systemctl enable amazon-ssm-agent
    systemctl start amazon-ssm-agent

    shutdown -h +8
"""


@task
def extract_instance_id(instance_ids: list) -> str:
    return instance_ids[0]


@task
def build_run_command_kwargs(instance_id: str):
    return {
        "InstanceIds": [instance_id],
        "Parameters": {"commands": ["touch /tmp/ssm_test_passed"]},
    }


@task
def wait_until_ssm_ready(instance_id: str, max_attempts: int = 10, delay_seconds: int = 15):
    """
    Waits for an EC2 instance to register with AWS Systems Manager (SSM).

    This may take over a minute even after the instance is running.
    Raises an exception if the instance is not ready after max_attempts.
    """
    ssm = boto3.client("ssm")

    for _ in range(max_attempts):
        response = ssm.describe_instance_information()
        instance_ids = [instance["InstanceId"] for instance in response.get("InstanceInformationList", [])]

        if instance_id in instance_ids:
            return

        time.sleep(delay_seconds)

    raise Exception(f"Instance {instance_id} not ready in SSM after {max_attempts} attempts.")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime.datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # Create EC2 instance with SSM agent
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    instance_name = f"{env_id}-instance"
    image_id = get_latest_ami_id()
    instance_profile_name = test_context[INSTANCE_PROFILE_NAME_KEY]

    config = {
        "InstanceType": "t2.micro",
        "IamInstanceProfile": {"Name": instance_profile_name},
        # Optional: Tags for identifying test resources in the AWS console
        "TagSpecifications": [
            {"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": instance_name}]}
        ],
        "UserData": USER_DATA,
        # Use IMDSv2 for greater security, see the following doc for more details:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
        "BlockDeviceMappings": [
            {"DeviceName": "/dev/xvda", "Ebs": {"Encrypted": True, "DeleteOnTermination": True}}
        ],
        "InstanceInitiatedShutdownBehavior": "terminate",
    }

    create_instance = EC2CreateInstanceOperator(
        task_id="create_instance",
        image_id=image_id,
        max_count=1,
        min_count=1,
        config=config,
        wait_for_completion=True,
    )

    instance_id = extract_instance_id(create_instance.output)

    run_command_kwargs = build_run_command_kwargs(instance_id)

    # [START howto_operator_run_command]
    run_command = SsmRunCommandOperator(
        task_id="run_command",
        document_name="AWS-RunShellScript",
        run_command_kwargs=run_command_kwargs,
        wait_for_completion=False,
    )
    # [END howto_operator_run_command]

    # [START howto_sensor_run_command]
    await_run_command = SsmRunCommandCompletedSensor(
        task_id="await_run_command", command_id=run_command.output
    )
    # [END howto_sensor_run_command]

    delete_instance = EC2TerminateInstanceOperator(
        task_id="terminate_instance",
        trigger_rule=TriggerRule.ALL_DONE,
        instance_ids=instance_id,
    )

    chain(
        # TEST SETUP
        test_context,
        image_id,
        create_instance,
        instance_id,
        run_command_kwargs,
        wait_until_ssm_ready(instance_id),
        # TEST BODY
        run_command,
        await_run_command,
        # TEST TEARDOWN
        delete_instance,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
