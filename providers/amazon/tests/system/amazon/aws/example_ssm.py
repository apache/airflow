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
import logging
import textwrap
import time

import boto3

from airflow.providers.amazon.aws.operators.ec2 import EC2CreateInstanceOperator, EC2TerminateInstanceOperator
from airflow.providers.amazon.aws.operators.ssm import SsmGetCommandInvocationOperator, SsmRunCommandOperator
from airflow.providers.amazon.aws.sensors.ssm import SsmRunCommandCompletedSensor

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, get_role_name
from system.amazon.aws.utils.ec2 import get_latest_ami_id

DAG_ID = "example_ssm"

ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

USER_DATA = textwrap.dedent("""\
    #!/bin/bash
    set -e

    # Update the system
    if command -v yum &> /dev/null; then
        PACKAGE_MANAGER="yum"
    elif command -v dnf &> /dev/null; then
        PACKAGE_MANAGER="dnf"
    else
        echo "No suitable package manager found"
        exit 1
    fi

    # Install SSM agent if it's not installed
    if ! command -v amazon-ssm-agent &> /dev/null; then
        echo "Installing SSM agent..."
        $PACKAGE_MANAGER install -y amazon-ssm-agent
    else
        echo "SSM agent already installed"
    fi

    echo "Enabling and starting SSM agent..."
    systemctl enable amazon-ssm-agent
    systemctl start amazon-ssm-agent

    shutdown -h +15

    echo "=== Finished user-data script ==="
""")

log = logging.getLogger(__name__)


@task
def create_instance_profile(role_name: str, instance_profile_name: str):
    client = boto3.client("iam")
    client.create_instance_profile(InstanceProfileName=instance_profile_name)
    client.add_role_to_instance_profile(InstanceProfileName=instance_profile_name, RoleName=role_name)


@task
def await_instance_profile_exists(instance_profile_name):
    client = boto3.client("iam")
    client.get_waiter("instance_profile_exists").wait(InstanceProfileName=instance_profile_name)


@task
def delete_instance_profile(instance_profile_name, role_name):
    client = boto3.client("iam")

    try:
        client.remove_role_from_instance_profile(
            InstanceProfileName=instance_profile_name, RoleName=role_name
        )
    except client.exceptions.NoSuchEntityException:
        log.info("Role %s not attached to %s or already removed.", role_name, instance_profile_name)

    try:
        client.delete_instance_profile(InstanceProfileName=instance_profile_name)
    except client.exceptions.NoSuchEntityException:
        log.info("Instance profile %s already deleted.", instance_profile_name)


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
        response = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        )

        if (
            response.get("InstanceInformationList")
            and response["InstanceInformationList"][0]["PingStatus"] == "Online"
        ):
            return

        time.sleep(delay_seconds)

    raise Exception(f"Instance {instance_id} not ready in SSM after {max_attempts} attempts.")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # Create EC2 instance with SSM agent
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    instance_name = f"{env_id}-instance"
    image_id = get_latest_ami_id()
    role_name = get_role_name(test_context[ROLE_ARN_KEY])
    instance_profile_name = f"{env_id}-ssm-instance-profile"

    config = {
        "InstanceType": "t4g.micro",
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
        retries=5,
        retry_delay=datetime.timedelta(seconds=15),
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
        task_id="await_run_command", command_id="{{ ti.xcom_pull(task_ids='run_command') }}"
    )
    # [END howto_sensor_run_command]

    # [START howto_operator_get_command_invocation]
    get_command_output = SsmGetCommandInvocationOperator(
        task_id="get_command_output",
        command_id="{{ ti.xcom_pull(task_ids='run_command') }}",
        instance_id=instance_id,
    )
    # [END howto_operator_get_command_invocation]

    # [START howto_operator_ssm_enhanced_async]
    run_command_async = SsmRunCommandOperator(
        task_id="run_command_async",
        document_name="AWS-RunShellScript",
        run_command_kwargs={
            "InstanceIds": [instance_id],
            "Parameters": {"commands": ["echo 'Testing async pattern'", "exit 1"]},
        },
        wait_for_completion=False,
        fail_on_nonzero_exit=False,
    )

    wait_command_async = SsmRunCommandCompletedSensor(
        task_id="wait_command_async",
        command_id="{{ ti.xcom_pull(task_ids='run_command_async') }}",
        fail_on_nonzero_exit=False,
    )
    # [END howto_operator_ssm_enhanced_async]

    # [START howto_operator_ssm_enhanced_sync]
    run_command_sync = SsmRunCommandOperator(
        task_id="run_command_sync",
        document_name="AWS-RunShellScript",
        run_command_kwargs={
            "InstanceIds": [instance_id],
            "Parameters": {"commands": ["echo 'Testing sync pattern'", "exit 2"]},
        },
        wait_for_completion=True,
        fail_on_nonzero_exit=False,
    )
    # [END howto_operator_ssm_enhanced_sync]

    # [START howto_operator_ssm_exit_code_routing]
    get_exit_code_output = SsmGetCommandInvocationOperator(
        task_id="get_exit_code_output",
        command_id="{{ ti.xcom_pull(task_ids='run_command_async') }}",
        instance_id=instance_id,
    )

    @task
    def route_based_on_exit_code(**context):
        output = context["ti"].xcom_pull(task_ids="get_exit_code_output")
        exit_code = output.get("response_code") if output else None
        log.info("Command exit code: %s", exit_code)
        return "handle_exit_code"

    route_task = route_based_on_exit_code()

    @task(task_id="handle_exit_code")
    def handle_exit_code():
        log.info("Handling exit code routing")
        return "exit_code_handled"

    handle_task = handle_exit_code()
    # [END howto_operator_ssm_exit_code_routing]

    # [START howto_operator_ssm_traditional]
    run_command_traditional = SsmRunCommandOperator(
        task_id="run_command_traditional",
        document_name="AWS-RunShellScript",
        run_command_kwargs={
            "InstanceIds": [instance_id],
            "Parameters": {"commands": ["echo 'Testing traditional pattern'", "exit 0"]},
        },
        wait_for_completion=False,
    )

    wait_command_traditional = SsmRunCommandCompletedSensor(
        task_id="wait_command_traditional",
        command_id="{{ ti.xcom_pull(task_ids='run_command_traditional') }}",
    )
    # [END howto_operator_ssm_traditional]

    delete_instance = EC2TerminateInstanceOperator(
        task_id="terminate_instance",
        trigger_rule=TriggerRule.ALL_DONE,
        instance_ids=instance_id,
    )

    chain(
        # TEST SETUP
        test_context,
        image_id,
        role_name,
        create_instance_profile(role_name, instance_profile_name),
        await_instance_profile_exists(instance_profile_name),
        create_instance,
        instance_id,
        run_command_kwargs,
        wait_until_ssm_ready(instance_id),
        # TEST BODY
        run_command,
        await_run_command,
        get_command_output,
    )

    # Exit code handling examples (run in parallel)
    wait_until_ssm_ready(instance_id) >> run_command_async >> wait_command_async >> get_exit_code_output
    get_exit_code_output >> route_task >> handle_task
    wait_until_ssm_ready(instance_id) >> run_command_sync
    wait_until_ssm_ready(instance_id) >> run_command_traditional >> wait_command_traditional

    # TEST TEARDOWN
    [get_command_output, handle_task, run_command_sync, wait_command_traditional] >> delete_instance
    delete_instance >> delete_instance_profile(instance_profile_name, role_name)

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
