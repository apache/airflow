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
System test for SSM operators and sensors with exit code handling.

This system test demonstrates the enhanced fail_on_nonzero_exit parameter
for both SsmRunCommandOperator and SsmRunCommandCompletedSensor, enabling
workflow routing based on command exit codes.
"""

from __future__ import annotations

import datetime
import logging
import textwrap
import time

import boto3

from airflow.providers.amazon.aws.operators.ec2 import EC2CreateInstanceOperator, EC2TerminateInstanceOperator
from airflow.providers.amazon.aws.operators.ssm import SsmGetCommandInvocationOperator, SsmRunCommandOperator
from airflow.providers.amazon.aws.sensors.ssm import SsmRunCommandCompletedSensor
from airflow.sdk import DAG, chain, task

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, get_role_name
from system.amazon.aws.utils.ec2 import get_latest_ami_id

DAG_ID = "example_ssm_exit_codes"

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
    tags=["example", "ssm", "exit-codes"],
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
        "TagSpecifications": [
            {"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": instance_name}]}
        ],
        "UserData": USER_DATA,
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

    # ============================================================================
    # PATTERN 1: Enhanced Async Pattern (wait_for_completion=False + sensor)
    # ============================================================================
    # This pattern is ideal when you want non-blocking execution with full
    # async benefits while handling any exit code gracefully.
    # Use case: Long-running commands where you want to free up worker slots
    # and handle non-zero exit codes without task failure.

    # [START howto_operator_ssm_enhanced_async]
    run_command_async = SsmRunCommandOperator(
        task_id="run_command_async",
        document_name="AWS-RunShellScript",
        run_command_kwargs={
            "InstanceIds": [instance_id],
            "Parameters": {
                "commands": [
                    "echo 'Testing enhanced async pattern with exit code 1'",
                    "echo 'This command will exit with code 1'",
                    "echo 'ERROR: Simulated failure' >&2",
                    "exit 1",
                ]
            },
        },
        wait_for_completion=False,
        fail_on_nonzero_exit=False,  # NEW: Won't fail on non-zero exit codes
    )

    wait_command_async = SsmRunCommandCompletedSensor(
        task_id="wait_command_async",
        command_id="{{ ti.xcom_pull(task_ids='run_command_async') }}",
        fail_on_nonzero_exit=False,  # NEW: Won't fail on command failure
        poke_interval=10,
        timeout=300,
    )
    # [END howto_operator_ssm_enhanced_async]

    get_async_output = SsmGetCommandInvocationOperator(
        task_id="get_async_output",
        command_id="{{ ti.xcom_pull(task_ids='run_command_async') }}",
        instance_id=instance_id,
    )

    # ============================================================================
    # PATTERN 2: Enhanced Sync Pattern (wait_for_completion=True)
    # ============================================================================
    # This pattern is ideal when you want synchronous execution with simplified
    # code while handling any exit code gracefully.
    # Use case: Short-running commands where blocking the worker is acceptable
    # and you want to handle non-zero exit codes without task failure.

    # [START howto_operator_ssm_enhanced_sync]
    run_command_sync = SsmRunCommandOperator(
        task_id="run_command_sync",
        document_name="AWS-RunShellScript",
        run_command_kwargs={
            "InstanceIds": [instance_id],
            "Parameters": {
                "commands": [
                    "echo 'Testing enhanced sync pattern with exit code 2'",
                    "echo 'This command will exit with code 2'",
                    "echo 'ERROR: Configuration error' >&2",
                    "exit 2",
                ]
            },
        },
        wait_for_completion=True,
        fail_on_nonzero_exit=False,  # NEW: Won't fail on non-zero exit codes
    )
    # [END howto_operator_ssm_enhanced_sync]

    get_sync_output = SsmGetCommandInvocationOperator(
        task_id="get_sync_output",
        command_id="{{ ti.xcom_pull(task_ids='run_command_sync') }}",
        instance_id=instance_id,
    )

    # ============================================================================
    # PATTERN 3: Exit Code Routing with BranchPythonOperator
    # ============================================================================
    # This pattern demonstrates how to route workflows based on command exit codes.
    # Use case: Complex workflows where different exit codes trigger different
    # downstream processing paths.

    # [START howto_operator_ssm_exit_code_routing]
    @task
    def route_based_on_exit_code(**context):
        """
        Route workflow based on command exit code.

        This demonstrates workflow routing where different exit codes
        trigger different downstream tasks.
        """
        # Get the command output from the async pattern
        async_output = context["ti"].xcom_pull(task_ids="get_async_output")
        sync_output = context["ti"].xcom_pull(task_ids="get_sync_output")

        async_exit_code = async_output.get("response_code") if async_output else None
        sync_exit_code = sync_output.get("response_code") if sync_output else None

        log.info("Async command exit code: %s", async_exit_code)
        log.info("Sync command exit code: %s", sync_exit_code)

        # Route based on exit codes
        # In a real scenario, you might have different downstream tasks
        # for different exit codes (e.g., retry, alert, cleanup, etc.)
        if async_exit_code == 1 and sync_exit_code == 2:
            log.info("Both commands completed with expected exit codes")
            return "handle_expected_failures"
        log.warning("Unexpected exit codes detected")
        return "handle_unexpected_results"

    route_task = route_based_on_exit_code()

    @task(task_id="handle_expected_failures")
    def handle_expected_failures():
        """Handle expected failure scenarios."""
        log.info("Handling expected failures - both commands exited as expected")
        log.info("In a real workflow, this might trigger:")
        log.info("  - Retry logic with different parameters")
        log.info("  - Alert notifications to operations team")
        log.info("  - Cleanup or rollback operations")
        log.info("  - Alternative processing paths")
        return "expected_failures_handled"

    @task(task_id="handle_unexpected_results")
    def handle_unexpected_results():
        """Handle unexpected results."""
        log.warning("Handling unexpected results")
        log.warning("In a real workflow, this might trigger:")
        log.warning("  - Emergency alerts")
        log.warning("  - Diagnostic data collection")
        log.warning("  - Escalation to on-call team")
        return "unexpected_results_handled"

    expected_task = handle_expected_failures()
    unexpected_task = handle_unexpected_results()
    # [END howto_operator_ssm_exit_code_routing]

    # ============================================================================
    # PATTERN 4: Traditional Pattern (for comparison)
    # ============================================================================
    # This shows the traditional behavior where non-zero exit codes cause
    # task failure. This is the default behavior when fail_on_nonzero_exit
    # is not specified or set to True.

    # [START howto_operator_ssm_traditional]
    run_command_traditional = SsmRunCommandOperator(
        task_id="run_command_traditional",
        document_name="AWS-RunShellScript",
        run_command_kwargs={
            "InstanceIds": [instance_id],
            "Parameters": {
                "commands": [
                    "echo 'Testing traditional pattern with exit code 0'",
                    "echo 'This command will succeed'",
                    "exit 0",
                ]
            },
        },
        wait_for_completion=False,
        # fail_on_nonzero_exit=True (default) - will fail on non-zero exit codes
    )

    wait_command_traditional = SsmRunCommandCompletedSensor(
        task_id="wait_command_traditional",
        command_id="{{ ti.xcom_pull(task_ids='run_command_traditional') }}",
        # fail_on_nonzero_exit=True (default) - will fail on command failure
        poke_interval=10,
        timeout=300,
    )
    # [END howto_operator_ssm_traditional]

    get_traditional_output = SsmGetCommandInvocationOperator(
        task_id="get_traditional_output",
        command_id="{{ ti.xcom_pull(task_ids='run_command_traditional') }}",
        instance_id=instance_id,
    )

    @task
    def verify_results(**context):
        """Verify all patterns completed successfully."""
        async_output = context["ti"].xcom_pull(task_ids="get_async_output")
        sync_output = context["ti"].xcom_pull(task_ids="get_sync_output")
        traditional_output = context["ti"].xcom_pull(task_ids="get_traditional_output")

        results = {
            "async_pattern": {
                "exit_code": async_output.get("response_code") if async_output else None,
                "status": async_output.get("status") if async_output else None,
                "expected": 1,
            },
            "sync_pattern": {
                "exit_code": sync_output.get("response_code") if sync_output else None,
                "status": sync_output.get("status") if sync_output else None,
                "expected": 2,
            },
            "traditional_pattern": {
                "exit_code": traditional_output.get("response_code") if traditional_output else None,
                "status": traditional_output.get("status") if traditional_output else None,
                "expected": 0,
            },
        }

        log.info("=" * 80)
        log.info("SSM EXIT CODE HANDLING SYSTEM TEST RESULTS")
        log.info("=" * 80)

        all_passed = True
        for pattern_name, result in results.items():
            passed = result["exit_code"] == result["expected"]
            all_passed = all_passed and passed
            status_text = "PASS" if passed else "FAIL"

            log.info("[%s] %s:", status_text, pattern_name)
            log.info("   Expected Exit Code: %s", result["expected"])
            log.info("   Actual Exit Code: %s", result["exit_code"])
            log.info("   SSM Status: %s", result["status"])

        log.info("=" * 80)
        if all_passed:
            log.info("ALL TESTS PASSED: Enhanced exit code handling is working correctly!")
        else:
            log.error("SOME TESTS FAILED: Check the results above")
        log.info("=" * 80)

        return results

    verify_task = verify_results()

    delete_instance = EC2TerminateInstanceOperator(
        task_id="terminate_instance",
        trigger_rule=TriggerRule.ALL_DONE,
        instance_ids=instance_id,
    )

    # Set up task dependencies
    chain(
        # TEST SETUP
        test_context,
        image_id,
        role_name,
        create_instance_profile(role_name, instance_profile_name),
        await_instance_profile_exists(instance_profile_name),
        create_instance,
        instance_id,
        wait_until_ssm_ready(instance_id),
        # PATTERN 1: Enhanced Async
        run_command_async,
        wait_command_async,
        get_async_output,
    )

    # PATTERN 2: Enhanced Sync (parallel with Pattern 1)
    wait_until_ssm_ready(instance_id) >> run_command_sync >> get_sync_output

    # PATTERN 3: Exit Code Routing
    [get_async_output, get_sync_output] >> route_task >> [expected_task, unexpected_task]

    # PATTERN 4: Traditional (parallel with Patterns 1 & 2)
    (
        wait_until_ssm_ready(instance_id)
        >> run_command_traditional
        >> wait_command_traditional
        >> get_traditional_output
    )

    # Verification and cleanup
    (
        [expected_task, unexpected_task, get_traditional_output]
        >> verify_task
        >> delete_instance
        >> delete_instance_profile(instance_profile_name, role_name)
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
