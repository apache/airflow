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

from datetime import datetime
from operator import itemgetter

import boto3

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2HibernateInstanceOperator,
    EC2RebootInstanceOperator,
    EC2StartInstanceOperator,
    EC2StopInstanceOperator,
    EC2TerminateInstanceOperator,
)
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_ec2"

sys_test_context_task = SystemTestContextBuilder().build()


@task
def get_latest_ami_id():
    """Returns the AMI ID of the most recently-created Amazon Linux image"""

    # Amazon is retiring AL2 in 2023 and replacing it with Amazon Linux 2022.
    # This image prefix should be futureproof, but may need adjusting depending
    # on how they name the new images.  This page should have AL2022 info when
    # it comes available: https://aws.amazon.com/linux/amazon-linux-2022/faqs/
    image_prefix = "Amazon Linux*"
    root_device_name = "/dev/xvda"

    images = boto3.client("ec2").describe_images(
        Filters=[
            {"Name": "description", "Values": [image_prefix]},
            {
                "Name": "architecture",
                "Values": ["x86_64"],
            },  # t3 instances are only compatible with x86 architecture
            {
                "Name": "root-device-type",
                "Values": ["ebs"],
            },  # instances which are capable of hibernation need to use an EBS-backed AMI
            {"Name": "root-device-name", "Values": [root_device_name]},
        ],
        Owners=["amazon"],
    )
    # Sort on CreationDate
    return max(images["Images"], key=itemgetter("CreationDate"))["ImageId"]


@task
def create_key_pair(key_name: str):
    client = boto3.client("ec2")

    key_pair_id = client.create_key_pair(KeyName=key_name)["KeyName"]
    # Creating the key takes a very short but measurable time, preventing race condition:
    client.get_waiter("key_pair_exists").wait(KeyNames=[key_pair_id])

    return key_pair_id


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_key_pair(key_pair_id: str):
    boto3.client("ec2").delete_key_pair(KeyName=key_pair_id)


@task
def parse_response(instance_ids: list):
    return instance_ids[0]


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    instance_name = f"{env_id}-instance"
    key_name = create_key_pair(key_name=f"{env_id}_key_pair")
    image_id = get_latest_ami_id()

    config = {
        "InstanceType": "t3.micro",
        "KeyName": key_name,
        "TagSpecifications": [
            {"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": instance_name}]}
        ],
        # Use IMDSv2 for greater security, see the following doc for more details:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
        "HibernationOptions": {"Configured": True},
        "BlockDeviceMappings": [
            {"DeviceName": "/dev/xvda", "Ebs": {"Encrypted": True, "DeleteOnTermination": True}}
        ],
    }

    # EC2CreateInstanceOperator creates and starts the EC2 instances. To test the EC2StartInstanceOperator,
    # we will stop the instance, then start them again before terminating them.

    # [START howto_operator_ec2_create_instance]
    create_instance = EC2CreateInstanceOperator(
        task_id="create_instance",
        image_id=image_id,
        max_count=1,
        min_count=1,
        config=config,
    )
    # [END howto_operator_ec2_create_instance]
    create_instance.wait_for_completion = True
    instance_id = parse_response(create_instance.output)
    # [START howto_operator_ec2_stop_instance]
    stop_instance = EC2StopInstanceOperator(
        task_id="stop_instance",
        instance_id=instance_id,
    )
    # [END howto_operator_ec2_stop_instance]
    stop_instance.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_ec2_start_instance]
    start_instance = EC2StartInstanceOperator(
        task_id="start_instance",
        instance_id=instance_id,
    )
    # [END howto_operator_ec2_start_instance]

    # [START howto_sensor_ec2_instance_state]
    await_instance = EC2InstanceStateSensor(
        task_id="await_instance",
        instance_id=instance_id,
        target_state="running",
    )
    # [END howto_sensor_ec2_instance_state]

    # [START howto_operator_ec2_reboot_instance]
    reboot_instance = EC2RebootInstanceOperator(
        task_id="reboot_instace",
        instance_ids=instance_id,
    )
    # [END howto_operator_ec2_reboot_instance]
    reboot_instance.wait_for_completion = True

    # [START howto_operator_ec2_hibernate_instance]
    hibernate_instance = EC2HibernateInstanceOperator(
        task_id="hibernate_instace",
        instance_ids=instance_id,
    )
    # [END howto_operator_ec2_hibernate_instance]
    hibernate_instance.wait_for_completion = True
    hibernate_instance.poll_interval = 60
    hibernate_instance.max_attempts = 40

    # [START howto_operator_ec2_terminate_instance]
    terminate_instance = EC2TerminateInstanceOperator(
        task_id="terminate_instance",
        instance_ids=instance_id,
        wait_for_completion=True,
    )
    # [END howto_operator_ec2_terminate_instance]
    terminate_instance.trigger_rule = TriggerRule.ALL_DONE
    chain(
        # TEST SETUP
        test_context,
        key_name,
        image_id,
        # TEST BODY
        create_instance,
        instance_id,
        stop_instance,
        start_instance,
        await_instance,
        reboot_instance,
        hibernate_instance,
        terminate_instance,
        # TEST TEARDOWN
        delete_key_pair(key_name),
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
