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

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator, EC2StopInstanceOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_ec2"

sys_test_context_task = SystemTestContextBuilder().build()


def _get_latest_ami_id():
    """Returns the AMI ID of the most recently-created Amazon Linux image"""

    # Amazon is retiring AL2 in 2023 and replacing it with Amazon Linux 2022.
    # This image prefix should be futureproof, but may need adjusting depending
    # on how they name the new images.  This page should have AL2022 info when
    # it comes available: https://aws.amazon.com/linux/amazon-linux-2022/faqs/
    image_prefix = "Amazon Linux*"

    images = boto3.client("ec2").describe_images(
        Filters=[{"Name": "description", "Values": [image_prefix]}], Owners=["amazon"]
    )
    # Sort on CreationDate
    sorted_images = sorted(images["Images"], key=itemgetter("CreationDate"), reverse=True)
    return sorted_images[0]["ImageId"]


@task
def create_key_pair(key_name: str):
    client = boto3.client("ec2")

    key_pair_id = client.create_key_pair(KeyName=key_name)["KeyName"]
    # Creating the key takes a very short but measurable time, preventing race condition:
    client.get_waiter("key_pair_exists").wait(KeyNames=[key_pair_id])

    return key_pair_id


@task
def create_instance(instance_name: str, key_pair_id: str):
    client = boto3.client("ec2")

    # Create the instance
    instance_id = client.run_instances(
        ImageId=_get_latest_ami_id(),
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName=key_pair_id,
        TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": instance_name}]}],
    )["Instances"][0]["InstanceId"]

    # Wait for it to exist
    waiter = client.get_waiter("instance_exists")
    waiter.wait(InstanceIds=[instance_id])

    return instance_id


@task(trigger_rule=TriggerRule.ALL_DONE)
def terminate_instance(instance: str):
    boto3.client("ec2").terminate_instances(InstanceIds=[instance])


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_key_pair(key_pair_id: str):
    boto3.client("ec2").delete_key_pair(KeyName=key_pair_id)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    key_name = create_key_pair(key_name=f"{env_id}_key_pair")
    instance_id = create_instance(instance_name=f"{env_id}-instance", key_pair_id=key_name)
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

    # [START howto_operator_ec2_stop_instance]
    stop_instance = EC2StopInstanceOperator(
        task_id="stop_instance",
        instance_id=instance_id,
    )
    # [END howto_operator_ec2_stop_instance]
    stop_instance.trigger_rule = TriggerRule.ALL_DONE

    chain(
        # TEST SETUP
        test_context,
        key_name,
        instance_id,
        # TEST BODY
        start_instance,
        await_instance,
        stop_instance,
        # TEST TEARDOWN
        terminate_instance(instance_id),
        delete_key_pair(key_name),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
