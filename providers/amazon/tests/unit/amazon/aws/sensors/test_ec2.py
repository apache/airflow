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

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.providers.common.compat.sdk import TaskDeferred


class TestEC2InstanceStateSensor:
    def test_init(self):
        ec2_operator = EC2InstanceStateSensor(
            task_id="task_test",
            target_state="stopped",
            instance_id="i-123abc",
            aws_conn_id="aws_conn_test",
            region_name="region-test",
        )
        assert ec2_operator.task_id == "task_test"
        assert ec2_operator.target_state == "stopped"
        assert ec2_operator.instance_id == "i-123abc"
        assert ec2_operator.aws_conn_id == "aws_conn_test"
        assert ec2_operator.region_name == "region-test"

    def test_init_invalid_target_state(self):
        invalid_target_state = "target_state_test"
        with pytest.raises(ValueError, match=f"Invalid target_state: {invalid_target_state}") as ctx:
            EC2InstanceStateSensor(
                task_id="task_test",
                target_state=invalid_target_state,
                instance_id="i-123abc",
            )
        msg = f"Invalid target_state: {invalid_target_state}"
        assert str(ctx.value) == msg

    @classmethod
    def _create_instance(cls, hook: EC2Hook):
        """Create Instance and return instance id."""
        conn = hook.get_conn()
        try:
            ec2_client = conn.meta.client
        except AttributeError:
            ec2_client = conn

        # We need existed AMI Image ID otherwise `moto` will raise DeprecationWarning.
        images = ec2_client.describe_images()["Images"]
        response = ec2_client.run_instances(MaxCount=1, MinCount=1, ImageId=images[0]["ImageId"])
        return response["Instances"][0]["InstanceId"]

    @mock_aws
    def test_running(self):
        # create instance
        ec2_hook = EC2Hook()
        instance_id = self._create_instance(ec2_hook)
        # stop instance
        ec2_hook.get_instance(instance_id=instance_id).stop()

        # start sensor, waits until ec2 instance state became running
        start_sensor = EC2InstanceStateSensor(
            task_id="start_sensor",
            target_state="running",
            instance_id=instance_id,
        )
        # assert instance state is not running
        assert not start_sensor.poke(None)
        # start instance
        ec2_hook.get_instance(instance_id=instance_id).start()
        # assert instance state is running
        assert start_sensor.poke(None)

    @mock_aws
    def test_stopped(self):
        # create instance
        ec2_hook = EC2Hook()
        instance_id = self._create_instance(ec2_hook)
        # start instance
        ec2_hook.get_instance(instance_id=instance_id).start()

        # stop sensor, waits until ec2 instance state became stopped
        stop_sensor = EC2InstanceStateSensor(
            task_id="stop_sensor",
            target_state="stopped",
            instance_id=instance_id,
        )
        # assert instance state is not stopped
        assert not stop_sensor.poke(None)
        # stop instance
        ec2_hook.get_instance(instance_id=instance_id).stop()
        # assert instance state is stopped
        assert stop_sensor.poke(None)

    @mock_aws
    def test_terminated(self):
        # create instance
        ec2_hook = EC2Hook()
        instance_id = self._create_instance(ec2_hook)
        # start instance
        ec2_hook.get_instance(instance_id=instance_id).start()

        # stop sensor, waits until ec2 instance state became terminated
        stop_sensor = EC2InstanceStateSensor(
            task_id="stop_sensor",
            target_state="terminated",
            instance_id=instance_id,
        )
        # assert instance state is not terminated
        assert not stop_sensor.poke(None)
        # stop instance
        ec2_hook.get_instance(instance_id=instance_id).terminate()
        # assert instance state is terminated
        assert stop_sensor.poke(None)

    @mock_aws
    def test_deferrable(self):
        # create instance
        ec2_hook = EC2Hook()
        instance_id = self._create_instance(ec2_hook)
        # start instance
        ec2_hook.get_instance(instance_id=instance_id).start()

        # stop sensor, waits until ec2 instance state became terminated
        deferrable_sensor = EC2InstanceStateSensor(
            task_id="deferrable_sensor",
            target_state="terminated",
            instance_id=instance_id,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred):
            deferrable_sensor.execute(context=None)
