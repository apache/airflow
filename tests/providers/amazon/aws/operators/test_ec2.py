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

from moto import mock_ec2

from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2StartInstanceOperator,
    EC2StopInstanceOperator,
    EC2TerminateInstanceOperator,
)


class BaseEc2TestClass:
    @classmethod
    def _get_image_id(cls, hook):
        """Get a valid image id to create an instance."""
        conn = hook.get_conn()
        try:
            ec2_client = conn.meta.client
        except AttributeError:
            ec2_client = conn

        # We need an existing AMI Image ID otherwise `moto` will raise DeprecationWarning.
        images = ec2_client.describe_images()["Images"]
        return images[0]["ImageId"]


class TestEC2CreateInstanceOperator(BaseEc2TestClass):
    def test_init(self):
        ec2_operator = EC2CreateInstanceOperator(
            task_id="test_create_instance",
            image_id="test_image_id",
        )

        assert ec2_operator.task_id == "test_create_instance"
        assert ec2_operator.image_id == "test_image_id"
        assert ec2_operator.max_count == 1
        assert ec2_operator.min_count == 1
        assert ec2_operator.max_attempts == 20
        assert ec2_operator.poll_interval == 20

    @mock_ec2
    def test_create_instance(self):
        ec2_hook = EC2Hook()
        create_instance = EC2CreateInstanceOperator(
            image_id=self._get_image_id(ec2_hook),
            task_id="test_create_instance",
        )
        instance_id = create_instance.execute(None)

        assert ec2_hook.get_instance_state(instance_id=instance_id[0]) == "running"

    @mock_ec2
    def test_create_multiple_instances(self):
        ec2_hook = EC2Hook()
        create_instances = EC2CreateInstanceOperator(
            task_id="test_create_multiple_instances",
            image_id=self._get_image_id(hook=ec2_hook),
            min_count=5,
            max_count=5,
        )
        instance_ids = create_instances.execute(None)
        assert len(instance_ids) == 5

        for id in instance_ids:
            assert ec2_hook.get_instance_state(instance_id=id) == "running"


class TestEC2TerminateInstanceOperator(BaseEc2TestClass):
    def test_init(self):
        ec2_operator = EC2TerminateInstanceOperator(
            task_id="test_terminate_instance",
            instance_ids="test_image_id",
        )

        assert ec2_operator.task_id == "test_terminate_instance"
        assert ec2_operator.max_attempts == 20
        assert ec2_operator.poll_interval == 20

    @mock_ec2
    def test_terminate_instance(self):
        ec2_hook = EC2Hook()

        create_instance = EC2CreateInstanceOperator(
            image_id=self._get_image_id(ec2_hook),
            task_id="test_create_instance",
        )
        instance_id = create_instance.execute(None)

        assert ec2_hook.get_instance_state(instance_id=instance_id[0]) == "running"

        terminate_instance = EC2TerminateInstanceOperator(
            task_id="test_terminate_instance", instance_ids=instance_id
        )
        terminate_instance.execute(None)

        assert ec2_hook.get_instance_state(instance_id=instance_id[0]) == "terminated"

    @mock_ec2
    def test_terminate_multiple_instances(self):
        ec2_hook = EC2Hook()
        create_instances = EC2CreateInstanceOperator(
            task_id="test_create_multiple_instances",
            image_id=self._get_image_id(hook=ec2_hook),
            min_count=5,
            max_count=5,
        )
        instance_ids = create_instances.execute(None)
        assert len(instance_ids) == 5

        for id in instance_ids:
            assert ec2_hook.get_instance_state(instance_id=id) == "running"

        terminate_instance = EC2TerminateInstanceOperator(
            task_id="test_terminate_instance", instance_ids=instance_ids
        )
        terminate_instance.execute(None)
        for id in instance_ids:
            assert ec2_hook.get_instance_state(instance_id=id) == "terminated"


class TestEC2StartInstanceOperator(BaseEc2TestClass):
    def test_init(self):
        ec2_operator = EC2StartInstanceOperator(
            task_id="task_test",
            instance_id="i-123abc",
            aws_conn_id="aws_conn_test",
            region_name="region-test",
            check_interval=3,
        )
        assert ec2_operator.task_id == "task_test"
        assert ec2_operator.instance_id == "i-123abc"
        assert ec2_operator.aws_conn_id == "aws_conn_test"
        assert ec2_operator.region_name == "region-test"
        assert ec2_operator.check_interval == 3

    @mock_ec2
    def test_start_instance(self):
        # create instance
        ec2_hook = EC2Hook()
        create_instance = EC2CreateInstanceOperator(
            image_id=self._get_image_id(ec2_hook),
            task_id="test_create_instance",
        )
        instance_id = create_instance.execute(None)

        # start instance
        start_test = EC2StartInstanceOperator(
            task_id="start_test",
            instance_id=instance_id[0],
        )
        start_test.execute(None)
        # assert instance state is running
        assert ec2_hook.get_instance_state(instance_id=instance_id[0]) == "running"


class TestEC2StopInstanceOperator(BaseEc2TestClass):
    def test_init(self):
        ec2_operator = EC2StopInstanceOperator(
            task_id="task_test",
            instance_id="i-123abc",
            aws_conn_id="aws_conn_test",
            region_name="region-test",
            check_interval=3,
        )
        assert ec2_operator.task_id == "task_test"
        assert ec2_operator.instance_id == "i-123abc"
        assert ec2_operator.aws_conn_id == "aws_conn_test"
        assert ec2_operator.region_name == "region-test"
        assert ec2_operator.check_interval == 3

    @mock_ec2
    def test_stop_instance(self):
        # create instance
        ec2_hook = EC2Hook()
        create_instance = EC2CreateInstanceOperator(
            image_id=self._get_image_id(ec2_hook),
            task_id="test_create_instance",
        )
        instance_id = create_instance.execute(None)

        # stop instance
        stop_test = EC2StopInstanceOperator(
            task_id="stop_test",
            instance_id=instance_id[0],
        )
        stop_test.execute(None)
        # assert instance state is running
        assert ec2_hook.get_instance_state(instance_id=instance_id[0]) == "stopped"
