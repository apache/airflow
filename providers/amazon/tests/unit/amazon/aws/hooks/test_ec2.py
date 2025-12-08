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
from airflow.providers.common.compat.sdk import AirflowException


class TestEC2Hook:
    def test_init(self):
        ec2_hook = EC2Hook(
            aws_conn_id="aws_conn_test",
            region_name="region-test",
        )
        # We're mocking all actual AWS calls and don't need a connection. This
        # avoids an Airflow warning about connection cannot be found.
        ec2_hook.get_connection = lambda _: None
        assert ec2_hook.aws_conn_id == "aws_conn_test"
        assert ec2_hook.region_name == "region-test"

    @classmethod
    def _create_instances(cls, hook: EC2Hook, max_count=1, min_count=1):
        """Create Instances and return all instance ids."""
        conn = hook.get_conn()
        try:
            ec2_client = conn.meta.client
        except AttributeError:
            ec2_client = conn

        # We need existed AMI Image ID otherwise `moto` will raise DeprecationWarning.
        images = ec2_client.describe_images()["Images"]
        response = ec2_client.run_instances(
            MaxCount=max_count, MinCount=min_count, ImageId=images[0]["ImageId"]
        )
        return [instance["InstanceId"] for instance in response["Instances"]]

    @classmethod
    def _create_instance(cls, hook: EC2Hook):
        """Create Instance and return instance id."""
        return cls._create_instances(hook)[0]

    @mock_aws
    def test_get_conn_returns_boto3_resource(self):
        ec2_hook = EC2Hook()
        instances = list(ec2_hook.conn.instances.all())
        assert instances is not None

    @mock_aws
    def test_client_type_get_conn_returns_boto3_resource(self):
        ec2_hook = EC2Hook(api_type="client_type")
        instances = list(ec2_hook.get_instances())
        assert instances is not None

    @mock_aws
    def test_get_instance(self):
        ec2_hook = EC2Hook()
        created_instance_id = self._create_instance(ec2_hook)
        # test get_instance method
        existing_instance = ec2_hook.get_instance(instance_id=created_instance_id)
        assert created_instance_id == existing_instance.instance_id

    @mock_aws
    def test_get_instance_client_type(self):
        ec2_hook = EC2Hook(api_type="client_type")
        created_instance_id = self._create_instance(ec2_hook)
        # test get_instance method
        existing_instance = ec2_hook.get_instance(instance_id=created_instance_id)
        assert created_instance_id == existing_instance["InstanceId"]

    @mock_aws
    def test_get_instance_state(self):
        ec2_hook = EC2Hook()
        created_instance_id = self._create_instance(ec2_hook)
        all_instances = list(ec2_hook.conn.instances.all())
        created_instance_state = all_instances[0].state["Name"]
        # test get_instance_state method
        existing_instance_state = ec2_hook.get_instance_state(instance_id=created_instance_id)
        assert created_instance_state == existing_instance_state

    @mock_aws
    def test_client_type_get_instance_state(self):
        ec2_hook = EC2Hook(api_type="client_type")
        created_instance_id = self._create_instance(ec2_hook)
        all_instances = ec2_hook.get_instances()
        created_instance_state = all_instances[0]["State"]["Name"]

        existing_instance_state = ec2_hook.get_instance_state(instance_id=created_instance_id)
        assert created_instance_state == existing_instance_state

    @mock_aws
    def test_client_type_start_instances(self):
        ec2_hook = EC2Hook(api_type="client_type")
        created_instance_id = self._create_instance(ec2_hook)
        response = ec2_hook.start_instances(instance_ids=[created_instance_id])

        assert response["StartingInstances"][0]["InstanceId"] == created_instance_id
        assert ec2_hook.get_instance_state(created_instance_id) == "running"

    @mock_aws
    def test_client_type_stop_instances(self):
        ec2_hook = EC2Hook(api_type="client_type")
        created_instance_id = self._create_instance(ec2_hook)
        response = ec2_hook.stop_instances(instance_ids=[created_instance_id])

        assert response["StoppingInstances"][0]["InstanceId"] == created_instance_id
        assert ec2_hook.get_instance_state(created_instance_id) == "stopped"

    @mock_aws
    def test_client_type_terminate_instances(self):
        ec2_hook = EC2Hook(api_type="client_type")
        created_instance_id = self._create_instance(ec2_hook)
        response = ec2_hook.terminate_instances(instance_ids=[created_instance_id])

        assert response["TerminatingInstances"][0]["InstanceId"] == created_instance_id
        assert ec2_hook.get_instance_state(created_instance_id) == "terminated"

    @mock_aws
    def test_client_type_describe_instances(self):
        ec2_hook = EC2Hook(api_type="client_type")
        created_instance_id = self._create_instance(ec2_hook)

        # Without filter
        response = ec2_hook.describe_instances(instance_ids=[created_instance_id])

        assert response["Reservations"][0]["Instances"][0]["InstanceId"] == created_instance_id
        assert response["Reservations"][0]["Instances"][0]["State"]["Name"] == "running"

        # With valid filter
        response = ec2_hook.describe_instances(
            filters=[{"Name": "instance-id", "Values": [created_instance_id]}]
        )

        assert len(response["Reservations"]) == 1
        assert response["Reservations"][0]["Instances"][0]["InstanceId"] == created_instance_id
        assert response["Reservations"][0]["Instances"][0]["State"]["Name"] == "running"

        # With invalid filter
        response = ec2_hook.describe_instances(
            filters=[{"Name": "instance-id", "Values": ["invalid_instance_id"]}]
        )

        assert len(response["Reservations"]) == 0

    @mock_aws
    def test_client_type_get_instances(self):
        ec2_hook = EC2Hook(api_type="client_type")
        created_instances = self._create_instances(ec2_hook, max_count=2, min_count=2)
        created_instance_id_1, created_instance_id_2 = created_instances

        # Without filter
        response = ec2_hook.get_instances(instance_ids=[created_instance_id_1, created_instance_id_2])

        assert response[0]["InstanceId"] == created_instance_id_1
        assert response[1]["InstanceId"] == created_instance_id_2

        # With valid filter
        response = ec2_hook.get_instances(
            filters=[{"Name": "instance-id", "Values": [created_instance_id_1, created_instance_id_2]}]
        )

        assert len(response) == 2
        assert response[0]["InstanceId"] == created_instance_id_1
        assert response[1]["InstanceId"] == created_instance_id_2

        # With filter and instance ids
        response = ec2_hook.get_instances(
            filters=[{"Name": "instance-id", "Values": [created_instance_id_1]}],
            instance_ids=[created_instance_id_1, created_instance_id_2],
        )

        assert len(response) == 1
        assert response[0]["InstanceId"] == created_instance_id_1

        # With invalid filter
        response = ec2_hook.get_instances(
            filters=[{"Name": "instance-id", "Values": ["invalid_instance_id"]}]
        )

        assert len(response) == 0

    @mock_aws
    def test_client_type_get_instance_ids(self):
        ec2_hook = EC2Hook(api_type="client_type")
        created_instances = self._create_instances(ec2_hook, max_count=2, min_count=2)
        created_instance_id_1, created_instance_id_2 = created_instances

        # Without filter
        response = ec2_hook.get_instance_ids()

        assert len(response) == 2
        assert response[0] == created_instance_id_1
        assert response[1] == created_instance_id_2

        # With valid filter
        response = ec2_hook.get_instance_ids(filters=[{"Name": "instance-type", "Values": ["m1.small"]}])

        assert len(response) == 2
        assert response[0] == created_instance_id_1
        assert response[1] == created_instance_id_2

        # With invalid filter
        response = ec2_hook.get_instance_ids(
            filters=[{"Name": "instance-type", "Values": ["invalid_instance_type"]}]
        )

        assert len(response) == 0

    @mock_aws
    def test_decorator_only_client_type(self):
        ec2_hook = EC2Hook()

        # Try calling a method which is only supported by client_type API
        with pytest.raises(AirflowException):
            ec2_hook.get_instances()

        # Explicitly provide resource_type as api_type
        ec2_hook = EC2Hook(api_type="resource_type")

        # Try calling a method which is only supported by client_type API
        with pytest.raises(AirflowException):
            ec2_hook.describe_instances()
