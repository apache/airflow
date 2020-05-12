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
#

import unittest

from moto import mock_ec2

from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook


class TestEC2Hook(unittest.TestCase):

    def test_init(self):
        ec2_hook = EC2Hook(
            aws_conn_id="aws_conn_test",
            region_name="region-test",
        )
        self.assertEqual(ec2_hook.aws_conn_id, "aws_conn_test")
        self.assertEqual(ec2_hook.region_name, "region-test")

    @mock_ec2
    def test_get_conn_returns_boto3_resource(self):
        ec2_hook = EC2Hook()
        instances = list(ec2_hook.conn.instances.all())
        self.assertIsNotNone(instances)

    @mock_ec2
    def test_get_instance(self):
        ec2_hook = EC2Hook()
        created_instances = ec2_hook.conn.create_instances(
            MaxCount=1,
            MinCount=1,
        )
        created_instance_id = created_instances[0].instance_id
        # test get_instance method
        existing_instance = ec2_hook.get_instance(
            instance_id=created_instance_id
        )
        self.assertEqual(created_instance_id, existing_instance.instance_id)

    @mock_ec2
    def test_get_instance_state(self):
        ec2_hook = EC2Hook()
        created_instances = ec2_hook.conn.create_instances(
            MaxCount=1,
            MinCount=1,
        )
        created_instance_id = created_instances[0].instance_id
        all_instances = list(ec2_hook.conn.instances.all())
        created_instance_state = all_instances[0].state["Name"]
        # test get_instance_state method
        existing_instance_state = ec2_hook.get_instance_state(
            instance_id=created_instance_id
        )
        self.assertEqual(created_instance_state, existing_instance_state)
