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
import unittest

import boto3
from mock import MagicMock, patch

from airflow.providers.amazon.aws.sensors.cloud_formation import (
    CloudFormationCreateStackSensor, CloudFormationDeleteStackSensor,
)

try:
    from moto import mock_cloudformation
except ImportError:
    mock_cloudformation = None


@unittest.skipIf(mock_cloudformation is None,
                 "Skipping test because moto.mock_cloudformation is not available")
class TestCloudFormationCreateStackSensor(unittest.TestCase):
    task_id = 'test_cloudformation_cluster_create_sensor'

    @mock_cloudformation
    def setUp(self):
        self.client = boto3.client('cloudformation', region_name='us-east-1')

        self.cloudformation_client_mock = MagicMock()

        cloudformation_session_mock = MagicMock()
        cloudformation_session_mock.client.return_value = self.cloudformation_client_mock

        self.boto3_session_mock = MagicMock(return_value=cloudformation_session_mock)

    @mock_cloudformation
    def test_poke(self):
        stack_name = 'foobar'
        self.client.create_stack(StackName=stack_name, TemplateBody='{"Resources": {}}')
        op = CloudFormationCreateStackSensor(task_id='task', stack_name='foobar')
        self.assertTrue(op.poke({}))

    def test_poke_false(self):
        with patch('boto3.session.Session', self.boto3_session_mock):
            self.cloudformation_client_mock.describe_stacks.return_value = {
                'Stacks': [{'StackStatus': 'CREATE_IN_PROGRESS'}]
            }
            op = CloudFormationCreateStackSensor(task_id='task', stack_name='foo')
            self.assertFalse(op.poke({}))

    def test_poke_stack_in_unsuccessful_state(self):
        with patch('boto3.session.Session', self.boto3_session_mock):
            self.cloudformation_client_mock.describe_stacks.return_value = {
                'Stacks': [{'StackStatus': 'bar'}]
            }
            with self.assertRaises(ValueError) as error:
                op = CloudFormationCreateStackSensor(task_id='task', stack_name='foo')
                op.poke({})

            self.assertEqual('Stack foo in bad state: bar', str(error.exception))


@unittest.skipIf(mock_cloudformation is None,
                 "Skipping test because moto.mock_cloudformation is not available")
class TestCloudFormationDeleteStackSensor(unittest.TestCase):
    task_id = 'test_cloudformation_cluster_delete_sensor'

    @mock_cloudformation
    def setUp(self):
        self.client = boto3.client('cloudformation', region_name='us-east-1')

        self.cloudformation_client_mock = MagicMock()

        cloudformation_session_mock = MagicMock()
        cloudformation_session_mock.client.return_value = self.cloudformation_client_mock

        self.boto3_session_mock = MagicMock(return_value=cloudformation_session_mock)

    @mock_cloudformation
    def test_poke(self):
        stack_name = 'foobar'
        self.client.create_stack(StackName=stack_name, TemplateBody='{"Resources": {}}')
        self.client.delete_stack(StackName=stack_name)
        op = CloudFormationDeleteStackSensor(task_id='task', stack_name=stack_name)
        self.assertTrue(op.poke({}))

    def test_poke_false(self):
        with patch('boto3.session.Session', self.boto3_session_mock):
            self.cloudformation_client_mock.describe_stacks.return_value = {
                'Stacks': [{'StackStatus': 'DELETE_IN_PROGRESS'}]
            }
            op = CloudFormationDeleteStackSensor(task_id='task', stack_name='foo')
            self.assertFalse(op.poke({}))

    def test_poke_stack_in_unsuccessful_state(self):
        with patch('boto3.session.Session', self.boto3_session_mock):
            self.cloudformation_client_mock.describe_stacks.return_value = {
                'Stacks': [{'StackStatus': 'bar'}]
            }
            with self.assertRaises(ValueError) as error:
                op = CloudFormationDeleteStackSensor(task_id='task', stack_name='foo')
                op.poke({})

            self.assertEqual('Stack foo in bad state: bar', str(error.exception))

    @mock_cloudformation
    def test_poke_stack_does_not_exist(self):
        op = CloudFormationDeleteStackSensor(task_id='task', stack_name='foo')
        self.assertTrue(op.poke({}))


if __name__ == '__main__':
    unittest.main()
