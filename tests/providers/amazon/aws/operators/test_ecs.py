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

import sys
import unittest
from copy import deepcopy

import mock
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.ecs import ECSOperator

# fmt: off
RESPONSE_WITHOUT_FAILURES = {
    "failures": [],
    "tasks": [
        {
            "containers": [
                {
                    "containerArn":
                        "arn:aws:ecs:us-east-1:012345678910:container/e1ed7aac-d9b2-4315-8726-d2432bf11868",
                    "lastStatus": "PENDING",
                    "name": "wordpress",
                    "taskArn": "arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55"
                }
            ],
            "desiredStatus": "RUNNING",
            "lastStatus": "PENDING",
            "taskArn": "arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55",
            "taskDefinitionArn": "arn:aws:ecs:us-east-1:012345678910:task-definition/hello_world:11"
        }
    ]
}
# fmt: on


class TestECSOperator(unittest.TestCase):
    @mock.patch('airflow.providers.amazon.aws.operators.ecs.AwsBaseHook')
    def set_up_operator(self, aws_hook_mock, **kwargs):
        self.aws_hook_mock = aws_hook_mock

        self.ecs_operator_args = {
            'task_id': 'task',
            'task_definition': 't',
            'cluster': 'c',
            'overrides': {},
            'aws_conn_id': None,
            'region_name': 'eu-west-1',
            'group': 'group',
            'placement_constraints': [
                {'expression': 'attribute:ecs.instance-type =~ t2.*', 'type': 'memberOf'}
            ],
            'network_configuration': {
                'awsvpcConfiguration': {'securityGroups': ['sg-123abc'], 'subnets': ['subnet-123456ab']}
            },
            'propagate_tags': 'TASK_DEFINITION',
        }
        self.ecs = ECSOperator(**self.ecs_operator_args, **kwargs)
        self.ecs.get_hook()

    def setUp(self):
        self.set_up_operator()  # pylint: disable=no-value-for-parameter

    def test_init(self):
        self.assertEqual(self.ecs.region_name, 'eu-west-1')
        self.assertEqual(self.ecs.task_definition, 't')
        self.assertEqual(self.ecs.aws_conn_id, None)
        self.assertEqual(self.ecs.cluster, 'c')
        self.assertEqual(self.ecs.overrides, {})
        self.ecs.get_hook()
        self.assertEqual(self.ecs.hook, self.aws_hook_mock.return_value)
        self.aws_hook_mock.assert_called_once()

    def test_template_fields_overrides(self):
        self.assertEqual(self.ecs.template_fields, ('overrides',))

    @parameterized.expand(
        [
            ['EC2', None],
            ['FARGATE', None],
            ['EC2', {'testTagKey': 'testTagValue'}],
            ['', {'testTagKey': 'testTagValue'}],
        ]
    )
    @mock.patch.object(ECSOperator, '_wait_for_task_ended')
    @mock.patch.object(ECSOperator, '_check_success_task')
    def test_execute_without_failures(self, launch_type, tags, check_mock, wait_mock):

        self.set_up_operator(launch_type=launch_type, tags=tags)  # pylint: disable=no-value-for-parameter
        client_mock = self.aws_hook_mock.return_value.get_conn.return_value
        client_mock.run_task.return_value = RESPONSE_WITHOUT_FAILURES

        self.ecs.execute(None)

        self.aws_hook_mock.return_value.get_conn.assert_called_once()
        extend_args = {}
        if launch_type:
            extend_args['launchType'] = launch_type
        if launch_type == 'FARGATE':
            extend_args['platformVersion'] = 'LATEST'
        if tags:
            extend_args['tags'] = [{'key': k, 'value': v} for (k, v) in tags.items()]

        client_mock.run_task.assert_called_once_with(
            cluster='c',
            overrides={},
            startedBy=mock.ANY,  # Can by 'airflow' or 'Airflow'
            taskDefinition='t',
            group='group',
            placementConstraints=[{'expression': 'attribute:ecs.instance-type =~ t2.*', 'type': 'memberOf'}],
            networkConfiguration={
                'awsvpcConfiguration': {'securityGroups': ['sg-123abc'], 'subnets': ['subnet-123456ab']}
            },
            propagateTags='TASK_DEFINITION',
            **extend_args,
        )

        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        self.assertEqual(
            self.ecs.arn, 'arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55'
        )

    def test_execute_with_failures(self):
        client_mock = self.aws_hook_mock.return_value.get_conn.return_value
        resp_failures = deepcopy(RESPONSE_WITHOUT_FAILURES)
        resp_failures['failures'].append('dummy error')
        client_mock.run_task.return_value = resp_failures

        with self.assertRaises(AirflowException):
            self.ecs.execute(None)

        self.aws_hook_mock.return_value.get_conn.assert_called_once()
        client_mock.run_task.assert_called_once_with(
            cluster='c',
            launchType='EC2',
            overrides={},
            startedBy=mock.ANY,  # Can by 'airflow' or 'Airflow'
            taskDefinition='t',
            group='group',
            placementConstraints=[{'expression': 'attribute:ecs.instance-type =~ t2.*', 'type': 'memberOf'}],
            networkConfiguration={
                'awsvpcConfiguration': {'securityGroups': ['sg-123abc'], 'subnets': ['subnet-123456ab'],}
            },
            propagateTags='TASK_DEFINITION',
        )

    def test_wait_end_tasks(self):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        self.ecs._wait_for_task_ended()
        client_mock.get_waiter.assert_called_once_with('tasks_stopped')
        client_mock.get_waiter.return_value.wait.assert_called_once_with(cluster='c', tasks=['arn'])
        self.assertEqual(sys.maxsize, client_mock.get_waiter.return_value.config.max_attempts)

    def test_check_success_tasks_raises(self):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED', 'exitCode': 1}]}]
        }
        with self.assertRaises(Exception) as e:
            self.ecs._check_success_task()

        # Ordering of str(dict) is not guaranteed.
        self.assertIn("This task is not in success state ", str(e.exception))
        self.assertIn("'name': 'foo'", str(e.exception))
        self.assertIn("'lastStatus': 'STOPPED'", str(e.exception))
        self.assertIn("'exitCode': 1", str(e.exception))
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_check_success_tasks_raises_pending(self):
        client_mock = mock.Mock()
        self.ecs.client = client_mock
        self.ecs.arn = 'arn'
        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'container-name', 'lastStatus': 'PENDING'}]}]
        }
        with self.assertRaises(Exception) as e:
            self.ecs._check_success_task()
        # Ordering of str(dict) is not guaranteed.
        self.assertIn("This task is still pending ", str(e.exception))
        self.assertIn("'name': 'container-name'", str(e.exception))
        self.assertIn("'lastStatus': 'PENDING'", str(e.exception))
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_check_success_tasks_raises_multiple(self):
        client_mock = mock.Mock()
        self.ecs.client = client_mock
        self.ecs.arn = 'arn'
        client_mock.describe_tasks.return_value = {
            'tasks': [
                {
                    'containers': [
                        {'name': 'foo', 'exitCode': 1},
                        {'name': 'bar', 'lastStatus': 'STOPPED', 'exitCode': 0},
                    ]
                }
            ]
        }
        self.ecs._check_success_task()
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_host_terminated_raises(self):
        client_mock = mock.Mock()
        self.ecs.client = client_mock
        self.ecs.arn = 'arn'
        client_mock.describe_tasks.return_value = {
            'tasks': [
                {
                    'stoppedReason': 'Host EC2 (instance i-1234567890abcdef) terminated.',
                    "containers": [
                        {
                            "containerArn": "arn:aws:ecs:us-east-1:012345678910:container/e1ed7aac-d9b2-4315-8726-d2432bf11868",  # noqa: E501 # pylint: disable=line-too-long
                            "lastStatus": "RUNNING",
                            "name": "wordpress",
                            "taskArn": "arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55",  # noqa: E501 # pylint: disable=line-too-long
                        }
                    ],
                    "desiredStatus": "STOPPED",
                    "lastStatus": "STOPPED",
                    "taskArn": "arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55",  # noqa: E501 # pylint: disable=line-too-long
                    "taskDefinitionArn": "arn:aws:ecs:us-east-1:012345678910:task-definition/hello_world:11",  # noqa: E501 # pylint: disable=line-too-long
                }
            ]
        }

        with self.assertRaises(AirflowException) as e:
            self.ecs._check_success_task()

        self.assertIn("The task was stopped because the host instance terminated:", str(e.exception))
        self.assertIn("Host EC2 (", str(e.exception))
        self.assertIn(") terminated", str(e.exception))
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_check_success_task_not_raises(self):
        client_mock = mock.Mock()
        self.ecs.client = client_mock
        self.ecs.arn = 'arn'
        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'container-name', 'lastStatus': 'STOPPED', 'exitCode': 0}]}]
        }
        self.ecs._check_success_task()
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])
