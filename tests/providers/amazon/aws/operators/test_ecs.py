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
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.exceptions import ECSOperatorError
from airflow.providers.amazon.aws.operators.ecs import ECSOperator, should_retry

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
            'placement_strategy': [{'field': 'memory', 'type': 'binpack'}],
            'network_configuration': {
                'awsvpcConfiguration': {'securityGroups': ['sg-123abc'], 'subnets': ['subnet-123456ab']}
            },
            'propagate_tags': 'TASK_DEFINITION',
        }
        self.ecs = ECSOperator(**self.ecs_operator_args, **kwargs)
        self.ecs.get_hook()

    def setUp(self):
        self.set_up_operator()
        self.mock_context = mock.MagicMock()

    def test_init(self):
        assert self.ecs.region_name == 'eu-west-1'
        assert self.ecs.task_definition == 't'
        assert self.ecs.aws_conn_id is None
        assert self.ecs.cluster == 'c'
        assert self.ecs.overrides == {}
        self.ecs.get_hook()
        assert self.ecs.hook == self.aws_hook_mock.return_value
        self.aws_hook_mock.assert_called_once()

    def test_template_fields_overrides(self):
        assert self.ecs.template_fields == ('overrides',)

    @parameterized.expand(
        [
            ['EC2', None, None, {'launchType': 'EC2'}],
            ['FARGATE', None, None, {'launchType': 'FARGATE', 'platformVersion': 'LATEST'}],
            [
                'EC2',
                None,
                {'testTagKey': 'testTagValue'},
                {'launchType': 'EC2', 'tags': [{'key': 'testTagKey', 'value': 'testTagValue'}]},
            ],
            [
                '',
                None,
                {'testTagKey': 'testTagValue'},
                {'tags': [{'key': 'testTagKey', 'value': 'testTagValue'}]},
            ],
            [
                None,
                {'capacityProvider': 'FARGATE_SPOT'},
                None,
                {
                    'capacityProviderStrategy': {'capacityProvider': 'FARGATE_SPOT'},
                    'platformVersion': 'LATEST',
                },
            ],
            [
                'FARGATE',
                {'capacityProvider': 'FARGATE_SPOT', 'weight': 123, 'base': 123},
                None,
                {
                    'capacityProviderStrategy': {
                        'capacityProvider': 'FARGATE_SPOT',
                        'weight': 123,
                        'base': 123,
                    },
                    'platformVersion': 'LATEST',
                },
            ],
            [
                'EC2',
                {'capacityProvider': 'FARGATE_SPOT'},
                None,
                {
                    'capacityProviderStrategy': {'capacityProvider': 'FARGATE_SPOT'},
                    'platformVersion': 'LATEST',
                },
            ],
        ]
    )
    @mock.patch.object(ECSOperator, '_wait_for_task_ended')
    @mock.patch.object(ECSOperator, '_check_success_task')
    def test_execute_without_failures(
        self, launch_type, capacity_provider_strategy, tags, expected_args, check_mock, wait_mock
    ):

        self.set_up_operator(
            launch_type=launch_type, capacity_provider_strategy=capacity_provider_strategy, tags=tags
        )
        client_mock = self.aws_hook_mock.return_value.get_conn.return_value
        client_mock.run_task.return_value = RESPONSE_WITHOUT_FAILURES

        self.ecs.execute(None)

        self.aws_hook_mock.return_value.get_conn.assert_called_once()

        client_mock.run_task.assert_called_once_with(
            cluster='c',
            overrides={},
            startedBy=mock.ANY,  # Can by 'airflow' or 'Airflow'
            taskDefinition='t',
            group='group',
            placementConstraints=[{'expression': 'attribute:ecs.instance-type =~ t2.*', 'type': 'memberOf'}],
            placementStrategy=[{'field': 'memory', 'type': 'binpack'}],
            networkConfiguration={
                'awsvpcConfiguration': {'securityGroups': ['sg-123abc'], 'subnets': ['subnet-123456ab']}
            },
            propagateTags='TASK_DEFINITION',
            **expected_args,
        )

        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        assert self.ecs.arn == 'arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55'

    def test_execute_with_failures(self):
        client_mock = self.aws_hook_mock.return_value.get_conn.return_value
        resp_failures = deepcopy(RESPONSE_WITHOUT_FAILURES)
        resp_failures['failures'].append('dummy error')
        client_mock.run_task.return_value = resp_failures

        with pytest.raises(ECSOperatorError):
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
            placementStrategy=[{'field': 'memory', 'type': 'binpack'}],
            networkConfiguration={
                'awsvpcConfiguration': {
                    'securityGroups': ['sg-123abc'],
                    'subnets': ['subnet-123456ab'],
                }
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
        assert sys.maxsize == client_mock.get_waiter.return_value.config.max_attempts

    @mock.patch.object(
        ECSOperator, '_cloudwatch_log_events', return_value=({"message": str(i)} for i in range(10))
    )
    def test_last_log_messages(self, mock_cloudwatch_log_events):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        assert self.ecs._last_log_messages(5) == ["5", "6", "7", "8", "9"]

    @mock.patch.object(ECSOperator, '_cloudwatch_log_events', return_value=())
    def test_last_log_messages_empty(self, mock_cloudwatch_log_events):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        assert self.ecs._last_log_messages(10) == []

    @mock.patch.object(ECSOperator, '_last_log_messages', return_value=["1", "2", "3", "4", "5"])
    def test_check_success_tasks_raises_cloudwatch_logs(self, mock_last_log_messages):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED', 'exitCode': 1}]}]
        }
        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == (
            "This task is not in success state - last 10 logs from Cloudwatch:\n1\n2\n3\n4\n5"
        )
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    @mock.patch.object(ECSOperator, '_last_log_messages', return_value=[])
    def test_check_success_tasks_raises_cloudwatch_logs_empty(self, mock_last_log_messages):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED', 'exitCode': 1}]}]
        }
        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == "This task is not in success state - last 10 logs from Cloudwatch:\n"
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_check_success_tasks_raises_pending(self):
        client_mock = mock.Mock()
        self.ecs.client = client_mock
        self.ecs.arn = 'arn'
        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'container-name', 'lastStatus': 'PENDING'}]}]
        }
        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()
        # Ordering of str(dict) is not guaranteed.
        assert "This task is still pending " in str(ctx.value)
        assert "'name': 'container-name'" in str(ctx.value)
        assert "'lastStatus': 'PENDING'" in str(ctx.value)
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
                            "containerArn": "arn:aws:ecs:us-east-1:012345678910:container/e1ed7aac-d9b2-4315-8726-d2432bf11868",  # noqa: E501
                            "lastStatus": "RUNNING",
                            "name": "wordpress",
                            "taskArn": "arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55",  # noqa: E501
                        }
                    ],
                    "desiredStatus": "STOPPED",
                    "lastStatus": "STOPPED",
                    "taskArn": "arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55",  # noqa: E501
                    "taskDefinitionArn": "arn:aws:ecs:us-east-1:012345678910:task-definition/hello_world:11",  # noqa: E501
                }
            ]
        }

        with pytest.raises(AirflowException) as ctx:
            self.ecs._check_success_task()

        assert "The task was stopped because the host instance terminated:" in str(ctx.value)
        assert "Host EC2 (" in str(ctx.value)
        assert ") terminated" in str(ctx.value)
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

    @parameterized.expand(
        [
            ['EC2', None],
            ['FARGATE', None],
            ['EC2', {'testTagKey': 'testTagValue'}],
            ['', {'testTagKey': 'testTagValue'}],
        ]
    )
    @mock.patch.object(ECSOperator, "_xcom_del")
    @mock.patch.object(
        ECSOperator,
        "xcom_pull",
        return_value="arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55",
    )
    @mock.patch.object(ECSOperator, '_wait_for_task_ended')
    @mock.patch.object(ECSOperator, '_check_success_task')
    @mock.patch.object(ECSOperator, '_start_task')
    def test_reattach_successful(
        self, launch_type, tags, start_mock, check_mock, wait_mock, xcom_pull_mock, xcom_del_mock
    ):

        self.set_up_operator(launch_type=launch_type, tags=tags)  # pylint: disable=no-value-for-parameter
        client_mock = self.aws_hook_mock.return_value.get_conn.return_value
        client_mock.describe_task_definition.return_value = {'taskDefinition': {'family': 'f'}}
        client_mock.list_tasks.return_value = {
            'taskArns': [
                'arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b54',
                'arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55',
            ]
        }

        self.ecs.reattach = True
        self.ecs.execute(self.mock_context)

        self.aws_hook_mock.return_value.get_conn.assert_called_once()
        extend_args = {}
        if launch_type:
            extend_args['launchType'] = launch_type
        if launch_type == 'FARGATE':
            extend_args['platformVersion'] = 'LATEST'
        if tags:
            extend_args['tags'] = [{'key': k, 'value': v} for (k, v) in tags.items()]

        client_mock.describe_task_definition.assert_called_once_with(taskDefinition='t')

        client_mock.list_tasks.assert_called_once_with(cluster='c', desiredStatus='RUNNING', family='f')

        start_mock.assert_not_called()
        xcom_pull_mock.assert_called_once_with(
            self.mock_context,
            key=self.ecs.REATTACH_XCOM_KEY,
            task_ids=self.ecs.REATTACH_XCOM_TASK_ID_TEMPLATE.format(task_id=self.ecs.task_id),
        )
        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        xcom_del_mock.assert_called_once()
        assert self.ecs.arn == 'arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55'

    @parameterized.expand(
        [
            ['EC2', None],
            ['FARGATE', None],
            ['EC2', {'testTagKey': 'testTagValue'}],
            ['', {'testTagKey': 'testTagValue'}],
        ]
    )
    @mock.patch.object(ECSOperator, '_xcom_del')
    @mock.patch.object(ECSOperator, '_xcom_set')
    @mock.patch.object(ECSOperator, '_try_reattach_task')
    @mock.patch.object(ECSOperator, '_wait_for_task_ended')
    @mock.patch.object(ECSOperator, '_check_success_task')
    def test_reattach_save_task_arn_xcom(
        self, launch_type, tags, check_mock, wait_mock, reattach_mock, xcom_set_mock, xcom_del_mock
    ):

        self.set_up_operator(launch_type=launch_type, tags=tags)  # pylint: disable=no-value-for-parameter
        client_mock = self.aws_hook_mock.return_value.get_conn.return_value
        client_mock.describe_task_definition.return_value = {'taskDefinition': {'family': 'f'}}
        client_mock.list_tasks.return_value = {'taskArns': []}
        client_mock.run_task.return_value = RESPONSE_WITHOUT_FAILURES

        self.ecs.reattach = True
        self.ecs.execute(self.mock_context)

        self.aws_hook_mock.return_value.get_conn.assert_called_once()
        extend_args = {}
        if launch_type:
            extend_args['launchType'] = launch_type
        if launch_type == 'FARGATE':
            extend_args['platformVersion'] = 'LATEST'
        if tags:
            extend_args['tags'] = [{'key': k, 'value': v} for (k, v) in tags.items()]

        reattach_mock.assert_called_once()
        client_mock.run_task.assert_called_once()
        xcom_set_mock.assert_called_once_with(
            self.mock_context,
            key=self.ecs.REATTACH_XCOM_KEY,
            task_id=self.ecs.REATTACH_XCOM_TASK_ID_TEMPLATE.format(task_id=self.ecs.task_id),
            value="arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55",
        )
        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        xcom_del_mock.assert_called_once()
        assert self.ecs.arn == 'arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55'

    @mock.patch.object(ECSOperator, '_last_log_messages', return_value=["Log output"])
    def test_execute_xcom_with_log(self, mock_cloudwatch_log_message):
        self.ecs.do_xcom_push = True
        assert self.ecs.execute(None) == "Log output"

    @mock.patch.object(ECSOperator, '_last_log_messages', return_value=[])
    def test_execute_xcom_with_no_log(self, mock_cloudwatch_log_message):
        self.ecs.do_xcom_push = True
        assert self.ecs.execute(None) is None

    @mock.patch.object(ECSOperator, '_last_log_messages', return_value=["Log output"])
    def test_execute_xcom_disabled(self, mock_cloudwatch_log_message):
        self.ecs.do_xcom_push = False
        assert self.ecs.execute(None) is None


class TestShouldRetry(unittest.TestCase):
    def test_return_true_on_valid_reason(self):
        self.assertTrue(should_retry(ECSOperatorError([{'reason': 'RESOURCE:MEMORY'}], 'Foo')))

    def test_return_false_on_invalid_reason(self):
        self.assertFalse(should_retry(ECSOperatorError([{'reason': 'CLUSTER_NOT_FOUND'}], 'Foo')))
