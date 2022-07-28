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
from datetime import timedelta
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart
from airflow.providers.amazon.aws.operators.ecs import (
    EcsOperator,
    EcsTaskLogFetcher,
    should_retry,
    should_retry_eni,
)

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


class TestEcsOperator(unittest.TestCase):
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
        self.ecs = EcsOperator(**self.ecs_operator_args, **kwargs)
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
        assert self.ecs.template_fields == (
            'cluster',
            'task_definition',
            'overrides',
            'network_configuration',
        )

    @parameterized.expand(
        [
            [
                'EC2',
                None,
                None,
                None,
                {'launchType': 'EC2'},
            ],
            [
                'EXTERNAL',
                None,
                None,
                None,
                {'launchType': 'EXTERNAL'},
            ],
            [
                'FARGATE',
                None,
                'LATEST',
                None,
                {'launchType': 'FARGATE', 'platformVersion': 'LATEST'},
            ],
            [
                'EC2',
                None,
                None,
                {'testTagKey': 'testTagValue'},
                {'launchType': 'EC2', 'tags': [{'key': 'testTagKey', 'value': 'testTagValue'}]},
            ],
            [
                '',
                None,
                None,
                {'testTagKey': 'testTagValue'},
                {'tags': [{'key': 'testTagKey', 'value': 'testTagValue'}]},
            ],
            [
                None,
                {'capacityProvider': 'FARGATE_SPOT'},
                'LATEST',
                None,
                {
                    'capacityProviderStrategy': {'capacityProvider': 'FARGATE_SPOT'},
                    'platformVersion': 'LATEST',
                },
            ],
            [
                'FARGATE',
                {'capacityProvider': 'FARGATE_SPOT', 'weight': 123, 'base': 123},
                'LATEST',
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
                'LATEST',
                None,
                {
                    'capacityProviderStrategy': {'capacityProvider': 'FARGATE_SPOT'},
                    'platformVersion': 'LATEST',
                },
            ],
        ]
    )
    @mock.patch.object(EcsOperator, '_wait_for_task_ended')
    @mock.patch.object(EcsOperator, '_check_success_task')
    def test_execute_without_failures(
        self,
        launch_type,
        capacity_provider_strategy,
        platform_version,
        tags,
        expected_args,
        check_mock,
        wait_mock,
    ):

        self.set_up_operator(
            launch_type=launch_type,
            capacity_provider_strategy=capacity_provider_strategy,
            platform_version=platform_version,
            tags=tags,
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
        assert self.ecs.ecs_task_id == 'd8c67b3c-ac87-4ffe-a847-4785bc3a8b55'

    def test_execute_with_failures(self):
        client_mock = self.aws_hook_mock.return_value.get_conn.return_value
        resp_failures = deepcopy(RESPONSE_WITHOUT_FAILURES)
        resp_failures['failures'].append('dummy error')
        client_mock.run_task.return_value = resp_failures

        with pytest.raises(EcsOperatorError):
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

    def test_check_success_tasks_raises_failed_to_start(self):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        client_mock.describe_tasks.return_value = {
            'tasks': [
                {
                    'stopCode': 'TaskFailedToStart',
                    'stoppedReason': 'Task failed to start',
                    'containers': [{'name': 'foo', 'lastStatus': 'STOPPED'}],
                }
            ]
        }

        with pytest.raises(EcsTaskFailToStart) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == "The task failed to start due to: Task failed to start"
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_check_success_tasks_raises_cloudwatch_logs(self):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED', 'exitCode': 1}]}]
        }

        task_log_fetcher = mock.Mock()
        self.ecs.task_log_fetcher = task_log_fetcher

        task_log_fetcher.get_last_log_messages.return_value = ["1", "2", "3", "4", "5"]

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == (
            "This task is not in success state - last 10 logs from Cloudwatch:\n1\n2\n3\n4\n5"
        )
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_check_success_tasks_raises_cloudwatch_logs_empty(self):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED', 'exitCode': 1}]}]
        }

        task_log_fetcher = mock.Mock()
        self.ecs.task_log_fetcher = task_log_fetcher

        task_log_fetcher.get_last_log_messages.return_value = []

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == "This task is not in success state - last 10 logs from Cloudwatch:\n"
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_check_success_tasks_raises_logs_disabled(self):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED', 'exitCode': 1}]}]
        }

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert "This task is not in success state " in str(ctx.value)
        assert "'name': 'foo'" in str(ctx.value)
        assert "'lastStatus': 'STOPPED'" in str(ctx.value)
        assert "'exitCode': 1" in str(ctx.value)
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    def test_check_success_tasks_handles_initialization_failure(self):
        client_mock = mock.Mock()
        self.ecs.arn = 'arn'
        self.ecs.client = client_mock

        # exitCode is missing during some container initialization failures
        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED'}]}]
        }

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        print(str(ctx.value))
        assert "This task is not in success state " in str(ctx.value)
        assert "'name': 'foo'" in str(ctx.value)
        assert "'lastStatus': 'STOPPED'" in str(ctx.value)
        assert "exitCode" not in str(ctx.value)
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
    @mock.patch.object(EcsOperator, "_xcom_del")
    @mock.patch.object(
        EcsOperator,
        "xcom_pull",
        return_value="arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b55",
    )
    @mock.patch.object(EcsOperator, '_wait_for_task_ended')
    @mock.patch.object(EcsOperator, '_check_success_task')
    @mock.patch.object(EcsOperator, '_start_task')
    def test_reattach_successful(
        self, launch_type, tags, start_mock, check_mock, wait_mock, xcom_pull_mock, xcom_del_mock
    ):

        self.set_up_operator(launch_type=launch_type, tags=tags)
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
        assert self.ecs.ecs_task_id == 'd8c67b3c-ac87-4ffe-a847-4785bc3a8b55'

    @parameterized.expand(
        [
            ['EC2', None],
            ['FARGATE', None],
            ['EC2', {'testTagKey': 'testTagValue'}],
            ['', {'testTagKey': 'testTagValue'}],
        ]
    )
    @mock.patch.object(EcsOperator, '_xcom_del')
    @mock.patch.object(EcsOperator, '_xcom_set')
    @mock.patch.object(EcsOperator, '_try_reattach_task')
    @mock.patch.object(EcsOperator, '_wait_for_task_ended')
    @mock.patch.object(EcsOperator, '_check_success_task')
    def test_reattach_save_task_arn_xcom(
        self, launch_type, tags, check_mock, wait_mock, reattach_mock, xcom_set_mock, xcom_del_mock
    ):

        self.set_up_operator(launch_type=launch_type, tags=tags)
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
        assert self.ecs.ecs_task_id == 'd8c67b3c-ac87-4ffe-a847-4785bc3a8b55'

    def test_execute_xcom_with_log(self):
        self.ecs.do_xcom_push = True
        self.ecs.task_log_fetcher = mock.Mock()
        self.ecs.task_log_fetcher.get_last_log_message.return_value = "Log output"
        assert self.ecs.execute(None) == "Log output"

    def test_execute_xcom_with_no_log(self):
        self.ecs.do_xcom_push = True
        self.ecs.task_log_fetcher = mock.Mock()
        self.ecs.task_log_fetcher.get_last_log_message.return_value = None
        assert self.ecs.execute(None) is None

    def test_execute_xcom_with_no_log_fetcher(self):
        self.ecs.do_xcom_push = True
        assert self.ecs.execute(None) is None

    def test_execute_xcom_disabled(self):
        self.ecs.do_xcom_push = False
        self.ecs.task_log_fetcher = mock.Mock()
        self.ecs.task_log_fetcher.get_last_log_message.return_value = "Log output"
        assert self.ecs.execute(None) is None


class TestShouldRetry(unittest.TestCase):
    def test_return_true_on_valid_reason(self):
        self.assertTrue(should_retry(EcsOperatorError([{'reason': 'RESOURCE:MEMORY'}], 'Foo')))

    def test_return_false_on_invalid_reason(self):
        self.assertFalse(should_retry(EcsOperatorError([{'reason': 'CLUSTER_NOT_FOUND'}], 'Foo')))


class TestShouldRetryEni(unittest.TestCase):
    def test_return_true_on_valid_reason(self):
        self.assertTrue(
            should_retry_eni(
                EcsTaskFailToStart(
                    "The task failed to start due to: "
                    "Timeout waiting for network interface provisioning to complete."
                )
            )
        )

    def test_return_false_on_invalid_reason(self):
        self.assertFalse(
            should_retry_eni(
                EcsTaskFailToStart(
                    "The task failed to start due to: "
                    "CannotPullContainerError: "
                    "ref pull has been retried 5 time(s): failed to resolve reference"
                )
            )
        )


class TestEcsTaskLogFetcher(unittest.TestCase):
    @mock.patch('logging.Logger')
    def set_up_log_fetcher(self, logger_mock):
        self.logger_mock = logger_mock

        self.log_fetcher = EcsTaskLogFetcher(
            log_group="test_log_group",
            log_stream_name="test_log_stream_name",
            fetch_interval=timedelta(milliseconds=1),
            logger=logger_mock,
        )

    def setUp(self):
        self.set_up_log_fetcher()

    @mock.patch(
        'threading.Event.is_set',
        side_effect=(False, False, False, True),
    )
    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        side_effect=(
            iter(
                [
                    {'timestamp': 1617400267123, 'message': 'First'},
                    {'timestamp': 1617400367456, 'message': 'Second'},
                ]
            ),
            iter(
                [
                    {'timestamp': 1617400467789, 'message': 'Third'},
                ]
            ),
            iter([]),
        ),
    )
    def test_run(self, get_log_events_mock, event_is_set_mock):

        self.log_fetcher.run()

        self.logger_mock.info.assert_has_calls(
            [
                mock.call('[2021-04-02 21:51:07,123] First'),
                mock.call('[2021-04-02 21:52:47,456] Second'),
                mock.call('[2021-04-02 21:54:27,789] Third'),
            ]
        )

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        side_effect=ClientError({"Error": {"Code": "ResourceNotFoundException"}}, None),
    )
    def test_get_log_events_with_expected_error(self, get_log_events_mock):
        with pytest.raises(StopIteration):
            next(self.log_fetcher._get_log_events())

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        side_effect=Exception(),
    )
    def test_get_log_events_with_unexpected_error(self, get_log_events_mock):
        with pytest.raises(Exception):
            next(self.log_fetcher._get_log_events())

    def test_event_to_str(self):
        events = [
            {'timestamp': 1617400267123, 'message': 'First'},
            {'timestamp': 1617400367456, 'message': 'Second'},
            {'timestamp': 1617400467789, 'message': 'Third'},
        ]
        assert [self.log_fetcher._event_to_str(event) for event in events] == (
            [
                '[2021-04-02 21:51:07,123] First',
                '[2021-04-02 21:52:47,456] Second',
                '[2021-04-02 21:54:27,789] Third',
            ]
        )

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        return_value=(),
    )
    def test_get_last_log_message_with_no_log_events(self, mock_log_events):
        assert self.log_fetcher.get_last_log_message() is None

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        return_value=iter(
            [
                {'timestamp': 1617400267123, 'message': 'First'},
                {'timestamp': 1617400367456, 'message': 'Second'},
            ]
        ),
    )
    def test_get_last_log_message_with_log_events(self, mock_log_events):
        assert self.log_fetcher.get_last_log_message() == 'Second'

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        return_value=iter(
            [
                {'timestamp': 1617400267123, 'message': 'First'},
                {'timestamp': 1617400367456, 'message': 'Second'},
                {'timestamp': 1617400367458, 'message': 'Third'},
            ]
        ),
    )
    def test_get_last_log_messages_with_log_events(self, mock_log_events):
        assert self.log_fetcher.get_last_log_messages(2) == ['Second', 'Third']

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        return_value=(),
    )
    def test_get_last_log_messages_with_no_log_events(self, mock_log_events):
        assert self.log_fetcher.get_last_log_messages(2) == []
