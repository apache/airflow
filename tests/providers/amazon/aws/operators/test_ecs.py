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
from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart
from airflow.providers.amazon.aws.hooks.ecs import EcsHook
from airflow.providers.amazon.aws.operators.ecs import (
    EcsBaseOperator,
    EcsCreateClusterOperator,
    EcsDeleteClusterOperator,
    EcsDeregisterTaskDefinitionOperator,
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
    EcsTaskLogFetcher,
)
from airflow.providers.amazon.aws.sensors.ecs import EcsClusterStateSensor, EcsTaskDefinitionStateSensor

try:
    from moto import mock_ecs
except ImportError:
    mock_ecs = None

CLUSTER_NAME = 'test_cluster'
CONTAINER_NAME = 'e1ed7aac-d9b2-4315-8726-d2432bf11868'
TASK_ID = 'd8c67b3c-ac87-4ffe-a847-4785bc3a8b55'
TASK_DEFINITION_NAME = 'td_name'
TASK_DEFINITION_CONFIG = {
    'family': 'family_name',
    'register_task_kwargs': {
        'cpu': '256',
        'memory': '512',
        'networkMode': 'awsvpc',
    },
    'container_definitions': [
        {
            'name': CONTAINER_NAME,
            'image': 'ubuntu',
            'workingDirectory': '/usr/bin',
            'entryPoint': ['sh', '-c'],
            'command': ['ls'],
        }
    ],
}
RESPONSE_WITHOUT_FAILURES = {
    "failures": [],
    "tasks": [
        {
            "containers": [
                {
                    "containerArn": f"arn:aws:ecs:us-east-1:012345678910:container/{CONTAINER_NAME}",
                    "lastStatus": "PENDING",
                    "name": "wordpress",
                    "taskArn": f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
                }
            ],
            "desiredStatus": "RUNNING",
            "lastStatus": "PENDING",
            "taskArn": f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
            "taskDefinitionArn": "arn:aws:ecs:us-east-1:012345678910:task-definition/hello_world:11",
        }
    ],
}


@pytest.mark.skipif(mock_ecs is None, reason="mock_ecs package not present")
class TestEcsRunTaskOperator(unittest.TestCase):
    def set_up_operator(self, **kwargs):
        self.ecs_operator_args = {
            'task_id': 'task',
            'task_definition': 't',
            'cluster': 'c',
            'overrides': {},
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
        self.ecs = EcsRunTaskOperator(**self.ecs_operator_args, **kwargs)

    def setUp(self):
        self.set_up_operator()
        self.mock_context = mock.MagicMock()

    def test_init(self):
        assert self.ecs.task_definition == 't'
        assert self.ecs.cluster == 'c'
        assert self.ecs.overrides == {}

    def test_template_fields_overrides(self):
        assert self.ecs.template_fields == (
            'task_definition',
            'cluster',
            'overrides',
            'launch_type',
            'capacity_provider_strategy',
            'group',
            'placement_constraints',
            'placement_strategy',
            'platform_version',
            'network_configuration',
            'tags',
            'awslogs_group',
            'awslogs_region',
            'awslogs_stream_prefix',
            'awslogs_fetch_interval',
            'propagate_tags',
            'reattach',
            'number_logs_exception',
            'wait_for_completion',
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
    @mock.patch.object(EcsRunTaskOperator, '_wait_for_task_ended')
    @mock.patch.object(EcsRunTaskOperator, '_check_success_task')
    @mock.patch.object(EcsBaseOperator, 'client')
    def test_execute_without_failures(
        self,
        launch_type,
        capacity_provider_strategy,
        platform_version,
        tags,
        expected_args,
        client_mock,
        check_mock,
        wait_mock,
    ):

        self.set_up_operator(
            launch_type=launch_type,
            capacity_provider_strategy=capacity_provider_strategy,
            platform_version=platform_version,
            tags=tags,
        )
        client_mock.run_task.return_value = RESPONSE_WITHOUT_FAILURES

        self.ecs.execute(None)

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
        assert self.ecs.arn == f'arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}'
        assert self.ecs.ecs_task_id == TASK_ID

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_execute_with_failures(self, client_mock):
        resp_failures = deepcopy(RESPONSE_WITHOUT_FAILURES)
        resp_failures['failures'].append('dummy error')
        client_mock.run_task.return_value = resp_failures

        with pytest.raises(EcsOperatorError):
            self.ecs.execute(None)

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

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_wait_end_tasks(self, client_mock):
        self.ecs.arn = 'arn'

        self.ecs._wait_for_task_ended()
        client_mock.get_waiter.assert_called_once_with('tasks_stopped')
        client_mock.get_waiter.return_value.wait.assert_called_once_with(cluster='c', tasks=['arn'])
        assert sys.maxsize == client_mock.get_waiter.return_value.config.max_attempts

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_check_success_tasks_raises_failed_to_start(self, client_mock):
        self.ecs.arn = 'arn'

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

    @mock.patch.object(EcsBaseOperator, 'client')
    @mock.patch('airflow.providers.amazon.aws.hooks.ecs.EcsTaskLogFetcher')
    def test_check_success_tasks_raises_cloudwatch_logs(self, log_fetcher_mock, client_mock):
        self.ecs.arn = 'arn'
        self.ecs.task_log_fetcher = log_fetcher_mock

        log_fetcher_mock.get_last_log_messages.return_value = ["1", "2", "3", "4", "5"]
        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED', 'exitCode': 1}]}]
        }

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == (
            "This task is not in success state - last 10 logs from Cloudwatch:\n1\n2\n3\n4\n5"
        )
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    @mock.patch.object(EcsBaseOperator, 'client')
    @mock.patch('airflow.providers.amazon.aws.hooks.ecs.EcsTaskLogFetcher')
    def test_check_success_tasks_raises_cloudwatch_logs_empty(self, log_fetcher_mock, client_mock):
        self.ecs.arn = 'arn'
        self.ecs.task_log_fetcher = log_fetcher_mock

        log_fetcher_mock.get_last_log_messages.return_value = []
        client_mock.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'foo', 'lastStatus': 'STOPPED', 'exitCode': 1}]}]
        }

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == "This task is not in success state - last 10 logs from Cloudwatch:\n"
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_check_success_tasks_raises_logs_disabled(self, client_mock):
        self.ecs.arn = 'arn'

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

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_check_success_tasks_handles_initialization_failure(self, client_mock):
        self.ecs.arn = 'arn'

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

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_check_success_tasks_raises_pending(self, client_mock):
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

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_check_success_tasks_raises_multiple(self, client_mock):
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

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_host_terminated_raises(self, client_mock):
        self.ecs.arn = 'arn'
        client_mock.describe_tasks.return_value = {
            'tasks': [
                {
                    'stoppedReason': 'Host EC2 (instance i-1234567890abcdef) terminated.',
                    "containers": [
                        {
                            "containerArn": f"arn:aws:ecs:us-east-1:012345678910:container/{CONTAINER_NAME}",
                            "lastStatus": "RUNNING",
                            "name": "wordpress",
                            "taskArn": f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
                        }
                    ],
                    "desiredStatus": "STOPPED",
                    "lastStatus": "STOPPED",
                    "taskArn": f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
                    "taskDefinitionArn": "arn:aws:ecs:us-east-1:012345678910:task-definition/hello_world:11",
                }
            ]
        }

        with pytest.raises(AirflowException) as ctx:
            self.ecs._check_success_task()

        assert "The task was stopped because the host instance terminated:" in str(ctx.value)
        assert "Host EC2 (" in str(ctx.value)
        assert ") terminated" in str(ctx.value)
        client_mock.describe_tasks.assert_called_once_with(cluster='c', tasks=['arn'])

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_check_success_task_not_raises(self, client_mock):
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
    @mock.patch.object(EcsRunTaskOperator, "_xcom_del")
    @mock.patch.object(
        EcsRunTaskOperator,
        "xcom_pull",
        return_value=f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
    )
    @mock.patch.object(EcsRunTaskOperator, '_wait_for_task_ended')
    @mock.patch.object(EcsRunTaskOperator, '_check_success_task')
    @mock.patch.object(EcsRunTaskOperator, '_start_task')
    @mock.patch.object(EcsBaseOperator, 'client')
    def test_reattach_successful(
        self,
        launch_type,
        tags,
        client_mock,
        start_mock,
        check_mock,
        wait_mock,
        xcom_pull_mock,
        xcom_del_mock,
    ):

        self.set_up_operator(launch_type=launch_type, tags=tags)
        client_mock.describe_task_definition.return_value = {'taskDefinition': {'family': 'f'}}
        client_mock.list_tasks.return_value = {
            'taskArns': [
                'arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b54',
                f'arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}',
            ]
        }

        self.ecs.reattach = True
        self.ecs.execute(self.mock_context)

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
        assert self.ecs.arn == f'arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}'
        assert self.ecs.ecs_task_id == TASK_ID

    @parameterized.expand(
        [
            ['EC2', None],
            ['FARGATE', None],
            ['EC2', {'testTagKey': 'testTagValue'}],
            ['', {'testTagKey': 'testTagValue'}],
        ]
    )
    @mock.patch.object(EcsRunTaskOperator, '_xcom_del')
    @mock.patch.object(EcsRunTaskOperator, '_try_reattach_task')
    @mock.patch.object(EcsRunTaskOperator, '_wait_for_task_ended')
    @mock.patch.object(EcsRunTaskOperator, '_check_success_task')
    @mock.patch.object(EcsBaseOperator, 'client')
    def test_reattach_save_task_arn_xcom(
        self,
        launch_type,
        tags,
        client_mock,
        check_mock,
        wait_mock,
        reattach_mock,
        xcom_del_mock,
    ):

        self.set_up_operator(launch_type=launch_type, tags=tags)
        client_mock.describe_task_definition.return_value = {'taskDefinition': {'family': 'f'}}
        client_mock.list_tasks.return_value = {'taskArns': []}
        client_mock.run_task.return_value = RESPONSE_WITHOUT_FAILURES

        self.ecs.reattach = True
        self.ecs.execute(self.mock_context)

        extend_args = {}
        if launch_type:
            extend_args['launchType'] = launch_type
        if launch_type == 'FARGATE':
            extend_args['platformVersion'] = 'LATEST'
        if tags:
            extend_args['tags'] = [{'key': k, 'value': v} for (k, v) in tags.items()]

        reattach_mock.assert_called_once()
        client_mock.run_task.assert_called_once()
        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        xcom_del_mock.assert_called_once()
        assert self.ecs.arn == f'arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}'
        assert self.ecs.ecs_task_id == TASK_ID

    @mock.patch.object(EcsBaseOperator, 'client')
    @mock.patch('airflow.providers.amazon.aws.hooks.ecs.EcsTaskLogFetcher')
    def test_execute_xcom_with_log(self, log_fetcher_mock, client_mock):
        self.ecs.do_xcom_push = True
        self.ecs.task_log_fetcher = log_fetcher_mock

        log_fetcher_mock.get_last_log_message.return_value = "Log output"

        assert self.ecs.execute(None) == "Log output"

    @mock.patch.object(EcsBaseOperator, 'client')
    @mock.patch('airflow.providers.amazon.aws.hooks.ecs.EcsTaskLogFetcher')
    def test_execute_xcom_with_no_log(self, log_fetcher_mock, client_mock):
        self.ecs.do_xcom_push = True
        self.ecs.task_log_fetcher = log_fetcher_mock

        log_fetcher_mock.get_last_log_message.return_value = None

        assert self.ecs.execute(None) is None

    @mock.patch.object(EcsBaseOperator, 'client')
    def test_execute_xcom_with_no_log_fetcher(self, client_mock):
        self.ecs.do_xcom_push = True
        assert self.ecs.execute(None) is None

    @mock.patch.object(EcsBaseOperator, 'client')
    @mock.patch.object(EcsTaskLogFetcher, 'get_last_log_message', return_value='Log output')
    def test_execute_xcom_disabled(self, log_fetcher_mock, client_mock):
        self.ecs.do_xcom_push = False
        assert self.ecs.execute(None) is None


@pytest.mark.skipif(mock_ecs is None, reason="mock_ecs package not present")
class TestEcsCreateClusterOperator(unittest.TestCase):
    @mock.patch.object(EcsClusterStateSensor, 'poke')
    @mock.patch.object(EcsHook, 'conn')
    def test_execute(self, mock_conn, mock_sensor):
        op = EcsCreateClusterOperator(task_id='task', cluster_name=CLUSTER_NAME)
        result = op.execute(None)

        mock_sensor.assert_called_once()
        mock_conn.create_cluster.assert_called_once_with(clusterName=CLUSTER_NAME)
        assert result is not None

    @mock.patch.object(EcsClusterStateSensor, 'poke')
    @mock.patch.object(EcsHook, 'conn')
    def test_execute_without_wait(self, mock_conn, mock_sensor):
        op = EcsCreateClusterOperator(task_id='task', cluster_name=CLUSTER_NAME, wait_for_completion=False)
        result = op.execute(None)

        mock_sensor.assert_not_called()
        mock_conn.create_cluster.assert_called_once_with(clusterName=CLUSTER_NAME)
        assert result is not None


@pytest.mark.skipif(mock_ecs is None, reason="mock_ecs package not present")
class TestEcsDeleteClusterOperator(unittest.TestCase):
    @mock.patch.object(EcsClusterStateSensor, 'poke')
    @mock.patch.object(EcsHook, 'conn')
    def test_execute(self, mock_client, mock_sensor):
        op = EcsDeleteClusterOperator(task_id='task', cluster_name=CLUSTER_NAME)
        result = op.execute(None)

        mock_client.delete_cluster.assert_called_once_with(cluster=CLUSTER_NAME)
        mock_sensor.assert_called_once()
        assert result is not None

    @mock.patch.object(EcsClusterStateSensor, 'poke')
    @mock.patch.object(EcsHook, 'conn')
    def test_execute_without_wait(self, mock_conn, mock_sensor):
        op = EcsDeleteClusterOperator(task_id='task', cluster_name=CLUSTER_NAME, wait_for_completion=False)
        result = op.execute(None)

        mock_sensor.assert_not_called()
        mock_conn.delete_cluster.assert_called_once_with(cluster=CLUSTER_NAME)
        assert result is not None


@pytest.mark.skipif(mock_ecs is None, reason="mock_ecs package not present")
class TestEcsDeregisterTaskDefinitionOperator(unittest.TestCase):
    @mock.patch.object(EcsTaskDefinitionStateSensor, 'poke')
    @mock.patch.object(EcsHook, 'conn')
    def test_execute(self, mock_conn, mock_sensor):
        op = EcsDeregisterTaskDefinitionOperator(task_id='task', task_definition=TASK_DEFINITION_NAME)
        result = op.execute(None)

        mock_conn.deregister_task_definition.assert_called_once_with(taskDefinition=TASK_DEFINITION_NAME)
        mock_sensor.assert_called_once()
        assert result is not None

    @mock.patch.object(EcsTaskDefinitionStateSensor, 'poke')
    @mock.patch.object(EcsHook, 'conn')
    def test_execute_without_wait(self, mock_conn, mock_sensor):
        op = EcsDeregisterTaskDefinitionOperator(
            task_id='task', task_definition=TASK_DEFINITION_NAME, wait_for_completion=False
        )
        result = op.execute(None)

        mock_sensor.assert_not_called()
        mock_conn.deregister_task_definition.assert_called_once_with(taskDefinition=TASK_DEFINITION_NAME)
        assert result is not None


@pytest.mark.skipif(mock_ecs is None, reason="mock_ecs package not present")
class TestEcsRegisterTaskDefinitionOperator(unittest.TestCase):
    @mock.patch.object(EcsTaskDefinitionStateSensor, 'poke')
    @mock.patch.object(EcsHook, 'conn')
    def test_execute(self, mock_conn, mock_sensor):
        mock_context = mock.MagicMock()
        expected_task_definition_config = {
            'family': 'family_name',
            'containerDefinitions': [
                {
                    'name': CONTAINER_NAME,
                    'image': 'ubuntu',
                    'workingDirectory': '/usr/bin',
                    'entryPoint': ['sh', '-c'],
                    'command': ['ls'],
                }
            ],
            'cpu': '256',
            'memory': '512',
            'networkMode': 'awsvpc',
        }

        op = EcsRegisterTaskDefinitionOperator(task_id='task', **TASK_DEFINITION_CONFIG)
        result = op.execute(mock_context)

        mock_conn.register_task_definition.assert_called_once_with(**expected_task_definition_config)
        mock_sensor.assert_called_once()
        assert result is not None

    @mock.patch.object(EcsTaskDefinitionStateSensor, 'poke')
    @mock.patch.object(EcsHook, 'conn')
    def test_execute_without_wait(self, mock_conn, mock_sensor):
        mock_context = mock.MagicMock()
        expected_task_definition_config = {
            'family': 'family_name',
            'containerDefinitions': [
                {
                    'name': CONTAINER_NAME,
                    'image': 'ubuntu',
                    'workingDirectory': '/usr/bin',
                    'entryPoint': ['sh', '-c'],
                    'command': ['ls'],
                }
            ],
            'cpu': '256',
            'memory': '512',
            'networkMode': 'awsvpc',
        }

        op = EcsRegisterTaskDefinitionOperator(
            task_id='task', **TASK_DEFINITION_CONFIG, wait_for_completion=False
        )
        result = op.execute(mock_context)

        mock_sensor.assert_not_called()
        mock_conn.register_task_definition.assert_called_once_with(**expected_task_definition_config)
        assert result is not None
