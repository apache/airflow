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

from copy import deepcopy
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

import boto3
import pytest

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, TaskDeferred
from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart
from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates, EcsHook
from airflow.providers.amazon.aws.operators.ecs import (
    DEFAULT_CONN_ID,
    EcsBaseOperator,
    EcsCreateClusterOperator,
    EcsDeleteClusterOperator,
    EcsDeregisterTaskDefinitionOperator,
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.providers.amazon.aws.triggers.ecs import TaskDoneTrigger
from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher
from airflow.utils.types import NOTSET

CLUSTER_NAME = "test_cluster"
CONTAINER_NAME = "e1ed7aac-d9b2-4315-8726-d2432bf11868"
TASK_ID = "d8c67b3c-ac87-4ffe-a847-4785bc3a8b55"
TASK_DEFINITION_NAME = "td_name"
TASK_DEFINITION_CONFIG = {
    "family": "family_name",
    "register_task_kwargs": {
        "cpu": "256",
        "memory": "512",
        "networkMode": "awsvpc",
    },
    "container_definitions": [
        {
            "name": CONTAINER_NAME,
            "image": "ubuntu",
            "workingDirectory": "/usr/bin",
            "entryPoint": ["sh", "-c"],
            "command": ["ls"],
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
WAITERS_TEST_CASES = [
    pytest.param(None, None, id="default-values"),
    pytest.param(3.14, None, id="set-delay-only"),
    pytest.param(None, 42, id="set-max-attempts-only"),
    pytest.param(2.71828, 9000, id="user-defined"),
]


@pytest.fixture
def patch_hook_waiters():
    with mock.patch.object(EcsHook, "get_waiter") as m:
        yield m


class EcsBaseTestCase:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("ecs", region_name="eu-west-3")
        monkeypatch.setattr(EcsHook, "conn", self.client)


class TestEcsBaseOperator(EcsBaseTestCase):
    """Test Base ECS Operator."""

    @pytest.mark.parametrize("aws_conn_id", [None, NOTSET, "aws_test_conn"])
    @pytest.mark.parametrize("region_name", [None, NOTSET, "ca-central-1"])
    def test_initialise_operator(self, aws_conn_id, region_name):
        """Test initialize operator."""
        op_kw = {"aws_conn_id": aws_conn_id, "region": region_name}
        op_kw = {k: v for k, v in op_kw.items() if v is not NOTSET}
        op = EcsBaseOperator(task_id="test_ecs_base", **op_kw)

        assert op.aws_conn_id == (aws_conn_id if aws_conn_id is not NOTSET else DEFAULT_CONN_ID)
        assert op.region == (region_name if region_name is not NOTSET else None)

    @mock.patch("airflow.providers.amazon.aws.operators.ecs.EcsHook")
    @pytest.mark.parametrize("aws_conn_id", [None, NOTSET, "aws_test_conn"])
    @pytest.mark.parametrize("region_name", [None, NOTSET, "ca-central-1"])
    def test_hook_and_client(self, mock_ecs_hook_cls, aws_conn_id, region_name):
        """Test initialize ``EcsHook`` and ``boto3.client``."""
        mock_ecs_hook = mock_ecs_hook_cls.return_value
        mock_conn = mock.MagicMock()
        type(mock_ecs_hook).conn = mock.PropertyMock(return_value=mock_conn)

        op_kw = {"aws_conn_id": aws_conn_id, "region": region_name}
        op_kw = {k: v for k, v in op_kw.items() if v is not NOTSET}
        op = EcsBaseOperator(task_id="test_ecs_base_hook_client", **op_kw)

        hook = op.hook
        assert op.hook is hook
        mock_ecs_hook_cls.assert_called_once_with(aws_conn_id=op.aws_conn_id, region_name=op.region)

        client = op.client
        mock_ecs_hook_cls.assert_called_once_with(aws_conn_id=op.aws_conn_id, region_name=op.region)
        assert client == mock_conn
        assert op.client is client


class TestEcsRunTaskOperator(EcsBaseTestCase):
    def set_up_operator(self, **kwargs):
        self.ecs_operator_args = {
            "task_id": "task",
            "task_definition": "t",
            "cluster": "c",
            "overrides": {},
            "group": "group",
            "placement_constraints": [
                {"expression": "attribute:ecs.instance-type =~ t2.*", "type": "memberOf"}
            ],
            "placement_strategy": [{"field": "memory", "type": "binpack"}],
            "network_configuration": {
                "awsvpcConfiguration": {"securityGroups": ["sg-123abc"], "subnets": ["subnet-123456ab"]}
            },
            "propagate_tags": "TASK_DEFINITION",
        }
        self.ecs = EcsRunTaskOperator(**self.ecs_operator_args, **kwargs)

    def setup_method(self):
        self.set_up_operator()
        self.mock_context = mock.MagicMock()

    def test_init(self):
        assert self.ecs.task_definition == "t"
        assert self.ecs.cluster == "c"
        assert self.ecs.overrides == {}

    def test_template_fields_overrides(self):
        assert self.ecs.template_fields == (
            "task_definition",
            "cluster",
            "overrides",
            "launch_type",
            "capacity_provider_strategy",
            "group",
            "placement_constraints",
            "placement_strategy",
            "platform_version",
            "network_configuration",
            "tags",
            "awslogs_group",
            "awslogs_region",
            "awslogs_stream_prefix",
            "awslogs_fetch_interval",
            "propagate_tags",
            "reattach",
            "number_logs_exception",
            "wait_for_completion",
            "deferrable",
        )

    @pytest.mark.parametrize(
        "launch_type, capacity_provider_strategy,platform_version,tags,expected_args",
        [
            [
                "EC2",
                None,
                None,
                None,
                {"launchType": "EC2"},
            ],
            [
                "EXTERNAL",
                None,
                None,
                None,
                {"launchType": "EXTERNAL"},
            ],
            [
                "FARGATE",
                None,
                "LATEST",
                None,
                {"launchType": "FARGATE", "platformVersion": "LATEST"},
            ],
            [
                "EC2",
                None,
                None,
                {"testTagKey": "testTagValue"},
                {"launchType": "EC2", "tags": [{"key": "testTagKey", "value": "testTagValue"}]},
            ],
            [
                "",
                None,
                None,
                {"testTagKey": "testTagValue"},
                {"tags": [{"key": "testTagKey", "value": "testTagValue"}]},
            ],
            [
                None,
                {"capacityProvider": "FARGATE_SPOT"},
                "LATEST",
                None,
                {
                    "capacityProviderStrategy": {"capacityProvider": "FARGATE_SPOT"},
                    "platformVersion": "LATEST",
                },
            ],
            [
                "FARGATE",
                {"capacityProvider": "FARGATE_SPOT", "weight": 123, "base": 123},
                "LATEST",
                None,
                {
                    "capacityProviderStrategy": {
                        "capacityProvider": "FARGATE_SPOT",
                        "weight": 123,
                        "base": 123,
                    },
                    "platformVersion": "LATEST",
                },
            ],
            [
                "EC2",
                {"capacityProvider": "FARGATE_SPOT"},
                "LATEST",
                None,
                {
                    "capacityProviderStrategy": {"capacityProvider": "FARGATE_SPOT"},
                    "platformVersion": "LATEST",
                },
            ],
        ],
    )
    @mock.patch.object(EcsRunTaskOperator, "xcom_push")
    @mock.patch.object(EcsRunTaskOperator, "_wait_for_task_ended")
    @mock.patch.object(EcsRunTaskOperator, "_check_success_task")
    @mock.patch.object(EcsBaseOperator, "client")
    def test_execute_without_failures(
        self,
        client_mock,
        check_mock,
        wait_mock,
        xcom_mock,
        launch_type,
        capacity_provider_strategy,
        platform_version,
        tags,
        expected_args,
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
            cluster="c",
            overrides={},
            startedBy=mock.ANY,  # Can by 'airflow' or 'Airflow'
            taskDefinition="t",
            group="group",
            placementConstraints=[{"expression": "attribute:ecs.instance-type =~ t2.*", "type": "memberOf"}],
            placementStrategy=[{"field": "memory", "type": "binpack"}],
            networkConfiguration={
                "awsvpcConfiguration": {"securityGroups": ["sg-123abc"], "subnets": ["subnet-123456ab"]}
            },
            propagateTags="TASK_DEFINITION",
            **expected_args,
        )

        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        assert self.ecs.arn == f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}"

    def test_task_id_parsing(self):
        id = EcsRunTaskOperator._get_ecs_task_id(f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}")
        assert id == TASK_ID

    @mock.patch.object(EcsBaseOperator, "client")
    def test_execute_with_failures(self, client_mock):
        resp_failures = deepcopy(RESPONSE_WITHOUT_FAILURES)
        resp_failures["failures"].append("dummy error")
        client_mock.run_task.return_value = resp_failures

        with pytest.raises(EcsOperatorError):
            self.ecs.execute(None)

        client_mock.run_task.assert_called_once_with(
            cluster="c",
            launchType="EC2",
            overrides={},
            startedBy=mock.ANY,  # Can by 'airflow' or 'Airflow'
            taskDefinition="t",
            group="group",
            placementConstraints=[{"expression": "attribute:ecs.instance-type =~ t2.*", "type": "memberOf"}],
            placementStrategy=[{"field": "memory", "type": "binpack"}],
            networkConfiguration={
                "awsvpcConfiguration": {
                    "securityGroups": ["sg-123abc"],
                    "subnets": ["subnet-123456ab"],
                }
            },
            propagateTags="TASK_DEFINITION",
        )

    @mock.patch.object(EcsBaseOperator, "client")
    def test_wait_end_tasks(self, client_mock):
        self.ecs.arn = "arn"

        self.ecs._wait_for_task_ended()
        client_mock.get_waiter.assert_called_once_with("tasks_stopped")
        client_mock.get_waiter.return_value.wait.assert_called_once_with(
            cluster="c", tasks=["arn"], WaiterConfig={"Delay": 6, "MaxAttempts": 1000000}
        )

    @mock.patch.object(EcsBaseOperator, "client")
    def test_check_success_tasks_raises_failed_to_start(self, client_mock):
        self.ecs.arn = "arn"

        client_mock.describe_tasks.return_value = {
            "tasks": [
                {
                    "stopCode": "TaskFailedToStart",
                    "stoppedReason": "Task failed to start",
                    "containers": [{"name": "foo", "lastStatus": "STOPPED"}],
                }
            ]
        }

        with pytest.raises(EcsTaskFailToStart) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == "The task failed to start due to: Task failed to start"
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @mock.patch.object(EcsBaseOperator, "client")
    @mock.patch("airflow.providers.amazon.aws.utils.task_log_fetcher.AwsTaskLogFetcher")
    def test_check_success_tasks_raises_cloudwatch_logs(self, log_fetcher_mock, client_mock):
        self.ecs.arn = "arn"
        self.ecs.task_log_fetcher = log_fetcher_mock

        log_fetcher_mock.get_last_log_messages.return_value = ["1", "2", "3", "4", "5"]
        client_mock.describe_tasks.return_value = {
            "tasks": [{"containers": [{"name": "foo", "lastStatus": "STOPPED", "exitCode": 1}]}]
        }

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == (
            "This task is not in success state - last 10 logs from Cloudwatch:\n1\n2\n3\n4\n5"
        )
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @mock.patch.object(EcsBaseOperator, "client")
    @mock.patch("airflow.providers.amazon.aws.utils.task_log_fetcher.AwsTaskLogFetcher")
    def test_check_success_tasks_raises_cloudwatch_logs_empty(self, log_fetcher_mock, client_mock):
        self.ecs.arn = "arn"
        self.ecs.task_log_fetcher = log_fetcher_mock

        log_fetcher_mock.get_last_log_messages.return_value = []
        client_mock.describe_tasks.return_value = {
            "tasks": [{"containers": [{"name": "foo", "lastStatus": "STOPPED", "exitCode": 1}]}]
        }

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert str(ctx.value) == "This task is not in success state - last 10 logs from Cloudwatch:\n"
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @mock.patch.object(EcsBaseOperator, "client")
    def test_check_success_tasks_raises_logs_disabled(self, client_mock):
        self.ecs.arn = "arn"

        client_mock.describe_tasks.return_value = {
            "tasks": [{"containers": [{"name": "foo", "lastStatus": "STOPPED", "exitCode": 1}]}]
        }

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        assert "This task is not in success state " in str(ctx.value)
        assert "'name': 'foo'" in str(ctx.value)
        assert "'lastStatus': 'STOPPED'" in str(ctx.value)
        assert "'exitCode': 1" in str(ctx.value)
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @mock.patch.object(EcsBaseOperator, "client")
    def test_check_success_tasks_handles_initialization_failure(self, client_mock):
        self.ecs.arn = "arn"

        # exitCode is missing during some container initialization failures
        client_mock.describe_tasks.return_value = {
            "tasks": [{"containers": [{"name": "foo", "lastStatus": "STOPPED"}]}]
        }

        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()

        print(str(ctx.value))
        assert "This task is not in success state " in str(ctx.value)
        assert "'name': 'foo'" in str(ctx.value)
        assert "'lastStatus': 'STOPPED'" in str(ctx.value)
        assert "exitCode" not in str(ctx.value)
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @mock.patch.object(EcsBaseOperator, "client")
    def test_check_success_tasks_raises_pending(self, client_mock):
        self.ecs.arn = "arn"
        client_mock.describe_tasks.return_value = {
            "tasks": [{"containers": [{"name": "container-name", "lastStatus": "PENDING"}]}]
        }
        with pytest.raises(Exception) as ctx:
            self.ecs._check_success_task()
        # Ordering of str(dict) is not guaranteed.
        assert "This task is still pending " in str(ctx.value)
        assert "'name': 'container-name'" in str(ctx.value)
        assert "'lastStatus': 'PENDING'" in str(ctx.value)
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @mock.patch.object(EcsBaseOperator, "client")
    def test_check_success_tasks_raises_multiple(self, client_mock):
        self.ecs.arn = "arn"
        client_mock.describe_tasks.return_value = {
            "tasks": [
                {
                    "containers": [
                        {"name": "foo", "exitCode": 1},
                        {"name": "bar", "lastStatus": "STOPPED", "exitCode": 0},
                    ]
                }
            ]
        }
        self.ecs._check_success_task()
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @mock.patch.object(EcsBaseOperator, "client")
    def test_host_terminated_raises(self, client_mock):
        self.ecs.arn = "arn"
        client_mock.describe_tasks.return_value = {
            "tasks": [
                {
                    "stoppedReason": "Host EC2 (instance i-1234567890abcdef) terminated.",
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
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @mock.patch.object(EcsBaseOperator, "client")
    def test_check_success_task_not_raises(self, client_mock):
        self.ecs.arn = "arn"
        client_mock.describe_tasks.return_value = {
            "tasks": [{"containers": [{"name": "container-name", "lastStatus": "STOPPED", "exitCode": 0}]}]
        }
        self.ecs._check_success_task()
        client_mock.describe_tasks.assert_called_once_with(cluster="c", tasks=["arn"])

    @pytest.mark.parametrize(
        "launch_type, tags",
        [
            ["EC2", None],
            ["FARGATE", None],
            ["EC2", {"testTagKey": "testTagValue"}],
            ["", {"testTagKey": "testTagValue"}],
        ],
    )
    @pytest.mark.parametrize(
        "arns, expected_arn",
        [
            pytest.param(
                [
                    f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
                    "arn:aws:ecs:us-east-1:012345678910:task/d8c67b3c-ac87-4ffe-a847-4785bc3a8b54",
                ],
                f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
                id="multiple-arns",
            ),
            pytest.param(
                [
                    f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
                ],
                f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}",
                id="simgle-arn",
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.operators.ecs.generate_uuid")
    @mock.patch.object(EcsRunTaskOperator, "_wait_for_task_ended")
    @mock.patch.object(EcsRunTaskOperator, "_check_success_task")
    @mock.patch.object(EcsRunTaskOperator, "_start_task")
    @mock.patch.object(EcsBaseOperator, "client")
    def test_reattach_successful(
        self, client_mock, start_mock, check_mock, wait_mock, uuid_mock, launch_type, tags, arns, expected_arn
    ):
        """Test reattach on first running Task ARN."""
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        mock_ti.key.primary = ("mock_dag", "mock_ti", "mock_runid", 42)
        fake_uuid = "01-02-03-04"
        uuid_mock.return_value = fake_uuid

        self.set_up_operator(launch_type=launch_type, tags=tags)
        client_mock.list_tasks.return_value = {"taskArns": arns}

        self.ecs.reattach = True
        self.ecs.execute({"ti": mock_ti})

        uuid_mock.assert_called_once_with("mock_dag", "mock_ti", "mock_runid", "42")

        extend_args = {}
        if launch_type:
            extend_args["launchType"] = launch_type
        if launch_type == "FARGATE":
            extend_args["platformVersion"] = "LATEST"
        if tags:
            extend_args["tags"] = [{"key": k, "value": v} for (k, v) in tags.items()]

        client_mock.list_tasks.assert_called_once_with(
            cluster="c", desiredStatus="RUNNING", startedBy=fake_uuid
        )

        start_mock.assert_not_called()
        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        assert self.ecs.arn == expected_arn

    @pytest.mark.parametrize(
        "launch_type, tags",
        [
            ["EC2", None],
            ["FARGATE", None],
            ["EC2", {"testTagKey": "testTagValue"}],
            ["", {"testTagKey": "testTagValue"}],
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.operators.ecs.generate_uuid")
    @mock.patch.object(EcsRunTaskOperator, "_wait_for_task_ended")
    @mock.patch.object(EcsRunTaskOperator, "_check_success_task")
    @mock.patch.object(EcsBaseOperator, "client")
    def test_reattach_save_task_arn_xcom(
        self, client_mock, check_mock, wait_mock, uuid_mock, launch_type, tags, caplog
    ):
        """Test no reattach in no running Task started by this Task ID."""
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        mock_ti.key.primary = ("mock_dag", "mock_ti", "mock_runid", 42)
        fake_uuid = "01-02-03-04"
        uuid_mock.return_value = fake_uuid

        self.set_up_operator(launch_type=launch_type, tags=tags)
        client_mock.list_tasks.return_value = {"taskArns": []}
        client_mock.run_task.return_value = RESPONSE_WITHOUT_FAILURES

        self.ecs.reattach = True
        self.ecs.execute({"ti": mock_ti})

        extend_args = {}
        if launch_type:
            extend_args["launchType"] = launch_type
        if launch_type == "FARGATE":
            extend_args["platformVersion"] = "LATEST"
        if tags:
            extend_args["tags"] = [{"key": k, "value": v} for (k, v) in tags.items()]

        client_mock.list_tasks.assert_called_once_with(
            cluster="c", desiredStatus="RUNNING", startedBy=fake_uuid
        )
        client_mock.run_task.assert_called_once()
        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        assert self.ecs.arn == f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}"
        assert "No active previously launched task found to reattach" in caplog.messages

    @mock.patch.object(EcsRunTaskOperator, "xcom_push")
    @mock.patch.object(EcsBaseOperator, "client")
    @mock.patch("airflow.providers.amazon.aws.utils.task_log_fetcher.AwsTaskLogFetcher")
    def test_execute_xcom_with_log(self, log_fetcher_mock, client_mock, xcom_mock):
        self.ecs.do_xcom_push = True
        self.ecs.task_log_fetcher = log_fetcher_mock

        log_fetcher_mock.get_last_log_message.return_value = "Log output"

        assert self.ecs.execute(None) == "Log output"

    @mock.patch.object(EcsRunTaskOperator, "xcom_push")
    @mock.patch.object(EcsBaseOperator, "client")
    @mock.patch("airflow.providers.amazon.aws.utils.task_log_fetcher.AwsTaskLogFetcher")
    def test_execute_xcom_with_no_log(self, log_fetcher_mock, client_mock, xcom_mock):
        self.ecs.do_xcom_push = True
        self.ecs.task_log_fetcher = log_fetcher_mock

        log_fetcher_mock.get_last_log_message.return_value = None

        assert self.ecs.execute(None) is None

    @mock.patch.object(EcsRunTaskOperator, "xcom_push")
    @mock.patch.object(EcsBaseOperator, "client")
    def test_execute_xcom_with_no_log_fetcher(self, client_mock, xcom_mock):
        self.ecs.do_xcom_push = True
        assert self.ecs.execute(None) is None

    @mock.patch.object(EcsBaseOperator, "client")
    @mock.patch.object(AwsTaskLogFetcher, "get_last_log_message", return_value="Log output")
    def test_execute_xcom_disabled(self, log_fetcher_mock, client_mock):
        self.ecs.do_xcom_push = False
        assert self.ecs.execute(None) is None

    @mock.patch.object(EcsRunTaskOperator, "xcom_push")
    @mock.patch.object(EcsRunTaskOperator, "client")
    def test_with_defer(self, client_mock, xcom_mock):
        self.ecs.deferrable = True

        client_mock.run_task.return_value = RESPONSE_WITHOUT_FAILURES

        with pytest.raises(TaskDeferred) as deferred:
            self.ecs.execute(None)

        assert isinstance(deferred.value.trigger, TaskDoneTrigger)
        assert deferred.value.trigger.task_arn == f"arn:aws:ecs:us-east-1:012345678910:task/{TASK_ID}"

    @mock.patch.object(EcsRunTaskOperator, "client", new_callable=PropertyMock)
    def test_execute_complete(self, client_mock):
        event = {"status": "success", "task_arn": "my_arn"}
        self.ecs.reattach = True

        self.ecs.execute_complete(None, event)

        # task gets described to assert its success
        client_mock().describe_tasks.assert_called_once_with(cluster="c", tasks=["my_arn"])


class TestEcsCreateClusterOperator(EcsBaseTestCase):
    @pytest.mark.parametrize("waiter_delay, waiter_max_attempts", WAITERS_TEST_CASES)
    def test_execute_with_waiter(self, patch_hook_waiters, waiter_delay, waiter_max_attempts):
        mocked_waiters = mock.MagicMock(name="MockedHookWaitersMethod")
        patch_hook_waiters.return_value = mocked_waiters
        op = EcsCreateClusterOperator(
            task_id="task",
            cluster_name=CLUSTER_NAME,
            wait_for_completion=True,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
        )
        with mock.patch.object(self.client, "create_cluster") as mock_client_method:
            result = op.execute({})
            mock_client_method.assert_called_once_with(clusterName=CLUSTER_NAME)
        patch_hook_waiters.assert_called_once_with("cluster_active")

        expected_waiter_config = {}
        if waiter_delay:
            expected_waiter_config["Delay"] = waiter_delay
        if waiter_max_attempts:
            expected_waiter_config["MaxAttempts"] = waiter_max_attempts
        mocked_waiters.wait.assert_called_once_with(clusters=mock.ANY, WaiterConfig=expected_waiter_config)
        assert result is not None

    @mock.patch.object(EcsCreateClusterOperator, "client")
    def test_execute_deferrable(self, mock_client: MagicMock):
        op = EcsCreateClusterOperator(
            task_id="task",
            cluster_name=CLUSTER_NAME,
            deferrable=True,
            waiter_delay=12,
            waiter_max_attempts=34,
        )
        mock_client.create_cluster.return_value = {
            "cluster": {"status": EcsClusterStates.PROVISIONING, "clusterArn": "my arn"}
        }

        with pytest.raises(TaskDeferred) as defer:
            op.execute(context={})

        assert defer.value.trigger.waiter_delay == 12
        assert defer.value.trigger.attempts == 34

    def test_execute_immediate_create(self, patch_hook_waiters):
        """Test if cluster created during initial request."""
        op = EcsCreateClusterOperator(task_id="task", cluster_name=CLUSTER_NAME, wait_for_completion=True)
        with mock.patch.object(self.client, "create_cluster") as mock_client_method:
            mock_client_method.return_value = {"cluster": {"status": "ACTIVE", "foo": "bar"}}
            result = op.execute({})
            mock_client_method.assert_called_once_with(clusterName=CLUSTER_NAME)
        patch_hook_waiters.assert_not_called()
        assert result == {"status": "ACTIVE", "foo": "bar"}

    def test_execute_without_waiter(self, patch_hook_waiters):
        op = EcsCreateClusterOperator(task_id="task", cluster_name=CLUSTER_NAME, wait_for_completion=False)
        with mock.patch.object(self.client, "create_cluster") as mock_client_method:
            result = op.execute({})
            mock_client_method.assert_called_once_with(clusterName=CLUSTER_NAME)
        patch_hook_waiters.assert_not_called()
        assert result is not None


class TestEcsDeleteClusterOperator(EcsBaseTestCase):
    @pytest.mark.parametrize("waiter_delay, waiter_max_attempts", WAITERS_TEST_CASES)
    def test_execute_with_waiter(self, patch_hook_waiters, waiter_delay, waiter_max_attempts):
        mocked_waiters = mock.MagicMock(name="MockedHookWaitersMethod")
        patch_hook_waiters.return_value = mocked_waiters
        op = EcsDeleteClusterOperator(
            task_id="task",
            cluster_name=CLUSTER_NAME,
            wait_for_completion=True,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
        )

        with mock.patch.object(self.client, "delete_cluster") as mock_client_method:
            result = op.execute({})
            mock_client_method.assert_called_once_with(cluster=CLUSTER_NAME)
        patch_hook_waiters.assert_called_once_with("cluster_inactive")

        expected_waiter_config = {}
        if waiter_delay:
            expected_waiter_config["Delay"] = waiter_delay
        if waiter_max_attempts:
            expected_waiter_config["MaxAttempts"] = waiter_max_attempts
        mocked_waiters.wait.assert_called_once_with(clusters=mock.ANY, WaiterConfig=expected_waiter_config)
        assert result is not None

    @mock.patch.object(EcsDeleteClusterOperator, "client")
    def test_execute_deferrable(self, mock_client: MagicMock):
        op = EcsDeleteClusterOperator(
            task_id="task",
            cluster_name=CLUSTER_NAME,
            deferrable=True,
            waiter_delay=12,
            waiter_max_attempts=34,
        )
        mock_client.delete_cluster.return_value = {
            "cluster": {"status": EcsClusterStates.DEPROVISIONING, "clusterArn": "my arn"}
        }

        with pytest.raises(TaskDeferred) as defer:
            op.execute(context={})

        assert defer.value.trigger.waiter_delay == 12
        assert defer.value.trigger.attempts == 34

    def test_execute_immediate_delete(self, patch_hook_waiters):
        """Test if cluster deleted during initial request."""
        op = EcsDeleteClusterOperator(task_id="task", cluster_name=CLUSTER_NAME, wait_for_completion=True)
        with mock.patch.object(self.client, "delete_cluster") as mock_client_method:
            mock_client_method.return_value = {"cluster": {"status": "INACTIVE", "foo": "bar"}}
            result = op.execute({})
            mock_client_method.assert_called_once_with(cluster=CLUSTER_NAME)
        patch_hook_waiters.assert_not_called()
        assert result == {"status": "INACTIVE", "foo": "bar"}

    def test_execute_without_waiter(self, patch_hook_waiters):
        op = EcsDeleteClusterOperator(task_id="task", cluster_name=CLUSTER_NAME, wait_for_completion=False)
        with mock.patch.object(self.client, "delete_cluster") as mock_client_method:
            result = op.execute({})
            mock_client_method.assert_called_once_with(cluster=CLUSTER_NAME)
        patch_hook_waiters.assert_not_called()
        assert result is not None


class TestEcsDeregisterTaskDefinitionOperator(EcsBaseTestCase):
    def test_execute_immediate_delete(self):
        """Test if task definition deleted during initial request."""
        op = EcsDeregisterTaskDefinitionOperator(
            task_id="task", task_definition=TASK_DEFINITION_NAME, wait_for_completion=True
        )
        with mock.patch.object(self.client, "deregister_task_definition") as mock_client_method:
            mock_client_method.return_value = {
                "taskDefinition": {"status": "INACTIVE", "taskDefinitionArn": "foo-bar"}
            }
            result = op.execute({})
            mock_client_method.assert_called_once_with(taskDefinition=TASK_DEFINITION_NAME)
        assert result == "foo-bar"

    def test_deprecation(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            EcsDeregisterTaskDefinitionOperator(task_id="id", task_definition="def", wait_for_completion=True)


class TestEcsRegisterTaskDefinitionOperator(EcsBaseTestCase):
    def test_execute_immediate_create(self):
        """Test if task definition created during initial request."""
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        expected_task_definition_config = {
            "family": "family_name",
            "containerDefinitions": [
                {
                    "name": CONTAINER_NAME,
                    "image": "ubuntu",
                    "workingDirectory": "/usr/bin",
                    "entryPoint": ["sh", "-c"],
                    "command": ["ls"],
                }
            ],
            "cpu": "256",
            "memory": "512",
            "networkMode": "awsvpc",
        }
        op = EcsRegisterTaskDefinitionOperator(task_id="task", **TASK_DEFINITION_CONFIG)

        with mock.patch.object(self.client, "register_task_definition") as mock_client_method:
            mock_client_method.return_value = {
                "taskDefinition": {"status": "ACTIVE", "taskDefinitionArn": "foo-bar"}
            }
            result = op.execute({"ti": mock_ti})
            mock_client_method.assert_called_once_with(**expected_task_definition_config)

        mock_ti.xcom_push.assert_called_once_with(key="task_definition_arn", value="foo-bar")
        assert result == "foo-bar"

    def test_deprecation(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            EcsRegisterTaskDefinitionOperator(
                task_id="id", wait_for_completion=True, **TASK_DEFINITION_CONFIG
            )
