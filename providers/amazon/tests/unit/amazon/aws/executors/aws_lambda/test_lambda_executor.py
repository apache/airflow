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

import datetime as dt
import json
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from semver import VersionInfo

from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.aws_lambda import lambda_executor
from airflow.providers.amazon.aws.executors.aws_lambda.lambda_executor import AwsLambdaExecutor
from airflow.providers.amazon.aws.executors.aws_lambda.utils import CONFIG_GROUP_NAME, AllLambdaConfigKeys
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.state import TaskInstanceState
from airflow.version import version as airflow_version_str

from tests_common.test_utils.compat import timezone
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

airflow_version = VersionInfo(*map(int, airflow_version_str.split(".")[:3]))

DEFAULT_QUEUE_URL = "queue-url"
DEFAULT_DLQ_URL = "dlq-url"
DEFAULT_FUNCTION_NAME = "function-name"


@pytest.fixture
def set_env_vars():
    overrides: dict[tuple[str, str], str] = {
        (CONFIG_GROUP_NAME, AllLambdaConfigKeys.AWS_CONN_ID): "aws_default",
        (CONFIG_GROUP_NAME, AllLambdaConfigKeys.REGION_NAME): "us-west-1",
        (CONFIG_GROUP_NAME, AllLambdaConfigKeys.FUNCTION_NAME): DEFAULT_FUNCTION_NAME,
        (CONFIG_GROUP_NAME, AllLambdaConfigKeys.QUEUE_URL): DEFAULT_QUEUE_URL,
        (CONFIG_GROUP_NAME, AllLambdaConfigKeys.DLQ_URL): DEFAULT_DLQ_URL,
        (CONFIG_GROUP_NAME, AllLambdaConfigKeys.QUALIFIER): "1",
        (CONFIG_GROUP_NAME, AllLambdaConfigKeys.MAX_INVOKE_ATTEMPTS): "3",
        (CONFIG_GROUP_NAME, AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP): "True",
    }
    with conf_vars(overrides):
        yield


@pytest.fixture
def mock_airflow_key():
    def _key():
        key_mock = mock.Mock()
        # Use a "random" value (memory id of the mock obj) so each key serializes uniquely
        key_mock._asdict = mock.Mock(return_value={"mock_key": id(key_mock)})
        return key_mock

    return _key


def _generate_mock_cmd():
    return ["airflow", "tasks", "run", "dag_id", "task_id", "run_id", "--local"]


# The following two fixtures look different because no existing test
# cares if they have unique values, so the same value is always used.
@pytest.fixture
def mock_cmd():
    return _generate_mock_cmd()


@pytest.fixture
def mock_executor(set_env_vars) -> AwsLambdaExecutor:
    """Mock Lambda to a repeatable starting state.."""
    executor = AwsLambdaExecutor()
    executor.IS_BOTO_CONNECTION_HEALTHY = True

    # Replace boto3 clients with mocks
    lambda_mock = mock.Mock(spec=executor.lambda_client)
    lambda_mock.invoke.return_value = {"StatusCode": 0, "failures": []}
    executor.lambda_client = lambda_mock

    sqs_mock = mock.Mock(spec=executor.sqs_client)
    sqs_mock.receive_message.return_value = {"Messages": []}
    executor.sqs_client = sqs_mock

    return executor


class TestAwsLambdaExecutor:
    @mock.patch(
        "airflow.providers.amazon.aws.executors.aws_lambda.lambda_executor.AwsLambdaExecutor.change_state"
    )
    def test_execute(self, change_state_mock, mock_airflow_key, mock_executor, mock_cmd):
        """Test execution from end-to-end."""
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        assert len(mock_executor.pending_tasks) == 0
        mock_executor.execute_async(airflow_key, mock_cmd)
        assert len(mock_executor.pending_tasks) == 1

        mock_executor.attempt_task_runs()
        mock_executor.lambda_client.invoke.assert_called_once()
        payload = json.loads(mock_executor.lambda_client.invoke.call_args.kwargs["Payload"])
        assert payload["executor_config"] == {}

        # Task is stored in active worker.
        assert len(mock_executor.running_tasks) == 1
        assert json.dumps(airflow_key._asdict()) in mock_executor.running_tasks
        change_state_mock.assert_called_once_with(
            airflow_key, TaskInstanceState.RUNNING, ser_airflow_key, remove_running=False
        )

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3+")
    @mock.patch(
        "airflow.providers.amazon.aws.executors.aws_lambda.lambda_executor.AwsLambdaExecutor.change_state"
    )
    def test_task_sdk(self, change_state_mock, mock_airflow_key, mock_executor, mock_cmd):
        """Test task sdk execution from end-to-end."""
        from airflow.executors.workloads import ExecuteTask

        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())
        executor_config = {"config_key": "config_value"}

        workload = mock.Mock(spec=ExecuteTask)
        workload.ti = mock.Mock(spec=TaskInstance)
        workload.ti.key = airflow_key
        workload.ti.executor_config = executor_config
        ser_workload = json.dumps({"test_key": "test_value"})
        workload.model_dump_json.return_value = ser_workload

        mock_executor.queue_workload(workload, mock.Mock())

        assert mock_executor.queued_tasks[workload.ti.key] == workload
        assert len(mock_executor.pending_tasks) == 0
        assert len(mock_executor.running) == 0
        mock_executor._process_workloads([workload])
        assert len(mock_executor.queued_tasks) == 0
        assert len(mock_executor.running) == 1
        assert workload.ti.key in mock_executor.running
        assert len(mock_executor.pending_tasks) == 1
        assert mock_executor.pending_tasks[0].command == [
            "python",
            "-m",
            "airflow.sdk.execution_time.execute_workload",
            "--json-string",
            '{"test_key": "test_value"}',
        ]

        mock_executor.attempt_task_runs()
        mock_executor.lambda_client.invoke.assert_called_once()
        payload = json.loads(mock_executor.lambda_client.invoke.call_args.kwargs["Payload"])
        assert payload["executor_config"] == executor_config
        assert len(mock_executor.pending_tasks) == 0

        # Task is stored in active worker.
        assert len(mock_executor.running_tasks) == 1
        assert mock_executor.running_tasks[ser_airflow_key] == workload.ti.key
        change_state_mock.assert_called_once_with(
            workload.ti.key, TaskInstanceState.RUNNING, ser_airflow_key, remove_running=False
        )

    @mock.patch.object(lambda_executor, "calculate_next_attempt_delay", return_value=dt.timedelta(seconds=0))
    def test_success_execute_api_exception(self, mock_backoff, mock_executor, mock_cmd, mock_airflow_key):
        """Test what happens when Lambda throws an initial exception on invoke, but ultimately passes on retries."""
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        invoke_exception = Exception("Test exception")
        invoke_success = {"StatusCode": 0, "failures": []}
        mock_executor.lambda_client.invoke.side_effect = [invoke_exception, invoke_exception, invoke_success]
        mock_executor.execute_async(airflow_key, mock_cmd)
        expected_retry_count = 2

        # Fail 2 times
        for _ in range(expected_retry_count):
            mock_executor.attempt_task_runs()
            # Task is not stored in active workers.
            assert len(mock_executor.running_tasks) == 0

        # Pass in last attempt
        mock_executor.attempt_task_runs()
        assert len(mock_executor.pending_tasks) == 0
        assert ser_airflow_key in mock_executor.running_tasks
        assert mock_backoff.call_count == expected_retry_count
        for attempt_number in range(1, expected_retry_count):
            mock_backoff.assert_has_calls([mock.call(attempt_number)])

    def test_failed_execute_api_exception(self, mock_executor, mock_cmd, mock_airflow_key):
        """Test what happens when Lambda refuses to execute a task and throws an exception"""
        mock_airflow_key = mock_airflow_key()

        mock_executor.lambda_client.invoke.side_effect = Exception("Test exception")
        mock_executor.execute_async(mock_airflow_key, mock_cmd)

        # No matter what, don't schedule until invoke becomes successful.
        for _ in range(int(mock_executor.max_invoke_attempts) * 2):
            mock_executor.attempt_task_runs()
            # Task is not stored in running tasks
            assert len(mock_executor.running_tasks) == 0

    def test_failed_execute_creds_exception(self, mock_executor, mock_cmd, mock_airflow_key):
        """Test what happens when Lambda refuses to execute a task and throws an exception due to credentials"""
        airflow_key = mock_airflow_key()

        mock_executor.IS_BOTO_CONNECTION_HEALTHY = True
        mock_executor.execute_async(airflow_key, mock_cmd)
        assert mock_executor.pending_tasks[0].attempt_number == 1

        error_to_raise = ClientError(
            {"Error": {"Code": "ExpiredTokenException", "Message": "foobar"}}, "OperationName"
        )
        mock_executor.lambda_client.invoke.side_effect = error_to_raise

        # Sync will ultimately call attempt_task_runs, which is the code under test
        mock_executor.sync()

        # Task should end up back in the queue
        assert mock_executor.pending_tasks[0].key == airflow_key
        # The connection should get marked as unhealthy
        assert not mock_executor.IS_BOTO_CONNECTION_HEALTHY
        # We retry on connections issues indefinitely, so the attempt number should be 1
        assert mock_executor.pending_tasks[0].attempt_number == 1

    def test_failed_execute_client_error_exception(self, mock_executor, mock_cmd, mock_airflow_key):
        """Test what happens when Lambda refuses to execute a task and throws an exception for non-credentials issue"""
        airflow_key = mock_airflow_key()
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = True
        mock_executor.execute_async(airflow_key, mock_cmd)
        assert mock_executor.pending_tasks[0].attempt_number == 1

        error_to_raise = ClientError(
            {"Error": {"Code": "RandomeError", "Message": "foobar"}}, "OperationName"
        )
        mock_executor.lambda_client.invoke.side_effect = error_to_raise

        # Sync will ultimately call attempt_task_runs, which is the code under test
        mock_executor.sync()

        # Task should end up back in the queue
        assert mock_executor.pending_tasks[0].key == airflow_key
        # The connection should stay marked as healthy because the error is something else
        assert mock_executor.IS_BOTO_CONNECTION_HEALTHY
        # Not a retry so increment attempts
        assert mock_executor.pending_tasks[0].attempt_number == 2

    @mock.patch.object(lambda_executor, "calculate_next_attempt_delay", return_value=dt.timedelta(seconds=0))
    def test_attempt_task_runs_attempts_when_tasks_fail(self, _, mock_executor):
        """
        Test case when all tasks fail to run.

        The executor should attempt each task exactly once per sync() iteration.
        It should preserve the order of tasks, and attempt each task up to
        `max_invoke_attempts` times before dropping the task.
        """
        airflow_keys = [
            TaskInstanceKey("a", "task_a", "c", 1, -1),
            TaskInstanceKey("a", "task_b", "c", 1, -1),
        ]
        airflow_cmd1 = _generate_mock_cmd()
        airflow_cmd2 = _generate_mock_cmd()
        commands = [airflow_cmd1, airflow_cmd2]

        failures = [Exception("Failure 1"), Exception("Failure 2")]

        mock_executor.execute_async(airflow_keys[0], commands[0])
        mock_executor.execute_async(airflow_keys[1], commands[1])

        assert len(mock_executor.pending_tasks) == 2
        assert len(mock_executor.running_tasks) == 0

        mock_executor.lambda_client.invoke.side_effect = failures
        mock_executor.attempt_task_runs()

        for i in range(2):
            payload = json.loads(mock_executor.lambda_client.invoke.call_args_list[i].kwargs["Payload"])
            assert airflow_keys[i].task_id in payload["task_key"]

        assert len(mock_executor.pending_tasks) == 2
        assert len(mock_executor.running_tasks) == 0

        mock_executor.lambda_client.invoke.call_args_list.clear()

        mock_executor.lambda_client.invoke.side_effect = failures
        mock_executor.attempt_task_runs()

        for i in range(2):
            payload = json.loads(mock_executor.lambda_client.invoke.call_args_list[i].kwargs["Payload"])
            assert airflow_keys[i].task_id in payload["task_key"]

        assert len(mock_executor.pending_tasks) == 2
        assert len(mock_executor.running_tasks) == 0

        mock_executor.lambda_client.invoke.call_args_list.clear()

        mock_executor.lambda_client.invoke.side_effect = failures
        mock_executor.attempt_task_runs()

        assert (
            len(mock_executor.pending_tasks) == 0
        )  # Pending now zero since we've had three failures to invoke
        assert len(mock_executor.running_tasks) == 0

        if airflow_version >= (2, 10, 0):
            events = [(x.event, x.task_id, x.try_number) for x in mock_executor._task_event_logs]
            assert events == [
                ("lambda invoke failure", "task_a", 1),
                ("lambda invoke failure", "task_b", 1),
            ]

    @mock.patch.object(lambda_executor, "calculate_next_attempt_delay", return_value=dt.timedelta(seconds=0))
    def test_attempt_task_runs_attempts_when_some_tasks_fal(self, _, mock_executor):
        """
        Test case when one task fail to run, others succeed, and a new task gets queued.

        """
        airflow_keys = [
            TaskInstanceKey("a", "task_a", "c", 1, -1),
            TaskInstanceKey("a", "task_b", "c", 1, -1),
        ]
        airflow_cmd1 = _generate_mock_cmd()
        airflow_cmd2 = _generate_mock_cmd()
        airflow_commands = [airflow_cmd1, airflow_cmd2]

        success_response = {"StatusCode": 0, "failures": []}

        responses = [Exception("Failure 1"), success_response]

        mock_executor.execute_async(airflow_keys[0], airflow_commands[0])
        mock_executor.execute_async(airflow_keys[1], airflow_commands[1])

        assert len(mock_executor.pending_tasks) == 2

        mock_executor.lambda_client.invoke.side_effect = responses
        mock_executor.attempt_task_runs()

        for i in range(2):
            payload = json.loads(mock_executor.lambda_client.invoke.call_args_list[i].kwargs["Payload"])
            assert airflow_keys[i].task_id in payload["task_key"]

        assert len(mock_executor.pending_tasks) == 1
        assert len(mock_executor.running_tasks) == 1

        mock_executor.lambda_client.invoke.call_args_list.clear()

        # queue new task
        airflow_keys[1] = TaskInstanceKey("a", "task_c", "c", 1, -1)
        airflow_commands[1] = _generate_mock_cmd()
        mock_executor.execute_async(airflow_keys[1], airflow_commands[1])

        assert len(mock_executor.pending_tasks) == 2
        # assert that the order of pending tasks is preserved i.e. the first task is 1st etc.
        assert mock_executor.pending_tasks[0].key == airflow_keys[0]
        assert mock_executor.pending_tasks[0].command == airflow_commands[0]

        responses = [Exception("Failure 1"), success_response]
        mock_executor.lambda_client.invoke.side_effect = responses
        mock_executor.attempt_task_runs()

        for i in range(2):
            payload = json.loads(mock_executor.lambda_client.invoke.call_args_list[i].kwargs["Payload"])
            assert airflow_keys[i].task_id in payload["task_key"]

        assert len(mock_executor.pending_tasks) == 1
        assert len(mock_executor.running_tasks) == 2

        mock_executor.lambda_client.invoke.call_args_list.clear()

        responses = [Exception("Failure 1")]
        mock_executor.lambda_client.invoke.side_effect = responses
        mock_executor.attempt_task_runs()

        payload = json.loads(mock_executor.lambda_client.invoke.call_args_list[0].kwargs["Payload"])
        assert airflow_keys[0].task_id in payload["task_key"]

        if airflow_version >= (2, 10, 0):
            events = [(x.event, x.task_id, x.try_number) for x in mock_executor._task_event_logs]
            assert events == [("lambda invoke failure", "task_a", 1)]

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync_running_dlq(self, success_mock, fail_mock, mock_executor, mock_airflow_key):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        mock_executor.sqs_client.receive_message.side_effect = [
            {},  # First request from the results queue will be empty
            {
                # Second request from the DLQ will have a message
                "Messages": [
                    {
                        "ReceiptHandle": "receipt_handle",
                        "Body": json.dumps(
                            {
                                "task_key": ser_airflow_key,
                                # DLQ messages will have the input (task_key, command) instead of return_code
                                "command": "command",
                            }
                        ),
                    }
                ]
            },
        ]

        mock_executor.sync_running_tasks()
        # Receive messages should be called twice
        assert mock_executor.sqs_client.receive_message.call_count == 2
        assert mock_executor.sqs_client.receive_message.call_args_list[0].kwargs == {
            "QueueUrl": DEFAULT_QUEUE_URL,
            "MaxNumberOfMessages": 10,
        }

        assert mock_executor.sqs_client.receive_message.call_args_list[1].kwargs == {
            "QueueUrl": DEFAULT_DLQ_URL,
            "MaxNumberOfMessages": 10,
        }

        # Task is not stored in active workers.
        assert len(mock_executor.running_tasks) == 0
        success_mock.assert_not_called()
        fail_mock.assert_called_once()
        assert mock_executor.sqs_client.delete_message.call_count == 1
        assert mock_executor.sqs_client.delete_message.call_args_list[0].kwargs == {
            "QueueUrl": DEFAULT_DLQ_URL,
            "ReceiptHandle": "receipt_handle",
        }

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync_running_success(self, success_mock, fail_mock, mock_executor, mock_airflow_key):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        # Success message
        mock_executor.sqs_client.receive_message.return_value = {
            "Messages": [
                {
                    "ReceiptHandle": "receipt_handle",
                    "Body": json.dumps(
                        {
                            "task_key": ser_airflow_key,
                            "return_code": 0,
                        }
                    ),
                }
            ]
        }

        mock_executor.sync_running_tasks()
        mock_executor.sqs_client.receive_message.assert_called_once()
        assert mock_executor.sqs_client.receive_message.call_args_list[0].kwargs == {
            "QueueUrl": DEFAULT_QUEUE_URL,
            "MaxNumberOfMessages": 10,
        }

        # Task is not stored in active workers.
        assert len(mock_executor.running_tasks) == 0
        # Task is immediately succeeded.
        success_mock.assert_called_once()
        fail_mock.assert_not_called()
        assert mock_executor.sqs_client.delete_message.call_count == 1
        assert mock_executor.sqs_client.delete_message.call_args_list[0].kwargs == {
            "QueueUrl": DEFAULT_QUEUE_URL,
            "ReceiptHandle": "receipt_handle",
        }

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync_running_fail(self, success_mock, fail_mock, mock_executor, mock_airflow_key):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        # Failure message
        mock_executor.sqs_client.receive_message.return_value = {
            "Messages": [
                {
                    "ReceiptHandle": "receipt_handle",
                    "Body": json.dumps(
                        {
                            "task_key": ser_airflow_key,
                            "return_code": 1,  # Non-zero return code, task failed
                        }
                    ),
                }
            ]
        }

        mock_executor.sync_running_tasks()
        mock_executor.sqs_client.receive_message.assert_called_once()

        # Task is not stored in active workers.
        assert len(mock_executor.running_tasks) == 0
        # Task is immediately succeeded.
        success_mock.assert_not_called()
        fail_mock.assert_called_once()
        assert mock_executor.sqs_client.delete_message.call_count == 1

    def test_sync_running_fail_bad_json(self, mock_executor, mock_airflow_key):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        mock_executor.sqs_client.receive_message.side_effect = [
            {
                "Messages": [
                    {
                        "ReceiptHandle": "receipt_handle",
                        "Body": "Banana",  # Body not json format
                    }
                ]
            },
            {},  # Second request from the DLQ will be empty
        ]

        mock_executor.sync_running_tasks()
        # Assert that the message is deleted if the message is not formatted as json
        assert mock_executor.sqs_client.receive_message.call_count == 2
        assert mock_executor.sqs_client.delete_message.call_count == 1

    def test_sync_running_fail_bad_format(self, mock_executor, mock_airflow_key):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        mock_executor.sqs_client.receive_message.side_effect = [
            {
                "Messages": [
                    {
                        "ReceiptHandle": "receipt_handle",
                        "Body": json.dumps(
                            {
                                "foo": "bar",  # Missing expected keys like "task_key"
                                "return_code": 1,  # Non-zero return code, task failed
                            }
                        ),
                    }
                ]
            },
            {},  # Second request from the DLQ will be empty
        ]

        mock_executor.sync_running_tasks()
        # Assert that the message is deleted if the message does not contain the expected keys
        assert mock_executor.sqs_client.receive_message.call_count == 2
        assert mock_executor.sqs_client.delete_message.call_count == 1

    def test_sync_running_fail_bad_format_dlq(self, mock_executor, mock_airflow_key):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        # Failure message
        mock_executor.sqs_client.receive_message.side_effect = [
            {},  # First request from the results queue will be empty
            {
                # Second request from the DLQ will have a message
                "Messages": [
                    {
                        "ReceiptHandle": "receipt_handle",
                        "Body": json.dumps(
                            {
                                "foo": "bar",  # Missing expected keys like "task_key"
                                "return_code": 1,
                            }
                        ),
                    }
                ]
            },
        ]

        mock_executor.sync_running_tasks()
        # Assert that the message is deleted if the message does not contain the expected keys
        assert mock_executor.sqs_client.receive_message.call_count == 2
        assert mock_executor.sqs_client.delete_message.call_count == 1

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync_running_short_circuit(self, success_mock, fail_mock, mock_executor, mock_airflow_key):
        mock_executor.running_tasks.clear()
        # No running tasks, so we will short circuit

        mock_executor.sync_running_tasks()
        mock_executor.sqs_client.receive_message.assert_not_called()

        # Task is still stored in active workers.
        assert len(mock_executor.running_tasks) == 0
        # Task is immediately succeeded.
        success_mock.assert_not_called()
        fail_mock.assert_not_called()
        assert mock_executor.sqs_client.delete_message.call_count == 0

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync_running_no_updates(self, success_mock, fail_mock, mock_executor, mock_airflow_key):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        # No messages, so we will not loop
        mock_executor.sqs_client.receive_message.return_value = {"Messages": []}

        mock_executor.sync_running_tasks()
        # Both the results queue and DLQ should have been checked
        assert mock_executor.sqs_client.receive_message.call_count == 2

        # Task is still stored in active workers.
        assert len(mock_executor.running_tasks) == 1
        # Task is immediately succeeded.
        success_mock.assert_not_called()
        fail_mock.assert_not_called()
        assert mock_executor.sqs_client.delete_message.call_count == 0

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync_running_two_tasks_one_relevant(
        self, success_mock, fail_mock, mock_executor, mock_airflow_key
    ):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())
        airflow_key_2 = mock_airflow_key()
        ser_airflow_key_2 = json.dumps(airflow_key_2._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        mock_executor.running_tasks[ser_airflow_key_2] = airflow_key_2
        # Success message
        mock_executor.sqs_client.receive_message.side_effect = [
            {
                "Messages": [
                    {
                        "ReceiptHandle": "receipt_handle",
                        "Body": json.dumps(
                            {
                                "task_key": ser_airflow_key,
                                "return_code": 0,
                            }
                        ),
                    }
                ]
            },
            {},  # No messages from DLQ
        ]

        mock_executor.sync_running_tasks()
        # Both the results queue and DLQ should have been checked
        assert mock_executor.sqs_client.receive_message.call_count == 2

        # One task left running
        assert len(mock_executor.running_tasks) == 1
        # Task one completed, task two is still running
        assert ser_airflow_key_2 in mock_executor.running_tasks
        # Task is immediately succeeded.
        success_mock.assert_called_once()
        fail_mock.assert_not_called()
        assert mock_executor.sqs_client.delete_message.call_count == 1

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync_running_unknown_task(self, success_mock, fail_mock, mock_executor, mock_airflow_key):
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())
        airflow_key_2 = mock_airflow_key()
        ser_airflow_key_2 = json.dumps(airflow_key_2._asdict())

        mock_executor.running_tasks.clear()
        # Only add one of the tasks to the running list, the other will be unknown
        mock_executor.running_tasks[ser_airflow_key] = airflow_key

        # Receive the known task and unknown task
        known_task_receipt = "receipt_handle_known"
        unknown_task_receipt = "receipt_handle_unknown"
        mock_executor.sqs_client.receive_message.return_value = {
            "Messages": [
                {
                    "ReceiptHandle": known_task_receipt,
                    "Body": json.dumps(
                        {
                            "task_key": ser_airflow_key,
                            "return_code": 0,
                        }
                    ),
                },
                {
                    "ReceiptHandle": unknown_task_receipt,
                    "Body": json.dumps(
                        {
                            "task_key": ser_airflow_key_2,
                            "return_code": 0,
                        }
                    ),
                },
            ]
        }

        mock_executor.sync_running_tasks()
        mock_executor.sqs_client.receive_message.assert_called_once()

        # The known task is set to succeeded, unknown task is dropped
        assert len(mock_executor.running_tasks) == 0
        success_mock.assert_called_once()
        fail_mock.assert_not_called()
        # Only the known message from the queue should be deleted, the other should be marked as visible again
        assert mock_executor.sqs_client.delete_message.call_count == 1
        assert mock_executor.sqs_client.change_message_visibility.call_count == 1
        # The argument to delete_message should be the known task
        assert mock_executor.sqs_client.delete_message.call_args_list[0].kwargs == {
            "QueueUrl": DEFAULT_QUEUE_URL,
            "ReceiptHandle": known_task_receipt,
        }
        # The change_message_visibility should be called with the unknown task
        assert mock_executor.sqs_client.change_message_visibility.call_args_list[0].kwargs == {
            "QueueUrl": DEFAULT_QUEUE_URL,
            "ReceiptHandle": unknown_task_receipt,
            "VisibilityTimeout": 0,
        }

    def test_start_no_check_health(self, mock_executor):
        mock_executor.check_health = mock.Mock()
        with conf_vars({(CONFIG_GROUP_NAME, AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP): "False"}):
            mock_executor.start()

        assert mock_executor.check_health.call_count == 0

    def test_start_check_health_success(self, mock_executor):
        mock_executor.check_health = mock.Mock()
        with conf_vars({(CONFIG_GROUP_NAME, AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP): "True"}):
            mock_executor.start()

        assert mock_executor.check_health.call_count == 1

    def test_start_check_health_fail(self, mock_executor):
        mock_executor.check_health = mock.Mock()
        mock_executor.check_health.side_effect = AirflowException("Test exception")
        with conf_vars({(CONFIG_GROUP_NAME, AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP): "True"}):
            with pytest.raises(AirflowException):
                mock_executor.start()

        assert mock_executor.check_health.call_count == 1

    def test_check_health_success(self, mock_executor):
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_executor.sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": 0}
        }
        mock_executor.lambda_client.get_function.return_value = {
            "Configuration": {
                "FunctionName": DEFAULT_FUNCTION_NAME,
                "State": "Active",
            }
        }
        mock_executor.check_health()
        assert mock_executor.sqs_client.get_queue_attributes.call_count == 2
        assert mock_executor.lambda_client.get_function.call_count == 1
        assert mock_executor.IS_BOTO_CONNECTION_HEALTHY

    def test_check_health_lambda_fails(self, mock_executor):
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_executor.sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": 0}
        }
        mock_executor.lambda_client.get_function.return_value = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "foobar"}}, "OperationName"
        )

        with pytest.raises(AirflowException):
            mock_executor.check_health()
        assert mock_executor.lambda_client.get_function.call_count == 1
        # Lambda has already failed so SQS should not be called
        assert mock_executor.sqs_client.get_queue_attributes.call_count == 0
        assert not mock_executor.IS_BOTO_CONNECTION_HEALTHY

    def test_check_health_sqs_fails(self, mock_executor):
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_executor.sqs_client.get_queue_attributes.return_value = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "foobar"}}, "OperationName"
        )
        mock_executor.lambda_client.get_function.return_value = {
            "Configuration": {
                "FunctionName": DEFAULT_FUNCTION_NAME,
                "State": "Active",
            }
        }
        with pytest.raises(AirflowException):
            mock_executor.check_health()
        assert mock_executor.lambda_client.get_function.call_count == 1
        # Lambda has already failed so SQS should not be called
        assert mock_executor.sqs_client.get_queue_attributes.call_count == 1
        assert not mock_executor.IS_BOTO_CONNECTION_HEALTHY

    def test_check_health_sqs_results_queue_success_dlq_fails(self, mock_executor):
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_executor.sqs_client.get_queue_attributes.side_effect = [
            {"Attributes": {"ApproximateNumberOfMessages": 0}},
            ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "foobar"}}, "OperationName"
            ),
        ]
        mock_executor.lambda_client.get_function.return_value = {
            "Configuration": {
                "FunctionName": DEFAULT_FUNCTION_NAME,
                "State": "Active",
            }
        }
        with pytest.raises(AirflowException):
            mock_executor.check_health()
        assert mock_executor.lambda_client.get_function.call_count == 1
        # Lambda has already failed so SQS should not be called
        assert mock_executor.sqs_client.get_queue_attributes.call_count == 2
        assert not mock_executor.IS_BOTO_CONNECTION_HEALTHY

    def test_sync_already_unhealthy(self, mock_executor):
        # Something has set the connection to unhealthy (tested elsewhere)
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_executor.sync_running_tasks = mock.Mock()
        mock_executor.attempt_task_runs = mock.Mock()
        mock_executor.load_connections = mock.Mock()
        # Set the last connection reload to be more than 60 seconds ago so that we get a reload
        mock_executor.last_connection_reload = timezone.utcnow() - dt.timedelta(seconds=100)
        # We should not be able to sync
        mock_executor.sync()
        assert not mock_executor.IS_BOTO_CONNECTION_HEALTHY
        mock_executor.sync_running_tasks.assert_not_called()
        mock_executor.attempt_task_runs.assert_not_called()
        mock_executor.load_connections.assert_called_once()

    def test_sync_already_unhealthy_then_repaired(self, mock_executor):
        # Something has set the connection to unhealthy (tested elsewhere)
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_executor.sync_running_tasks = mock.Mock()
        mock_executor.attempt_task_runs = mock.Mock()

        def check_health_side_effect():
            mock_executor.IS_BOTO_CONNECTION_HEALTHY = True

        mock_executor.check_health = mock.Mock(side_effect=check_health_side_effect)
        # Set the last connection reload to be more than 60 seconds ago so that we get a reload
        mock_executor.last_connection_reload = timezone.utcnow() - dt.timedelta(seconds=100)
        # Sync should repair itself and continue to call the sync methods
        mock_executor.sync()
        assert mock_executor.IS_BOTO_CONNECTION_HEALTHY
        mock_executor.sync_running_tasks.assert_called_once()
        mock_executor.attempt_task_runs.assert_called_once()

    @pytest.mark.parametrize(
        "error_code",
        [
            "ExpiredTokenException",
            "InvalidClientTokenId",
            "UnrecognizedClientException",
        ],
    )
    def test_sync_become_unhealthy_no_creds(self, error_code, mock_executor):
        # Something has set the connection to unhealthy (tested elsewhere)
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = True
        mock_executor.log.warning = mock.Mock()
        mock_executor.attempt_task_runs = mock.Mock()
        error_to_raise = ClientError({"Error": {"Code": error_code, "Message": "foobar"}}, "OperationName")
        mock_executor.sync_running_tasks = mock.Mock(side_effect=error_to_raise)

        # sync should catch the error and handle it, setting connection to unhealthy
        mock_executor.sync()
        assert not mock_executor.IS_BOTO_CONNECTION_HEALTHY
        mock_executor.sync_running_tasks.assert_called_once()
        mock_executor.attempt_task_runs.assert_not_called()
        # Check that the substring "AWS credentials are either missing or expired" was logged
        mock_executor.log.warning.assert_called_once()
        assert "AWS credentials are either missing or expired" in mock_executor.log.warning.call_args[0][0]

    def test_sync_exception(self, mock_executor):
        # Something has set the connection to unhealthy (tested elsewhere)
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = True
        mock_executor.log.exception = mock.Mock()
        mock_executor.attempt_task_runs = mock.Mock()
        mock_executor.sync_running_tasks = mock.Mock(side_effect=Exception())

        # sync should catch the error and log, don't kill scheduler by letting it raise up higher.
        mock_executor.sync()
        # Not a credentials error that we can tell, so connection stays healthy
        assert mock_executor.IS_BOTO_CONNECTION_HEALTHY
        mock_executor.sync_running_tasks.assert_called_once()
        mock_executor.attempt_task_runs.assert_not_called()
        # Check that the substring "AWS credentials are either missing or expired" was logged
        mock_executor.log.exception.assert_called_once()
        assert "An error occurred while syncing tasks" in mock_executor.log.exception.call_args[0][0]

    def test_try_adopt_task_instances(self, mock_executor, mock_airflow_key):
        """Test that executor can adopt orphaned task instances from a SchedulerJob shutdown event."""
        # airflow_key_1 = mock_airflow_key()
        airflow_key_1 = TaskInstanceKey("a", "task_a", "c", 1, -1)
        ser_airflow_key_1 = json.dumps(airflow_key_1._asdict())
        # airflow_key_2 = mock_airflow_key()
        airflow_key_2 = TaskInstanceKey("a", "task_b", "c", 1, -1)
        ser_airflow_key_2 = json.dumps(airflow_key_2._asdict())

        orphaned_tasks = [
            mock.Mock(spec=TaskInstance),
            mock.Mock(spec=TaskInstance),
            mock.Mock(spec=TaskInstance),
        ]
        orphaned_tasks[0].external_executor_id = ser_airflow_key_1
        orphaned_tasks[1].external_executor_id = ser_airflow_key_2
        orphaned_tasks[
            2
        ].external_executor_id = None  # One orphaned task has no external_executor_id, not adopted

        for task in orphaned_tasks:
            task.try_number = 1

        not_adopted_tasks = mock_executor.try_adopt_task_instances(orphaned_tasks)

        # Two of the three tasks should be adopted.
        assert len(orphaned_tasks) - 1 == len(mock_executor.running_tasks)
        assert ser_airflow_key_1 in mock_executor.running_tasks

        assert mock_executor.running_tasks[ser_airflow_key_1] == airflow_key_1
        assert ser_airflow_key_2 in mock_executor.running_tasks
        assert mock_executor.running_tasks[ser_airflow_key_2] == airflow_key_2

        # The remaining one task is unable to be adopted.
        assert len(not_adopted_tasks) == 1
        assert not_adopted_tasks[0] == orphaned_tasks[2]

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_end(self, success_mock, fail_mock, mock_executor, mock_airflow_key):
        """Test that executor can end successfully; waiting for all tasks to naturally exit."""
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        # First message is empty, so we loop again while waiting for tasks to finish
        mock_executor.sqs_client.receive_message.side_effect = [
            {},
            {},
            {
                "Messages": [
                    {
                        "ReceiptHandle": "receipt_handle",
                        "Body": json.dumps(
                            {
                                "task_key": ser_airflow_key,
                                "return_code": 0,
                            }
                        ),
                    }
                ]
            },
        ]
        mock_executor.end(heartbeat_interval=0)
        # Assert that the sqs_client mock method receive_message was called exactly twice
        assert mock_executor.sqs_client.receive_message.call_count == 3

        # Task is not stored in active workers.
        assert len(mock_executor.running_tasks) == 0
        success_mock.assert_called_once()
        fail_mock.assert_not_called()
        assert mock_executor.sqs_client.delete_message.call_count == 1

    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.lambda_executor.timezone")
    def test_end_timeout(self, mock_timezone, mock_executor, mock_airflow_key):
        """Test that executor can end successfully; waiting for all tasks to naturally exit."""
        # Mock the sync method of the mock_executor object so we can count how many times it was called
        mock_executor.sync = mock.Mock()
        mock_executor.log.warning = mock.Mock()
        current_time = timezone.utcnow()
        mock_timezone.utcnow.side_effect = [
            current_time,
            current_time,
            current_time + dt.timedelta(seconds=5),
            current_time + dt.timedelta(seconds=10),
        ]

        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key

        with conf_vars({(CONFIG_GROUP_NAME, AllLambdaConfigKeys.END_WAIT_TIMEOUT): "5"}):
            mock_executor.end(heartbeat_interval=0)

        # Task is still stored in active workers.
        assert len(mock_executor.running_tasks) == 1
        assert mock_executor.sync.call_count == 2
        mock_executor.log.warning.assert_called_once_with(
            "Timed out waiting for tasks to finish. Some tasks may not be handled gracefully"
            " as the executor is force ending due to timeout."
        )

    def test_terminate(self, mock_executor, mock_airflow_key):
        """Test that executor can terminate successfully."""
        airflow_key = mock_airflow_key()
        ser_airflow_key = json.dumps(airflow_key._asdict())

        mock_executor.running_tasks.clear()
        mock_executor.running_tasks[ser_airflow_key] = airflow_key
        mock_executor.log.warning = mock.Mock()

        mock_executor.terminate()
        mock_executor.log.warning.assert_called_once_with(
            "Terminating Lambda executor. In-flight tasks cannot be stopped."
        )
        assert len(mock_executor.running_tasks) == 1
