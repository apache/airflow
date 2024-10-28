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

import json
from unittest import mock

import pytest
from moto import mock_aws

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

REGION_NAME = "eu-central-1"
QUEUE_NAME = "test-queue"
QUEUE_URL = f"https://{QUEUE_NAME}"


@pytest.fixture
def mocked_context():
    return mock.MagicMock(name="FakeContext")


@pytest.fixture
def mocked_client():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.sqs.SqsHook.conn",
        new_callable=mock.PropertyMock,
    ) as m:
        yield m


class TestSqsSensor:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = {
            "task_id": "test_task",
            "aws_conn_id": None,
            "region_name": REGION_NAME,
            "sqs_queue": QUEUE_URL,
        }
        self.sensor = SqsSensor(**self.default_op_kwargs)
        self.sqs_client = SqsHook(aws_conn_id=None, region_name=REGION_NAME).conn

    def test_init(self):
        self.default_op_kwargs.pop("aws_conn_id", None)
        self.default_op_kwargs.pop("region_name", None)

        sensor = SqsSensor(**self.default_op_kwargs)
        assert sensor.hook.aws_conn_id == "aws_default"
        assert sensor.hook._region_name is None
        assert sensor.hook._verify is None
        assert sensor.hook._config is None

        sensor = SqsSensor(
            **self.default_op_kwargs,
            aws_conn_id=None,
            region_name=REGION_NAME,
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert sensor.hook.aws_conn_id is None
        assert sensor.hook._region_name == REGION_NAME
        assert sensor.hook._verify is True
        assert sensor.hook._config is not None
        assert sensor.hook._config.read_timeout == 42

    @mock_aws
    def test_poke_success(self, mocked_context):
        self.sqs_client.create_queue(QueueName=QUEUE_NAME)
        self.sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody="hello")

        assert self.sensor.poke(mocked_context) is True

        mocked_xcom_push = mocked_context["ti"].xcom_push
        assert mocked_xcom_push.call_count == 1
        call_args, call_kwargs = (
            mocked_xcom_push.call_args.args,
            mocked_xcom_push.call_args.kwargs,
        )
        assert not call_args
        assert "key" in call_kwargs
        assert call_kwargs["key"] == "messages"
        assert "value" in call_kwargs
        xcom_value = call_kwargs["value"]
        assert xcom_value[0]["Body"] == "hello"

    @mock_aws
    def test_poke_no_message(self, mocked_context):
        self.sqs_client.create_queue(QueueName=QUEUE_NAME)

        assert self.sensor.poke(mocked_context) is False

        mocked_xcom_push = mocked_context["ti"].xcom_push
        assert mocked_xcom_push.call_count == 0

    def test_poke_delete_raise_airflow_exception(self, mocked_client):
        message = {
            "Messages": [
                {
                    "MessageId": "c585e508-2ea0-44c7-bf3e-d1ba0cb87834",
                    "ReceiptHandle": "mockHandle",
                    "MD5OfBody": "e5a9d8684a8edfed460b8d42fd28842f",
                    "Body": "h21",
                }
            ],
            "ResponseMetadata": {
                "RequestId": "56cbf4aa-f4ef-5518-9574-a04e0a5f1411",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "x-amzn-requestid": "56cbf4aa-f4ef-5518-9574-a04e0a5f1411",
                    "date": "Mon, 18 Feb 2019 18:41:52 GMT",
                    "content-type": "text/xml",
                    "mock_sqs_hook-length": "830",
                },
                "RetryAttempts": 0,
            },
        }
        mocked_client.return_value.receive_message.return_value = message
        mocked_client.return_value.delete_message_batch.return_value = {
            "Failed": [{"Id": "22f67273-4dbc-4c19-83b5-aee71bfeb832"}]
        }

        with pytest.raises(AirflowException, match="Delete SQS Messages failed"):
            self.sensor.poke({})

    def test_poke_receive_raise_exception(self, mocked_client):
        mocked_client.return_value.receive_message.side_effect = Exception(
            "test exception"
        )
        with pytest.raises(Exception, match="test exception"):
            self.sensor.poke({})

    def test_poke_visibility_timeout(self, mocked_client, mocked_context):
        # Check without visibility_timeout parameter
        self.sensor.poke(mocked_context)
        calls_receive_message = [
            mock.call().receive_message(
                QueueUrl=QUEUE_URL, MaxNumberOfMessages=5, WaitTimeSeconds=1
            )
        ]
        mocked_client.assert_has_calls(calls_receive_message)

        # Check with visibility_timeout parameter
        SqsSensor(**self.default_op_kwargs, visibility_timeout=42).poke(mocked_context)
        calls_receive_message = [
            mock.call().receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=1,
                VisibilityTimeout=42,
            )
        ]
        mocked_client.assert_has_calls(calls_receive_message)

    @mock_aws
    def test_poke_message_invalid_filtering(self):
        self.sqs_client.create_queue(QueueName=QUEUE_NAME)
        self.sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody="hello")

        sensor = SqsSensor(**self.default_op_kwargs, message_filtering="invalid_option")
        with pytest.raises(
            NotImplementedError, match="Override this method to define custom filters"
        ):
            sensor.poke({})

    def test_poke_message_filtering_literal_values(self, mocked_client, mocked_context):
        matching = [{"id": 11, "body": "a matching message"}]
        non_matching = [{"id": 12, "body": "a non-matching message"}]

        def mock_receive_message(**kwargs):
            messages = []
            for message in [*matching, *non_matching]:
                messages.append(
                    {
                        "MessageId": message["id"],
                        "ReceiptHandle": 100 + message["id"],
                        "Body": message["body"],
                    }
                )
            return {"Messages": messages}

        mocked_client.return_value.receive_message.side_effect = mock_receive_message

        def mock_delete_message_batch(**kwargs):
            return {"Successful"}

        mocked_client.return_value.delete_message_batch.side_effect = (
            mock_delete_message_batch
        )

        # Test that messages are filtered
        sensor = SqsSensor(
            **self.default_op_kwargs,
            message_filtering="literal",
            message_filtering_match_values=["a matching message"],
        )
        assert sensor.poke(mocked_context) is True

        # Test that only filtered messages are deleted
        delete_entries = [
            {"Id": x["id"], "ReceiptHandle": 100 + x["id"]} for x in matching
        ]
        calls_delete_message_batch = [
            mock.call().delete_message_batch(QueueUrl=QUEUE_URL, Entries=delete_entries)
        ]
        mocked_client.assert_has_calls(calls_delete_message_batch)

    def test_poke_message_filtering_jsonpath(self, mocked_client, mocked_context):
        matching = [
            {"id": 11, "key": {"matches": [1, 2]}},
            {"id": 12, "key": {"matches": [3, 4, 5]}},
            {"id": 13, "key": {"matches": [10]}},
        ]
        non_matching = [
            {"id": 14, "key": {"nope": [5, 6]}},
            {"id": 15, "key": {"nope": [7, 8]}},
        ]

        def mock_receive_message(**kwargs):
            messages = []
            for message in [*matching, *non_matching]:
                messages.append(
                    {
                        "MessageId": message["id"],
                        "ReceiptHandle": 100 + message["id"],
                        "Body": json.dumps(message),
                    }
                )
            return {"Messages": messages}

        mocked_client.return_value.receive_message.side_effect = mock_receive_message

        def mock_delete_message_batch(**kwargs):
            return {"Successful"}

        mocked_client.return_value.delete_message_batch.side_effect = (
            mock_delete_message_batch
        )

        # Test that messages are filtered
        sensor = SqsSensor(
            **self.default_op_kwargs,
            message_filtering="jsonpath",
            message_filtering_config="key.matches[*]",
        )
        assert sensor.poke(mocked_context)

        # Test that only filtered messages are deleted
        delete_entries = [
            {"Id": x["id"], "ReceiptHandle": 100 + x["id"]} for x in matching
        ]
        calls_delete_message_batch = [
            mock.call().delete_message_batch(QueueUrl=QUEUE_URL, Entries=delete_entries)
        ]
        mocked_client.assert_has_calls(calls_delete_message_batch)

    def test_poke_message_filtering_jsonpath_values(self, mocked_client, mocked_context):
        matching = [
            {"id": 11, "key": {"matches": [1, 2]}},
            {"id": 12, "key": {"matches": [1, 4, 5]}},
            {"id": 13, "key": {"matches": [4, 5]}},
        ]
        non_matching = [
            {"id": 21, "key": {"matches": [10]}},
            {"id": 22, "key": {"nope": [5, 6]}},
            {"id": 23, "key": {"nope": [7, 8]}},
        ]

        def mock_receive_message(**kwargs):
            messages = []
            for message in [*matching, *non_matching]:
                messages.append(
                    {
                        "MessageId": message["id"],
                        "ReceiptHandle": 100 + message["id"],
                        "Body": json.dumps(message),
                    }
                )
            return {"Messages": messages}

        mocked_client.return_value.receive_message.side_effect = mock_receive_message

        def mock_delete_message_batch(**kwargs):
            return {"Successful"}

        mocked_client.return_value.delete_message_batch.side_effect = (
            mock_delete_message_batch
        )

        # Test that messages are filtered
        sensor = SqsSensor(
            **self.default_op_kwargs,
            message_filtering="jsonpath",
            message_filtering_config="key.matches[*]",
            message_filtering_match_values=[1, 4],
        )
        assert sensor.poke(mocked_context)

        # Test that only filtered messages are deleted
        delete_entries = [
            {"Id": x["id"], "ReceiptHandle": 100 + x["id"]} for x in matching
        ]
        calls_delete_message_batch = [
            mock.call().delete_message_batch(
                QueueUrl="https://test-queue", Entries=delete_entries
            )
        ]
        mocked_client.assert_has_calls(calls_delete_message_batch)

    def test_poke_message_filtering_jsonpath_ext(self, mocked_client, mocked_context):
        matching = [
            {"id": 11, "key": "a", "value": "b"},
        ]
        non_matching = [
            {"id": 14, "key": "a"},
            {"id": 14, "value": "b"},
        ]

        def mock_receive_message(**kwargs):
            messages = []
            for message in [*matching, *non_matching]:
                messages.append(
                    {
                        "MessageId": message["id"],
                        "ReceiptHandle": 100 + message["id"],
                        "Body": json.dumps(message),
                    }
                )
            return {"Messages": messages}

        mocked_client.return_value.receive_message.side_effect = mock_receive_message

        def mock_delete_message_batch(**kwargs):
            return {"Successful"}

        mocked_client.return_value.delete_message_batch.side_effect = (
            mock_delete_message_batch
        )

        # Test that messages are filtered
        sensor = SqsSensor(
            **self.default_op_kwargs,
            message_filtering="jsonpath-ext",
            message_filtering_config="$.key + $.value",
        )
        assert sensor.poke(mocked_context)

        # Test that only filtered messages are deleted
        delete_entries = [
            {"Id": x["id"], "ReceiptHandle": 100 + x["id"]} for x in matching
        ]
        calls_delete_message_batch = [
            mock.call().delete_message_batch(QueueUrl=QUEUE_URL, Entries=delete_entries)
        ]
        mocked_client.assert_has_calls(calls_delete_message_batch)

    def test_poke_message_filtering_jsonpath_ext_values(
        self, mocked_client, mocked_context
    ):
        matching = [
            {"id": 11, "key": "a1", "value": "b1"},
        ]
        non_matching = [
            {"id": 22, "key": "a2", "value": "b1"},
            {"id": 33, "key": "a1", "value": "b2"},
        ]

        def mock_receive_message(**kwargs):
            messages = []
            for message in [*matching, *non_matching]:
                messages.append(
                    {
                        "MessageId": message["id"],
                        "ReceiptHandle": 100 + message["id"],
                        "Body": json.dumps(message),
                    }
                )
            return {"Messages": messages}

        mocked_client.return_value.receive_message.side_effect = mock_receive_message

        def mock_delete_message_batch(**kwargs):
            return {"Successful"}

        mocked_client.return_value.delete_message_batch.side_effect = (
            mock_delete_message_batch
        )

        # Test that messages are filtered
        sensor = SqsSensor(
            **self.default_op_kwargs,
            message_filtering="jsonpath-ext",
            message_filtering_config="$.key + ' ' + $.value",
            message_filtering_match_values=["a1 b1"],
        )
        assert sensor.poke(mocked_context)

        # Test that only filtered messages are deleted
        delete_entries = [
            {"Id": x["id"], "ReceiptHandle": 100 + x["id"]} for x in matching
        ]
        calls_delete_message_batch = [
            mock.call().delete_message_batch(
                QueueUrl="https://test-queue", Entries=delete_entries
            )
        ]
        mocked_client.assert_has_calls(calls_delete_message_batch)

    def test_poke_do_not_delete_message_on_received(self, mocked_client, mocked_context):
        sensor = SqsSensor(**self.default_op_kwargs, delete_message_on_reception=False)
        sensor.poke(mocked_context)
        assert mocked_client.delete_message_batch.called is False

    @mock_aws
    def test_poke_batch_messages(self, mocked_context):
        messages = ["hello", "brave", "world"]
        self.sqs_client.create_queue(QueueName=QUEUE_NAME)
        # Do publish 3 messages
        for message in messages:
            self.sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody=message)

        # Init batch sensor to get 1 message for each SQS poll and perform 3 polls
        sensor = SqsSensor(**self.default_op_kwargs, max_messages=1, num_batches=3)
        assert sensor.poke(mocked_context)

        # expect all messages are retrieved
        mocked_xcom_push = mocked_context["ti"].xcom_push
        assert mocked_xcom_push.call_count == 1
        call_args, call_kwargs = (
            mocked_xcom_push.call_args.args,
            mocked_xcom_push.call_args.kwargs,
        )
        assert not call_args
        assert "key" in call_kwargs
        assert call_kwargs["key"] == "messages"
        assert "value" in call_kwargs
        xcom_value = call_kwargs["value"]
        assert len(xcom_value) == 3
        received_messages = list(map(lambda x: x["Body"], xcom_value))
        for message in messages:
            assert message in received_messages

    def test_sqs_deferrable(self):
        sensor = SqsSensor(**self.default_op_kwargs, deferrable=True)
        with pytest.raises(TaskDeferred):
            sensor.execute({})

    def test_fail_execute_complete(self):
        sensor = SqsSensor(**self.default_op_kwargs, deferrable=True)
        event = {"status": "failed"}
        message = f"Trigger error: event is {event}"
        with pytest.raises(AirflowException, match=message):
            sensor.execute_complete(context={}, event=event)

    @mock.patch("airflow.providers.amazon.aws.sensors.sqs.SqsSensor.poll_sqs")
    @mock.patch("airflow.providers.amazon.aws.sensors.sqs.process_response")
    @mock.patch("airflow.providers.amazon.aws.hooks.sqs.SqsHook.conn")
    def test_fail_poke(self, mocked_client, process_response, poll_sqs):
        response = "error message"
        messages = [{"MessageId": "1", "ReceiptHandle": "test"}]
        poll_sqs.return_value = response
        process_response.return_value = messages
        mocked_client.delete_message_batch.return_value = response
        error_message = f"Delete SQS Messages failed {response} for messages"

        sensor = SqsSensor(**self.default_op_kwargs)
        with pytest.raises(AirflowException, match=error_message):
            sensor.poke(context={})
