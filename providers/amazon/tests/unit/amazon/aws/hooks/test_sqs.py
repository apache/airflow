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

from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.sqs import SqsHook

QUEUE_URL = "https://sqs.region.amazonaws.com/123456789/test-queue"
QUEUE_NAME = "test-queue"
MESSAGE_BODY = "test message"

MAX_MESSAGE_SIZE = "262144"
DELAY = 5
DEDUPE = "banana"
MSG_ATTRIBUTES = {
    "Author": {
        "StringValue": "test-user",
        "DataType": "String",
    },
    "Priority": {
        "StringValue": "1",
        "DataType": "Number",
    },
}

MESSAGE_ID_KEY = "MessageId"

SEND_MESSAGE_DEFAULTS = {
    "DelaySeconds": 0,
    "MessageAttributes": {},
}


class TestSqsHook:
    @pytest.fixture(autouse=True)
    def setup_test_queue(self):
        """Create a test queue before each test."""
        with mock_aws():
            hook = SqsHook(aws_conn_id="aws_default")
            self.queue_url = hook.create_queue(queue_name=QUEUE_NAME)
            yield

    @pytest.fixture
    def hook(self):
        """Fixture to provide a SqsHook instance."""
        with mock_aws():
            yield SqsHook(aws_conn_id="aws_default")

    @mock_aws
    def test_get_conn(self):
        hook = SqsHook(aws_conn_id="aws_default")
        assert hook.get_conn() is not None

    def test_create_queue(self, hook):
        """Test that create_queue creates a queue and returns the queue URL."""
        queue_name = "test-create-queue"
        queue_url = hook.create_queue(queue_name=queue_name)

        assert isinstance(queue_url, str)
        assert queue_name in queue_url

    def test_create_queue_with_attributes(self, hook):
        """Test creating a queue with custom attributes."""
        queue_name = "test-queue-with-attributes"
        attributes = {
            "DelaySeconds": str(DELAY),
            "MaximumMessageSize": MAX_MESSAGE_SIZE,
        }

        queue_url = hook.create_queue(queue_name=queue_name, attributes=attributes)

        assert isinstance(queue_url, str)
        assert queue_name in queue_url

        # Verify attributes were actually set on the queue
        queue_attrs = hook.get_conn().get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["DelaySeconds", "MaximumMessageSize"]
        )
        assert queue_attrs["Attributes"]["DelaySeconds"] == str(DELAY)
        assert queue_attrs["Attributes"]["MaximumMessageSize"] == MAX_MESSAGE_SIZE

    def test_send_message(self, hook):
        """Test sending a message to a queue."""
        response = hook.send_message(queue_url=self.queue_url, message_body=MESSAGE_BODY)

        assert isinstance(response, dict)
        assert MESSAGE_ID_KEY in response
        assert "MD5OfMessageBody" in response

    def test_send_message_with_attributes(self, hook):
        """Test sending a message with message attributes."""

        response = hook.send_message(
            queue_url=self.queue_url,
            message_body=MESSAGE_BODY,
            message_attributes=MSG_ATTRIBUTES,
        )

        assert isinstance(response, dict)
        assert MESSAGE_ID_KEY in response

        # Verify attributes were actually attached to the message
        received = hook.get_conn().receive_message(QueueUrl=self.queue_url, MessageAttributeNames=["All"])
        assert "Messages" in received
        message = received["Messages"][0]
        assert (
            message["MessageAttributes"]["Author"]["StringValue"] == MSG_ATTRIBUTES["Author"]["StringValue"]
        )
        assert (
            message["MessageAttributes"]["Priority"]["StringValue"]
            == MSG_ATTRIBUTES["Priority"]["StringValue"]
        )

    def test_send_message_with_delay(self, hook):
        """Test sending a message with a delay."""
        delay_seconds = DELAY

        response = hook.send_message(
            queue_url=self.queue_url,
            message_body=MESSAGE_BODY,
            delay_seconds=delay_seconds,
        )

        assert isinstance(response, dict)
        assert MESSAGE_ID_KEY in response

        # Verify the message is not immediately available (due to delay)
        # Immediate receive should return no messages
        immediate_receive = hook.get_conn().receive_message(QueueUrl=self.queue_url, WaitTimeSeconds=0)
        assert "Messages" not in immediate_receive or len(immediate_receive.get("Messages", [])) == 0


@pytest.mark.asyncio
class TestAsyncSqsHook:
    """The mock_aws decorator uses `moto` which does not currently support async SQS so we mock it manually."""

    @pytest.fixture
    def hook(self):
        return SqsHook(aws_conn_id="aws_default")

    @pytest.fixture
    def mock_async_client(self):
        mock_client = mock.AsyncMock()
        mock_client.send_message.return_value = {MESSAGE_ID_KEY: "test-message-id"}
        return mock_client

    @pytest.fixture
    def mock_get_async_conn(self, mock_async_client):
        with mock.patch.object(SqsHook, "get_async_conn") as mocked_conn:
            mocked_conn.return_value = mock_async_client
            mocked_conn.return_value.__aenter__.return_value = mock_async_client
            yield mocked_conn

    async def test_get_async_conn(self, hook, mock_get_async_conn, mock_async_client):
        # Test context manager access
        async with await hook.get_async_conn() as async_conn:
            assert async_conn is mock_async_client

        # Test direct access
        async_conn = await hook.get_async_conn()
        assert async_conn is mock_async_client

    async def test_asend_message_minimal(self, hook, mock_get_async_conn, mock_async_client):
        response = await hook.asend_message(queue_url=QUEUE_URL, message_body=MESSAGE_BODY)

        assert MESSAGE_ID_KEY in response
        mock_async_client.send_message.assert_called_once_with(
            MessageBody=MESSAGE_BODY, QueueUrl=QUEUE_URL, **SEND_MESSAGE_DEFAULTS
        )

    async def test_asend_message_with_attributes(self, hook, mock_get_async_conn, mock_async_client):
        response = await hook.asend_message(
            queue_url=QUEUE_URL,
            message_body=MESSAGE_BODY,
            message_attributes=MSG_ATTRIBUTES,
            delay_seconds=DELAY,
            message_deduplication_id=DEDUPE,
        )

        assert MESSAGE_ID_KEY in response
        mock_async_client.send_message.assert_called_once_with(
            DelaySeconds=DELAY,
            MessageBody=MESSAGE_BODY,
            MessageAttributes=MSG_ATTRIBUTES,
            QueueUrl=QUEUE_URL,
            MessageDeduplicationId=DEDUPE,
        )
