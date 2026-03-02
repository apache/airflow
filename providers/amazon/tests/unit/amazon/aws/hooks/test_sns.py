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

from airflow.providers.amazon.aws.hooks.sns import SnsHook

DEDUPE_ID = "test-dedupe-id"
GROUP_ID = "test-group-id"
MESSAGE = "Hello world"
SUBJECT = "test-subject"
INVALID_ATTRIBUTES_MSG = r"Values in MessageAttributes must be one of bytes, str, int, float, or iterable"

TOPIC_NAME = "test-topic"
TOPIC_ARN = f"arn:aws:sns:us-east-1:123456789012:{TOPIC_NAME}"

INVALID_ATTRIBUTES = {"test-non-iterable": object()}
VALID_ATTRIBUTES = {
    "test-string": "string-value",
    "test-number": 123456,
    "test-array": ["first", "second", "third"],
    "test-binary": b"binary-value",
}

MESSAGE_ID_KEY = "MessageId"
TOPIC_ARN_KEY = "TopicArn"


class TestSnsHook:
    @pytest.fixture(autouse=True)
    def setup_moto(self):
        with mock_aws():
            yield

    @pytest.fixture
    def hook(self):
        return SnsHook(aws_conn_id="aws_default")

    @pytest.fixture
    def target(self, hook):
        return hook.get_conn().create_topic(Name=TOPIC_NAME).get(TOPIC_ARN_KEY)

    def test_get_conn_returns_a_boto3_connection(self, hook):
        assert hook.get_conn() is not None

    def test_publish_to_target_with_subject(self, hook, target):
        response = hook.publish_to_target(target, MESSAGE, SUBJECT)

        assert MESSAGE_ID_KEY in response

    def test_publish_to_target_with_attributes(self, hook, target):
        response = hook.publish_to_target(target, MESSAGE, message_attributes=VALID_ATTRIBUTES)

        assert MESSAGE_ID_KEY in response

    def test_publish_to_target_plain(self, hook, target):
        response = hook.publish_to_target(target, MESSAGE)

        assert MESSAGE_ID_KEY in response

    def test_publish_to_target_error(self, hook, target):
        with pytest.raises(TypeError, match=INVALID_ATTRIBUTES_MSG):
            hook.publish_to_target(target, MESSAGE, message_attributes=INVALID_ATTRIBUTES)

    def test_publish_to_target_with_deduplication(self, hook):
        fifo_target = (
            hook.get_conn()
            .create_topic(
                Name=f"{TOPIC_NAME}.fifo",
                Attributes={
                    "FifoTopic": "true",
                    "ContentBasedDeduplication": "false",
                },
            )
            .get("TopicArn")
        )

        response = hook.publish_to_target(
            fifo_target, MESSAGE, message_deduplication_id=DEDUPE_ID, message_group_id=GROUP_ID
        )
        assert MESSAGE_ID_KEY in response


@pytest.mark.asyncio
class TestAsyncSnsHook:
    """The mock_aws decorator uses `moto` which does not currently support async SNS so we mock it manually."""

    @pytest.fixture
    def hook(self):
        return SnsHook(aws_conn_id="aws_default")

    @pytest.fixture
    def mock_async_client(self):
        mock_client = mock.AsyncMock()
        mock_client.publish.return_value = {MESSAGE_ID_KEY: "test-message-id"}
        return mock_client

    @pytest.fixture
    def mock_get_async_conn(self, mock_async_client):
        with mock.patch.object(SnsHook, "get_async_conn") as mocked_conn:
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

    async def test_apublish_to_target_with_subject(self, hook, mock_get_async_conn, mock_async_client):
        response = await hook.apublish_to_target(TOPIC_ARN, MESSAGE, SUBJECT)

        assert MESSAGE_ID_KEY in response

    async def test_apublish_to_target_with_attributes(self, hook, mock_get_async_conn, mock_async_client):
        response = await hook.apublish_to_target(TOPIC_ARN, MESSAGE, message_attributes=VALID_ATTRIBUTES)

        assert MESSAGE_ID_KEY in response

    async def test_publish_to_target_plain(self, hook, mock_get_async_conn, mock_async_client):
        response = await hook.apublish_to_target(TOPIC_ARN, MESSAGE)

        assert MESSAGE_ID_KEY in response

    async def test_publish_to_target_error(self, hook, mock_get_async_conn, mock_async_client):
        with pytest.raises(TypeError, match=INVALID_ATTRIBUTES_MSG):
            await hook.apublish_to_target(TOPIC_ARN, MESSAGE, message_attributes=INVALID_ATTRIBUTES)

    async def test_apublish_to_target_with_deduplication(self, hook, mock_get_async_conn, mock_async_client):
        response = await hook.apublish_to_target(
            TOPIC_ARN, MESSAGE, message_deduplication_id=DEDUPE_ID, message_group_id=GROUP_ID
        )

        assert MESSAGE_ID_KEY in response
