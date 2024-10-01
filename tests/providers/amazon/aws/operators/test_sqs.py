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
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator
from tests.providers.amazon.aws.utils.test_template_fields import validate_template_fields

REGION_NAME = "eu-west-1"
QUEUE_NAME = "test-queue"
QUEUE_URL = f"https://{QUEUE_NAME}"
FIFO_QUEUE_NAME = "test-queue.fifo"
FIFO_QUEUE_URL = f"https://{FIFO_QUEUE_NAME}"


@pytest.fixture
def mocked_context():
    return mock.MagicMock(name="FakeContext")


class TestSqsPublishOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = {
            "task_id": "test_task",
            "message_content": "hello",
            "aws_conn_id": None,
            "region_name": REGION_NAME,
        }
        self.sqs_client = SqsHook(aws_conn_id=None, region_name=REGION_NAME).conn

    def test_init(self):
        self.default_op_kwargs.pop("aws_conn_id", None)
        self.default_op_kwargs.pop("region_name", None)

        op = SqsPublishOperator(sqs_queue=QUEUE_NAME, **self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = SqsPublishOperator(
            sqs_queue=FIFO_QUEUE_NAME,
            **self.default_op_kwargs,
            aws_conn_id=None,
            region_name=REGION_NAME,
            verify="/spam/egg.pem",
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id is None
        assert op.hook._region_name == REGION_NAME
        assert op.hook._verify == "/spam/egg.pem"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

    @mock_aws
    def test_execute_success(self, mocked_context):
        self.sqs_client.create_queue(QueueName=QUEUE_NAME)

        # Send SQS Message
        op = SqsPublishOperator(**self.default_op_kwargs, sqs_queue=QUEUE_NAME)
        result = op.execute(mocked_context)
        assert "MD5OfMessageBody" in result
        assert "MessageId" in result

        # Validate message through moto
        message = self.sqs_client.receive_message(QueueUrl=QUEUE_URL)
        assert len(message["Messages"]) == 1
        assert message["Messages"][0]["MessageId"] == result["MessageId"]
        assert message["Messages"][0]["Body"] == "hello"

    @mock_aws
    def test_execute_failure_fifo_queue(self, mocked_context):
        self.sqs_client.create_queue(QueueName=FIFO_QUEUE_NAME, Attributes={"FifoQueue": "true"})

        op = SqsPublishOperator(**self.default_op_kwargs, sqs_queue=FIFO_QUEUE_NAME)
        error_message = (
            r"An error occurred \(MissingParameter\) when calling the SendMessage operation: "
            r"The request must contain the parameter MessageGroupId."
        )
        with pytest.raises(ClientError, match=error_message):
            op.execute(mocked_context)

    @mock_aws
    def test_execute_success_fifo_queue(self, mocked_context):
        self.sqs_client.create_queue(
            QueueName=FIFO_QUEUE_NAME, Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"}
        )

        # Send SQS Message into the FIFO Queue
        op = SqsPublishOperator(**self.default_op_kwargs, sqs_queue=FIFO_QUEUE_NAME, message_group_id="abc")
        result = op.execute(mocked_context)
        assert "MD5OfMessageBody" in result
        assert "MessageId" in result

        # Validate message through moto
        message = self.sqs_client.receive_message(QueueUrl=FIFO_QUEUE_URL, AttributeNames=["MessageGroupId"])
        assert len(message["Messages"]) == 1
        assert message["Messages"][0]["MessageId"] == result["MessageId"]
        assert message["Messages"][0]["Body"] == "hello"
        assert message["Messages"][0]["Attributes"]["MessageGroupId"] == "abc"

    def test_template_fields(self):
        operator = SqsPublishOperator(
            **self.default_op_kwargs, sqs_queue=FIFO_QUEUE_NAME, message_group_id="abc"
        )
        validate_template_fields(operator)
