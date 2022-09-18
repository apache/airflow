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

import unittest
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from moto import mock_sqs

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2019, 1, 1)

QUEUE_NAME = 'test-queue'
QUEUE_URL = f'https://{QUEUE_NAME}'

FIFO_QUEUE_NAME = 'test-queue.fifo'
FIFO_QUEUE_URL = f'https://{FIFO_QUEUE_NAME}'


class TestSqsPublishOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

        self.dag = DAG('test_dag_id', default_args=args)
        self.operator = SqsPublishOperator(
            task_id='test_task',
            dag=self.dag,
            sqs_queue=QUEUE_URL,
            message_content='hello',
            aws_conn_id='aws_default',
        )

        self.mock_context = MagicMock()
        self.sqs_hook = SqsHook()

    @mock_sqs
    def test_execute_success(self):
        self.sqs_hook.create_queue(QUEUE_NAME)

        result = self.operator.execute(self.mock_context)
        assert 'MD5OfMessageBody' in result
        assert 'MessageId' in result

        message = self.sqs_hook.get_conn().receive_message(QueueUrl=QUEUE_URL)

        assert len(message['Messages']) == 1
        assert message['Messages'][0]['MessageId'] == result['MessageId']
        assert message['Messages'][0]['Body'] == 'hello'

        context_calls = []

        assert self.mock_context['ti'].method_calls == context_calls, "context call  should be same"

    @mock_sqs
    def test_execute_failure_fifo_queue(self):
        self.operator.sqs_queue = FIFO_QUEUE_URL
        self.sqs_hook.create_queue(FIFO_QUEUE_NAME, attributes={'FifoQueue': 'true'})
        with pytest.raises(ClientError) as ctx:
            self.operator.execute(self.mock_context)
        err_msg = (
            "An error occurred (MissingParameter) when calling the SendMessage operation: The request must "
            "contain the parameter MessageGroupId."
        )
        assert err_msg == str(ctx.value)

    @mock_sqs
    def test_execute_success_fifo_queue(self):
        self.operator.sqs_queue = FIFO_QUEUE_URL
        self.operator.message_group_id = "abc"
        self.sqs_hook.create_queue(FIFO_QUEUE_NAME, attributes={'FifoQueue': 'true'})
        result = self.operator.execute(self.mock_context)
        assert 'MD5OfMessageBody' in result
        assert 'MessageId' in result
        message = self.sqs_hook.get_conn().receive_message(
            QueueUrl=FIFO_QUEUE_URL, AttributeNames=['MessageGroupId']
        )
        assert len(message['Messages']) == 1
        assert message['Messages'][0]['MessageId'] == result['MessageId']
        assert message['Messages'][0]['Body'] == 'hello'
        assert message['Messages'][0]['Attributes']['MessageGroupId'] == 'abc'
