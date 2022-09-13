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
import unittest
from unittest import mock

import pytest
from moto import mock_sqs

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

QUEUE_NAME = 'test-queue'
QUEUE_URL = f'https://{QUEUE_NAME}'


class TestSqsSensor(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

        self.dag = DAG('test_dag_id', default_args=args)
        self.sensor = SqsSensor(
            task_id='test_task', dag=self.dag, sqs_queue=QUEUE_URL, aws_conn_id='aws_default'
        )

        self.mock_context = mock.MagicMock()
        self.sqs_hook = SqsHook()

    @mock_sqs
    def test_poke_success(self):
        self.sqs_hook.create_queue(QUEUE_NAME)
        self.sqs_hook.send_message(queue_url=QUEUE_URL, message_body='hello')

        result = self.sensor.poke(self.mock_context)
        assert result

        assert "'Body': 'hello'" in str(
            self.mock_context['ti'].method_calls
        ), "context call should contain message hello"

    @mock_sqs
    def test_poke_no_message_failed(self):

        self.sqs_hook.create_queue(QUEUE_NAME)
        result = self.sensor.poke(self.mock_context)
        assert not result

        context_calls = []

        assert self.mock_context['ti'].method_calls == context_calls, "context call  should be same"

    @mock.patch.object(SqsHook, 'get_conn')
    def test_poke_delete_raise_airflow_exception(self, mock_conn):
        message = {
            'Messages': [
                {
                    'MessageId': 'c585e508-2ea0-44c7-bf3e-d1ba0cb87834',
                    'ReceiptHandle': 'mockHandle',
                    'MD5OfBody': 'e5a9d8684a8edfed460b8d42fd28842f',
                    'Body': 'h21',
                }
            ],
            'ResponseMetadata': {
                'RequestId': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amzn-requestid': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                    'date': 'Mon, 18 Feb 2019 18:41:52 GMT',
                    'content-type': 'text/xml',
                    'mock_sqs_hook-length': '830',
                },
                'RetryAttempts': 0,
            },
        }
        mock_conn.return_value.receive_message.return_value = message
        mock_conn.return_value.delete_message_batch.return_value = {
            'Failed': [{'Id': '22f67273-4dbc-4c19-83b5-aee71bfeb832'}]
        }

        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(self.mock_context)

        assert 'Delete SQS Messages failed' in ctx.value.args[0]

    @mock.patch.object(SqsHook, 'get_conn')
    def test_poke_receive_raise_exception(self, mock_conn):
        mock_conn.return_value.receive_message.side_effect = Exception('test exception')
        with pytest.raises(Exception) as ctx:
            self.sensor.poke(self.mock_context)

        assert 'test exception' in ctx.value.args[0]

    @mock.patch.object(SqsHook, 'get_conn')
    def test_poke_visibility_timeout(self, mock_conn):
        # Check without visibility_timeout parameter
        self.sqs_hook.create_queue(QUEUE_NAME)
        self.sqs_hook.send_message(queue_url=QUEUE_URL, message_body='hello')

        self.sensor.poke(self.mock_context)

        calls_receive_message = [
            mock.call().receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=5, WaitTimeSeconds=1)
        ]
        mock_conn.assert_has_calls(calls_receive_message)
        # Check with visibility_timeout parameter
        self.sensor = SqsSensor(
            task_id='test_task2',
            dag=self.dag,
            sqs_queue=QUEUE_URL,
            aws_conn_id='aws_default',
            visibility_timeout=42,
        )
        self.sensor.poke(self.mock_context)

        calls_receive_message = [
            mock.call().receive_message(
                QueueUrl=QUEUE_URL, MaxNumberOfMessages=5, WaitTimeSeconds=1, VisibilityTimeout=42
            )
        ]
        mock_conn.assert_has_calls(calls_receive_message)

    @mock_sqs
    def test_poke_message_invalid_filtering(self):
        self.sqs_hook.create_queue(QUEUE_NAME)
        self.sqs_hook.send_message(queue_url=QUEUE_URL, message_body='hello')
        sensor = SqsSensor(
            task_id='test_task2',
            dag=self.dag,
            sqs_queue=QUEUE_URL,
            aws_conn_id='aws_default',
            message_filtering='invalid_option',
        )
        with pytest.raises(NotImplementedError) as ctx:
            sensor.poke(self.mock_context)
        assert 'Override this method to define custom filters' in ctx.value.args[0]

    @mock.patch.object(SqsHook, "get_conn")
    def test_poke_message_filtering_literal_values(self, mock_conn):
        self.sqs_hook.create_queue(QUEUE_NAME)
        matching = [{"id": 11, "body": "a matching message"}]
        non_matching = [{"id": 12, "body": "a non-matching message"}]
        all = matching + non_matching

        def mock_receive_message(**kwargs):
            messages = []
            for message in all:
                messages.append(
                    {
                        'MessageId': message['id'],
                        'ReceiptHandle': 100 + message['id'],
                        'Body': message['body'],
                    }
                )
            return {'Messages': messages}

        mock_conn.return_value.receive_message.side_effect = mock_receive_message

        def mock_delete_message_batch(**kwargs):
            return {'Successful'}

        mock_conn.return_value.delete_message_batch.side_effect = mock_delete_message_batch

        # Test that messages are filtered
        self.sensor.message_filtering = 'literal'
        self.sensor.message_filtering_match_values = ["a matching message"]
        result = self.sensor.poke(self.mock_context)
        assert result

        # Test that only filtered messages are deleted
        delete_entries = [{'Id': x['id'], 'ReceiptHandle': 100 + x['id']} for x in matching]
        calls_delete_message_batch = [
            mock.call().delete_message_batch(QueueUrl=QUEUE_URL, Entries=delete_entries)
        ]
        mock_conn.assert_has_calls(calls_delete_message_batch)

    @mock.patch.object(SqsHook, "get_conn")
    def test_poke_message_filtering_jsonpath(self, mock_conn):
        self.sqs_hook.create_queue(QUEUE_NAME)
        matching = [
            {"id": 11, "key": {"matches": [1, 2]}},
            {"id": 12, "key": {"matches": [3, 4, 5]}},
            {"id": 13, "key": {"matches": [10]}},
        ]
        non_matching = [
            {"id": 14, "key": {"nope": [5, 6]}},
            {"id": 15, "key": {"nope": [7, 8]}},
        ]
        all = matching + non_matching

        def mock_receive_message(**kwargs):
            messages = []
            for message in all:
                messages.append(
                    {
                        'MessageId': message['id'],
                        'ReceiptHandle': 100 + message['id'],
                        'Body': json.dumps(message),
                    }
                )
            return {'Messages': messages}

        mock_conn.return_value.receive_message.side_effect = mock_receive_message

        def mock_delete_message_batch(**kwargs):
            return {'Successful'}

        mock_conn.return_value.delete_message_batch.side_effect = mock_delete_message_batch

        # Test that messages are filtered
        self.sensor.message_filtering = 'jsonpath'
        self.sensor.message_filtering_config = 'key.matches[*]'
        result = self.sensor.poke(self.mock_context)
        assert result

        # Test that only filtered messages are deleted
        delete_entries = [{'Id': x['id'], 'ReceiptHandle': 100 + x['id']} for x in matching]
        calls_delete_message_batch = [
            mock.call().delete_message_batch(QueueUrl=QUEUE_URL, Entries=delete_entries)
        ]
        mock_conn.assert_has_calls(calls_delete_message_batch)

    @mock.patch.object(SqsHook, "get_conn")
    def test_poke_message_filtering_jsonpath_values(self, mock_conn):
        self.sqs_hook.create_queue(QUEUE_NAME)
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
        all = matching + non_matching

        def mock_receive_message(**kwargs):
            messages = []
            for message in all:
                messages.append(
                    {
                        'MessageId': message['id'],
                        'ReceiptHandle': 100 + message['id'],
                        'Body': json.dumps(message),
                    }
                )
            return {'Messages': messages}

        mock_conn.return_value.receive_message.side_effect = mock_receive_message

        def mock_delete_message_batch(**kwargs):
            return {'Successful'}

        mock_conn.return_value.delete_message_batch.side_effect = mock_delete_message_batch

        # Test that messages are filtered
        self.sensor.message_filtering = 'jsonpath'
        self.sensor.message_filtering_config = 'key.matches[*]'
        self.sensor.message_filtering_match_values = [1, 4]
        result = self.sensor.poke(self.mock_context)
        assert result

        # Test that only filtered messages are deleted
        delete_entries = [{'Id': x['id'], 'ReceiptHandle': 100 + x['id']} for x in matching]
        calls_delete_message_batch = [
            mock.call().delete_message_batch(QueueUrl='https://test-queue', Entries=delete_entries)
        ]
        mock_conn.assert_has_calls(calls_delete_message_batch)

    @mock.patch.object(SqsHook, "get_conn")
    def test_poke_do_not_delete_message_on_received(self, mock_conn):

        self.sqs_hook.create_queue(QUEUE_NAME)
        self.sqs_hook.send_message(queue_url=QUEUE_URL, message_body='hello')

        self.sensor = SqsSensor(
            task_id='test_task2',
            dag=self.dag,
            sqs_queue=QUEUE_URL,
            aws_conn_id='aws_default',
            # do not delete message upon reception
            delete_message_on_reception=False,
        )
        self.sensor.poke(self.mock_context)
        assert mock_conn.delete_message_batch.called is False

    @mock_sqs
    def test_poke_batch_messages(self):
        messages = ["hello", "brave", "world"]

        self.sqs_hook.create_queue(QUEUE_NAME)
        # Do publish 3 messages
        for message in messages:
            self.sqs_hook.send_message(queue_url=QUEUE_URL, message_body=message)

        # Init batch sensor to get 1 message for each SQS poll
        # and perform 3 polls
        self.sensor = SqsSensor(
            task_id='test_task3',
            dag=self.dag,
            sqs_queue=QUEUE_URL,
            aws_conn_id='aws_default',
            max_messages=1,
            num_batches=3,
        )
        result = self.sensor.poke(self.mock_context)
        assert result

        # expect all messages are retrieved
        for message in messages:
            assert f"'Body': '{message}'" in str(
                self.mock_context['ti'].method_calls
            ), "context call should contain message '{message}'"
