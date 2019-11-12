# -*- coding: utf-8 -*-
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


import unittest
from unittest.mock import MagicMock

from moto import mock_sqs

from airflow import DAG
from airflow.providers.amazon.aws.hooks.sqs import SQSHook
from airflow.providers.amazon.aws.operators.sqs import SQSPublishOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2019, 1, 1)


class TestSQSPublishOperator(unittest.TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.dag = DAG('test_dag_id', default_args=args)
        self.operator = SQSPublishOperator(
            task_id='test_task',
            dag=self.dag,
            sqs_queue='test',
            message_content='hello',
            aws_conn_id='aws_default'
        )

        self.mock_context = MagicMock()
        self.sqs_hook = SQSHook()

    @mock_sqs
    def test_execute_success(self):
        self.sqs_hook.create_queue('test')

        result = self.operator.execute(self.mock_context)
        self.assertTrue('MD5OfMessageBody' in result)
        self.assertTrue('MessageId' in result)

        message = self.sqs_hook.get_conn().receive_message(QueueUrl='test')

        self.assertEqual(len(message['Messages']), 1)
        self.assertEqual(message['Messages'][0]['MessageId'], result['MessageId'])
        self.assertEqual(message['Messages'][0]['Body'], 'hello')

        context_calls = []

        self.assertTrue(self.mock_context['ti'].method_calls == context_calls, "context call  should be same")


if __name__ == '__main__':
    unittest.main()
