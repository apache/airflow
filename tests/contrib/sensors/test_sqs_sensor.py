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
from airflow import DAG, configuration
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.utils import timezone
from mock import patch, call, MagicMock
from airflow.exceptions import AirflowException

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSQSSensor(unittest.TestCase):

    @patch('airflow.contrib.sensors.aws_sqs_sensor.SQSHook')
    def setUp(self, mock_sqs_hook):
        configuration.load_test_config()

        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.dag = DAG('test_dag_id', default_args=args)
        self.mock_sqs_hook = mock_sqs_hook
        self.sensor = SQSSensor(
            task_id='test_task',
            dag=self.dag,
            sqs_queue='test',
            aws_conn_id='aws_default'
        )

        self.mock_context = MagicMock()

    def test_poke_success(self):
        message = {'Messages': [{'MessageId': 'c585e508-2ea0-44c7-bf3e-d1ba0cb87834',
                                 'ReceiptHandle': 'mockHandle',
                                 'MD5OfBody': 'e5a9d8684a8edfed460b8d42fd28842f',
                                 'Body': 'h21'}],
                   'ResponseMetadata': {'RequestId': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                                        'HTTPStatusCode': 200,
                                        'HTTPHeaders': {
                                            'x-amzn-requestid': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                                            'date': 'Mon, 18 Feb 2019 18:41:52 GMT',
                                            'content-type': 'text/xml', 'mock_sqs_hook-length': '830'},
                                        'RetryAttempts': 0}}

        self.mock_sqs_hook().get_conn().delete_message_batch.return_value = \
            {'Successful': [{'Id': '22f67273-4dbc-4c19-83b5-aee71bfeb832'}]}
        self.mock_sqs_hook().get_conn().receive_message.return_value = message

        result = self.sensor.poke(self.mock_context)
        self.assertTrue(result)

        context_calls = [call.xcom_push(key='messages', value=message)]

        self.assertTrue(self.mock_context['ti'].method_calls == context_calls, "context call  should be same")

    def test_poke_no_messsage_failed(self):
        message = {
            'ResponseMetadata': {'RequestId': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                                 'HTTPStatusCode': 200,
                                 'HTTPHeaders': {
                                     'x-amzn-requestid': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                                     'date': 'Mon, 18 Feb 2019 18:41:52 GMT',
                                     'content-type': 'text/xml', 'mock_sqs_hook-length': '830'},
                                 'RetryAttempts': 0}}
        self.mock_sqs_hook().get_conn().receive_message.return_value = message

        result = self.sensor.poke(self.mock_context)
        self.assertFalse(result)

        context_calls = []

        self.assertTrue(self.mock_context['ti'].method_calls == context_calls, "context call  should be same")

    def test_poke_messsage_array_empty_failed(self):
        message = {'Messages': [],
                   'ResponseMetadata': {'RequestId': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                                        'HTTPStatusCode': 200,
                                        'HTTPHeaders': {
                                            'x-amzn-requestid': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                                            'date': 'Mon, 18 Feb 2019 18:41:52 GMT',
                                            'content-type': 'text/xml', 'mock_sqs_hook-length': '830'},
                                        'RetryAttempts': 0}}
        self.mock_sqs_hook().get_conn().receive_message.return_value = message

        result = self.sensor.poke(self.mock_context)
        self.assertFalse(result)

        context_calls = []

        self.assertTrue(self.mock_context['ti'].method_calls == context_calls, "context call  should be same")

    def test_poke_delete_raise_airflow_exception(self):
        message = {'Messages': [{'MessageId': 'c585e508-2ea0-44c7-bf3e-d1ba0cb87834',
                                 'ReceiptHandle': 'mockHandle',
                                 'MD5OfBody': 'e5a9d8684a8edfed460b8d42fd28842f',
                                 'Body': 'h21'}],
                   'ResponseMetadata': {'RequestId': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                                        'HTTPStatusCode': 200,
                                        'HTTPHeaders': {
                                            'x-amzn-requestid': '56cbf4aa-f4ef-5518-9574-a04e0a5f1411',
                                            'date': 'Mon, 18 Feb 2019 18:41:52 GMT',
                                            'content-type': 'text/xml', 'mock_sqs_hook-length': '830'},
                                        'RetryAttempts': 0}}
        self.mock_sqs_hook().get_conn().receive_message.return_value = message
        self.mock_sqs_hook().get_conn().delete_message_batch.return_value = \
            {'Failed': [{'Id': '22f67273-4dbc-4c19-83b5-aee71bfeb832'}]}

        with self.assertRaises(AirflowException) as context:
            self.sensor.poke(self.mock_context)

        self.assertTrue('Delete SQS Messages failed' in context.exception.args[0])

    def test_poke_receive_raise_exception(self):
        self.mock_sqs_hook().get_conn().receive_message.return_value = Exception('test exception')

        result = self.sensor.poke(self.mock_context)

        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
