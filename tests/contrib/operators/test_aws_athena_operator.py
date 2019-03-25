# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import unittest

from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.contrib.hooks.aws_athena_hook import AWSAthenaHook
from airflow import configuration

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

MOCK_DATA = {
    'task_id': 'test_aws_athena_operator',
    'query': 'SELECT * FROM TEST_TABLE',
    'database': 'TEST_DATABASE',
    'outputLocation': 's3://test_s3_bucket/',
    'client_request_token': 'eac427d0-1c6d-4dfb-96aa-2835d3ac6595'
}

query_context = {
    'Database': MOCK_DATA['database']
}
result_configuration = {
    'OutputLocation': MOCK_DATA['outputLocation']
}


class TestAWSAthenaOperator(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

        self.athena = AWSAthenaOperator(task_id='test_aws_athena_operator', query='SELECT * FROM TEST_TABLE',
                                        database='TEST_DATABASE', output_location='s3://test_s3_bucket/',
                                        client_request_token='eac427d0-1c6d-4dfb-96aa-2835d3ac6595',
                                        sleep_time=1, max_tries=3)

    def test_init(self):
        self.assertEqual(self.athena.task_id, MOCK_DATA['task_id'])
        self.assertEqual(self.athena.query, MOCK_DATA['query'])
        self.assertEqual(self.athena.database, MOCK_DATA['database'])
        self.assertEqual(self.athena.aws_conn_id, 'aws_default')
        self.assertEqual(self.athena.client_request_token, MOCK_DATA['client_request_token'])
        self.assertEqual(self.athena.sleep_time, 1)

    @mock.patch.object(AWSAthenaHook, 'check_query_status', side_effect=("SUCCESS",))
    @mock.patch.object(AWSAthenaHook, 'run_query', return_value='1234')
    @mock.patch.object(AWSAthenaHook, 'get_conn')
    def test_hook_run_small_success_query(self, mock_conn, mock_run_query, mock_check_query_status):
        self.athena.execute(None)
        mock_run_query.assert_called_once_with(MOCK_DATA['query'], query_context, result_configuration,
                                               MOCK_DATA['client_request_token'])
        self.assertEqual(mock_check_query_status.call_count, 1)

    @mock.patch.object(AWSAthenaHook, 'check_query_status', side_effect=("RUNNING", "RUNNING", "SUCCESS",))
    @mock.patch.object(AWSAthenaHook, 'run_query', return_value='1234')
    @mock.patch.object(AWSAthenaHook, 'get_conn')
    def test_hook_run_big_success_query(self, mock_conn, mock_run_query, mock_check_query_status):
        self.athena.execute(None)
        mock_run_query.assert_called_once_with(MOCK_DATA['query'], query_context, result_configuration,
                                               MOCK_DATA['client_request_token'])
        self.assertEqual(mock_check_query_status.call_count, 3)

    @mock.patch.object(AWSAthenaHook, 'check_query_status', side_effect=(None, None,))
    @mock.patch.object(AWSAthenaHook, 'run_query', return_value='1234')
    @mock.patch.object(AWSAthenaHook, 'get_conn')
    def test_hook_run_failed_query_with_none(self, mock_conn, mock_run_query, mock_check_query_status):
        with self.assertRaises(Exception):
            self.athena.execute(None)
        mock_run_query.assert_called_once_with(MOCK_DATA['query'], query_context, result_configuration,
                                               MOCK_DATA['client_request_token'])
        self.assertEqual(mock_check_query_status.call_count, 3)

    @mock.patch.object(AWSAthenaHook, 'check_query_status', side_effect=("RUNNING", "FAILED",))
    @mock.patch.object(AWSAthenaHook, 'run_query', return_value='1234')
    @mock.patch.object(AWSAthenaHook, 'get_conn')
    def test_hook_run_failure_query(self, mock_conn, mock_run_query, mock_check_query_status):
        with self.assertRaises(Exception):
            self.athena.execute(None)
        mock_run_query.assert_called_once_with(MOCK_DATA['query'], query_context, result_configuration,
                                               MOCK_DATA['client_request_token'])
        self.assertEqual(mock_check_query_status.call_count, 2)

    @mock.patch.object(AWSAthenaHook, 'check_query_status', side_effect=("RUNNING", "RUNNING", "CANCELLED",))
    @mock.patch.object(AWSAthenaHook, 'run_query', return_value='1234')
    @mock.patch.object(AWSAthenaHook, 'get_conn')
    def test_hook_run_cancelled_query(self, mock_conn, mock_run_query, mock_check_query_status):
        with self.assertRaises(Exception):
            self.athena.execute(None)
        mock_run_query.assert_called_once_with(MOCK_DATA['query'], query_context, result_configuration,
                                               MOCK_DATA['client_request_token'])
        self.assertEqual(mock_check_query_status.call_count, 3)

    @mock.patch.object(AWSAthenaHook, 'check_query_status', side_effect=("RUNNING", "RUNNING", "RUNNING",))
    @mock.patch.object(AWSAthenaHook, 'run_query', return_value='1234')
    @mock.patch.object(AWSAthenaHook, 'get_conn')
    def test_hook_run_failed_query_with_max_tries(self, mock_conn, mock_run_query, mock_check_query_status):
        with self.assertRaises(Exception):
            self.athena.execute(None)
        mock_run_query.assert_called_once_with(MOCK_DATA['query'], query_context, result_configuration,
                                               MOCK_DATA['client_request_token'])
        self.assertEqual(mock_check_query_status.call_count, 3)


if __name__ == '__main__':
    unittest.main()
