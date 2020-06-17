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

from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook

MOCK_DATA = {
    'query': 'SELECT * FROM company',
    'database': 'airflow',
    'outputLocation': 's3://test-airflow-demo/result',
    'client_request_token': 'C24616C0-3C49-44F9-AEF9-D68924B614B8',
    'workgroup': 'primary'
}

query_context = {
    'Database': MOCK_DATA['database']
}
result_configuration = {
    'OutputLocation': MOCK_DATA['outputLocation']
}
ATHENA_QUERY_ID = '769036e5-de4c-47f7-8a49-85297a144d33'


class TestAthenaHook(unittest.TestCase):

    def setUp(self):
        self.athena = AWSAthenaHook()

    def test_init(self):
        self.assertEqual(self.athena.sleep_time, 30)

    def test_run_query(self):
        response = self.athena.run_query(
            query='SELECT * FROM COMPANY',
            query_context={'Database': 'airflow'},
            result_configuration={'OutputLocation': 's3://test-airflow-demo/result'},
            client_request_token='C24616C0-3C49-44F9-AEF9-D68924B614B8',
            workgroup='primary')
        query_execution_id = response
        self.assertEqual(query_execution_id, ATHENA_QUERY_ID)

    def test_check_query_status(self):
        response = self.athena.run_query(
            query='SELECT * FROM COMPANY',
            query_context={'Database': 'airflow'},
            result_configuration={'OutputLocation': 's3://test-airflow-demo/result'},
            client_request_token='C24616C0-3C49-44F9-AEF9-D68924B614B8',
            workgroup='primary')
        state = self.athena.check_query_status(response)
        self.assertEqual(state, 'SUCCEEDED')

    def test_poll_query_status(self):
        response = self.athena.run_query(
            query='SELECT * FROM COMPANY',
            query_context={'Database': 'airflow'},
            result_configuration={'OutputLocation': 's3://test-airflow-demo/result'},
            client_request_token='C24616C0-3C49-44F9-AEF9-D68924B614B8',
            workgroup='primary')
        state = self.athena.poll_query_status(response)
        self.assertEqual(state, 'SUCCEEDED')

    def test_get_query_results(self):
        response = self.athena.run_query(
            query='SELECT * FROM COMPANY',
            query_context={'Database': 'airflow'},
            result_configuration={'OutputLocation': 's3://test-airflow-demo/result'},
            client_request_token='C24616C0-3C49-44F9-AEF9-D68924B614B8',
            workgroup='primary')
        result = self.athena.get_query_results(response)
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_stop_query(self):
        response = self.athena.run_query(
            query='SELECT * FROM COMPANY',
            query_context={'Database': 'airflow'},
            result_configuration={'OutputLocation': 's3://test-airflow-demo/result'},
            client_request_token='C24616C0-3C49-44F9-AEF9-D68924B614B8',
            workgroup='primary')
        response = self.athena.stop_query(response)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
