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

import os
import unittest

import requests_mock
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.exceptions import AirflowException
from tests.compat import mock


class SimpleHttpOpTests(unittest.TestCase):
    def setUp(self):
        os.environ['AIRFLOW_CONN_HTTP_EXAMPLE'] = 'http://www.example.com'

    @requests_mock.mock()
    def test_response_in_logs(self, m):
        """
        Test that when using SimpleHttpOperator with 'GET',
        the log contains 'Example Domain' in it
        """

        m.get('http://www.example.com', text='Example.com fake response')
        operator = SimpleHttpOperator(
            task_id='test_HTTP_op',
            method='GET',
            endpoint='/',
            http_conn_id='HTTP_EXAMPLE',
            log_response=True,
        )

        with mock.patch.object(operator.log, 'info') as mock_info:
            operator.execute(None)
            mock_info.assert_called_with('Example.com fake response')

    @requests_mock.mock()
    def test_response_in_logs_after_failed_check(self, m):
        """
        Test that when using SimpleHttpOperator with log_response=True,
        the reponse is logged even if request_check fails
        """

        def response_check(response):
            return response.text != 'invalid response'

        m.get('http://www.example.com', text='invalid response')
        operator = SimpleHttpOperator(
            task_id='test_HTTP_op',
            method='GET',
            endpoint='/',
            http_conn_id='HTTP_EXAMPLE',
            log_response=True,
            response_check=response_check
        )

        with mock.patch.object(operator.log, 'info') as mock_info:
            self.assertRaises(AirflowException, operator.execute, None)
            mock_info.assert_called_with('invalid response')
