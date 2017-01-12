# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import requests

from airflow.contrib.operators.jira_operator import \
  JIRATransitionOperator
from airflow.exceptions import AirflowException
from airflow import configuration

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class JIRATransitionOperatorTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('requests.request')
    def test_execute(self, request_mock):
        resp = requests.Response()
        resp.status_code = 200
        request_mock.return_value = resp

        operator = JIRATransitionOperator(
            task_id='jira_execute_success',
            api_user='username',
            api_password="password",
            endpoint="urlofjira",
            ticket_id="DE-3",
            transition_id="5"
        )

        operator.execute(None)

    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('requests.request')
    def test_execute_error_response(self, request_mock):
        resp = requests.Response()
        resp.status_code = 404
        resp.reason = 'Not Found'
        request_mock.return_value = resp

        operator = JIRATransitionOperator(
            task_id='jira_execute_error',
            api_user='username',
            api_password="password",
            endpoint="urlofjira",
            ticket_id="DE-3",
            transition_id="5"
        )

        with self.assertRaises(AirflowException):
            operator.execute(None)


if __name__ == '__main__':
    unittest.main()
