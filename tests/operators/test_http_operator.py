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

from mock import patch
from airflow.operators.http_operator import SimpleHttpOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class AnyStringWith(str):
    """
    Helper class to check if a substring is a part of a string
    """
    def __eq__(self, other):
        return self in other


class SimpleHttpOpTests(unittest.TestCase):
    def setUp(self):
        # Creating a local Http connection to Airflow Webserver
        os.environ['AIRFLOW_CONN_HTTP_GOOGLE'] = 'http://www.google.com'

    def test_response_in_logs(self):
        """
        Test that when using SimpleHttpOperator with 'GET' on localhost:8080,
        the log contains 'Google' in it
        """
        operator = SimpleHttpOperator(
            task_id='test_HTTP_op',
            method='GET',
            endpoint='/',
            http_conn_id='HTTP_GOOGLE',
            log_response=True,
        )

        with patch.object(operator.log, 'info') as mock_info:
            operator.execute(None)
            mock_info.assert_called_with(AnyStringWith('Google'))
