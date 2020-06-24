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
from unittest import mock

from airflow.api_connexion.exceptions import BadRequest
from airflow.api_connexion.parameters import check_limit, format_parameters
from tests.test_utils.config import conf_vars


class TestMaximumPagelimit(unittest.TestCase):

    @conf_vars({("api", "maximum_page_limit"): "320"})
    def test_maximum_limit_return_val(self):
        limit = check_limit(300)
        self.assertEqual(limit, 300)

    @conf_vars({("api", "maximum_page_limit"): "320"})
    def test_maximum_limit_returns_configured_if_limit_above_conf(self):
        limit = check_limit(350)
        self.assertEqual(limit, 320)

    @conf_vars({("api", "maximum_page_limit"): "1000"})
    def test_limit_returns_set_max_if_give_limit_is_exceeded(self):
        limit = check_limit(1500)
        self.assertEqual(limit, 1000)

    @conf_vars({("api", "fallback_page_limit"): "100"})
    def test_limit_of_zero_returns_default(self):
        limit = check_limit(0)
        self.assertEqual(limit, 100)

    @conf_vars({("api", "maximum_page_limit"): "1500"})
    def test_negative_limit_raises(self):
        with self.assertRaises(BadRequest):
            check_limit(-1)


class TestFormatParameters(unittest.TestCase):

    @conf_vars({("api", "maximum_page_limit"): "100"})
    def test_should_work_with_limit(self):
        decorator = format_parameters({"limit": check_limit})
        endpoint = mock.MagicMock()
        decorated_endpoint = decorator(endpoint)
        decorated_endpoint(limit=89)
        endpoint.assert_called_once_with(limit=89)
