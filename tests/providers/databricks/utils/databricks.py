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
#

import unittest

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import RunState
from airflow.providers.databricks.utils.databricks import deep_string_coerce, validate_trigger_event

RUN_ID = 1
RUN_PAGE_URL = 'run-page-url'


class TestDatabricksOperatorSharedFunctions(unittest.TestCase):
    def test_deep_string_coerce(self):
        test_json = {
            'test_int': 1,
            'test_float': 1.0,
            'test_dict': {'key': 'value'},
            'test_list': [1, 1.0, 'a', 'b'],
            'test_tuple': (1, 1.0, 'a', 'b'),
        }

        expected = {
            'test_int': '1',
            'test_float': '1.0',
            'test_dict': {'key': 'value'},
            'test_list': ['1', '1.0', 'a', 'b'],
            'test_tuple': ['1', '1.0', 'a', 'b'],
        }
        assert deep_string_coerce(test_json) == expected

    def test_validate_trigger_event_success(self):
        event = {
            'run_id': RUN_ID,
            'run_page_url': RUN_PAGE_URL,
            'run_state': RunState('TERMINATED', 'SUCCESS', '').to_json(),
        }
        self.assertIsNone(validate_trigger_event(event))

    def test_validate_trigger_event_failure(self):
        event = {}
        with pytest.raises(AirflowException):
            validate_trigger_event(event)
