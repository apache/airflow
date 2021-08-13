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

"""
Module for all tests airflow.utils.log.json_formatter.JSONFormatter
"""
import json
import unittest
from logging import makeLogRecord

from airflow.utils.log.json_formatter import JSONFormatter, merge_dicts


class TestJSONFormatter(unittest.TestCase):
    """
    TestJSONFormatter class combine all tests for JSONFormatter
    """
    def test_json_formatter_is_not_none(self):
        """
        JSONFormatter instance  should return not none
        """
        json_fmt = JSONFormatter()
        self.assertIsNotNone(json_fmt)

    def test_merge_dicts(self):
        """
        Test _merge method from JSONFormatter
        """
        dict1 = {'a': 1, 'b': 2, 'c': 3}
        dict2 = {'a': 1, 'b': 3, 'd': 42}
        merged = merge_dicts(dict1, dict2)
        self.assertDictEqual(merged, {'a': 1, 'b': 3, 'c': 3, 'd': 42})

    def test_merge_dicts_recursive_overlap_l1(self):
        """
        Test merge_dicts with recursive dict; one level of nesting
        """
        dict1 = {'a': 1, 'r': {'a': 1, 'b': 2}}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': 0}}
        merged = merge_dicts(dict1, dict2)
        self.assertDictEqual(merged, {'a': 1, 'r': {'a': 1, 'b': 0, 'c': 3}})

    def test_merge_dicts_recursive_overlap_l2(self):
        """
        Test merge_dicts with recursive dict; two levels of nesting
        """

        dict1 = {'a': 1, 'r': {'a': 1, 'b': {'a': 1}}}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': {'b': 1}}}
        merged = merge_dicts(dict1, dict2)
        self.assertDictEqual(merged, {'a': 1, 'r': {'a': 1, 'b': {'a': 1, 'b': 1}, 'c': 3}})

    def test_merge_dicts_recursive_right_only(self):
        """
        Test merge_dicts with recursive when dict1 doesn't have any nested dict
        """
        dict1 = {'a': 1}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': 0}}
        merged = merge_dicts(dict1, dict2)
        self.assertDictEqual(merged, {'a': 1, 'r': {'b': 0, 'c': 3}})

    def test_uses_time(self):
        """
        Test usesTime method from JSONFormatter
        """
        json_fmt_asctime = JSONFormatter(json_fields=["asctime", "label"])
        json_fmt_no_asctime = JSONFormatter(json_fields=["label"])
        self.assertTrue(json_fmt_asctime.usesTime())
        self.assertFalse(json_fmt_no_asctime.usesTime())

    def test_format(self):
        """
        Test format method from JSONFormatter
        """
        log_record = makeLogRecord({"label": "value"})
        json_fmt = JSONFormatter(json_fields=["label"])
        self.assertEqual(json_fmt.format(log_record), '{"label": "value"}')

    def test_format_with_extras(self):
        """
        Test format with extras method from JSONFormatter
        """
        log_record = makeLogRecord({"label": "value"})
        json_fmt = JSONFormatter(json_fields=["label"], extras={'pod_extra': 'useful_message'})
        # compare as a dicts to not fail on sorting errors
        self.assertDictEqual(json.loads(json_fmt.format(log_record)),
                             {"label": "value", "pod_extra": "useful_message"})
