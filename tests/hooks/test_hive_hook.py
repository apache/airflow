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
#

import unittest

from airflow.exceptions import AirflowException
from airflow.hooks.hive_hooks import HiveMetastoreHook


class TestHiveMetastoreHook(unittest.TestCase):
    VALID_FILTER_MAP = {'key2': 'value2'}

    def test_get_max_partition_from_empty_part_specs(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs([],
                                                                 'key1',
                                                                 self.VALID_FILTER_MAP)
        self.assertIsNone(max_partition)

    def test_get_max_partition_from_valid_part_specs_and_invalid_filter_map(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                {'key3': 'value5'})

    def test_get_max_partition_from_valid_part_specs_and_invalid_partition_key(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key3',
                self.VALID_FILTER_MAP)

    def test_get_max_partition_from_valid_part_specs_and_none_partition_key(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                None,
                self.VALID_FILTER_MAP)

    def test_get_max_partition_from_valid_part_specs_and_none_filter_map(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                None)

        # No partition will be filtered out.
        self.assertEqual(max_partition, b'value3')

    def test_get_max_partition_from_valid_part_specs(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                self.VALID_FILTER_MAP)
        self.assertEqual(max_partition, b'value1')
