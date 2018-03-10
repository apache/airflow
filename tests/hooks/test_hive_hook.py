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
    def test_get_max_partition_from_empty_part_names(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_names([], 'some_key')
        self.assertIsNone(max_partition)

    def test_get_max_partition_from_mal_formatted_part_names(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_names(
                ['bad_partition_name'], 'some_key')

    def test_get_max_partition_from_mal_valid_part_names(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_names(['some_key=value1',
                                                                  'some_key=value2',
                                                                  'some_key=value3'],
                                                                 'some_key')
        self.assertEqual(max_partition, 'value3')
