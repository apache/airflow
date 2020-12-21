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

from airflow.providers.qubole.hooks.qubole import QuboleHook

add_tags = QuboleHook._add_tags


class TestQuboleHook(unittest.TestCase):
    def test_add_string_to_tags(self):
        tags = {'dag_id', 'task_id'}
        add_tags(tags, 'string')
        self.assertEqual({'dag_id', 'task_id', 'string'}, tags)

    def test_add_list_to_tags(self):
        tags = {'dag_id', 'task_id'}
        add_tags(tags, ['value1', 'value2'])
        self.assertEqual({'dag_id', 'task_id', 'value1', 'value2'}, tags)

    def test_add_tuple_to_tags(self):
        tags = {'dag_id', 'task_id'}
        add_tags(tags, ('value1', 'value2'))
        self.assertEqual({'dag_id', 'task_id', 'value1', 'value2'}, tags)
