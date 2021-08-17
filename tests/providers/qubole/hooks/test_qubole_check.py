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

from airflow.providers.qubole.hooks.qubole_check import parse_first_row


class TestQuboleCheckHook(unittest.TestCase):
    def test_single_row_bool(self):
        query_result = ['true\ttrue']
        record_list = parse_first_row(query_result)
        assert [True, True] == record_list

    def test_multi_row_bool(self):
        query_result = ['true\tfalse', 'true\tfalse']
        record_list = parse_first_row(query_result)
        assert [True, False] == record_list

    def test_single_row_float(self):
        query_result = ['0.23\t34']
        record_list = parse_first_row(query_result)
        assert [0.23, 34] == record_list

    def test_single_row_mixed_types(self):
        query_result = ['name\t44\t0.23\tTrue']
        record_list = parse_first_row(query_result)
        assert ["name", 44, 0.23, True] == record_list
