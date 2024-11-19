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
from __future__ import annotations

from unittest.mock import MagicMock

from airflow.providers.common.sql.hooks.handlers import (
    fetch_all_handler,
    fetch_one_handler,
    return_single_query_results,
)


class TestHandlers:
    def test_return_single_query_results(self):
        assert return_single_query_results("SELECT 1", return_last=True, split_statements=False)
        assert return_single_query_results("SELECT 1", return_last=False, split_statements=False)
        assert return_single_query_results(["SELECT 1"], return_last=True, split_statements=False) is False
        assert return_single_query_results(["SELECT 1"], return_last=False, split_statements=False) is False
        assert return_single_query_results("SELECT 1", return_last=False, split_statements=True) is False
        assert return_single_query_results(["SELECT 1"], return_last=False, split_statements=True) is False
        assert return_single_query_results(["SELECT 1"], return_last=True, split_statements=True) is False

    def test_fetch_all_handler(self):
        cursor = MagicMock()
        cursor.description = [("col1", "int"), ("col2", "string")]
        cursor.fetchall.return_value = [(1, "hello")]

        assert fetch_all_handler(cursor) == [(1, "hello")]

        cursor.description = None
        assert fetch_all_handler(cursor) is None

    def test_fetch_one_handler(self):
        cursor = MagicMock()
        cursor.description = [("col1", "int")]
        cursor.fetchone.return_value = 1

        assert fetch_one_handler(cursor) == (1)

        cursor.description = None
        assert fetch_one_handler(cursor) is None
