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

from airflow.providers.oracle.hooks.handlers import (
    fetch_all_handler,
    fetch_one_handler,
)

from unit.oracle.test_utils import mock_oracle_lob


class TestHandlers:
    def test_fetch_all_handler(self):
        cursor = MagicMock()
        cursor.description = [("col1", "int"), ("col2", "string")]
        cursor.fetchall.return_value = [(1, mock_oracle_lob("hello"))]

        assert fetch_all_handler(cursor) == [(1, "hello")]

        cursor.description = None
        assert fetch_all_handler(cursor) is None

    def test_fetch_one_handler(self):
        cursor = MagicMock()
        cursor.description = [("col1", "int")]
        cursor.fetchone.return_value = (mock_oracle_lob("hello"),)

        assert fetch_one_handler(cursor) == ("hello",)

        cursor.description = None
        assert fetch_one_handler(cursor) is None
