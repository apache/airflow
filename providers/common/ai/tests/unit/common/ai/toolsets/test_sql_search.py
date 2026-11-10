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

import pytest

from airflow.providers.common.ai.toolsets.sql_search import KeywordTableSearchBackend

CATALOG = [
    ("orders", "Customer purchase orders"),
    ("order_items", None),
    ("users", "Registered customers and their ORDER history"),
    ("audit_log", "Security events"),
]


def _search(query: str, limit: int = 10) -> list[str]:
    result = KeywordTableSearchBackend().search_tables(query, limit, get_catalog=lambda: CATALOG)
    return [name for name, _ in result]


class TestKeywordTableSearchBackend:
    @pytest.mark.parametrize(
        ("query", "expected"),
        [
            pytest.param("ORDERS", ["orders"], id="case-insensitive"),
            pytest.param("order", ["orders", "order_items", "users"], id="name-hits-outrank-comment-hits"),
            pytest.param("customer order", ["orders", "order_items", "users"], id="multiple-words-add-up"),
            pytest.param("security", ["audit_log"], id="comment-only-match"),
            pytest.param("zzz", [], id="no-match"),
            pytest.param("   ", [], id="blank-query"),
        ],
    )
    def test_ranking(self, query, expected):
        assert _search(query) == expected

    def test_ties_break_by_name_and_limit_applies(self):
        catalog = [("b_t", None), ("a_t", None), ("c_t", None)]
        result = KeywordTableSearchBackend().search_tables("t", 2, get_catalog=lambda: catalog)
        assert result == [("a_t", None), ("b_t", None)]
