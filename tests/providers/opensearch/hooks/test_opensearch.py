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

from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook

pytestmark = pytest.mark.db_test


MOCK_SEARCH_RETURN = {"status": "test"}


class TestOpenSearchHook:
    def test_hook_search(self, mock_hook):
        self.hook = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)
        result = self.hook.search(
            index_name="testIndex",
            query={"size": 1, "query": {"multi_match": {"query": "test", "fields": ["testField"]}}},
        )

        assert result == MOCK_SEARCH_RETURN

    def test_hook_index(self, mock_hook):
        self.hook = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)
        result = self.hook.index(index_name="test_index", document={"title": "Monty Python"}, doc_id=3)
        assert result == 3
