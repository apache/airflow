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

from unittest import mock

import pytest

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.opensearch import OpenSearchHook
from airflow.providers.amazon.aws.operators.opensearch import OpenSearchQueryOperator, \
    OpenSearchAddDocumentOperator, OpenSearchCreateIndexOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime

TEST_DAG_ID = "unit_tests"
DEFAULT_DATE = datetime(2018, 1, 1)
MOCK_TEST_DATA = {
    "result": "success"
}


class TestOpenSearchQueryOperator:
    def setup_method(self):
        args = {
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        }

        self.dag = DAG(f"{TEST_DAG_ID}test_schedule_dag_once", default_args=args, schedule="@once")

        self.open_search = OpenSearchQueryOperator(
            task_id="test_opensearch_query_operator",
            index_name="test_index",
            query={
                "size": 5,
                "query": {"multi_match": {"query": "test", "fields": ["test_title^2", "test_type"]}},
            },
        )

    def test_init(self):
        assert self.open_search.task_id == "test_opensearch_query_operator"
        assert self.open_search.opensearch_conn_id == "opensearch_default"
        assert self.open_search.query["size"] == 5

    @mock.patch.object(OpenSearchHook, "search", return_value=MOCK_TEST_DATA)
    @mock.patch.object(OpenSearchHook, "get_conn")
    def test_search_query(self, mock_search):
        self.open_search.execute({})
        mock_search.assert_called_once_with(
            {"size": 5, "query": {"multi_match": {"query": "test", "fields": ["test_title^2", "test_type"]}}},
            "test_index"
        )
