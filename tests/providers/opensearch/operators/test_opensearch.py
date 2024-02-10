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
from opensearchpy import Document, Keyword, Text

from airflow.models import DAG
from airflow.providers.opensearch.operators.opensearch import (
    OpenSearchAddDocumentOperator,
    OpenSearchCreateIndexOperator,
    OpenSearchQueryOperator,
)
from airflow.utils.timezone import datetime

pytestmark = pytest.mark.db_test


TEST_DAG_ID = "unit_tests"
DEFAULT_DATE = datetime(2018, 1, 1)
EXPECTED_SEARCH_RETURN = {"status": "test"}


class FakeDocument(Document):
    # TODO: FIXME - this Fake document has something tricky about typing
    title = Text(fields={"raw": Keyword()})  # type: ignore[call-arg]
    author = Text()
    published = Text()

    def save(self, **kwargs):
        return super().save(**kwargs)


@pytest.fixture
def dag_setup():
    return DAG(
        f"{TEST_DAG_ID}test_schedule_dag_once",
        default_args={
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        },
        schedule="@once",
    )


class TestOpenSearchQueryOperator:
    def setup_method(self, dag_setup):
        self.dag = dag_setup

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

    def test_search_query(self, mock_hook):
        result = self.open_search.execute({})
        assert result == EXPECTED_SEARCH_RETURN


class TestOpenSearchCreateIndexOperator:
    # This test does not test execute logic because there is only a redirect to the OpenSearch
    # client.
    def setup_method(self, dag_setup):
        self.dag = dag_setup

        self.open_search = OpenSearchCreateIndexOperator(
            task_id="test_opensearch_query_operator", index_name="test_index", index_body={"test": 1}
        )

    def test_init(self):
        assert self.open_search.task_id == "test_opensearch_query_operator"
        assert self.open_search.opensearch_conn_id == "opensearch_default"
        assert self.open_search.index_name == "test_index"


class TestOpenSearchAddDocumentOperator:
    def setup_method(self, dag_setup):
        self.dag = dag_setup

        self.open_search = OpenSearchAddDocumentOperator(
            task_id="test_opensearch_doc_operator",
            index_name="test_index",
            document={"title": "Monty Python"},
            doc_id=1,
        )

        self.open_search_with_doc_class = OpenSearchAddDocumentOperator(
            task_id="test_opensearch_doc_class_operator",
            doc_class=FakeDocument(meta={"id": 2}, title="Hamlet", author="Shakespeare", published="1299"),
        )

    def test_init_with_args(self):
        assert self.open_search.task_id == "test_opensearch_doc_operator"
        assert self.open_search.opensearch_conn_id == "opensearch_default"
        assert self.open_search.index_name == "test_index"

    def test_init_with_class(self):
        # This operator uses the OpenSearch client method directly, testing here just
        # confirming that the object is an instance of the class.
        assert isinstance(self.open_search_with_doc_class.doc_class, FakeDocument)
        assert self.open_search_with_doc_class.task_id == "test_opensearch_doc_class_operator"

    def test_add_document_using_args(self, mock_hook):
        result = self.open_search.execute({})
        assert result == 1
