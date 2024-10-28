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

from typing import Any

import pytest

from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.utils import db

try:
    from opensearchpy import OpenSearch

    from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook
except ImportError:
    OpenSearch = None  # type: ignore[assignment, misc]
    OpenSearchHook = BaseHook  # type: ignore[assignment,misc]

# TODO: FIXME - those Mocks have overrides that are not used but they also do not make Mypy Happy
# mypy: disable-error-code="override"

MOCK_RETURN = {"status": "test"}


class MockSearch(OpenSearchHook):
    # Mock class to override the Hook for monkeypatching
    def client(self) -> OpenSearch:
        return OpenSearch()

    def search(self, query: dict, index_name: str, **kwargs: Any) -> Any:
        return MOCK_RETURN

    def index(self, document: dict, index_name: str, doc_id: int, **kwargs: Any) -> Any:
        return doc_id


class MockClient:
    def count(self, index: Any = None, body: Any = None):
        return {
            "count": 1,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        }

    def search(self, index=None, body=None, sort=None, size=None, from_=None):
        return self.sample_log_response()

    def sample_log_response(self):
        return {
            "_shards": {"failed": 0, "skipped": 0, "successful": 7, "total": 7},
            "hits": {
                "hits": [
                    {
                        "_id": "jdeZT4kBjAZqZnexVUxk",
                        "_index": ".ds-filebeat-8.8.2-2023.07.09-000001",
                        "_score": 2.482621,
                        "_source": {
                            "@timestamp": "2023-07-13T14:13:15.140Z",
                            "asctime": "2023-07-09T07:47:43.907+0000",
                            "container": {"id": "airflow"},
                            "dag_id": "example_bash_operator",
                            "ecs": {"version": "8.0.0"},
                            "execution_date": "2023_07_09T07_47_32_000000",
                            "filename": "taskinstance.py",
                            "input": {"type": "log"},
                            "levelname": "INFO",
                            "lineno": 1144,
                            "log": {
                                "file": {
                                    "path": "/opt/airflow/Documents/GitHub/airflow/logs/"
                                    "dag_id=example_bash_operator'"
                                    "/run_id=owen_run_run/task_id=run_after_loop/attempt=1.log"
                                },
                                "offset": 0,
                            },
                            "offset": 1688888863907337472,
                            "log_id": "example_bash_operator-run_after_loop-owen_run_run--1-1",
                            "message": "Dependencies all met for "
                            "dep_context=non-requeueable deps "
                            "ti=<TaskInstance: "
                            "example_bash_operator.run_after_loop "
                            "owen_run_run [queued]>",
                            "task_id": "run_after_loop",
                            "try_number": "1",
                        },
                        "_type": "_doc",
                    },
                    {
                        "_id": "qteZT4kBjAZqZnexVUxl",
                        "_index": ".ds-filebeat-8.8.2-2023.07.09-000001",
                        "_score": 2.482621,
                        "_source": {
                            "@timestamp": "2023-07-13T14:13:15.141Z",
                            "asctime": "2023-07-09T07:47:43.917+0000",
                            "container": {"id": "airflow"},
                            "dag_id": "example_bash_operator",
                            "ecs": {"version": "8.0.0"},
                            "execution_date": "2023_07_09T07_47_32_000000",
                            "filename": "taskinstance.py",
                            "input": {"type": "log"},
                            "levelname": "INFO",
                            "lineno": 1347,
                            "log": {
                                "file": {
                                    "path": "/opt/airflow/Documents/GitHub/airflow/logs/"
                                    "dag_id=example_bash_operator"
                                    "/run_id=owen_run_run/task_id=run_after_loop/attempt=1.log"
                                },
                                "offset": 988,
                            },
                            "offset": 1688888863917961216,
                            "log_id": "example_bash_operator-run_after_loop-owen_run_run--1-1",
                            "message": "Starting attempt 1 of 1",
                            "task_id": "run_after_loop",
                            "try_number": "1",
                        },
                        "_type": "_doc",
                    },
                    {
                        "_id": "v9eZT4kBjAZqZnexVUx2",
                        "_index": ".ds-filebeat-8.8.2-2023.07.09-000001",
                        "_score": 2.482621,
                        "_source": {
                            "@timestamp": "2023-07-13T14:13:15.143Z",
                            "asctime": "2023-07-09T07:47:43.928+0000",
                            "container": {"id": "airflow"},
                            "dag_id": "example_bash_operator",
                            "ecs": {"version": "8.0.0"},
                            "execution_date": "2023_07_09T07_47_32_000000",
                            "filename": "taskinstance.py",
                            "input": {"type": "log"},
                            "levelname": "INFO",
                            "lineno": 1368,
                            "log": {
                                "file": {
                                    "path": "/opt/airflow/Documents/GitHub/airflow/logs/"
                                    "dag_id=example_bash_operator"
                                    "/run_id=owen_run_run/task_id=run_after_loop/attempt=1.log"
                                },
                                "offset": 1372,
                            },
                            "offset": 1688888863928218880,
                            "log_id": "example_bash_operator-run_after_loop-owen_run_run--1-1",
                            "message": "Executing <Task(BashOperator): "
                            "run_after_loop> on 2023-07-09 "
                            "07:47:32+00:00",
                            "task_id": "run_after_loop",
                            "try_number": "1",
                        },
                        "_type": "_doc",
                    },
                ],
                "max_score": 2.482621,
                "total": {"relation": "eq", "value": 36},
            },
            "timed_out": False,
            "took": 7,
        }


@pytest.fixture
def mock_hook(monkeypatch):
    monkeypatch.setattr(OpenSearchHook, "search", MockSearch.search)
    monkeypatch.setattr(OpenSearchHook, "client", MockSearch.client)
    monkeypatch.setattr(OpenSearchHook, "index", MockSearch.index)


@pytest.fixture(autouse=True)
def setup_connection():
    # We need to set up a Connection into the database for all tests.
    db.merge_conn(
        Connection(
            conn_id="opensearch_default",
            conn_type="opensearch",
            host="myopensearch.com",
            login="test_user",
            password="test",
        )
    )
