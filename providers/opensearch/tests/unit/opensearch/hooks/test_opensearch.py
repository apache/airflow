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

import json
from unittest import mock

import pytest
from opensearchpy import Urllib3HttpConnection

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"
opensearchpy = pytest.importorskip("opensearchpy")

MOCK_SEARCH_RETURN = {"status": "test"}
DEFAULT_CONN = opensearchpy.connection.http_requests.RequestsHttpConnection


class TestOpenSearchHook:
    def test_hook_search(self, mock_hook):
        hook = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)
        result = hook.search(
            index_name="testIndex",
            query={"size": 1, "query": {"multi_match": {"query": "test", "fields": ["testField"]}}},
        )

        assert result == MOCK_SEARCH_RETURN

    def test_hook_index(self, mock_hook):
        hook = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)
        result = hook.index(index_name="test_index", document={"title": "Monty Python"}, doc_id=3)
        assert result == 3

    def test_delete_check_parameters(self):
        hook = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)
        with pytest.raises(AirflowException, match="must include one of either a query or a document id"):
            hook.delete(index_name="test_index")

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_hook_param_bool(self, mock_get_connection):
        mock_conn = Connection(
            conn_id="opensearch_default", extra={"use_ssl": "True", "verify_certs": "True"}
        )
        mock_get_connection.return_value = mock_conn
        hook = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)

        assert isinstance(hook.use_ssl, bool)
        assert isinstance(hook.verify_certs, bool)

    def test_load_conn_param(self, mock_hook):
        hook_default = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)
        assert hook_default.connection_class == DEFAULT_CONN

        hook_Urllib3 = OpenSearchHook(
            open_search_conn_id="opensearch_default",
            log_query=True,
            open_search_conn_class=Urllib3HttpConnection,
        )
        assert hook_Urllib3.connection_class == Urllib3HttpConnection

    def test_hook_with_auth(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW_CONN_OPENSEARCH_DEFAULT",
            json.dumps(
                {
                    "conn_type": "opensearch hook",
                    "host": "testhost",
                    "login": "testuser",
                    "password": "testpass",
                }
            ),
        )
        hook = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)
        assert hook.client.transport.kwargs["http_auth"] == ("testuser", "testpass")

    def test_hook_no_auth(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW_CONN_OPENSEARCH_DEFAULT",
            json.dumps(
                {
                    "conn_type": "opensearch hook",
                    "host": "testhost",
                }
            ),
        )
        hook = OpenSearchHook(open_search_conn_id="opensearch_default", log_query=True)
        assert "http_auth" not in hook.client.transport.kwargs
