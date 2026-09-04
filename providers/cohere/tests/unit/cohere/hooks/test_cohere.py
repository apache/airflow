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

from unittest.mock import MagicMock, patch

from airflow.models import Connection
from airflow.providers.cohere.hooks.cohere import (
    CohereHook,
)


class TestCohereHook:
    """
    Test for CohereHook
    """

    def test__get_api_key(self):
        api_key = "test"
        base_url = "http://some_host.com"
        timeout = 150
        with (
            patch.object(
                CohereHook,
                "get_connection",
                return_value=Connection(conn_type="cohere", password=api_key, host=base_url),
            ),
            patch("cohere.ClientV2") as client,
        ):
            hook = CohereHook(timeout=timeout)
            _ = hook.get_conn()
            client.assert_called_once_with(api_key=api_key, timeout=timeout, base_url=base_url)

    @patch.object(CohereHook, "get_conn")
    def test_rerank(self, mock_get_conn):
        response = MagicMock()
        response.model_dump.return_value = {
            "id": "rerank-id",
            "results": [{"index": 1, "relevance_score": 0.9}],
        }
        mock_get_conn.return_value.rerank.return_value = response
        request_options = {"timeout_in_seconds": 10}
        hook = CohereHook(request_options=request_options)

        result = hook.rerank(
            query="Where is the capital?",
            documents=["first", "second"],
            model="rerank-v3.5",
            top_n=1,
            max_tokens_per_doc=512,
        )

        mock_get_conn.return_value.rerank.assert_called_once_with(
            query="Where is the capital?",
            documents=["first", "second"],
            model="rerank-v3.5",
            top_n=1,
            max_tokens_per_doc=512,
            request_options=request_options,
        )
        response.model_dump.assert_called_once_with(mode="json")
        assert result == {"id": "rerank-id", "results": [{"index": 1, "relevance_score": 0.9}]}
