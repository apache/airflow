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

from unittest.mock import patch

import httpx
import pytest
from httpx import Response

from airflow.api.client.json_client import Client


class TestJsonClient:
    def setup_method(self):
        # Init the pool as the return value of the function for all pool operations in the test case.
        self.pool = {"pool": "foo", "slots": 1, "description": "foo_test", "include_deferred": True}
        self.client = Client(api_base_url=None, auth=None)

    @patch.object(httpx.Client, "get")
    def test_request_ok(self, mock_get):
        mock_res = Response(status_code=200, json={"get_ok": "yes"})
        mock_get.return_value = mock_res
        res = self.client._request("/test/ok", {"dag_id": "foo"})
        assert res["get_ok"] == "yes"

    @patch.object(httpx.Client, "get")
    def test_request_exception(self, mock_get):
        mock_get.return_value = Response(status_code=500, json={"get_ok": "no"})
        with pytest.raises(OSError) as exc_info:
            self.client._request("/test/except", {"dag_id": "foo"})
        assert exc_info.type == OSError
        assert "Server error" in str(exc_info.value)

    @patch.object(httpx.Client, "post")
    def test_trigger_dag(self, mock_post):
        mock_post.return_value = Response(status_code=200, json={"message": {"post": "trigger_dag"}})
        test_dag_id = "example_bash_operator"
        res = self.client.trigger_dag(test_dag_id)
        assert res["post"] == "trigger_dag"

    @patch.object(httpx.Client, "delete")
    def test_delete_dag(self, mock_delete):
        mock_delete.return_value = Response(status_code=200, json={"message": {"delete": "delete_dag"}})
        test_dag_id = "example_bash_operator"
        res = self.client.delete_dag(test_dag_id)
        assert res["delete"] == "delete_dag"

    @patch.object(httpx.Client, "post")
    def test_create_pool(self, mock_create_pool):
        mock_create_pool.return_value = Response(status_code=200, json=self.pool)
        pool = self.client.create_pool(name="foo", slots=1, description="fool_test", include_deferred=True)
        assert pool == ("foo", 1, "foo_test", True)

    @patch.object(httpx.Client, "get")
    def test_get_pool(self, mock_get_pool):
        mock_get_pool.return_value = Response(status_code=200, json=self.pool)
        pool = self.client.get_pool(name="foo")
        assert pool == ("foo", 1, "foo_test")

    @patch.object(httpx.Client, "get")
    def test_get_pools(self, mock_get_pools):
        mock_get_pools.return_value = Response(status_code=200, json=[self.pool])
        pools = self.client.get_pools()
        assert pools == [("foo", 1, "foo_test")]

    @patch.object(httpx.Client, "delete")
    def test_delete_pool(self, mock_delete_pool):
        mock_delete_pool.return_value = Response(status_code=200, json=self.pool)
        pool = self.client.delete_pool(name="foo")
        assert pool == ("foo", 1, "foo_test")
