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

import httpx
import pytest

from airflow.sdk.api.client import Client, RemoteValidationError, ServerResponseError


class TestClient:
    def test_error_parsing(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            """
            A transport handle that always returns errors
            """

            return httpx.Response(422, json={"detail": [{"loc": ["#0"], "msg": "err", "type": "required"}]})

        client = Client(
            base_url=None, dry_run=True, token="", mounts={"'http://": httpx.MockTransport(handle_request)}
        )

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")

        assert isinstance(err.value, ServerResponseError)
        assert isinstance(err.value.detail, list)
        assert err.value.detail == [
            RemoteValidationError(loc=["#0"], msg="err", type="required"),
        ]

    def test_error_parsing_plain_text(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            """
            A transport handle that always returns errors
            """

            return httpx.Response(422, content=b"Internal Server Error")

        client = Client(
            base_url=None, dry_run=True, token="", mounts={"'http://": httpx.MockTransport(handle_request)}
        )

        with pytest.raises(httpx.HTTPStatusError) as err:
            client.get("http://error")
        assert not isinstance(err.value, ServerResponseError)

    def test_error_parsing_other_json(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            # Some other json than an error body.
            return httpx.Response(404, json={"detail": "Not found"})

        client = Client(
            base_url=None, dry_run=True, token="", mounts={"'http://": httpx.MockTransport(handle_request)}
        )

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")
        assert err.value.args == ("Not found",)
        assert err.value.detail is None
