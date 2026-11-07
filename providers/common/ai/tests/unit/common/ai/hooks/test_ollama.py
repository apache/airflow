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

import httpx2
import pytest

from airflow.providers.common.ai.hooks.ollama import OllamaBackend, build_ollama_native_root


def _build_backend(
    handler, base_url: str = "http://ollama:11434/v1", model_name: str = "llama3"
) -> OllamaBackend:
    return OllamaBackend(base_url, model_name, transport=httpx2.MockTransport(handler))


class TestBuildNativeRoot:
    @pytest.mark.parametrize(
        "base_url",
        [
            "http://h:11434/v1",
            "http://h:11434/v1/",
            "http://h:11434",
            "http://h:11434/",
        ],
    )
    def test_returns_server_root(self, base_url):
        assert build_ollama_native_root(base_url) == "http://h:11434"


class TestOllamaBackendLoad:
    def test_posts_only_model_and_stream_to_generate_with_the_given_timeout(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"response": "", "done": True, "done_reason": "load"})

        _build_backend(handler).load(timeout=12.0)

        (request,) = requests
        assert (request.method, request.url.path) == ("POST", "/api/generate")
        assert json.loads(request.content) == {"model": "llama3", "stream": False}
        assert request.extensions["timeout"]["read"] == 12.0

    def test_http_error_propagates(self):
        backend = _build_backend(lambda request: httpx2.Response(500))

        with pytest.raises(httpx2.HTTPStatusError):
            backend.load(timeout=5.0)

    def test_timeout_is_raised_as_timeout_error_naming_model_and_seconds(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ReadTimeout("slow", request=request)

        with pytest.raises(TimeoutError, match=r"'llama3'.*5\.0 seconds") as exc_info:
            _build_backend(handler).load(timeout=5.0)

        assert isinstance(exc_info.value.__cause__, httpx2.ReadTimeout)

    def test_connect_timeout_says_server_was_not_reached(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectTimeout("no route", request=request)

        with pytest.raises(
            TimeoutError, match=r"Could not reach Ollama at http://ollama:11434 within 5\.0"
        ) as exc_info:
            _build_backend(handler).load(timeout=5.0)

        assert isinstance(exc_info.value.__cause__, httpx2.ConnectTimeout)
