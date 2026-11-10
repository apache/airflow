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

from airflow.providers.common.ai.hooks.lmstudio import LMStudioBackend, build_lmstudio_native_root

_BASE_URL = "http://lmstudio:1234/v1"


def _build_backend(handler, base_url: str = _BASE_URL, model_name: str = "qwen3") -> LMStudioBackend:
    return LMStudioBackend(base_url, model_name, transport=httpx2.MockTransport(handler))


def _build_models_response(*, loaded: bool, key: str = "qwen3") -> httpx2.Response:
    instances = [{"id": key, "config": {"context_length": 4096}}] if loaded else []
    return httpx2.Response(
        200,
        json={
            "models": [
                {"type": "llm", "key": "other-model", "loaded_instances": [{"id": "other-model"}]},
                {"type": "llm", "key": key, "loaded_instances": instances},
            ]
        },
    )


class TestBuildNativeRoot:
    @pytest.mark.parametrize(
        "base_url",
        [
            "http://h:1234/v1",
            "http://h:1234/v1/",
            "http://h:1234/api/v1",
            "http://h:1234/api/v1/",
            "http://h:1234",
            "http://h:1234/",
        ],
    )
    def test_returns_server_root(self, base_url):
        assert build_lmstudio_native_root(base_url) == "http://h:1234"


class TestLMStudioBackendBaseUrl:
    @pytest.mark.parametrize("base_url", [None, ""], ids=["base-url-none", "base-url-empty"])
    def test_missing_base_url_raises_value_error(self, base_url):
        with pytest.raises(ValueError, match="needs a base_url"):
            LMStudioBackend(base_url=base_url, model_name="qwen3")

    @pytest.mark.parametrize("base_url", ["http://h:1234/v1", "http://h:1234/api/v1"])
    def test_root_drops_api_suffix(self, base_url):
        assert LMStudioBackend(base_url=base_url, model_name="qwen3").root == "http://h:1234"


class TestLMStudioBackendLoad:
    def test_loaded_model_skips_the_load_request(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _build_models_response(loaded=True)

        _build_backend(handler).load(timeout=12.0)

        (request,) = requests
        assert (request.method, request.url.path) == ("GET", "/api/v1/models")
        assert request.extensions["timeout"]["read"] == 12.0

    def test_unloaded_model_is_listed_then_loaded_with_only_the_model_key(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.method == "GET":
                return _build_models_response(loaded=False)
            return httpx2.Response(
                200,
                json={"type": "llm", "instance_id": "qwen3", "load_time_seconds": 3.2, "status": "loaded"},
            )

        _build_backend(handler).load(timeout=12.0)

        get_request, post_request = requests
        assert (get_request.method, get_request.url.path) == ("GET", "/api/v1/models")
        assert (post_request.method, post_request.url.path) == ("POST", "/api/v1/models/load")
        assert json.loads(post_request.content) == {"model": "qwen3"}
        assert post_request.extensions["timeout"]["read"] == 12.0

    def test_model_missing_from_the_list_is_still_loaded(self):
        methods: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            methods.append(request.method)
            if request.method == "GET":
                return _build_models_response(loaded=True, key="another")
            return httpx2.Response(200, json={"status": "loaded"})

        _build_backend(handler).load(timeout=5.0)

        assert methods == ["GET", "POST"]

    def test_http_error_names_model_and_server_error_field(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "GET":
                return _build_models_response(loaded=False)
            return httpx2.Response(404, json={"error": "model 'qwen3' not found"})

        with pytest.raises(httpx2.HTTPStatusError) as exc_info:
            _build_backend(handler).load(timeout=5.0)

        assert str(exc_info.value) == (
            "LM Studio could not load model 'qwen3' (404): model 'qwen3' not found"
        )
        assert exc_info.value.response.status_code == 404
        assert isinstance(exc_info.value.__cause__, httpx2.HTTPStatusError)

    def test_list_error_is_reported_the_same_way(self):
        backend = _build_backend(lambda request: httpx2.Response(401, json={"error": "unauthorized"}))

        with pytest.raises(httpx2.HTTPStatusError) as exc_info:
            backend.load(timeout=5.0)

        assert str(exc_info.value) == "LM Studio could not load model 'qwen3' (401): unauthorized"

    @pytest.mark.parametrize(
        ("response", "expected_detail"),
        [
            pytest.param(
                httpx2.Response(502, text="<html>Bad Gateway</html>"),
                "<html>Bad Gateway</html>",
                id="non-json-body",
            ),
            pytest.param(
                httpx2.Response(500, json={"message": "boom"}), '{"message":"boom"}', id="json-without-error"
            ),
            pytest.param(httpx2.Response(500, text="x" * 500), "x" * 200, id="long-body-truncated"),
            pytest.param(httpx2.Response(500), "Internal Server Error", id="empty-body"),
        ],
    )
    def test_http_error_falls_back_to_body_text(self, response, expected_detail):
        backend = _build_backend(lambda request: response)

        with pytest.raises(httpx2.HTTPStatusError) as exc_info:
            backend.load(timeout=5.0)

        assert str(exc_info.value) == (
            f"LM Studio could not load model 'qwen3' ({response.status_code}): {expected_detail}"
        )

    def test_timeout_is_raised_as_timeout_error_naming_model_and_seconds(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ReadTimeout("slow", request=request)

        with pytest.raises(TimeoutError, match=r"'qwen3'.*5\.0 seconds") as exc_info:
            _build_backend(handler).load(timeout=5.0)

        assert isinstance(exc_info.value.__cause__, httpx2.ReadTimeout)

    def test_connect_timeout_says_server_was_not_reached(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectTimeout("no route", request=request)

        with pytest.raises(
            TimeoutError, match=r"Could not reach LM Studio at http://lmstudio:1234 within 5\.0"
        ) as exc_info:
            _build_backend(handler).load(timeout=5.0)

        assert isinstance(exc_info.value.__cause__, httpx2.ConnectTimeout)

    @pytest.mark.parametrize("base_url", ["http://lmstudio:1234/v1", "http://lmstudio:1234/api/v1"])
    def test_requests_go_to_the_native_root_for_either_base_url_form(self, base_url):
        paths: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            paths.append(request.url.path)
            if request.method == "GET":
                return _build_models_response(loaded=False)
            return httpx2.Response(200, json={"status": "loaded"})

        _build_backend(handler, base_url=base_url).load(timeout=5.0)

        assert paths == ["/api/v1/models", "/api/v1/models/load"]
