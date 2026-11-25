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

import httpx2
import pytest

from airflow.providers.common.ai.utils.readiness_errors import (
    extract_error_detail,
    translate_readiness_errors,
)

ROOT = "http://localhost:1234"
REQUEST = httpx2.Request("GET", f"{ROOT}/api/v1/models")


def _build_response(status_code: int = 500, **kwargs) -> httpx2.Response:
    return httpx2.Response(status_code, request=REQUEST, **kwargs)


def _translate(**overrides):
    kwargs = {
        "server": "LM Studio",
        "root": ROOT,
        "timeout": 7.5,
        "failure": "could not list models",
        "timeout_failure": "timed out listing models",
    }
    return translate_readiness_errors(**{**kwargs, **overrides})


class TestExtractErrorDetail:
    def test_returns_the_error_field_when_it_is_a_non_empty_string(self):
        assert extract_error_detail(_build_response(json={"error": "model not found"})) == "model not found"

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            pytest.param({"json": {"error": ""}}, '{"error":""}', id="empty-error"),
            pytest.param({"json": {"error": {"message": "x"}}}, '{"error":{"message":"x"}}', id="non-string"),
            pytest.param({"json": {"other": "x"}}, '{"other":"x"}', id="no-error-key"),
            pytest.param({"json": ["a"]}, '["a"]', id="non-dict-body"),
            pytest.param({"content": b"  plain text  "}, "plain text", id="not-json-stripped"),
        ],
    )
    def test_falls_back_to_the_stripped_body(self, kwargs, expected):
        assert extract_error_detail(_build_response(**kwargs)) == expected

    def test_cuts_the_body_to_200_characters(self):
        assert extract_error_detail(_build_response(content=b"x" * 500)) == "x" * 200

    def test_falls_back_to_the_reason_phrase_when_the_body_is_empty(self):
        assert extract_error_detail(_build_response(503)) == "Service Unavailable"


class TestTranslateReadinessErrors:
    def test_status_error_names_server_failure_status_and_detail(self):
        response = _build_response(404, json={"error": "model not found"})

        with pytest.raises(httpx2.HTTPStatusError) as exc_info:
            with _translate():
                response.raise_for_status()

        assert str(exc_info.value) == "LM Studio could not list models (404): model not found"
        assert exc_info.value.response is response
        assert exc_info.value.request is REQUEST
        assert isinstance(exc_info.value.__cause__, httpx2.HTTPStatusError)

    def test_connect_timeout_says_server_was_not_reached_and_names_root_and_timeout(self):
        with pytest.raises(
            TimeoutError, match=r"^Could not reach LM Studio at http://localhost:1234 within 7\.5 seconds\.$"
        ) as exc_info:
            with _translate():
                raise httpx2.ConnectTimeout("boom")

        assert isinstance(exc_info.value.__cause__, httpx2.ConnectTimeout)

    @pytest.mark.parametrize("exc_type", [httpx2.ReadTimeout, httpx2.WriteTimeout, httpx2.PoolTimeout])
    def test_other_timeouts_use_the_timeout_failure_message(self, exc_type):
        with pytest.raises(TimeoutError, match=r"^LM Studio timed out listing models within 7\.5 seconds\.$"):
            with _translate():
                raise exc_type("boom")

    def test_connect_error_names_server_and_root_and_keeps_the_original_text(self):
        original = httpx2.ConnectError("refused", request=REQUEST)

        with pytest.raises(httpx2.ConnectError) as exc_info:
            with _translate():
                raise original

        assert str(exc_info.value) == "Could not reach LM Studio at http://localhost:1234: refused"
        assert exc_info.value.request is REQUEST
        assert exc_info.value.__cause__ is original

    @pytest.mark.parametrize(
        ("exc_type", "message"),
        [
            pytest.param(
                httpx2.ConnectTimeout,
                r"^Could not reach LM Studio at http://localhost:1234 within 12\.3 seconds\.$",
                id="connect-timeout",
            ),
            pytest.param(
                httpx2.ReadTimeout,
                r"^LM Studio timed out listing models within 12\.3 seconds\.$",
                id="read-timeout",
            ),
        ],
    )
    def test_timeout_is_shown_with_one_decimal(self, exc_type, message):
        with pytest.raises(TimeoutError, match=message):
            with _translate(timeout=12.345678901234567):
                raise exc_type("boom")

    def test_unrelated_exceptions_are_not_translated(self):
        with pytest.raises(ValueError, match="unrelated"):
            with _translate():
                raise ValueError("unrelated")
