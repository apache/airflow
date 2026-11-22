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

import asyncio
import base64
import json

import httpx2
import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.exceptions import OCRBusyError, OCRError, OCRTimeoutError
from airflow.providers.common.ai.ocr.base import OCRPage
from airflow.providers.common.ai.ocr.mistral import (
    REQUEST_TIMEOUT,
    MistralOCRAdapter,
    _retry_after_seconds,
)

ANSWER = {
    "pages": [
        {"index": 0, "markdown": "# One", "images": [{"id": "img-0"}]},
        {"index": 1, "markdown": "two"},
    ],
    "model": "mistral-ocr-2512",
    "usage_info": {"pages_processed": 2},
}


def make_conn(**kwargs) -> Connection:
    return Connection(
        **{"conn_id": "ocr", "conn_type": "http", "host": "https://ocr.example", "password": "key", **kwargs}
    )


def read(handler, *, conn: Connection | None = None, extra: dict | None = None, **kwargs):
    """Send one document through an adapter whose server is ``handler``."""

    async def ocr():
        adapter = MistralOCRAdapter(
            conn or make_conn(), extra or {}, "mistral-ocr-latest", transport=httpx2.MockTransport(handler)
        )
        async with adapter:
            return await adapter.ocr(b"%PDF", **kwargs)

    return asyncio.run(ocr())


def answer_with(status_code: int = 200, **kwargs):
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(status_code, **{"json": ANSWER, **kwargs})

    return handler, requests


class TestMistralOCRAdapter:
    def test_posts_the_document_as_a_data_url_with_the_key_as_bearer_token(self):
        handler, requests = answer_with()

        result = read(handler)

        (request,) = requests
        assert str(request.url) == "https://ocr.example/v1/ocr"
        assert request.headers["Authorization"] == "Bearer key"
        assert json.loads(request.content) == {
            "model": "mistral-ocr-latest",
            "document": {
                "type": "document_url",
                "document_url": f"data:application/pdf;base64,{base64.b64encode(b'%PDF').decode()}",
            },
            "include_image_base64": False,
        }
        assert result.pages == [OCRPage(0, "# One", [{"id": "img-0"}]), OCRPage(1, "two")]
        assert result.model == "mistral-ocr-2512"
        assert result.usage == {"pages_processed": 2}

    @pytest.mark.parametrize(
        ("conn_kwargs", "extra", "url"),
        [
            pytest.param(
                {"host": "ocr.example"}, {}, "https://ocr.example/v1/ocr", id="scheme-defaults-to-https"
            ),
            pytest.param(
                {"host": "ocr.example", "schema": "http"}, {}, "http://ocr.example/v1/ocr", id="schema-field"
            ),
            pytest.param(
                {"host": "https://account.services.ai.azure.com/"},
                {"ocr_path": "/providers/mistral/azure/ocr"},
                "https://account.services.ai.azure.com/providers/mistral/azure/ocr",
                id="ocr-path-extra",
            ),
        ],
    )
    def test_builds_the_url_from_the_connection(self, conn_kwargs, extra, url):
        handler, requests = answer_with()

        read(handler, conn=make_conn(**conn_kwargs), extra=extra)

        assert str(requests[0].url) == url

    def test_asks_for_pictures_and_their_annotation_when_told(self):
        handler, requests = answer_with()
        schema = {"type": "object", "properties": {"description": {"type": "string"}}}

        read(handler, include_images=True, image_annotation_schema=schema)

        body = json.loads(requests[0].content)
        assert body["include_image_base64"] is True
        assert body["bbox_annotation_format"] == {
            "type": "json_schema",
            "json_schema": {"name": "image_annotation", "schema": schema, "strict": True},
        }

    @pytest.mark.parametrize(
        ("status_code", "headers", "retry_after"),
        [(429, {"Retry-After": "7"}, 7.0), (503, {}, None)],
    )
    def test_a_throttling_or_failing_service_is_busy(self, status_code, headers, retry_after):
        handler, _ = answer_with(status_code, headers=headers, json={"message": "slow down"})

        with pytest.raises(OCRBusyError, match="slow down") as raised:
            read(handler)

        assert not isinstance(raised.value, OCRTimeoutError)
        assert raised.value.status == status_code
        assert raised.value.retry_after == retry_after

    def test_a_refused_document_is_an_error_that_is_not_busy(self):
        handler, _ = answer_with(400, json={"code": "document_parser_too_many_pages"})

        with pytest.raises(OCRError, match="document_parser_too_many_pages") as raised:
            read(handler)

        assert not isinstance(raised.value, OCRBusyError)
        assert raised.value.status == 400

    def test_no_answer_in_time_is_a_timeout(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ReadTimeout("timed out", request=request)

        with pytest.raises(OCRTimeoutError, match=f"did not answer within {REQUEST_TIMEOUT} s"):
            read(handler)

    def test_a_connection_without_api_key_is_refused(self):
        with pytest.raises(OCRError, match="'ocr' has no API key"):
            MistralOCRAdapter(make_conn(password=None), {}, "mistral-ocr-latest")

    def test_cannot_be_used_outside_its_async_with_block(self):
        adapter = MistralOCRAdapter(make_conn(), {}, "mistral-ocr-latest")

        with pytest.raises(RuntimeError, match="outside its 'async with' block"):
            asyncio.run(adapter.ocr(b"%PDF"))

    @pytest.mark.parametrize(("extra", "limit"), [({}, None), ({"max_pages_per_request": 30}, 30)])
    def test_takes_its_page_limit_from_the_connection(self, extra, limit):
        assert MistralOCRAdapter(make_conn(), extra, "mistral-ocr-latest").max_pages_per_request == limit


@pytest.mark.parametrize(
    ("header", "seconds"),
    [
        ("12", 12.0),
        ("0.5", 0.5),
        ("-3", 0),
        (None, None),
        ("", None),
        ("Wed, 21 Oct 2026 07:28:00 GMT", None),
    ],
)
def test_retry_after_seconds(header, seconds):
    assert _retry_after_seconds(header) == seconds
