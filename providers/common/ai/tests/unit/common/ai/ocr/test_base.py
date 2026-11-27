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

import dataclasses

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.ocr.base import OCRAdapter, OCRPage, OCRResult

CONN = Connection(conn_id="ocr", conn_type="http", host="https://ocr.example", password="key")


class EchoAdapter(OCRAdapter):
    """Answers every document with one page that holds its size."""

    name = "echo"
    conn_types = frozenset({"http"})

    async def ocr(self, document, *, mime_type="application/pdf", include_images=False, **kwargs):
        return OCRResult(pages=[OCRPage(0, f"{len(document)} bytes")], model=self.model)


class TestOCRPage:
    def test_pages_do_not_share_their_images(self):
        first, second = OCRPage(0, "a"), OCRPage(1, "b")

        first.images.append({"id": "img-0"})

        assert second.images == []

    def test_is_immutable(self):
        page = OCRPage(0, "a")

        with pytest.raises(dataclasses.FrozenInstanceError):
            page.markdown = "b"  # type: ignore[misc]


class TestOCRResult:
    def test_results_do_not_share_their_usage(self):
        first, second = OCRResult(pages=[], model="m"), OCRResult(pages=[], model="m")

        first.usage["pages_processed"] = 3

        assert second.usage == {}


class TestOCRAdapter:
    def test_needs_an_ocr_method(self):
        class NoOCR(OCRAdapter):
            name = "none"
            conn_types = frozenset({"http"})

        with pytest.raises(TypeError, match="ocr"):
            NoOCR(CONN, {}, "model")  # type: ignore[abstract]

    def test_keeps_what_it_is_built_with(self):
        extra = {"ocr_path": "/v1/ocr"}

        adapter = EchoAdapter(CONN, extra, "echo-1")

        assert (adapter.conn, adapter.extra, adapter.model) == (CONN, extra, "echo-1")

    def test_has_no_page_limit_by_default(self):
        assert EchoAdapter(CONN, {}, "echo-1").max_pages_per_request is None

    @pytest.mark.asyncio
    async def test_is_its_own_context_manager(self):
        adapter = EchoAdapter(CONN, {}, "echo-1")

        async with adapter as entered:
            result = await entered.ocr(b"12345")

        assert entered is adapter
        assert result == OCRResult(pages=[OCRPage(0, "5 bytes")], model="echo-1")

    @pytest.mark.asyncio
    async def test_does_not_swallow_an_error_raised_inside_it(self):
        with pytest.raises(RuntimeError, match="boom"):
            async with EchoAdapter(CONN, {}, "echo-1"):
                raise RuntimeError("boom")
