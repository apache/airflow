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
import io
from unittest.mock import patch

import pytest

pypdf = pytest.importorskip("pypdf")

from airflow.models.connection import Connection  # noqa: E402
from airflow.providers.common.ai.exceptions import (  # noqa: E402
    OCRBusyError,
    OCRTimeoutError,
    UnsupportedOCRProviderError,
)
from airflow.providers.common.ai.ocr import dispatch  # noqa: E402
from airflow.providers.common.ai.ocr.base import OCRAdapter, OCRPage, OCRResult  # noqa: E402
from airflow.providers.common.ai.operators import llm_ocr  # noqa: E402
from airflow.providers.common.ai.operators.llm_ocr import LLMOCROperator  # noqa: E402

CONN = Connection(conn_id="ocr", conn_type="http", host="https://ocr.example", password="key")


def make_pdf(pages: int) -> bytes:
    """Return a PDF of blank pages; page ``i`` is ``100 + i`` wide, so the fake model can name it."""
    writer = pypdf.PdfWriter()
    for page in range(pages):
        writer.add_blank_page(width=100 + page, height=100)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


class TwoPageAdapter(OCRAdapter):
    """Takes two pages a request and answers each page with the width that names it."""

    name = "two"
    conn_types = frozenset({"http"})
    max_pages_per_request = 2

    in_flight = 0
    most_in_flight = 0
    requests = 0
    #: Raised by the request for the part that starts at this page width.
    fails_at: dict[int, Exception] = {}
    #: Requests of more pages than this are not answered in time.
    answers_up_to_pages = 99
    schemas: list = []

    async def ocr(
        self, document, *, mime_type="application/pdf", include_images=False, image_annotation_schema=None
    ):
        cls = type(self)
        widths = [int(page.mediabox.width) for page in pypdf.PdfReader(io.BytesIO(document)).pages]
        cls.schemas.append(image_annotation_schema)
        cls.requests += 1
        if len(widths) > cls.answers_up_to_pages:
            raise OCRTimeoutError("no answer in time")
        cls.in_flight += 1
        cls.most_in_flight = max(cls.most_in_flight, cls.in_flight)
        try:
            # The first part answers last: the pages must not come back in arrival order.
            await asyncio.sleep(0.05 if widths[0] == 100 else 0.01)
            if widths[0] in cls.fails_at:
                raise cls.fails_at[widths[0]]
        finally:
            cls.in_flight -= 1
        return OCRResult(
            pages=[OCRPage(index, f"page {width}") for index, width in enumerate(widths)], model=self.model
        )


class UnlimitedAdapter(TwoPageAdapter):
    name = "unlimited"
    max_pages_per_request = None


@pytest.fixture(autouse=True)
def _adapters(monkeypatch):
    monkeypatch.setattr(dispatch, "_REGISTERED_ADAPTERS", {})
    for adapter in (TwoPageAdapter, UnlimitedAdapter):
        dispatch.register_adapter(adapter)
        for name, value in (("in_flight", 0), ("most_in_flight", 0), ("requests", 0)):
            monkeypatch.setattr(adapter, name, value)
        monkeypatch.setattr(adapter, "fails_at", {})
        monkeypatch.setattr(adapter, "answers_up_to_pages", 99)
        monkeypatch.setattr(adapter, "schemas", [])


def run(**kwargs) -> list[dict]:
    task = LLMOCROperator(task_id="ocr", llm_conn_id="ocr", **{"model_id": "two:model", **kwargs})
    with (
        patch.object(llm_ocr, "get_async_connection", autospec=True, return_value=CONN),
        patch.object(llm_ocr, "get_async_extra_dejson", autospec=True, return_value={}),
    ):
        return asyncio.run(task.aexecute({}))


class TestLLMOCROperator:
    def test_template_fields(self):
        assert LLMOCROperator.template_fields == ("llm_conn_id", "model_id", "file_name")

    def test_a_large_document_is_cut_and_its_pages_come_back_in_document_order(self):
        pages = run(source_bytes=make_pdf(5), file_name="rules.pdf")

        assert TwoPageAdapter.requests == 3
        assert [page["text"] for page in pages] == [f"page {width}" for width in range(100, 105)]
        assert [page["metadata"] for page in pages] == [
            {"page_number": number, "file_name": "rules.pdf", "model": "model"} for number in range(1, 6)
        ]

    def test_no_more_requests_are_in_flight_than_max_concurrency(self):
        run(source_bytes=make_pdf(9), max_concurrency=2)

        assert TwoPageAdapter.requests == 5
        assert TwoPageAdapter.most_in_flight == 2

    def test_the_document_is_also_taken_as_the_base64_string_an_xcom_carries(self):
        pages = run(source_bytes=base64.b64encode(make_pdf(2)).decode())

        assert [page["text"] for page in pages] == ["page 100", "page 101"]

    def test_fewer_pages_per_request_than_the_adapters_limit_are_sent_when_asked(self):
        run(source_bytes=make_pdf(4), pages_per_request=1)

        assert TwoPageAdapter.requests == 4

    def test_more_pages_per_request_than_the_adapter_takes_are_refused(self):
        with pytest.raises(ValueError, match="takes at most 2 pages"):
            run(source_bytes=make_pdf(4), pages_per_request=3)

    def test_a_document_goes_whole_to_an_adapter_without_page_limit(self):
        pages = run(source_bytes=make_pdf(7), model_id="unlimited:model")

        assert UnlimitedAdapter.requests == 1
        assert len(pages) == 7

    def test_an_adapter_without_page_limit_still_gets_the_parts_that_were_asked_for(self):
        run(source_bytes=make_pdf(7), model_id="unlimited:model", pages_per_request=3)

        assert UnlimitedAdapter.requests == 3

    def test_a_busy_service_fails_the_task_with_its_own_error(self):
        TwoPageAdapter.fails_at = {102: OCRBusyError("try later", status=429, retry_after=7)}

        with pytest.raises(OCRBusyError) as raised:
            run(source_bytes=make_pdf(5))

        assert raised.value.retry_after == 7

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"pages_per_request": 0}, "pages_per_request must be at least 1"),
            ({"max_concurrency": 0}, "max_concurrency must be at least 1"),
        ],
    )
    def test_wrong_settings_are_refused_when_the_task_is_built(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            LLMOCROperator(task_id="ocr", source_bytes=b"", llm_conn_id="ocr", model_id="two:model", **kwargs)

    def test_a_model_id_without_provider_is_refused_when_the_task_runs(self):
        with pytest.raises(UnsupportedOCRProviderError, match="<provider>:<model>"):
            run(source_bytes=make_pdf(1), model_id="model-without-provider")

    def test_a_part_not_read_in_time_is_sent_again_as_two_halves_and_the_pages_stay_in_order(self):
        TwoPageAdapter.answers_up_to_pages = 1

        pages = run(source_bytes=make_pdf(3))

        # Pages 1-2 time out as one request and are read as two; page 3 goes through at once.
        assert TwoPageAdapter.requests == 4
        assert [page["text"] for page in pages] == ["page 100", "page 101", "page 102"]
        assert [page["metadata"]["page_number"] for page in pages] == [1, 2, 3]

    def test_a_single_page_not_read_in_time_fails_the_task_as_a_timeout(self):
        TwoPageAdapter.answers_up_to_pages = 0

        with pytest.raises(OCRTimeoutError):
            run(source_bytes=make_pdf(1))

    def test_the_image_annotation_schema_goes_with_every_request(self):
        schema = {"type": "object"}

        run(source_bytes=make_pdf(3), image_annotation_schema=schema)

        assert TwoPageAdapter.schemas == [schema, schema]
