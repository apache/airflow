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

import io

import pytest

pypdf = pytest.importorskip("pypdf")

from airflow.providers.common.ai.ocr.pdf import halve_pdf, split_pdf  # noqa: E402


def make_pdf(pages: int) -> bytes:
    """Return a PDF of blank pages; page ``i`` is ``100 + i`` wide, which tells the pages apart."""
    writer = pypdf.PdfWriter()
    for page in range(pages):
        writer.add_blank_page(width=100 + page, height=100)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def page_widths(document: bytes) -> list[int]:
    return [int(page.mediabox.width) for page in pypdf.PdfReader(io.BytesIO(document)).pages]


class TestSplitPdf:
    def test_cuts_into_parts_of_at_most_the_asked_pages_in_page_order(self):
        parts = split_pdf(make_pdf(5), 2)

        assert [page_widths(part) for part in parts] == [[100, 101], [102, 103], [104]]

    @pytest.mark.parametrize("pages_per_part", [3, 4])
    def test_a_document_that_fits_is_returned_as_it_is(self, pages_per_part):
        document = make_pdf(3)

        assert split_pdf(document, pages_per_part) == [document]

    def test_rejects_fewer_than_one_page_per_part(self):
        with pytest.raises(ValueError, match="pages_per_part must be at least 1"):
            split_pdf(make_pdf(2), 0)

    def test_raises_on_a_document_that_is_not_a_pdf(self):
        with pytest.raises(pypdf.errors.PdfReadError):
            split_pdf(b"not a pdf", 2)


class TestHalvePdf:
    @pytest.mark.parametrize(
        ("pages", "expected"),
        [(2, [[100], [101]]), (5, [[100, 101, 102], [103, 104]])],
    )
    def test_returns_two_parts_of_half_the_pages(self, pages, expected):
        assert [page_widths(part) for part in halve_pdf(make_pdf(pages))] == expected

    def test_a_single_page_is_returned_as_it_is(self):
        document = make_pdf(1)

        assert halve_pdf(document) == [document]
