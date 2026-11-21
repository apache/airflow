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
"""Cutting a PDF into smaller PDFs, for OCR services that limit the pages of a document."""

from __future__ import annotations

import io

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from pypdf import PdfReader, PdfWriter
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(e)


def split_pdf(document: bytes, pages_per_part: int) -> list[bytes]:
    """
    Return the document as PDFs of at most ``pages_per_part`` pages, in page order.

    A document that fits is returned as it is, without being rewritten. Reading is lenient
    (``strict=False``): a PDF with small defects is still cut. Pure Python and CPU-bound, and
    it can take seconds on a large file: call it off the event loop.

    :raises ValueError: ``pages_per_part`` is below 1.
    :raises pypdf.errors.PdfReadError: The document cannot be read as a PDF.
    """
    if pages_per_part < 1:
        raise ValueError(f"pages_per_part must be at least 1, got {pages_per_part}")
    reader = PdfReader(io.BytesIO(document), strict=False)
    page_count = len(reader.pages)
    if page_count <= pages_per_part:
        return [document]
    parts = []
    for first in range(0, page_count, pages_per_part):
        writer = PdfWriter()
        for number in range(first, min(first + pages_per_part, page_count)):
            writer.add_page(reader.pages[number])
        part = io.BytesIO()
        writer.write(part)
        parts.append(part.getvalue())
    return parts


def halve_pdf(document: bytes) -> list[bytes]:
    """
    Return the document as two PDFs of half its pages each; a document of one page as it is.

    For a part a service did not read in time: half the pages may get through.
    """
    page_count = len(PdfReader(io.BytesIO(document), strict=False).pages)
    if page_count < 2:
        return [document]
    return split_pdf(document, (page_count + 1) // 2)
