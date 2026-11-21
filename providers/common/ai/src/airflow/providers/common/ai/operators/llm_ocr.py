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
"""Operator that reads a document with a vendor's OCR model."""

from __future__ import annotations

import asyncio
import base64
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.exceptions import OCRTimeoutError
from airflow.providers.common.ai.ocr.dispatch import get_adapter_class, split_model_id
from airflow.providers.common.compat.connection import get_async_connection, get_async_extra_dejson
from airflow.providers.common.compat.standard.operators import BaseAsyncOperator

if TYPE_CHECKING:
    from airflow.providers.common.ai.ocr.base import OCRAdapter, OCRResult
    from airflow.sdk import Context


class LLMOCROperator(BaseAsyncOperator):
    """
    Read a PDF with an OCR model and return its pages as Markdown.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    A document with more pages than one request may carry is cut into parts, ``max_concurrency``
    of which are sent at once on the task's event loop; the pages come back in document order.
    A part the service does not read in time is sent again as two halves, down to single pages.
    Cutting needs the ``pdf`` extra (``pypdf``).

    Returns one dict per page, in the shape of
    :class:`~airflow.providers.common.ai.operators.document_loader.DocumentLoaderOperator`:
    ``{"text": <markdown>, "metadata": {"page_number": <from 1>, "file_name": ..., "model": ...}}``.

    A failed part fails the task with the adapter's error, which names what to do:
    :class:`~airflow.providers.common.ai.exceptions.OCRBusyError` (429, 5xx, with
    ``retry_after``) is worth a retry, :class:`~airflow.providers.common.ai.exceptions.OCRError`
    is not.

    Requires Airflow 3.2 or later: the requests run on the worker's event loop.

    :param source_bytes: The document: bytes, or their base64 string as an XCom carries them.
        Not a template field: Jinja would render bytes as their ``repr`` text.
    :param llm_conn_id: The connection of the OCR service.
    :param model_id: ``"<provider>:<model>"``; the provider selects the adapter.
    :param file_name: Name of the document, for the pages' metadata.
    :param pages_per_request: The most pages to send in one request. Defaults to the adapter's
        own limit; it cannot exceed it.
    :param max_concurrency: How many requests of this task are in flight at once.
    :param include_images: Whether to ask for the pictures of the pages, base64-encoded.
    :param image_annotation_schema: A JSON Schema the model fills for every picture of a page;
        the annotations come back in the pages' ``metadata["images"]``.
    """

    template_fields: Sequence[str] = ("llm_conn_id", "model_id", "file_name")

    def __init__(
        self,
        *,
        source_bytes: bytes | str,
        llm_conn_id: str,
        model_id: str,
        file_name: str | None = None,
        pages_per_request: int | None = None,
        max_concurrency: int = 4,
        include_images: bool = False,
        image_annotation_schema: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if pages_per_request is not None and pages_per_request < 1:
            raise ValueError(f"pages_per_request must be at least 1, got {pages_per_request}")
        if max_concurrency < 1:
            raise ValueError(f"max_concurrency must be at least 1, got {max_concurrency}")
        self.source_bytes = source_bytes
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.file_name = file_name
        self.pages_per_request = pages_per_request
        self.max_concurrency = max_concurrency
        self.include_images = include_images
        self.image_annotation_schema = image_annotation_schema

    async def aexecute(self, context: Context) -> list[dict[str, Any]]:
        conn = await get_async_connection(self.llm_conn_id)
        extra = await get_async_extra_dejson(conn)
        _, model = split_model_id(self.model_id)
        adapter = get_adapter_class(conn.conn_type, self.model_id)(conn, extra, model)

        document = self._decode_document()
        parts = await self._split_document(document, adapter)
        name = self.file_name or "a document"
        self.log.info(
            "OCR of %s: %d bytes in %d request(s), %d at once",
            name,
            len(document),
            len(parts),
            self.max_concurrency,
        )
        started = time.monotonic()
        async with adapter:
            results = await self._read_parts(adapter, parts)
        pages = self._build_pages(results)
        self.log.info("OCR of %s done: %d pages in %.1f s", name, len(pages), time.monotonic() - started)
        return pages

    def _decode_document(self) -> bytes:
        if isinstance(self.source_bytes, str):
            return base64.b64decode(self.source_bytes)
        return self.source_bytes

    async def _split_document(self, document: bytes, adapter: OCRAdapter) -> list[bytes]:
        """Return the document as the parts to send: itself when nothing limits its pages."""
        limit = adapter.max_pages_per_request
        size = self.pages_per_request or limit
        if size is None:
            return [document]
        if limit is not None and size > limit:
            raise ValueError(
                f"pages_per_request is {size}, the {adapter.name!r} adapter takes at most "
                f"{limit} pages in a request"
            )
        # Imported here: only a task that cuts a PDF needs pypdf, the provider's ``pdf`` extra.
        from airflow.providers.common.ai.ocr.pdf import split_pdf

        # Off the event loop: cutting a large PDF is seconds of CPU.
        return await asyncio.to_thread(split_pdf, document, size)

    async def _read_parts(self, adapter: OCRAdapter, parts: list[bytes]) -> list[OCRResult]:
        """Send the parts, ``max_concurrency`` at once; the results are in the order of the pages."""
        in_flight = asyncio.Semaphore(self.max_concurrency)

        async def read(part: bytes) -> list[OCRResult]:
            try:
                async with in_flight:
                    return [
                        await adapter.ocr(
                            part,
                            include_images=self.include_images,
                            image_annotation_schema=self.image_annotation_schema,
                        )
                    ]
            except OCRTimeoutError:
                # Imported here: only a part that timed out needs pypdf, the ``pdf`` extra.
                from airflow.providers.common.ai.ocr.pdf import halve_pdf

                halves = await asyncio.to_thread(halve_pdf, part)
                if len(halves) == 1:
                    raise
                self.log.warning(
                    "A part of %d bytes was not read in time, sending it again as two halves", len(part)
                )
                return [result for half in halves for result in await read(half)]

        try:
            async with asyncio.TaskGroup() as group:
                requests = [group.create_task(read(part)) for part in parts]
        except* Exception as failed:
            # One error tells a retry policy what happened; the group wraps even a single one.
            raise failed.exceptions[0] from None
        return [result for request in requests for result in request.result()]

    def _build_pages(self, results: list[OCRResult]) -> list[dict[str, Any]]:
        """Return every page of every part, numbered from 1 across the whole document."""
        pages: list[dict[str, Any]] = []
        for result in results:
            for page in sorted(result.pages, key=lambda page: page.index):
                metadata: dict[str, Any] = {
                    "page_number": len(pages) + 1,
                    "file_name": self.file_name,
                    "model": result.model,
                }
                if page.images:
                    metadata["images"] = page.images
                pages.append({"text": page.markdown, "metadata": metadata})
        return pages
