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
"""
Vendor-agnostic types and the adapter contract for ``@task.llm_ocr``.

This module must never import a vendor SDK: ``ocr/dispatch.py`` and the provider-yaml validation
check import every registered module, so a top-level SDK import here would make them fail in any
environment without that SDK installed.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Self

if TYPE_CHECKING:
    from airflow.sdk import Connection


@dataclass(frozen=True)
class OCRPage:
    """One page read by the model."""

    #: Position of the page in the document that was sent, from 0.
    index: int
    markdown: str
    #: What the vendor says of the page's pictures: an id, a bounding box, base64 when asked,
    #: and the annotation of each when an ``image_annotation_schema`` was given.
    images: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class OCRResult:
    """The pages of one request, in the order the vendor returned them."""

    pages: list[OCRPage]
    model: str
    #: The vendor's own usage figures (pages processed, bytes, ...).
    usage: dict[str, Any] = field(default_factory=dict)


class OCRAdapter(ABC):
    """
    Sends a document to one vendor's OCR model and returns its pages.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    An adapter is used as an async context manager, so its requests share one HTTP client::

        async with adapter_class(conn, extra, model="mistral-ocr-latest") as adapter:
            result = await adapter.ocr(document)

    :param conn: The connection of the service. Read its fields, never ``extra_dejson``: that
        masks secrets with a synchronous call, which an event loop cannot make.
    :param extra: The connection's extra, already read off the event loop.
    :param model: The model name, without the prefix that selected this adapter.
    """

    #: Prefix of the ``model_id`` that selects this adapter: ``"mistral"`` in ``"mistral:..."``.
    name: ClassVar[str]
    #: The connection types this adapter knows how to read.
    conn_types: ClassVar[frozenset[str]]
    #: The most pages the service takes in one document, or None when it has no limit. The
    #: operator cuts larger documents. An adapter whose limit depends on where the service runs
    #: sets it per instance.
    max_pages_per_request: int | None = None

    def __init__(self, conn: Connection, extra: dict[str, Any], model: str) -> None:
        self.conn = conn
        self.extra = extra
        self.model = model

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    @abstractmethod
    async def ocr(
        self,
        document: bytes,
        *,
        mime_type: str = "application/pdf",
        include_images: bool = False,
        image_annotation_schema: dict[str, Any] | None = None,
    ) -> OCRResult:
        """
        Read one document in one request.

        :param document: The document, small enough for one request.
        :param mime_type: The media type of the document.
        :param include_images: Whether to ask for the pictures of the pages, base64-encoded.
        :param image_annotation_schema: A JSON Schema the model fills for every picture it
            finds (its type, a description, ...); None asks for no annotation.
        :raises OCRTimeoutError: The service did not answer in time.
        :raises OCRBusyError: The service is throttling or failing for now.
        :raises OCRError: The service refused the document.
        """
