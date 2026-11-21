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
The OCR adapter for the Mistral OCR API (``POST /v1/ocr``), natively async.

The same contract is served by Mistral itself, by Mistral Document AI on Azure AI Foundry
(under ``/providers/mistral/azure/ocr``) and by gateways such as LiteLLM: the connection says
where, the adapter is the same.
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any, Self

import httpx2

from airflow.providers.common.ai.exceptions import OCRBusyError, OCRError, OCRTimeoutError
from airflow.providers.common.ai.ocr.base import OCRAdapter, OCRPage, OCRResult

if TYPE_CHECKING:
    from airflow.sdk import Connection

#: Seconds one request may take. A request that has not answered by then is better asked again,
#: as two smaller ones, than waited for.
REQUEST_TIMEOUT = 90


def _retry_after_seconds(retry_after: str | None) -> float | None:
    """Return the seconds a ``Retry-After`` header asks to wait, or None (absent, or an HTTP date)."""
    try:
        return max(float(retry_after), 0) if retry_after else None
    except ValueError:
        return None


class MistralOCRAdapter(OCRAdapter):
    """
    Reads documents with a Mistral OCR model.

    Connection fields:

    =========  ==================================================================
    Airflow    Meaning
    =========  ==================================================================
    host       Base URL of the service, ``https://api.mistral.ai`` or an Azure AI
               Foundry account (``https://<account>.services.ai.azure.com``)
    schema     ``https`` (default) when the host has no scheme
    password   API key, sent as a Bearer token
    extra      ``ocr_path``: path of the OCR endpoint, ``/v1/ocr`` by default;
               on Azure AI Foundry ``/providers/mistral/azure/ocr``.
               ``max_pages_per_request``: the most pages the service takes in
               one document (30 on Azure AI Foundry); unset means no limit
    =========  ==================================================================

    The ``HTTPS_PROXY``/``NO_PROXY`` environment is honoured.

    :param transport: Optional httpx2 transport, mainly to stub the server in tests.
    """

    name = "mistral"
    conn_types = frozenset({"pydanticai", "http"})

    def __init__(
        self,
        conn: Connection,
        extra: dict[str, Any],
        model: str,
        *,
        transport: httpx2.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(conn, extra, model)
        if not conn.password:
            raise OCRError(f"Connection {conn.conn_id!r} has no API key")
        host = (conn.host or "").rstrip("/")
        base_url = host if "://" in host else f"{conn.schema or 'https'}://{host}"
        self.url = base_url + extra.get("ocr_path", "/v1/ocr")
        self.max_pages_per_request = extra.get("max_pages_per_request")
        self._transport = transport
        self._client: httpx2.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        self._client = httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {self.conn.password}"},
            timeout=REQUEST_TIMEOUT,
            transport=self._transport,
        )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def ocr(
        self,
        document: bytes,
        *,
        mime_type: str = "application/pdf",
        include_images: bool = False,
        image_annotation_schema: dict[str, Any] | None = None,
    ) -> OCRResult:
        body: dict[str, Any] = {
            "model": self.model,
            "document": {
                "type": "document_url",
                "document_url": f"data:{mime_type};base64,{base64.b64encode(document).decode()}",
            },
            "include_image_base64": include_images,
        }
        if image_annotation_schema is not None:
            body["bbox_annotation_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "image_annotation",
                    "schema": image_annotation_schema,
                    "strict": True,
                },
            }
        answer = await self._post(body)
        return OCRResult(
            pages=[
                OCRPage(
                    index=page["index"],
                    markdown=page.get("markdown") or "",
                    images=page.get("images") or [],
                )
                for page in answer["pages"]
            ],
            model=answer.get("model") or self.model,
            usage=answer.get("usage_info") or {},
        )

    async def _post(self, body: dict[str, Any]) -> dict[str, Any]:
        """Return the service's answer to one request, or raise the error its status means."""
        if self._client is None:
            raise RuntimeError(f"{type(self).__name__} is used outside its 'async with' block")
        try:
            response = await self._client.post(self.url, json=body)
        except httpx2.TimeoutException as e:
            raise OCRTimeoutError(f"{self.url} did not answer within {REQUEST_TIMEOUT} s") from e
        if response.status_code == 429 or response.status_code >= 500:
            raise OCRBusyError(
                f"{self.url} answered {response.status_code}: {response.text}",
                status=response.status_code,
                retry_after=_retry_after_seconds(response.headers.get("Retry-After")),
            )
        if response.status_code >= 400:
            raise OCRError(
                f"{self.url} answered {response.status_code}: {response.text}",
                status=response.status_code,
            )
        return response.json()
