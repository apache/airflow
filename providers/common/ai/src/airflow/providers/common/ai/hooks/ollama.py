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

import os

import httpx2


def build_ollama_native_root(base_url: str) -> str:
    """Return the server root of an Ollama URL, dropping a trailing ``/v1`` (the OpenAI-compatible path)."""
    return base_url.rstrip("/").removesuffix("/v1")


class OllamaBackend:
    """
    Load control for a model served by Ollama, through its native ``/api/generate`` endpoint.

    :param base_url: Ollama URL; a trailing ``/v1`` is dropped to reach the native API. When ``None``
        or empty, the ``OLLAMA_BASE_URL`` environment variable is used.
    :param model_name: Model name without the ``ollama:`` prefix.
    :param transport: Optional httpx2 transport, mainly to stub the server in tests.

    :raises ValueError: If *base_url* is not given and ``OLLAMA_BASE_URL`` is not set.

    Known limitation: the connection password is not sent, so no ``Authorization`` header is
    added and servers behind authentication are not supported.
    """

    def __init__(
        self,
        base_url: str | None,
        model_name: str,
        *,
        transport: httpx2.BaseTransport | None = None,
    ) -> None:
        if not (resolved_url := base_url or os.getenv("OLLAMA_BASE_URL")):
            raise ValueError(
                "OllamaBackend needs a base_url or the OLLAMA_BASE_URL environment variable; neither is set"
            )
        self.root = build_ollama_native_root(resolved_url)
        self.model_name = model_name
        self._transport = transport

    def load(self, *, timeout: float) -> None:
        """
        Load the model, waiting at most *timeout* seconds.

        :raises TimeoutError: If the server cannot be reached or does not finish loading within *timeout*.
        :raises httpx2.HTTPStatusError: If the server answers with an error status; the message names the
            model and carries the server's ``error`` text (the response body when it has none).
        """
        # A /api/generate request with no prompt and stream=False returns once the model is
        # available. keep_alive only controls how long the model stays loaded afterwards.
        try:
            with httpx2.Client(transport=self._transport, timeout=timeout) as client:
                response = client.post(
                    f"{self.root}/api/generate", json={"model": self.model_name, "stream": False}
                )
                response.raise_for_status()
        except httpx2.HTTPStatusError as e:
            raise httpx2.HTTPStatusError(
                f"Ollama could not load model '{self.model_name}' ({e.response.status_code}): "
                f"{self._extract_error_detail(e.response)}",
                request=e.request,
                response=e.response,
            ) from e
        except httpx2.ConnectTimeout as e:
            raise TimeoutError(f"Could not reach Ollama at {self.root} within {timeout} seconds.") from e
        except httpx2.TimeoutException as e:
            raise TimeoutError(
                f"Ollama did not finish loading model '{self.model_name}' within {timeout} seconds."
            ) from e

    @staticmethod
    def _extract_error_detail(response: httpx2.Response) -> str:
        """Return Ollama's ``error`` field, or the body cut to a short length when there is none."""
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict) and isinstance(error := body.get("error"), str) and error:
            return error
        return response.text.strip()[:200] or response.reason_phrase
