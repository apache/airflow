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

from typing import Protocol

import httpx

_DEFAULT_REQUEST_TIMEOUT = 30.0


def build_native_root(base_url: str) -> str:
    """Return the server root of an Ollama URL, dropping a trailing ``/v1`` (the OpenAI-compatible path)."""
    root = base_url.rstrip("/")
    root = root.removesuffix("/v1")
    return root.rstrip("/")


def normalize_model_name(model_name: str) -> str:
    """Return *model_name* with an explicit tag, treating a bare name as ``:latest``."""
    return model_name if ":" in model_name else f"{model_name}:latest"


class LocalModelBackend(Protocol):
    """Load state control for a model served by a local inference server."""

    def is_ready(self) -> bool:
        """Return whether the model is currently loaded."""
        ...

    def load(self, *, timeout: float) -> None:
        """Ask the server to load the model, waiting at most *timeout* seconds for the request."""
        ...

    def unload(self) -> None:
        """Ask the server to unload the model."""
        ...


class OllamaBackend:
    """
    :class:`LocalModelBackend` for the native Ollama API (``/api/ps`` and ``/api/generate``).

    ``/api/ps`` reports names as stored by Ollama, so a ``hf.co/org/model:tag`` style name is
    compared as written; only a missing tag is normalized (to ``:latest``).

    :param base_url: Ollama URL; a trailing ``/v1`` is dropped to reach the native API.
    :param model_name: Model name without the ``ollama:`` prefix.
    :param transport: Optional httpx transport, mainly to stub the server in tests.

    Known limitation: the connection password is not sent, so no ``Authorization`` header is
    added and servers behind authentication are not supported.
    """

    def __init__(
        self,
        base_url: str,
        model_name: str,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.root = build_native_root(base_url)
        self.model_name = model_name
        self._transport = transport

    def is_ready(self) -> bool:
        with httpx.Client(transport=self._transport, timeout=_DEFAULT_REQUEST_TIMEOUT) as client:
            response = client.get(f"{self.root}/api/ps")
            response.raise_for_status()
        wanted = normalize_model_name(self.model_name)
        return any(model.get("name") == wanted for model in response.json().get("models") or [])

    def load(self, *, timeout: float) -> None:
        """
        Load the model, waiting at most *timeout* seconds.

        :raises TimeoutError: If the server does not answer within *timeout*.
        """
        # Without keep_alive this call blocks until the model is loaded; callers still poll
        # is_ready() afterwards rather than trusting the response alone.
        try:
            self._post_generate({"model": self.model_name, "stream": False}, timeout=timeout)
        except httpx.TimeoutException as e:
            raise TimeoutError(
                f"Ollama did not finish loading model '{self.model_name}' within {timeout} seconds."
            ) from e

    def unload(self) -> None:
        self._post_generate(
            {"model": self.model_name, "keep_alive": 0, "stream": False},
            timeout=_DEFAULT_REQUEST_TIMEOUT,
        )

    def _post_generate(self, body: dict, *, timeout: float) -> None:
        with httpx.Client(transport=self._transport, timeout=timeout) as client:
            response = client.post(f"{self.root}/api/generate", json=body)
            response.raise_for_status()
