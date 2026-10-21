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

import httpx2

DEFAULT_REQUEST_TIMEOUT = 30.0


def build_ollama_native_root(base_url: str) -> str:
    """Return the server root of an Ollama URL, dropping a trailing ``/v1`` (the OpenAI-compatible path)."""
    return base_url.rstrip("/").removesuffix("/v1")


def normalize_ollama_model_name(model_name: str) -> str:
    """Return *model_name* with an explicit tag, treating a bare name as ``:latest``."""
    last_segment = model_name.rsplit("/", 1)[-1]
    return model_name if ":" in last_segment else f"{model_name}:latest"


class OllamaBackend:
    """
    Load state control for a model served by Ollama, through its native API (``/api/ps`` and ``/api/generate``).

    Model names are compared as written; only a missing tag is normalized (to ``:latest``).

    :param base_url: Ollama URL; a trailing ``/v1`` is dropped to reach the native API.
    :param model_name: Model name without the ``ollama:`` prefix.
    :param transport: Optional httpx2 transport, mainly to stub the server in tests.

    Known limitation: the connection password is not sent, so no ``Authorization`` header is
    added and servers behind authentication are not supported.
    """

    def __init__(
        self,
        base_url: str,
        model_name: str,
        *,
        transport: httpx2.BaseTransport | None = None,
    ) -> None:
        self.root = build_ollama_native_root(base_url)
        self.model_name = model_name
        self._transport = transport

    def is_ready(self, *, timeout: float = DEFAULT_REQUEST_TIMEOUT) -> bool:
        """
        Return whether the model is loaded, waiting at most *timeout* seconds for the server.

        :raises TimeoutError: If the server does not answer within *timeout*.
        """
        try:
            with httpx2.Client(transport=self._transport, timeout=timeout) as client:
                response = client.get(f"{self.root}/api/ps")
                response.raise_for_status()
        except httpx2.TimeoutException as e:
            raise TimeoutError(
                f"Ollama did not answer the readiness check for model '{self.model_name}' "
                f"within {timeout} seconds."
            ) from e
        wanted = normalize_ollama_model_name(self.model_name)
        return any(model.get("name") == wanted for model in response.json()["models"])

    def load(self, *, timeout: float) -> None:
        """
        Load the model, waiting at most *timeout* seconds.

        :raises TimeoutError: If the server cannot be reached or does not finish loading within *timeout*.
        """
        # Without keep_alive this call blocks until the model is loaded; callers still poll
        # is_ready() afterwards rather than trusting the response alone.
        try:
            with httpx2.Client(transport=self._transport, timeout=timeout) as client:
                response = client.post(
                    f"{self.root}/api/generate", json={"model": self.model_name, "stream": False}
                )
                response.raise_for_status()
        except httpx2.ConnectTimeout as e:
            raise TimeoutError(f"Could not reach Ollama at {self.root} within {timeout} seconds.") from e
        except httpx2.TimeoutException as e:
            raise TimeoutError(
                f"Ollama did not finish loading model '{self.model_name}' within {timeout} seconds."
            ) from e
