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


def build_ollama_native_root(base_url: str) -> str:
    """Return the server root of an Ollama URL, dropping a trailing ``/v1`` (the OpenAI-compatible path)."""
    return base_url.rstrip("/").removesuffix("/v1")


class OllamaBackend:
    """
    Load control for a model served by Ollama, through its native ``/api/generate`` endpoint.

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

    def load(self, *, timeout: float) -> None:
        """
        Load the model, waiting at most *timeout* seconds.

        :raises TimeoutError: If the server cannot be reached or does not finish loading within *timeout*.
        """
        # A /api/generate request with no prompt and stream=False returns once the model is
        # available. keep_alive only controls how long the model stays loaded afterwards.
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
