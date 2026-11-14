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


def build_lmstudio_native_root(base_url: str) -> str:
    """Return the server root of an LM Studio URL, dropping a trailing ``/api/v1`` or ``/v1``."""
    return base_url.rstrip("/").removesuffix("/api/v1").removesuffix("/v1")


class LMStudioBackend:
    """
    Load control for a model served by LM Studio, through its native ``/api/v1/models`` endpoints.

    Pass it to ``ensure_model_ready`` with
    ``backend_factory=lambda model_name, base_url: LMStudioBackend(base_url, model_name)``.

    :param base_url: LM Studio URL; a trailing ``/v1`` (OpenAI-compatible) or ``/api/v1`` (native)
        is dropped to reach the server root.
    :param model_name: Model key as listed by LM Studio.
    :param transport: Optional httpx2 transport, mainly to stub the server in tests.

    :raises ValueError: If *base_url* is ``None`` or empty.

    Known limitation: no ``Authorization`` header is sent, so servers behind authentication
    are not supported.
    """

    def __init__(
        self,
        base_url: str | None,
        model_name: str,
        *,
        transport: httpx2.BaseTransport | None = None,
    ) -> None:
        if not base_url:
            raise ValueError("LMStudioBackend needs a base_url; pass the LM Studio server URL")
        self.root = build_lmstudio_native_root(base_url)
        self.model_name = model_name
        self._transport = transport

    def load(self, *, timeout: float) -> None:
        """
        Load the model unless LM Studio already has it loaded, waiting at most *timeout* seconds.

        :raises TimeoutError: If the server cannot be reached or does not finish loading within *timeout*.
        :raises httpx2.HTTPStatusError: If the server answers with an error status; the message names the
            model and carries the server's ``error`` text (the response body when it has none).
        """
        try:
            with httpx2.Client(transport=self._transport, timeout=timeout) as client:
                # The load docs do not say whether loading a loaded model is a no-op, so check first.
                if self._is_loaded(client):
                    return
                response = client.post(f"{self.root}/api/v1/models/load", json={"model": self.model_name})
                response.raise_for_status()
        except httpx2.HTTPStatusError as e:
            raise httpx2.HTTPStatusError(
                f"LM Studio could not load model '{self.model_name}' ({e.response.status_code}): "
                f"{self._extract_error_detail(e.response)}",
                request=e.request,
                response=e.response,
            ) from e
        except httpx2.ConnectTimeout as e:
            raise TimeoutError(f"Could not reach LM Studio at {self.root} within {timeout} seconds.") from e
        except httpx2.TimeoutException as e:
            raise TimeoutError(
                f"LM Studio did not finish loading model '{self.model_name}' within {timeout} seconds."
            ) from e

    def _is_loaded(self, client: httpx2.Client) -> bool:
        """Return whether the model's entry in ``GET /api/v1/models`` has a loaded instance."""
        response = client.get(f"{self.root}/api/v1/models")
        response.raise_for_status()
        models = response.json().get("models", [])
        return any(m.get("key") == self.model_name and m.get("loaded_instances") for m in models)

    @staticmethod
    def _extract_error_detail(response: httpx2.Response) -> str:
        """Return the ``error`` field when it is a string, or the body cut to a short length."""
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict) and isinstance(error := body.get("error"), str) and error:
            return error
        return response.text.strip()[:200] or response.reason_phrase
