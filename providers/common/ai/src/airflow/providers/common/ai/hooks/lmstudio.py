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

import time
from typing import Any

import httpx2

from airflow.providers.common.ai.utils.readiness_errors import translate_readiness_errors

# Endpoints and fields: https://lmstudio.ai/docs/developer/rest/list and
# https://lmstudio.ai/docs/developer/rest/load
_LIST_MODELS_PATH = "/api/v1/models"
_LOAD_MODEL_PATH = "/api/v1/models/load"


def build_lmstudio_native_root(base_url: str) -> str:
    """Return the server root of an LM Studio URL, dropping a trailing ``/api/v1`` or ``/v1``."""
    return base_url.rstrip("/").removesuffix("/api/v1").removesuffix("/v1")


class LMStudioBackend:
    """
    Load control for a model served by LM Studio, through its native ``/api/v1/models`` endpoints.

    Pass the class to ``ensure_model_ready`` as ``backend_factory=LMStudioBackend``.

    The model list request gets *timeout* seconds and the load request gets what the list request left
    of them. Each is an httpx2 timeout, which applies to the connect, write and read phases separately,
    so the two requests together can still take longer than *timeout*.

    :param base_url: LM Studio URL; a trailing ``/v1`` (OpenAI-compatible) or ``/api/v1`` (native)
        is dropped to reach the server root.
    :param model_name: Model key as listed by ``GET /api/v1/models``; any other name raises
        ``ValueError`` instead of loading a model.
    :param transport: Optional httpx2 transport, mainly to stub the server in tests.

    :raises ValueError: If *base_url* is ``None`` or empty.

    Known limitation: no ``Authorization`` header is sent, so LM Studio with API token
    authentication turned on is not supported.
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
        Load the model unless LM Studio already lists it with a loaded instance, within *timeout* seconds.

        :raises TimeoutError: If a request exceeds the time it was given (see the class docstring).
        :raises httpx2.ConnectError: If the server cannot be reached.
        :raises httpx2.HTTPStatusError: If the server answers with an error status; the message says
            whether listing or loading failed and carries the ``error`` text when the body has one,
            otherwise the start of the response body.
        :raises ValueError: If the model list is not a JSON object whose ``models`` is a list of objects,
            or if none of its entries has a ``key`` equal to the model name; nothing is loaded then.
        """
        deadline = time.monotonic() + timeout
        with httpx2.Client(transport=self._transport) as client:
            with translate_readiness_errors(
                server="LM Studio",
                root=self.root,
                timeout=timeout,
                failure="could not list models",
                timeout_failure="did not list its models",
            ):
                loaded = self._check_model_loaded(client, timeout=timeout)
            if loaded:
                return
            if (remaining := deadline - time.monotonic()) <= 0:
                raise TimeoutError(
                    f"LM Studio spent all {timeout} seconds listing models, leaving no time to load "
                    f"model '{self.model_name}'."
                )
            with translate_readiness_errors(
                server="LM Studio",
                root=self.root,
                timeout=remaining,
                failure=f"could not load model '{self.model_name}'",
                timeout_failure=f"did not finish loading model '{self.model_name}'",
            ):
                # LM Studio loads a second instance of an already-loaded model, hence the list first.
                response = client.post(
                    f"{self.root}{_LOAD_MODEL_PATH}", json={"model": self.model_name}, timeout=remaining
                )
                response.raise_for_status()

    def _check_model_loaded(self, client: httpx2.Client, *, timeout: float) -> bool:
        """
        Return whether the model's entry in the model list has a loaded instance.

        :raises ValueError: If no entry in the model list has a ``key`` equal to the model name.
        """
        response = client.get(f"{self.root}{_LIST_MODELS_PATH}", timeout=timeout)
        response.raise_for_status()
        models = self._extract_models(response)
        keys = [key for m in models if isinstance(key := m.get("key"), str)]
        if self.model_name not in keys:
            listed = ", ".join(f"'{key}'" for key in keys) if keys else "none"
            raise ValueError(
                f"LM Studio at {self.root} lists no model with key '{self.model_name}'; "
                f"listed keys: {listed}. Use one of the listed keys as the model name."
            )
        return any(
            m.get("key") == self.model_name
            and isinstance(instances := m.get("loaded_instances"), list)
            and bool(instances)
            for m in models
        )

    def _extract_models(self, response: httpx2.Response) -> list[dict[str, Any]]:
        """Return the ``models`` entries of the list response, or raise ``ValueError`` on another shape."""
        prefix = f"LM Studio at {self.root} answered GET {_LIST_MODELS_PATH} with"
        try:
            body = response.json()
        except ValueError as e:
            raise ValueError(f"{prefix} a body that is not JSON.") from e
        if not isinstance(body, dict):
            raise ValueError(f"{prefix} a JSON {type(body).__name__}, expected an object with 'models'.")
        models = body.get("models")
        if not isinstance(models, list):
            raise ValueError(f"{prefix} 'models' of type {type(models).__name__}, expected a list.")
        if not all(isinstance(m, dict) for m in models):
            raise ValueError(f"{prefix} a 'models' list with entries that are not objects.")
        return models
