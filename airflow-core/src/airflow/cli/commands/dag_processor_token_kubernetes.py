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
"""Publish Dag processor session tokens to an existing Kubernetes Secret."""

from __future__ import annotations

import base64
import binascii
import os
import re
import ssl
from pathlib import Path
from typing import TYPE_CHECKING

import httpx2

if TYPE_CHECKING:
    from types import TracebackType

_SERVICE_ACCOUNT_DIRECTORY = Path("/var/run/secrets/kubernetes.io/serviceaccount")
_TOKEN_KEY = "processor.jwt"
_DNS_LABEL = r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?"


class KubernetesSecretTokenError(RuntimeError):
    """A Secret could not be read or updated without exposing its contents."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class KubernetesSecretTokenStore:
    """Read and conditionally update one pre-created Secret using in-cluster credentials."""

    def __init__(self, secret_name: str, namespace: str):
        if len(namespace) > 63 or re.fullmatch(_DNS_LABEL, namespace) is None:
            raise ValueError("Kubernetes namespace must be a DNS label of at most 63 characters")
        if len(secret_name) > 253 or re.fullmatch(rf"{_DNS_LABEL}(?:\.{_DNS_LABEL})*", secret_name) is None:
            raise ValueError("Kubernetes Secret name must be a DNS subdomain of at most 253 characters")
        self._path = f"/api/v1/namespaces/{namespace}/secrets/{secret_name}"
        self._resource_version: str | None = None
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS") or os.environ.get("KUBERNETES_SERVICE_PORT")
        if not host or not port:
            raise ValueError(
                "Kubernetes API access requires KUBERNETES_SERVICE_HOST and "
                "KUBERNETES_SERVICE_PORT_HTTPS or KUBERNETES_SERVICE_PORT"
            )
        try:
            port_number = int(port)
            if not 1 <= port_number <= 65535:
                raise ValueError
        except ValueError:
            raise ValueError("Kubernetes service port must be an integer between 1 and 65535") from None
        self._client = httpx2.Client(
            base_url=httpx2.URL(scheme="https", host=host, port=port_number),
            verify=ssl.create_default_context(cafile=str(_SERVICE_ACCOUNT_DIRECTORY / "ca.crt")),
            timeout=10,
            follow_redirects=False,
            trust_env=False,
        )

    def __enter__(self) -> KubernetesSecretTokenStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._client.close()

    def _request(self, method: str, *, body: dict | None = None) -> httpx2.Response:
        # Kubelet rotates this credential independently of the session token we publish.
        try:
            bearer = (_SERVICE_ACCOUNT_DIRECTORY / "token").read_text().strip()
        except OSError:
            raise KubernetesSecretTokenError("Could not read the Kubernetes service account token") from None
        if not bearer:
            raise KubernetesSecretTokenError("The Kubernetes service account token is empty")
        headers = {"Authorization": f"Bearer {bearer}"}
        if body is not None:
            headers["Content-Type"] = "application/merge-patch+json"
        try:
            response = self._client.request(method, self._path, headers=headers, json=body)
        except httpx2.HTTPError as error:
            raise KubernetesSecretTokenError(
                f"Kubernetes Secret {method} request failed ({type(error).__name__})"
            ) from None
        if not response.is_success:
            raise KubernetesSecretTokenError(
                f"Kubernetes Secret {method} returned HTTP {response.status_code}",
                status_code=response.status_code,
            )
        return response

    def read_token(self) -> str | None:
        """Read the current token and retain the version required for the next write."""
        self._resource_version = None
        response = self._request("GET")
        try:
            secret = response.json()
            resource_version = secret["metadata"]["resourceVersion"]
            if not isinstance(resource_version, str) or not resource_version:
                raise ValueError
            data = secret.get("data")
            if data is None:
                data = {}
            if not isinstance(data, dict):
                raise ValueError
            encoded = data.get(_TOKEN_KEY, "")
            if not isinstance(encoded, str):
                raise ValueError
            token = base64.b64decode(encoded, validate=True).decode().strip() or None
        except (ValueError, TypeError, KeyError, AttributeError, binascii.Error):
            raise KubernetesSecretTokenError("Kubernetes Secret has invalid token data or metadata") from None
        self._resource_version = resource_version
        return token

    def write_token(self, token: str) -> None:
        """Update only the token, rejecting writes based on a stale Secret version."""
        resource_version = self._resource_version
        self._resource_version = None
        if resource_version is None:
            raise KubernetesSecretTokenError("Read the Kubernetes Secret successfully before writing a token")
        if not token.strip():
            raise ValueError("The Dag processor session token must not be empty")
        # Consume the version even on an uncertain response: the next attempt must read the persisted session.
        self._request(
            "PATCH",
            body={
                "metadata": {"resourceVersion": resource_version},
                "data": {_TOKEN_KEY: base64.b64encode(token.encode()).decode("ascii")},
            },
        )
