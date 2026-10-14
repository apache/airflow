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

"""Session credential sources for Dag processor API clients."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from time import monotonic
from typing import Protocol

import httpx

from airflow.api_fastapi.execution_api.datamodels.job import DagProcessorSessionTokenResponse
from airflow.api_fastapi.execution_api.versions import bundle
from airflow.sdk.api.client import Client  # noqa: SDK001 - reuse Execution API TLS and retry settings


class SessionTokenProvider(Protocol):
    """Provide credentials without prescribing where they are obtained."""

    def get_token(
        self, *, force_refresh: bool = False, retry: bool = False, timeout: httpx.Timeout | None = None
    ) -> str: ...

    def close(self) -> None: ...


class FileSessionTokenProvider:
    """Read credentials written by the existing trusted provisioner."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def get_token(
        self, *, force_refresh: bool = False, retry: bool = False, timeout: httpx.Timeout | None = None
    ) -> str:
        token = self.path.read_text().strip()
        if not token:
            raise ValueError(f"Dag processor token file is empty: {self.path}")
        return token

    def close(self) -> None:
        pass


class _SessionTokenClient(Client):
    def _update_auth(self, response: httpx.Response) -> None:
        # Runtime refresh headers must not replace the external workload identity.
        pass


class WorkloadIdentitySessionTokenProvider:
    """
    Exchange deployment identity for a session JWT, retaining the JWT only in memory.

    The identity supplier must return a current, renewable deployment credential. For example, it may
    read a projected service-account token that the platform rotates. The owning Dag processor client
    closes this provider; do not share it between clients.
    """

    def __init__(
        self,
        *,
        base_url: str,
        identity_token: Callable[[], str],
        **kwargs,
    ):
        if httpx.URL(base_url).scheme != "https":
            raise ValueError("Workload identity exchange requires an HTTPS Execution API URL")
        self._identity_token = identity_token
        self._token: str | None = None
        self._renew_at = 0.0
        self._client = _SessionTokenClient(base_url=base_url, token="", follow_redirects=False, **kwargs)

    def get_token(
        self, *, force_refresh: bool = False, retry: bool = False, timeout: httpx.Timeout | None = None
    ) -> str:
        if not force_refresh and self._token is not None and monotonic() < self._renew_at:
            return self._token
        identity = self._identity_token().strip()
        if not identity:
            raise ValueError("Workload identity credential is empty")
        started_at = monotonic()
        response = self._client.request(
            "POST",
            "dag-processor/session-token",
            headers={
                "Authorization": f"Bearer {identity}",
                "Airflow-API-Version": bundle.version_values[0],
            },
            retry=retry,
            timeout=timeout or self._client.timeout,
        )
        response.raise_for_status()
        issued = DagProcessorSessionTokenResponse.model_validate_json(response.content)
        expires_at = started_at + issued.expires_in
        if monotonic() >= expires_at:
            raise ValueError("Session credential expired during the exchange")
        self._token = issued.token
        self._renew_at = started_at + issued.expires_in * 0.8
        return self._token

    def close(self) -> None:
        self._token = None
        self._client.close()
