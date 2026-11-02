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

import math
from collections.abc import Callable
from pathlib import Path
from time import monotonic
from typing import Protocol

import httpx2
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_random_exponential

from airflow.api_fastapi.execution_api.datamodels.job import DagProcessorSessionTokenResponse
from airflow.api_fastapi.execution_api.versions import bundle
from airflow.sdk.api.client import (  # noqa: SDK001 - reuse Execution API TLS and retry settings
    API_RETRIES,
    API_RETRY_WAIT_MAX,
    API_RETRY_WAIT_MIN,
    Client,
    _log_and_trace_retry,
    _should_retry_api_request,
)

_backoff = wait_random_exponential(min=API_RETRY_WAIT_MIN, max=API_RETRY_WAIT_MAX)


class WorkloadIdentityUnavailableError(OSError):
    """The identity supplier failed; the client handles it like an unreadable credential file."""


def _wait_for_retry_after(retry_state: RetryCallState) -> float:
    """Back off like the SDK client, but never retry a 503 before its ``Retry-After`` elapses."""
    wait = _backoff(retry_state)
    error = retry_state.outcome.exception() if retry_state.outcome else None
    if not isinstance(error, httpx2.HTTPStatusError) or error.response.status_code != 503:
        return wait
    try:
        retry_after = float(error.response.headers["Retry-After"])
    except (KeyError, ValueError):  # Missing, or an HTTP-date rather than seconds.
        return wait
    if not math.isfinite(retry_after):
        return wait
    return max(wait, retry_after)


class SessionTokenProvider(Protocol):
    """Provide credentials without prescribing where they are obtained."""

    def get_token(
        self, *, force_refresh: bool = False, retry: bool = False, timeout: httpx2.Timeout | None = None
    ) -> str: ...

    def close(self) -> None: ...


class FileSessionTokenProvider:
    """Read credentials written by the existing trusted provisioner."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def get_token(
        self, *, force_refresh: bool = False, retry: bool = False, timeout: httpx2.Timeout | None = None
    ) -> str:
        token = self.path.read_text().strip()
        if not token:
            raise ValueError(f"Dag processor token file is empty: {self.path}")
        return token

    def close(self) -> None:
        pass


class _SessionTokenClient(Client):
    def _update_auth(self, response: httpx2.Response) -> None:
        # Runtime refresh headers must not replace the external workload identity.
        pass

    # The server refetches signing keys at most every 10 seconds; the SDK's backoff alone often retries
    # every attempt inside that cooldown.
    @retry(
        retry=retry_if_exception(_should_retry_api_request),
        stop=stop_after_attempt(API_RETRIES),
        wait=_wait_for_retry_after,
        before_sleep=_log_and_trace_retry,
        reraise=True,
    )
    def _request_with_retry(self, *args, **kwargs):
        return self._request_without_retry(*args, **kwargs)


class WorkloadIdentitySessionTokenProvider:
    """
    Exchange deployment identity for a session JWT, retaining the JWT only in memory.

    The identity supplier must return a current, renewable deployment credential. For example, it may
    read a projected service-account token that the platform rotates. Any exception it raises is
    re-raised as ``WorkloadIdentityUnavailableError``, so a failed early renewal keeps the valid Job
    credential in use. The owning Dag processor client closes this provider; do not share it between
    clients.
    """

    def __init__(
        self,
        *,
        base_url: str,
        identity_token: Callable[[], str],
        **kwargs,
    ):
        if httpx2.URL(base_url).scheme != "https":
            raise ValueError("Workload identity exchange requires an HTTPS Execution API URL")
        self._identity_token = identity_token
        self._token: str | None = None
        self._renew_at = 0.0
        self._client = _SessionTokenClient(base_url=base_url, token="", follow_redirects=False, **kwargs)

    def get_token(
        self, *, force_refresh: bool = False, retry: bool = False, timeout: httpx2.Timeout | None = None
    ) -> str:
        if not force_refresh and self._token is not None and monotonic() < self._renew_at:
            return self._token
        try:
            identity = self._identity_token().strip()
        except Exception as error:
            raise WorkloadIdentityUnavailableError("Unable to obtain the workload identity") from error
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
