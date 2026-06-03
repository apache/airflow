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
"""HTTP client used by the DAG processor to persist parse results via the API (AIP-92)."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from importlib import import_module
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from airflow.dag_processing.processor import DagFileParsingResult

log = logging.getLogger(__name__)

# Connection-level retries are safe for every request (the request never reached the server),
# so they are applied uniformly by the transport. Application-level retries (5xx / read timeout)
# are only added for idempotent calls; claim/delete calls must not be retried after the request
# may have been processed server-side, or callbacks/priority-requests could be silently lost.
_CONNECT_RETRIES = 3
_TRANSIENT_RETRIES = 3
_TRANSIENT_BACKOFF = 0.5


def _parse_iso_datetime(value: str) -> datetime:
    """
    Parse an ISO-8601 timestamp from the API server, tolerating a trailing ``Z``.

    The server emits UTC timestamps with a ``Z`` designator (e.g. ``2026-06-03T03:52:13.938753Z``),
    but ``datetime.fromisoformat`` only accepts ``Z`` on Python 3.11+. Normalise it to an explicit
    offset so parsing works on the oldest supported runtime (3.10).
    """
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    return datetime.fromisoformat(value)


# How long a token read from the provisioned file is reused before re-reading. Keeps the tight
# parse loop from stat-ing the file on every request while still picking up a rotated token quickly.
_TOKEN_CACHE_TTL = 30.0


class _CallableBearerAuth(httpx.Auth):
    """
    Attach a bearer token read from a callable, re-reading it as the deployment rotates it.

    The DAG processor never holds the signing key; a trusted component provisions its token to a
    file (``[dag_processor] api_token_path``) and rotates it there before expiry. Reading the token
    per-request (rather than baking it into the client once) lets a long-running processor pick up a
    rotated token without restarting. The value is cached for ``_TOKEN_CACHE_TTL`` to avoid a file
    read on every call; a ``401`` forces an immediate re-read and one retry, so a rotation that lands
    mid-window is honoured without waiting out the cache.
    """

    def __init__(self, token_getter: Callable[[], str | None], *, cache_ttl: float = _TOKEN_CACHE_TTL):
        self._token_getter = token_getter
        self._cache_ttl = cache_ttl
        self._cached: str | None = None
        self._cached_at = 0.0

    def _token(self, *, refresh: bool = False) -> str | None:
        now = time.monotonic()
        if refresh or self._cached is None or now - self._cached_at >= self._cache_ttl:
            self._cached = self._token_getter()
            self._cached_at = now
        return self._cached

    def auth_flow(self, request: httpx.Request):
        token = self._token()
        if token:
            request.headers["Authorization"] = f"Bearer {token}"
        response = yield request
        if response.status_code == 401:
            # The token may have rotated on disk since the cached read; re-read and retry once.
            fresh = self._token(refresh=True)
            if fresh and fresh != token:
                request.headers["Authorization"] = f"Bearer {fresh}"
                yield request


class DagProcessingApiClient:
    """
    Forward DAG-processor persistence to the ``/dag-processing`` API sub-app.

    Replaces the DAG processor manager's direct metadata-DB writes: parse results
    and stale reconciliation are sent over HTTP, so the processor process needs no
    DB credentials for persistence. A single :class:`httpx.Client` is reused so
    connections are pooled across the manager's parse loop.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token_getter: Callable[[], str | None] | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        # The token is read from ``token_getter`` per request (see _CallableBearerAuth), not baked
        # in, so a token rotated on disk by the deployment is picked up without a restart.
        auth = _CallableBearerAuth(token_getter) if token_getter is not None else None
        # ``retries`` retries connection failures (request never sent) at the transport level.
        self._client = httpx.Client(
            auth=auth,
            timeout=timeout,
            transport=httpx.HTTPTransport(retries=_CONNECT_RETRIES),
        )

    def close(self) -> None:
        self._client.close()

    def wait_until_ready(self, *, timeout: float = 60.0) -> None:
        """
        Block until the DAG Processing API answers ``/health`` or ``timeout`` elapses.

        The DAG processor and API server may start concurrently (e.g. ``airflow standalone``);
        this avoids crashing on a cold-start race before the server has bound its socket.
        """
        deadline = time.monotonic() + timeout
        delay = 0.5
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                resp = self._client.get(f"{self._base_url}/health")
                if resp.status_code < 500:
                    return
            except httpx.HTTPError as exc:
                last_exc = exc
            time.sleep(delay)
            delay = min(delay * 2, 5.0)
        log.warning(
            "DAG Processing API at %s not ready after %ss", self._base_url, timeout, exc_info=last_exc
        )

    def _send(
        self,
        method: str,
        path: str,
        *,
        retry_transient: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Send a request, retrying transient failures.

        Connection failures are retried by the transport. When ``retry_transient`` is set
        (idempotent calls only), read timeouts and 5xx responses are additionally retried with
        backoff. Non-idempotent claim/delete calls pass ``retry_transient=False`` so a response
        lost after the server processed the request does not cause a silent retry that drops rows.
        """
        url = f"{self._base_url}{path}"
        attempts = _TRANSIENT_RETRIES if retry_transient else 1
        for attempt in range(attempts):
            try:
                resp = self._client.request(method, url, **kwargs)
            except httpx.TransportError:
                if not retry_transient or attempt == attempts - 1:
                    raise
            else:
                if not (retry_transient and resp.status_code >= 500 and attempt < attempts - 1):
                    resp.raise_for_status()
                    return resp
            time.sleep(_TRANSIENT_BACKOFF * (2**attempt))
        # Unreachable: the loop either returns or raises on the final attempt.
        raise RuntimeError("unreachable")

    def persist_parsing_result(
        self,
        *,
        bundle_name: str,
        bundle_version: str | None,
        version_data: dict | None,
        parsing_result: DagFileParsingResult,
        run_duration: float,
        relative_fileloc: str | None,
    ) -> None:
        payload = {
            "bundle_name": bundle_name,
            "bundle_version": bundle_version,
            "version_data": version_data,
            "relative_fileloc": relative_fileloc,
            "run_duration": run_duration,
            "serialized_dags": [d.model_dump(mode="json") for d in parsing_result.serialized_dags],
            "import_errors": parsing_result.import_errors,
            "warnings": parsing_result.warnings or [],
        }
        self._send("POST", "/parsing-results", json=payload)
        log.debug("Persisted parse result via API: bundle=%s file=%s", bundle_name, relative_fileloc)

    def reconcile(self, *, bundle_name: str, observed_filelocs: Iterable[str]) -> None:
        payload = {"observed_filelocs": sorted(observed_filelocs)}
        self._send("POST", f"/bundles/{quote(bundle_name, safe='')}/reconcile", json=payload)
        log.debug("Reconciled bundle via API: bundle=%s", bundle_name)

    def get_bundle_state(self, bundle_name: str) -> dict | None:
        """Return ``{last_refreshed, version}`` for a bundle, or ``None`` if it has no record."""
        resp = self._send("GET", f"/bundles/{quote(bundle_name, safe='')}/state")
        data = resp.json()
        if not data.get("found"):
            return None
        last_refreshed = data.get("last_refreshed")
        return {
            "last_refreshed": _parse_iso_datetime(last_refreshed) if last_refreshed else None,
            "version": data.get("version"),
        }

    def update_bundle_state(self, bundle_name: str, *, last_refreshed: datetime, version: str | None) -> None:
        payload = {"last_refreshed": last_refreshed.isoformat(), "version": version}
        self._send("PATCH", f"/bundles/{quote(bundle_name, safe='')}/state", json=payload)

    def sync_bundles(self) -> None:
        self._send("POST", "/bundles/sync")

    def deactivate_stale_dags(self, *, stale_dag_threshold: int, last_parsed: list[dict]) -> None:
        payload = {"stale_dag_threshold": stale_dag_threshold, "last_parsed": last_parsed}
        self._send("POST", "/stale-dags", json=payload)

    def purge_inactive_dag_warnings(self) -> None:
        self._send("POST", "/purge-warnings")

    def claim_priority_files(self, bundle_names: list[str]) -> list[dict]:
        """Claim (select + delete) priority parse requests for the given bundles."""
        # Not idempotent (claim + delete): only the transport's connection-failure retry applies.
        resp = self._send(
            "POST",
            "/priority-parse-requests/claim",
            retry_transient=False,
            json={"bundle_names": bundle_names},
        )
        return resp.json().get("claimed", [])

    def fetch_callbacks(self, *, bundle_names: list[str], limit: int) -> list:
        """Claim callbacks via the API and rebuild typed ``CallbackRequest`` objects."""
        # Not idempotent (claim + delete): only the transport's connection-failure retry applies.
        resp = self._send(
            "POST",
            "/callbacks/claim",
            retry_transient=False,
            json={"bundle_names": bundle_names, "limit": limit},
        )
        module = import_module("airflow.callbacks.callback_requests")
        callbacks = []
        for data in resp.json().get("callbacks", []):
            callback_request_class = getattr(module, data["req_class"])
            callbacks.append(callback_request_class.from_json(data["req_data"]))
        return callbacks

    def register_job(self, job_type: str) -> int:
        """Register the processor's liveness Job row server-side and return its id."""
        resp = self._send("POST", "/jobs", json={"job_type": job_type})
        return resp.json()["job_id"]

    def job_heartbeat(self, job_id: int) -> None:
        self._send("POST", f"/jobs/{job_id}/heartbeat")

    def complete_job(self, job_id: int, *, state: str) -> None:
        self._send("POST", f"/jobs/{job_id}/complete", json={"state": state})
