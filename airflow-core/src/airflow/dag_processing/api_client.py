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
    from collections.abc import Iterable

    from airflow.dag_processing.processor import DagFileParsingResult

log = logging.getLogger(__name__)

# Connection-level retries are safe for every request (the request never reached the server),
# so they are applied uniformly by the transport. Application-level retries (5xx / read timeout)
# are only added for idempotent calls; claim/delete calls must not be retried after the request
# may have been processed server-side, or callbacks/priority-requests could be silently lost.
_CONNECT_RETRIES = 3
_TRANSIENT_RETRIES = 3
_TRANSIENT_BACKOFF = 0.5


class DagProcessingApiClient:
    """
    Forward DAG-processor persistence to the ``/dag-processing`` API sub-app.

    Replaces the DAG processor manager's direct metadata-DB writes: parse results
    and stale reconciliation are sent over HTTP, so the processor process needs no
    DB credentials for persistence. A single :class:`httpx.Client` is reused so
    connections are pooled across the manager's parse loop.
    """

    def __init__(self, base_url: str, *, token: str | None = None, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        # ``retries`` retries connection failures (request never sent) at the transport level.
        self._client = httpx.Client(
            headers=headers,
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
            "last_refreshed": datetime.fromisoformat(last_refreshed) if last_refreshed else None,
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
