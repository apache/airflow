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
"""Claim and acknowledgment lifecycle for Dag processor work from the Execution API."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import attrs
import httpx2

from airflow.callbacks.callback_requests import CallbackRequest, DagCallbackRequest
from airflow.configuration import conf
from airflow.dag_processing.collection import is_source_discovery_failed
from airflow.dag_processing.requested_work import RequestedWorkSource
from airflow.utils.file import find_enclosing_file
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from collections.abc import Collection
    from uuid import UUID

    from airflow.api_fastapi.execution_api.datamodels.dag_parsing import ProcessorWorkItem
    from airflow.dag_processing.api_client import DagProcessorAPIClient
    from airflow.dag_processing.bundles.base import BaseDagBundle
    from airflow.dag_processing.manager import DagFileInfo

_API_SEND_BUDGET_SECONDS = 1.0
_MAX_CALLBACK_RETRY_DELAY = 60.0


@attrs.define
class DeferredCallback:
    """A claimed callback whose bundle could not be prepared, retried until it expires."""

    expires_at: float
    attempts: int = 0
    next_attempt_time: float = 0.0


@attrs.define
class PriorityParseClaim:
    """A priority request served only after an inventory captured following the claim."""

    work: ProcessorWorkItem
    inventory_sequence: int
    attempt_id: UUID | None = None


@attrs.define
class ExecutionAPIWorkSource(RequestedWorkSource, LoggingMixin):
    """Retain claimed work until its terminal outcome can be acknowledged by the API."""

    client: DagProcessorAPIClient
    _callback_claims: dict[int, ProcessorWorkItem] = attrs.field(factory=dict, init=False)
    _deferred_api_callbacks: list[CallbackRequest] = attrs.field(factory=list, init=False)
    _deferred_callback_retries: dict[int, DeferredCallback] = attrs.field(factory=dict, init=False)
    _callback_attempts: dict[UUID, list[ProcessorWorkItem]] = attrs.field(factory=dict, init=False)
    _priority_claims: dict[tuple[str, UUID], PriorityParseClaim] = attrs.field(factory=dict, init=False)
    _pending_work_acks: list[tuple[Literal["callbacks", "priority"], ProcessorWorkItem]] = attrs.field(
        factory=list, init=False
    )

    requires_acknowledgment = True

    def claim_priority_files(
        self,
        bundles: Collection[BaseDagBundle],
        *,
        capacity: int,
        unavailable_bundles: Collection[str],
        active_attempts: Collection[UUID],
        inventory_sequence: int,
    ) -> list[DagFileInfo]:
        """Claim priority files within the capacity left by active and retained work."""
        from airflow.dag_processing.manager import DagFileInfo

        bundles_by_name = {bundle.name: bundle for bundle in bundles}
        unavailable = set(unavailable_bundles)
        available = (
            capacity
            - sum(
                claim.attempt_id in active_attempts or claim.work.bundle_name not in unavailable
                for claim in self._priority_claims.values()
            )
            - sum(kind == "priority" for kind, _ in self._pending_work_acks)
        )
        if available <= 0:
            return []
        # Retain failed-bundle claims for recovery without letting them consume healthy bundles' capacity.
        # Excluding those bundles from new claims also bounds the suspended backlog.
        eligible_bundles = [name for name in bundles_by_name if name not in unavailable]
        try:
            work = self.client.claim_work("priority", eligible_bundles, available)
        except httpx2.HTTPError:
            self.log.warning("Unable to claim priority parses; retrying on a later loop")
            return []
        files = []
        for item in work:
            self._priority_claims[item.id, item.claim_id] = PriorityParseClaim(
                work=item, inventory_sequence=inventory_sequence
            )
            bundle = bundles_by_name[item.bundle_name]
            path = find_enclosing_file(bundle.path / item.relative_fileloc)
            files.append(
                DagFileInfo(
                    rel_path=path.relative_to(bundle.path) if path else Path(item.relative_fileloc),
                    bundle_name=bundle.name,
                    bundle_path=bundle.path,
                )
            )
        return files

    def fetch_callbacks(self, bundles: Collection[BaseDagBundle], *, limit: int) -> list[CallbackRequest]:
        """Claim callbacks for ready bundles and include requests awaiting bundle preparation."""
        available = (
            limit
            - len(self._callback_claims)
            - sum(len(work) for work in self._callback_attempts.values())
            - sum(kind == "callbacks" for kind, _ in self._pending_work_acks)
        )
        if available <= 0:
            return list(self._deferred_api_callbacks)
        eligible_bundles = [b.name for b in bundles if not b.supports_versioning or b.is_initialized]
        try:
            work = self.client.claim_work("callbacks", eligible_bundles, available)
        except httpx2.HTTPError:
            self.log.warning("Unable to claim callbacks; retrying on a later loop")
            return list(self._deferred_api_callbacks)
        for item in work:
            if item.callback is None:
                raise ValueError("Callback claim returned no request")
            # Each claim needs its own identity, even when a client reuses an equal callback object.
            request = item.callback.model_copy(deep=True)
            self._callback_claims[id(request)] = item
            self._deferred_api_callbacks.append(request)
        return list(self._deferred_api_callbacks)

    def callbacks_started(self, attempt_id: UUID, callbacks: Collection[CallbackRequest]) -> None:
        """Associate a callback process with the claims it will execute."""
        claims = [
            self._callback_claims.pop(id(request))
            for request in callbacks
            if id(request) in self._callback_claims
        ]
        if claims:
            self._callback_attempts[attempt_id] = claims

    def parse_started(self, file: DagFileInfo, attempt_id: UUID) -> None:
        """Associate an ordinary parse with the priority requests it will satisfy."""
        for claim in self._get_priority_claims(file):
            claim.attempt_id = attempt_id

    def attempt_finished(self, attempt_id: UUID, *, had_callbacks: bool) -> None:
        """Queue acknowledgments only for claims assigned to this terminal attempt."""
        if had_callbacks:
            for item in self._callback_attempts.pop(attempt_id, []):
                self._pending_work_acks.append(("callbacks", item))
            return
        for key, claim in list(self._priority_claims.items()):
            if claim.attempt_id == attempt_id:
                self._pending_work_acks.append(("priority", claim.work))
                del self._priority_claims[key]

    def flush(self, *, deadline: float | None = None) -> None:
        """Send queued acknowledgments in order until the deadline, keeping claims after a failure."""
        if deadline is None:
            deadline = time.monotonic() + _API_SEND_BUDGET_SECONDS
        while self._pending_work_acks and time.monotonic() < deadline:
            kind, item = self._pending_work_acks[0]
            try:
                self.client.acknowledge_work(kind, item)
            except httpx2.HTTPStatusError as error:
                if error.response.status_code in (408, 429) or error.response.status_code >= 500:
                    self.log.warning("Unable to acknowledge requested work; retaining the claim")
                    return
                if error.response.status_code != 409:
                    # Retrying cannot succeed; the server releases the claim when this Job ends.
                    self.log.warning(
                        "The API refused to acknowledge %s work %s (HTTP %s)",
                        kind,
                        item.id,
                        error.response.status_code,
                    )
            except httpx2.RequestError:
                self.log.warning("Unable to acknowledge requested work; retaining the claim")
                return
            self._pending_work_acks.pop(0)

    def has_outstanding_work(self) -> bool:
        return bool(
            self._pending_work_acks
            or self._callback_claims
            or self._callback_attempts
            or self._priority_claims
        )

    def _get_priority_claims(self, file: DagFileInfo) -> list[PriorityParseClaim]:
        if file.bundle_version is not None:
            return []
        return [
            claim
            for claim in self._priority_claims.values()
            if claim.work.bundle_name == file.bundle_name
            and (
                claim.work.relative_fileloc == str(file.rel_path)
                or claim.work.relative_fileloc.startswith(str(file.rel_path) + "/")
            )
        ]

    def is_priority_ready(self, file: DagFileInfo, inventory_sequence: int) -> bool:
        return all(
            inventory_sequence >= claim.inventory_sequence for claim in self._get_priority_claims(file)
        )

    def has_priority_request(self, file: DagFileInfo) -> bool:
        return bool(self._get_priority_claims(file))

    def has_callback_claim(self, request: CallbackRequest) -> bool:
        return id(request) in self._callback_claims

    def has_callback_attempt(self, attempt_id: UUID) -> bool:
        return attempt_id in self._callback_attempts

    def is_callback_preparation_ready(self, request: CallbackRequest) -> bool:
        retry = self._deferred_callback_retries.get(id(request))
        return retry is None or time.monotonic() >= retry.next_attempt_time

    def callback_queued(self, request: CallbackRequest) -> None:
        """Stop retrying preparation while retaining the queued callback's claim."""
        if self.has_callback_claim(request):
            self._remove_deferred_callback(request)

    def callback_discarded(self, request: CallbackRequest) -> None:
        """Acknowledge a claimed callback that cannot execute."""
        if item := self._callback_claims.pop(id(request), None):
            self._pending_work_acks.append(("callbacks", item))
            self._remove_deferred_callback(request)

    def _remove_deferred_callback(self, request: CallbackRequest) -> None:
        # Equal callback payloads can belong to different claims.
        self._deferred_api_callbacks = [
            deferred for deferred in self._deferred_api_callbacks if deferred is not request
        ]
        self._deferred_callback_retries.pop(id(request), None)

    def callback_deferred(self, request: CallbackRequest) -> None:
        """Retry preparing a claimed callback's bundle with backoff, then drop the unpreparable request."""
        if not self.has_callback_claim(request):
            return
        now = time.monotonic()
        timeout = conf.getint("dag_processor", "job_heartbeat_timeout")
        retry = self._deferred_callback_retries.setdefault(id(request), DeferredCallback(now + timeout))
        if now >= retry.expires_at:
            if isinstance(request, DagCallbackRequest):
                target = f"dag_id={request.dag_id} run_id={request.run_id}"
            else:
                ti = request.ti
                target = f"dag_id={ti.dag_id} run_id={ti.run_id} task_id={ti.task_id}"
            self.log.error(
                "Dropping callback after bundle preparation timed out",
                callback_type=type(request).__name__,
                filepath=request.filepath,
                bundle_name=request.bundle_name,
                bundle_version=request.bundle_version,
                target=target,
                timeout=timeout,
                timeout_setting="[dag_processor] job_heartbeat_timeout",
            )
            self.callback_discarded(request)
            return
        retry.attempts += 1
        retry.next_attempt_time = min(
            now + min(2 ** (retry.attempts - 1), _MAX_CALLBACK_RETRY_DELAY), retry.expires_at
        )

    def inventory_published(
        self,
        bundle_name: str,
        *,
        sequence: int,
        files: Collection[str],
        failed_sources: Collection[str],
    ) -> None:
        """Acknowledge missing files only after an authoritative inventory newer than their claims."""
        failed = set(failed_sources)
        for key, claim in list(self._priority_claims.items()):
            item = claim.work
            if (
                item.bundle_name == bundle_name
                and sequence >= claim.inventory_sequence
                and item.relative_fileloc not in files
                and not is_source_discovery_failed(item.relative_fileloc, failed)
            ):
                self._pending_work_acks.append(("priority", item))
                del self._priority_claims[key]

    def bundle_unusable(self, bundle_name: str, *, active_attempts: Collection[UUID]) -> None:
        """Release unusable-bundle requests except those still assigned to active attempts."""
        for key, claim in list(self._priority_claims.items()):
            if claim.work.bundle_name == bundle_name and claim.attempt_id not in active_attempts:
                self._pending_work_acks.append(("priority", claim.work))
                del self._priority_claims[key]
