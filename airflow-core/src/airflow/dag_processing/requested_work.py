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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Sequence
    from uuid import UUID

    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.dag_processing.bundles.base import BaseDagBundle
    from airflow.dag_processing.manager import DagFileInfo


class RequestedWorkSource:
    """Supply requested parses and callbacks, and track their delivery lifecycle."""

    requires_acknowledgment = False

    def claim_priority_files(
        self,
        bundles: Collection[BaseDagBundle],
        *,
        capacity: int,
        unavailable_bundles: Collection[str],
        active_attempts: Collection[UUID],
        inventory_sequence: int,
    ) -> list[DagFileInfo]:
        """Claim priority parses using the manager's current dispatch context."""
        return []

    def fetch_callbacks(self, bundles: Collection[BaseDagBundle], *, limit: int) -> list[CallbackRequest]:
        """Claim callbacks for the configured bundles."""
        return []

    def attempt_finished(self, attempt_id: UUID, *, had_callbacks: bool) -> None:
        """
        Finish claims for this attempt's work kind.

        Callback completion must not release priority claims. Ordinary parses finish only after
        publication reaches a terminal outcome or the parse fails without producing a result.
        """

    def flush(self, *, deadline: float | None = None) -> None:
        """Deliver pending acknowledgments within the supplied time budget."""

    def has_outstanding_work(self) -> bool:
        """Return whether claimed work or acknowledgments remain pending."""
        return False

    def callbacks_started(self, attempt_id: UUID, callbacks: Sequence[CallbackRequest]) -> None:
        """Assign the supplied callbacks to a processing attempt."""

    def parse_started(self, file: DagFileInfo, attempt_id: UUID) -> None:
        """Assign priority requests for this file to a processing attempt."""

    def is_priority_ready(self, file: DagFileInfo, inventory_sequence: int) -> bool:
        """Return whether the inventory is recent enough for this file's requests."""
        return True

    def has_priority_request(self, file: DagFileInfo) -> bool:
        """Return whether the source holds a priority request for this file."""
        return False

    def has_callback_claim(self, request: CallbackRequest) -> bool:
        """Return whether the source holds a claim for this callback."""
        return False

    def has_callback_attempt(self, attempt_id: UUID) -> bool:
        """Return whether callbacks are assigned to this processing attempt."""
        return False

    def is_callback_preparation_ready(self, request: CallbackRequest) -> bool:
        """Return whether preparation of this callback's bundle may be retried."""
        return True

    def callback_deferred(self, request: CallbackRequest) -> None:
        """Retain a callback whose bundle could not be prepared."""

    def callback_queued(self, request: CallbackRequest) -> None:
        """Record that a callback is ready for dispatch."""

    def callback_discarded(self, request: CallbackRequest) -> None:
        """Finish a callback that cannot be executed."""

    def inventory_published(
        self,
        bundle_name: str,
        *,
        sequence: int,
        files: Collection[str],
        failed_sources: Collection[str],
    ) -> None:
        """Reconcile priority requests against the bundle's accepted inventory."""

    def bundle_unusable(self, bundle_name: str, *, active_attempts: Collection[UUID]) -> None:
        """Finish requests for an unusable bundle unless their attempts are still active."""


class DatabaseRequestedWorkSource(RequestedWorkSource):
    """Use the manager's existing transactional database claim operations."""

    def __init__(
        self,
        claim_priority_files: Callable[[], list[DagFileInfo]],
        fetch_callbacks: Callable[[], list[CallbackRequest]],
    ) -> None:
        self._claim_priority_files = claim_priority_files
        self._fetch_callbacks = fetch_callbacks

    def claim_priority_files(
        self,
        bundles: Collection[BaseDagBundle],
        *,
        capacity: int,
        unavailable_bundles: Collection[str],
        active_attempts: Collection[UUID],
        inventory_sequence: int,
    ) -> list[DagFileInfo]:
        return self._claim_priority_files()

    def fetch_callbacks(self, bundles: Collection[BaseDagBundle], *, limit: int) -> list[CallbackRequest]:
        return self._fetch_callbacks()
