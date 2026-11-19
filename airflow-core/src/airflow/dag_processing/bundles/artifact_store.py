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
"""
Contract for the published-artifact backend of manifest-backed Dag bundles.

An artifact store holds a bundle's release reference, the one mutable document naming the
current version, and one immutable snapshot per published version: the bundle files and the
manifest that lists them. Manifest-backed Dag bundles access published artifacts only through
this interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.dag_processing.bundles.manifest import BundleManifestError

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from pathlib import Path


class BundleManifestNotFoundError(BundleManifestError, FileNotFoundError):
    """
    Raised when a published artifact -- the release reference or a snapshot -- does not exist.

    It is a ``FileNotFoundError`` so that code which already handles a missing file treats a
    missing artifact the same way, and a ``BundleManifestError`` so that it is still reported
    as a bundle error. It is distinct from ``BundleManifestSourceChangedError``: an artifact
    that is absent is not a source that changed while it was read.
    """


class BundleManifestRefDurabilityError(BundleManifestError):
    """
    Raised when the release reference was, or may have been, replaced but its durability is unconfirmed.

    ``ref_locator`` identifies the reference to inspect before retrying publication. ``version``
    is the attempted version, or ``None`` when the payload has no string version. The replacement
    may already be visible to readers. The underlying storage error is chained.
    """

    def __init__(self, *, ref_locator: str, version: str | None) -> None:
        self.ref_locator = ref_locator
        self.version = version
        super().__init__(
            f"Release reference at {ref_locator} may have been replaced, but durability could not be confirmed. "
            "Check the current reference before retrying publication."
        )


class ArtifactStoreConflictError(BundleManifestError):
    """
    Raised by a compare-and-swap write, such as ``write_ref``, when another publisher wrote first.

    It is retryable: the caller re-reads the reference and decides again.
    """


@dataclass(frozen=True)
class PublishedSnapshot:
    """
    Result of ``ArtifactStore.publish_snapshot``.

    ``manifest`` is the manifest of the committed snapshot: for a version that was already
    published, the existing one. ``created`` is whether this call committed the snapshot.
    """

    manifest: dict[str, Any]
    created: bool


class ArtifactStore(ABC):
    """
    Storage backend for a bundle's published release reference and snapshots.

    Storage failures raise ``BundleManifestError``, or ``BundleManifestNotFoundError`` when an
    artifact is missing. ``BundleManifestSourceChangedError`` is reserved for reads of the
    publish source.
    """

    bundle_name: str

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """``backend.type`` recorded in the manifests and release references this store publishes."""

    @property
    @abstractmethod
    def ref_locator(self) -> str:
        """Opaque identifier of the release reference, for messages and results."""

    @abstractmethod
    def snapshot_locator(self, version: str) -> str:
        """Opaque identifier of one published snapshot, for messages and results."""

    @abstractmethod
    def read_ref(self) -> dict[str, Any]:
        """
        Read the release reference payload.

        Raises ``BundleManifestNotFoundError`` when no reference is published, and
        ``BundleManifestError`` when it is not a JSON object.
        """

    @abstractmethod
    def write_ref(self, payload: dict[str, Any]) -> None:
        """
        Atomically replace the release reference. Must be called inside ``publication_guard``.

        Returns only once the replacement is durable. Raises ``BundleManifestRefDurabilityError``
        when the replacement completed or may have completed but its durability is unconfirmed;
        the replacement must not be rolled back. Other failures raise ``BundleManifestError``.

        When ``publication_guard`` is not a lock, the replacement is conditional on the reference
        read inside the same guard being unchanged, or still absent if that read found none; with
        no such read, on the reference read just before writing. Raises
        ``ArtifactStoreConflictError`` when another publisher replaced it first, unless the
        winner wrote byte-identical content.
        """

    @abstractmethod
    def publication_guard(self) -> AbstractContextManager[None]:
        """
        Protect a publisher's read-check-write section against concurrent publishers.

        Either a lock that serializes the bundle's publishers across hosts and, on acquire,
        removes leftovers of earlier failed publications; or, for a store that writes no
        temporary artifacts, the scope of the conditions ``write_ref`` applies. Entering such a
        guard discards conditions from reads made before it.
        """

    @abstractmethod
    def prepare_publish_areas(self) -> None:
        """Create the snapshot and reference areas publishers write to."""

    @abstractmethod
    def snapshot_exists(self, version: str) -> bool:
        """Whether a committed snapshot exists for the version."""

    @abstractmethod
    def fetch_snapshot(
        self,
        version: str,
        destination: Path,
        *,
        structural_validator: Callable[[Path], None],
    ) -> None:
        """
        Transfer a published snapshot into ``destination``, which must be absent or empty.

        ``destination`` receives only regular files: the files the manifest lists, with their
        executable bits, and the manifest at ``MANIFEST_FILE_NAME``. Each manifest path must pass
        ``validate_bundle_relative_path`` before anything is written to it. A store that can
        access the published tree must call ``structural_validator`` on it before transferring
        anything; the validator checks those paths and rejects any other entry. Raises
        ``BundleManifestNotFoundError`` when the version is not published, and ``ValueError``
        when ``destination`` is not empty.
        """

    @abstractmethod
    def publish_snapshot(
        self,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
    ) -> PublishedSnapshot:
        """
        Commit an immutable snapshot of ``manifest`` from the prepared tree at ``source_root``.

        The snapshot is published under ``manifest["version"]`` and is never visible
        half-written. Must be called inside ``publication_guard``.

        When the version is already committed, the existing snapshot is kept and returned:
        ``validate_existing`` checks it against its version and bundle, or, without a local tree
        to pass, the store checks its manifest against the version. That manifest may differ
        from ``manifest`` in fields outside the version, such as per-file object version ids.
        When ``publication_guard`` is not a lock, the manifest is committed only if absent, so
        a concurrent publisher of the same version cannot overwrite it.

        Returns the committed manifest and whether this call created the snapshot, once the
        snapshot is durable, including one committed by an earlier failed attempt.
        """
