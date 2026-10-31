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

An artifact store keeps everything that lives under a bundle's ``published_root``:

- the **release reference** -- one mutable document naming the current version;
- the **immutable snapshots** -- one content-addressed tree per published version.

Manifest-backed Dag bundles perform all published-artifact access through this interface.

Contract notes for implementations:

- The release reference is the only mutable artifact. ``publication_guard`` must make the
  read-check-write sections that publishers run inside it safe against concurrent publishers.
- ``publish_snapshot`` and ``write_ref`` must be called inside ``publication_guard``. The guard
  removes leftovers of earlier failed publications when it is acquired, which is only safe
  while no other publisher can be mid-publication.
- Snapshots are immutable once committed and must not become visible half-written.
- Storage failures raise ``BundleManifestError``, or ``BundleManifestNotFoundError`` when the
  artifact is missing. ``BundleManifestSourceChangedError`` is reserved for reads of the
  publish source, which publishers retry as a benign concurrent edit.
- If a reference replacement completed or may have completed, but durability is unconfirmed,
  raise ``BundleManifestRefDurabilityError``. Readers may already have observed the replacement;
  callers must not roll it back or assume that retrying with the old expected version is safe.
- Locators (``root``, ``ref_path``, ``snapshots_root``, ``snapshot_path``) are opaque strings
  that identify artifacts in messages and results. They may look like, or be backed by,
  filesystem paths, but callers must not treat them as paths.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
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
    Raised when a release reference was, or may have been, replaced but its durability could not be confirmed.

    ``ref_path`` locates the reference to inspect before retrying publication. ``version``
    identifies the attempted version when the payload contains a string version, otherwise it is
    ``None``. It does not confirm the currently published version. The attempted replacement may
    already be visible, and another publisher may subsequently replace it; neither this context nor
    a read of the reference confirms crash durability. The underlying storage error is chained.
    """

    def __init__(self, *, ref_path: str, version: str | None) -> None:
        self.ref_path = ref_path
        self.version = version
        super().__init__(
            f"Release reference at {ref_path} may have been replaced, but durability could not be confirmed. "
            "Check the current reference before retrying publication."
        )


class ArtifactStore(ABC):
    """Storage backend for a bundle's published release reference and snapshots."""

    bundle_name: str

    @property
    @abstractmethod
    def root(self) -> str:
        """Locator of the published root this store manages."""

    @property
    @abstractmethod
    def ref_path(self) -> str:
        """Locator of the release reference document."""

    @property
    @abstractmethod
    def snapshots_root(self) -> str:
        """Locator of the published snapshots area for this bundle."""

    @abstractmethod
    def snapshot_path(self, version: str) -> str:
        """Locator of one published snapshot."""

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
        Atomically replace the release reference, the last step of a publication.

        Must be called inside ``publication_guard``.
        Return only after confirming durability. Raise ``BundleManifestRefDurabilityError``
        if replacement completed or may have completed, but durability could not be confirmed.
        The attempted replacement may already be visible and must not be rolled back; inspect
        the current reference before deciding whether to retry. Failures known to occur before
        replacement raise ``BundleManifestError``.
        """

    @abstractmethod
    def publication_guard(self) -> AbstractContextManager[None]:
        """
        Serialize publishers for this bundle across hosts.

        On acquire, remove temporary artifacts left by earlier failed publications.
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

        ``structural_validator`` runs against the published tree before any bytes are
        transferred when the backend exposes one; the caller fully validates the transferred
        copy afterwards either way. Raises ``BundleManifestNotFoundError`` when the version is
        not published, and ``ValueError`` when ``destination`` is not empty.
        """

    @abstractmethod
    def publish_snapshot(
        self,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
    ) -> bool:
        """
        Commit an immutable snapshot of ``manifest`` from a prepared local tree.

        The snapshot is published under ``manifest["version"]``. Must be called inside
        ``publication_guard``.

        Idempotent: when the version already exists, ``validate_existing`` checks it against
        the release reference and nothing is written. Returns whether a new snapshot was created,
        only once the snapshot is durable, including one committed by an earlier failed attempt.
        """
