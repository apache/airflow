#
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
"""Metadata-database artifact store for manifest-backed Dag bundles."""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import exists, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from airflow.dag_processing.bundles._filesystem import ensure_destination_is_within_root
from airflow.dag_processing.bundles.artifact_store import (
    ArtifactStore,
    ArtifactStoreConflictError,
    BundleManifestNotFoundError,
    BundleManifestRefDurabilityError,
    PublishedSnapshot,
)
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    serialize_bundle_version_manifest,
    validate_bundle_relative_path,
    validate_bundle_version_manifest_structure,
)
from airflow.models.dag_bundle_manifest import DagBundleManifest, DagBundleManifestRef, get_bundle_manifests
from airflow.serialization.definitions.notset import NOTSET, ArgNotSet, is_arg_set
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from sqlalchemy.engine import CursorResult, Row


class DatabaseArtifactStore(ArtifactStore):
    """
    Artifact store that records manifests in the metadata database and leaves the files in their own backend.

    The release reference, which names the bundle's current version, is a row in
    ``dag_bundle_manifest_ref``. Each snapshot is a row in ``dag_bundle_manifest``. A snapshot holds
    only the manifest, so publishing copies no file bytes. ``fetch_snapshot`` downloads the files the
    manifest lists through ``fetch_file``, using each entry's ``version_id`` to get the exact object
    that was published.

    The Dag processor publishes and refreshes through the database. A pinned worker has no database
    access, so it fetches from ``version_data``, the manifest the scheduler ships with the workload.

    Publishers are not serialized by a lock. A manifest is inserted only if its version is absent,
    and ``write_ref`` replaces the reference only if no one else has since it was read.
    Republishing a version keeps the first manifest and its ``version_id`` values.

    :param fetch_file: Called as ``fetch_file(relative_path, version_id, destination)`` to write one
        file. ``version_id`` is the object version recorded in the manifest, or ``None`` if it has
        none. Any exception it raises is reported as a ``BundleManifestError``.
    :param version_data: Manifest shipped with a pinned workload, if any.
    """

    def __init__(
        self,
        *,
        bundle_name: str,
        backend_type: str,
        fetch_file: Callable[[str, str | None, Path], None],
        version_data: dict[str, Any] | None = None,
    ) -> None:
        self.bundle_name = bundle_name
        self._backend_type = backend_type
        self._fetch_file = fetch_file
        self._version_data = version_data
        self._guard_active = False
        self._observed_ref_revision: int | None | ArgNotSet = NOTSET

    @property
    def backend_type(self) -> str:
        return self._backend_type

    @property
    def ref_locator(self) -> str:
        return f"database:dag_bundle_manifest_ref/{self.bundle_name}"

    def snapshot_locator(self, version: str) -> str:
        return f"database:dag_bundle_manifest/{self.bundle_name}/{version}"

    def read_ref(self) -> dict[str, Any]:
        with _raise_storage_error(f"read release reference {self.ref_locator}"):
            row = self._read_ref_row()
        if self._guard_active and not is_arg_set(self._observed_ref_revision):
            self._observed_ref_revision = None if row is None else row.revision
        if row is None:
            raise BundleManifestNotFoundError(
                f"Bundle '{self.bundle_name}' has no release reference at {self.ref_locator}"
            )
        if not isinstance(row.payload, dict):
            raise BundleManifestError(
                f"Bundle '{self.bundle_name}' release reference {self.ref_locator} is not a JSON object"
            )
        return row.payload

    def write_ref(self, payload: dict[str, Any]) -> None:
        with _raise_storage_error(f"write release reference {self.ref_locator}"):
            if self._guard_active and is_arg_set(self._observed_ref_revision):
                expected_revision = self._observed_ref_revision
            else:
                row = self._read_ref_row()
                expected_revision = None if row is None else row.revision
            try:
                new_revision = self._replace_ref(payload, expected_revision=expected_revision)
            except IntegrityError:
                new_revision = None
            if new_revision is None:
                row = self._read_ref_row()
                if row is None or row.payload != payload:
                    raise ArtifactStoreConflictError(
                        f"Release reference {self.ref_locator} was replaced by another publisher"
                    )
                new_revision = row.revision
            if self._guard_active:
                self._observed_ref_revision = new_revision

    def _replace_ref(self, payload: dict[str, Any], *, expected_revision: int | None) -> int | None:
        """Replace the reference if still at ``expected_revision``; return the new revision, else ``None``."""
        new_revision = 1 if expected_revision is None else expected_revision + 1
        with create_session(scoped=False) as session:
            if expected_revision is None:
                session.add(
                    DagBundleManifestRef(
                        bundle_name=self.bundle_name,
                        version=_get_payload_version(payload),
                        payload=payload,
                        revision=new_revision,
                    )
                )
                session.flush()
            else:
                result = cast(
                    "CursorResult",
                    session.execute(
                        update(DagBundleManifestRef)
                        .where(
                            DagBundleManifestRef.bundle_name == self.bundle_name,
                            DagBundleManifestRef.revision == expected_revision,
                        )
                        .values(version=_get_payload_version(payload), payload=payload, revision=new_revision)
                        .execution_options(synchronize_session=False)
                    ),
                )
                if result.rowcount != 1:
                    return None
            try:
                session.commit()
            except SQLAlchemyError as e:
                # The commit may have reached the database before the error.
                raise BundleManifestRefDurabilityError(
                    ref_locator=self.ref_locator, version=_get_payload_version(payload)
                ) from e
        return new_revision

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        """Scope the conditions ``write_ref`` applies. This guard is not a lock."""
        self._guard_active = True
        self._observed_ref_revision = NOTSET
        try:
            yield
        finally:
            self._guard_active = False
            self._observed_ref_revision = NOTSET

    def prepare_publish_areas(self) -> None:
        """Nothing to create; the tables come from the database migration."""

    def snapshot_exists(self, version: str) -> bool:
        with _raise_storage_error(f"check published snapshot {self.snapshot_locator(version)}"):
            with create_session(scoped=False) as session:
                return bool(
                    session.scalar(
                        select(
                            exists().where(
                                DagBundleManifest.bundle_name == self.bundle_name,
                                DagBundleManifest.bundle_version == version,
                            )
                        )
                    )
                )

    def fetch_snapshot(
        self,
        version: str,
        destination: Path,
        *,
        structural_validator: Callable[[Path], None],
    ) -> None:
        # There is no published tree for ``structural_validator`` to inspect; the manifest is
        # checked here instead, and the bundle validates the fetched files after.
        manifest = self._get_manifest(version)
        validate_bundle_version_manifest_structure(
            manifest,
            expected_version=version,
            expected_bundle_name=self.bundle_name,
            expected_backend_type=self.backend_type,
        )
        if destination.exists() and any(destination.iterdir()):
            raise ValueError(f"Snapshot fetch destination {destination} must be absent or empty")
        destination.mkdir(parents=True, exist_ok=True)
        for file_info in manifest["files"]:
            file_path = destination / validate_bundle_relative_path(file_info["path"])
            ensure_destination_is_within_root(destination, file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                self._fetch_file(file_info["path"], file_info.get("version_id"), file_path)
            except BundleManifestError:
                raise
            except Exception as e:
                raise BundleManifestError(
                    f"Cannot fetch {file_info['path']!r} (version id {file_info.get('version_id')!r}) "
                    f"of snapshot {self.snapshot_locator(version)}: {e}"
                ) from e
            file_path.chmod(0o755 if file_info["executable"] else 0o644)
        (destination / MANIFEST_FILE_NAME).write_bytes(serialize_bundle_version_manifest(manifest))

    def publish_snapshot(
        self,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
    ) -> PublishedSnapshot:
        version = manifest["version"]
        with _raise_storage_error(f"publish snapshot {self.snapshot_locator(version)}"):
            existing = self._read_manifest(version)
            if existing is None:
                try:
                    with create_session(scoped=False) as session:
                        session.add(
                            DagBundleManifest(
                                bundle_name=self.bundle_name, bundle_version=version, manifest=manifest
                            )
                        )
                        session.flush()
                except IntegrityError:
                    existing = self._read_manifest(version)
                    if existing is None:
                        raise
                else:
                    return PublishedSnapshot(manifest=manifest, created=True)
        validate_bundle_version_manifest_structure(
            existing,
            expected_version=version,
            expected_bundle_name=self.bundle_name,
            expected_backend_type=self.backend_type,
        )
        return PublishedSnapshot(manifest=existing, created=False)

    def _get_manifest(self, version: str) -> dict[str, Any]:
        if self._version_data and self._version_data.get("version") == version:
            return self._version_data
        with _raise_storage_error(f"fetch published snapshot {self.snapshot_locator(version)}"):
            manifest = self._read_manifest(version)
        if manifest is None:
            raise BundleManifestNotFoundError(
                f"Bundle '{self.bundle_name}' version '{version}' is not published at "
                f"{self.snapshot_locator(version)}"
            )
        return manifest

    def _read_manifest(self, version: str) -> dict[str, Any] | None:
        with create_session(scoped=False) as session:
            return get_bundle_manifests([(self.bundle_name, version)], session=session).get(
                (self.bundle_name, version)
            )

    def _read_ref_row(self) -> Row[Any] | None:
        with create_session(scoped=False) as session:
            return session.execute(
                select(DagBundleManifestRef.payload, DagBundleManifestRef.revision).where(
                    DagBundleManifestRef.bundle_name == self.bundle_name
                )
            ).one_or_none()


def _get_payload_version(payload: dict[str, Any]) -> str | None:
    version = payload.get("version")
    return version if isinstance(version, str) else None


@contextmanager
def _raise_storage_error(action: str) -> Iterator[None]:
    """Re-raise a database or stored-JSON error as a bundle error, as the ``ArtifactStore`` contract requires."""
    try:
        yield
    except BundleManifestError:
        raise
    except (SQLAlchemyError, json.JSONDecodeError) as e:
        raise BundleManifestError(f"Cannot {action}: {e}") from e
