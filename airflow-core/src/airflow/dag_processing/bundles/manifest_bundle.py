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
"""Shared lifecycle for immutable, content-addressed manifest-backed Dag bundles."""

from __future__ import annotations

import logging
import os
import stat
import tempfile
import weakref
from abc import ABC, abstractmethod
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.dag_processing.bundles.artifact_store import BundleManifestNotFoundError
from airflow.dag_processing.bundles.base import (
    BaseDagBundle,
    BundleVersion,
    BundleVersionLock,
    get_bundle_storage_root_path,
    get_bundle_tracking_file,
)
from airflow.dag_processing.bundles.filesystem_artifact_store import (
    FilesystemArtifactStore,
    ensure_destination_is_within_root,
    fsync_directory,
    fsync_file,
    fsync_tree,
    get_temp_snapshot_prefix,
    read_json_file,
    remove_bundle_tree_forcefully,
    remove_orphaned_temp_snapshots,
    set_cache_tree_permissions,
)
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    MANIFEST_SCHEMA_VERSION,
    BundleManifestError,
    BundleSourceSnapshot,
    build_bundle_version_manifest,
    build_ref_payload,
    compute_file_sha256,
    is_valid_bundle_version,
    validate_bundle_relative_path,
    validate_bundle_version,
    validate_bundle_version_manifest_structure,
    verify_bundle_version_manifest,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.dag_processing.bundles.artifact_store import ArtifactStore
    from airflow.dag_processing.bundles.manifest import BundleVersionManifest

log = logging.getLogger(__name__)

FILESYSTEM_ARTIFACT_BACKEND_TYPE = "local"


@contextmanager
def _reraise_oserror_as_manifest_error():
    """
    Re-raise ``OSError`` from published-artifact or cache access as ``BundleManifestError``.

    Keeps the Airflow-facing entry points to one error type for "this bundle cannot be read".
    A ``BundleManifestNotFoundError`` is already a ``BundleManifestError`` and passes through.
    """
    try:
        yield
    except BundleManifestError:
        raise
    except OSError as e:
        raise BundleManifestError(str(e)) from e


class BundleManifestReferenceChangedError(BundleManifestError):
    """Raised when a publisher's expected manifest reference is stale."""


@dataclass(frozen=True)
class BundleManifestRef:
    """Compact pointer to an immutable manifest bundle artifact."""

    version: str
    ref_payload: dict[str, Any]
    manifest_sha256: str


@dataclass(frozen=True)
class BundlePublishResult:
    """Result of publishing an immutable manifest bundle snapshot."""

    bundle_name: str
    version: str
    ref_payload: dict[str, Any]
    version_path: Path | str
    manifest_ref_path: Path | str
    manifest_sha256: str
    file_count: int
    total_size: int
    created_snapshot: bool


@dataclass(frozen=True)
class PreparedPublishSource:
    """One materialized source observation prepared by a concrete adapter."""

    root: Path
    source_snapshot: BundleSourceSnapshot


class ManifestDagBundleBase(BaseDagBundle, ABC):
    """
    Shared runtime for content-addressed manifest-backed Dag bundles.

    Publishers write immutable snapshots and a release reference under ``published_root``.
    Airflow parses and runs only validated, read-only cache copies of those snapshots, made
    in ``versions_dir``; the version is the content hash of the snapshot's manifest, so a
    pinned version is self-certifying.

    :param published_root: Shared filesystem path containing published snapshots and the
        current release reference.
    :param allow_empty_source: Permit publication of a source tree with no files. Off by
        default so that a failed or incomplete sync cannot remove every Dag from the release.
    """

    supports_versioning = True

    def __init__(
        self,
        *,
        published_root: str | None = None,
        allow_empty_source: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not published_root:
            # TypeError, not ValueError: callback bundle preparation treats a ValueError from
            # bundle construction as "bundle no longer configured" and silently drops the
            # callback. TypeError matches how any bundle class fails on a bad config kwarg.
            raise TypeError("published_root must be provided")
        published_root_str = str(published_root)
        # A URL-shaped value must fail loudly instead of becoming a filesystem directory
        # literally named after the URL. This also catches a URL mangled by pathlib
        # (``Path("s3://b")`` collapses to ``s3:/b``).
        if ":" in published_root_str.split("/", 1)[0]:
            raise TypeError(
                f"Unsupported published_root scheme: {published_root_str!r}. Use a filesystem path."
            )
        try:
            self._store: ArtifactStore = FilesystemArtifactStore(
                bundle_name=self.name,
                published_root=Path(published_root),
                cache_root=get_bundle_storage_root_path(),
            )
        except ValueError as e:
            raise TypeError(str(e)) from e
        if not isinstance(allow_empty_source, bool):
            raise TypeError("allow_empty_source must be a boolean")
        self.allow_empty_source = allow_empty_source
        self._validated_version_paths: set[tuple[str, str, str]] = set()
        self._current_manifest_ref: BundleManifestRef | None = None
        self._release_version_in_use: weakref.finalize | None = None

    # Path for a filesystem published_root; a store for a remote root may return URL strings.
    @property
    def published_root(self) -> Path | str:
        return self._store.root

    @property
    def manifest_ref_path(self) -> Path | str:
        return self._store.ref_path

    @property
    def published_versions_dir(self) -> Path | str:
        return self._store.snapshots_root

    @property
    def publication_lock_path(self) -> Path:
        lock_path = getattr(self._store, "lock_path", None)
        if lock_path is None:
            raise AttributeError("this bundle's artifact store has no publication lock file")
        return lock_path

    @abstractmethod
    def _confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        """Raise if the concrete source no longer matches ``prepared``."""

    def get_current_version(self) -> BundleVersion:
        """
        Return the current version.

        The version string alone is the whole contract: it is the content hash of the
        snapshot's manifest entries, so a pinned snapshot is self-certifying and no
        side-channel metadata needs to flow through Airflow.
        """
        if self.version:
            return BundleVersion(version=self.version)
        with _reraise_oserror_as_manifest_error():
            manifest_ref = self._ensure_current_manifest_ref()
        return BundleVersion(version=manifest_ref.version)

    def initialize(self) -> None:
        if self.version:
            self._validated_version_paths.clear()
            version = validate_bundle_version(self.version, source="pinned version")
            with _reraise_oserror_as_manifest_error():
                if not self._has_validated_cache(version, manifest_ref=None):
                    with self.lock():
                        self._materialize_cached_version(version=version, manifest_ref=None)
        else:
            self.refresh()
        super().initialize()

    def refresh(self) -> None:
        if self.version:
            raise ValueError("Refreshing a specific bundle version is not supported")

        self._validated_version_paths.clear()
        with _reraise_oserror_as_manifest_error():
            manifest_ref = self._read_current_manifest_ref()
            version_lock = self._acquire_version_in_use(manifest_ref.version)
            try:
                # Keep the previous release protected until the candidate is usable.
                if not self._has_validated_cache(manifest_ref.version, manifest_ref=manifest_ref):
                    with self.lock():
                        self._materialize_cached_version(
                            version=manifest_ref.version, manifest_ref=manifest_ref
                        )
                        if version_lock is not None and not self._is_version_lock_current(version_lock):
                            replacement_lock = self._acquire_version_in_use(manifest_ref.version)
                            version_lock.release()
                            version_lock = replacement_lock
                self._current_manifest_ref = manifest_ref
            except BaseException:
                # Another materializer may have replaced the tracking inode. Only remove
                # our own record, and serialize the missing-cache check with cache writers.
                with suppress(OSError), self.lock():
                    if (
                        version_lock is not None
                        and not (self.versions_dir / manifest_ref.version).exists()
                        and self._is_version_lock_current(version_lock)
                    ):
                        get_bundle_tracking_file(self.name, manifest_ref.version).unlink(missing_ok=True)
                raise
            else:
                if version_lock is not None:
                    previous_release = self._release_version_in_use
                    self._release_version_in_use = weakref.finalize(self, version_lock.release)
                    version_lock = None
                    if previous_release is not None:
                        previous_release()
            finally:
                if version_lock is not None:
                    version_lock.release()

    def _acquire_version_in_use(self, version: str) -> BundleVersionLock | None:
        """
        Acquire a candidate lock without releasing the currently served version's lock.

        The unpinned tree is served from ``versions_dir``, which that cleanup prunes by when
        tasks last used each version rather than by release order, so the current release can
        rank as stale. The cleanup skips a version whose tracking file is share-locked, and
        taking the lock before the cache is checked lets a removal already under way finish
        first. It is re-taken on every refresh: that keeps the timestamp current, and a
        corrupt-cache rebuild elsewhere deletes the tracking file, stranding an older lock.
        """
        version_lock = BundleVersionLock(bundle_name=self.name, bundle_version=version)
        try:
            version_lock.acquire()
        except OSError:
            with suppress(OSError):
                version_lock.release()
            # Like a task's lock, this must not stop the bundle from being served, e.g. when
            # another OS user owns the tracking file.
            log.warning(
                "Could not mark the bundle version as in use; stale-version cleanup may remove "
                "its cache. bundle=%s version=%s",
                self.name,
                version,
                exc_info=True,
            )
            return None
        return version_lock

    @staticmethod
    def _is_version_lock_current(version_lock: BundleVersionLock) -> bool:
        if version_lock.lock_file is None or version_lock.lock_file_path is None:
            return False
        try:
            return os.path.samestat(
                os.fstat(version_lock.lock_file.fileno()), version_lock.lock_file_path.stat()
            )
        except FileNotFoundError:
            return False

    def _get_validation_marker_path(self, version: str) -> Path:
        # A file, not a directory: the orphan sweep only matches temp snapshot directories.
        # Airflow's stale-version cleanup does not know about the marker; a marker without
        # its version dir is reaped by _remove_orphaned_validation_markers.
        return self.versions_dir / f".{version}.validated"

    def _has_validated_cache(self, version: str, *, manifest_ref: BundleManifestRef | None) -> bool:
        """
        Return whether a validated cache tree exists for the version.

        The persisted marker skips only the per-file hashing pass, not validation entirely:
        Airflow's stale cleanup can leave a truncated tree behind (an interrupted plain rmtree
        that knows nothing about the marker), and cache directories are owner-writable (a
        stale-cleanup requirement), so structure -- file set, types, no symlinks -- is
        re-checked on each initialization or refresh even when the marker vouches for the
        content hashes. Only repeated path lookups within that instance reuse the result.
        """
        cached_version_path = self.versions_dir / version
        if not cached_version_path.exists():
            return False
        cache_key = (self.name, version, str(cached_version_path))
        if cache_key in self._validated_version_paths:
            return True
        if not self._get_validation_marker_path(version).exists():
            return False
        try:
            self._validate_materialized_version(
                version_path=cached_version_path,
                version=version,
                manifest_ref=manifest_ref,
                check_content=False,
                allow_cached_bytecode=True,
            )
        except (BundleManifestError, OSError):
            log.warning(
                "Cached snapshot has a validation marker but failed the structural check; "
                "it must be rebuilt. bundle=%s version=%s",
                self.name,
                version,
                exc_info=True,
            )
            return False
        self._validated_version_paths.add(cache_key)
        return True

    def _record_validated_cache(self, version: str, cache_key: tuple[str, str, str]) -> None:
        """Persist a marker after the cache tree and its parent entry are durable."""
        marker_path = self._get_validation_marker_path(version)
        marker_path.write_text(version)
        with suppress(OSError):
            marker_path.chmod(0o644)
        fsync_file(marker_path)
        fsync_directory(marker_path.parent)
        self._validated_version_paths.add(cache_key)

    def acquire_publication_lock(self):
        """Serialize publishers through the artifact store's publication guard."""
        return self._store.publication_guard()

    def _materialize_cached_version(
        self,
        *,
        version: str,
        manifest_ref: BundleManifestRef | None,
    ) -> None:
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        remove_orphaned_temp_snapshots(self.versions_dir)
        self._remove_orphaned_validation_markers()
        cached_version_path = self.versions_dir / version
        cache_key = (self.name, version, str(cached_version_path))
        if cached_version_path.exists():
            # Cache files are read-only, so one full checksum validation per host is
            # enough; later calls need only the marker plus a structural re-check.
            if self._has_validated_cache(version, manifest_ref=manifest_ref):
                return
            try:
                self._validate_materialized_version(
                    version_path=cached_version_path,
                    version=version,
                    manifest_ref=manifest_ref,
                    allow_cached_bytecode=True,
                )
            except (BundleManifestError, OSError):
                log.warning(
                    "Cached snapshot failed validation and will be rebuilt. bundle=%s version=%s",
                    self.name,
                    version,
                    exc_info=True,
                )
                self._move_cached_version_aside(cached_version_path, version=version)
            else:
                snapshot_manifest = self._read_local_snapshot_manifest(cached_version_path)
                source_paths = {file_info["path"] for file_info in snapshot_manifest["files"]}
                fsync_tree(
                    cached_version_path,
                    ignore_file=lambda path: _is_cached_bytecode_file(
                        path.relative_to(cached_version_path), source_paths
                    ),
                )
                fsync_directory(self.versions_dir)
                self._record_validated_cache(version, cache_key)
                return

        tmp_path = Path(
            tempfile.mkdtemp(
                prefix=get_temp_snapshot_prefix(version),
                dir=self.versions_dir,
            )
        )
        try:
            # The store runs a structural pass on the published tree first (no hashing):
            # reject symlinks, special files, and unexpected entries before any bytes
            # are transferred.
            self._store.fetch_snapshot(
                version,
                tmp_path,
                structural_validator=lambda tree: self._validate_materialized_version(
                    version_path=tree,
                    version=version,
                    manifest_ref=manifest_ref,
                    check_content=False,
                ),
            )
            # Validating the copy also validates the published source in one read pass.
            self._validate_materialized_version(
                version_path=tmp_path,
                version=version,
                manifest_ref=manifest_ref,
            )
            set_cache_tree_permissions(tmp_path)
            fsync_tree(tmp_path)
            os.replace(tmp_path, cached_version_path)
            fsync_directory(self.versions_dir)
        except Exception:
            # Best effort only: a cleanup failure must not mask the original error.
            with suppress(OSError):
                remove_bundle_tree_forcefully(tmp_path)
            raise
        self._record_validated_cache(version, cache_key)

    def _move_cached_version_aside(self, cached_version_path: Path, *, version: str) -> None:
        # Rename instead of delete: a concurrent task may still read the old tree, and a
        # POSIX rename keeps its already-open files valid. The orphan sweep removes it later.
        self._get_validation_marker_path(version).unlink(missing_ok=True)
        # Also drop Airflow's usage-tracking file: its stale cleanup rmtree's the tracked
        # path catching only BlockingIOError, so a tracking file pointing at a version dir
        # this rename removes would crash every future cleanup sweep (and, on a scheduler
        # with a local executor, the scheduler itself). The next task run's
        # BundleVersionLock recreates the file.
        with suppress(OSError):
            get_bundle_tracking_file(bundle_name=self.name, version=version).unlink(missing_ok=True)
        cache_key = (self.name, version, str(cached_version_path))
        self._validated_version_paths.discard(cache_key)
        aside_dir = Path(
            tempfile.mkdtemp(
                prefix=get_temp_snapshot_prefix(version),
                dir=self.versions_dir,
            )
        )
        with suppress(OSError):
            cached_version_path.chmod(stat.S_IMODE(cached_version_path.stat().st_mode) | 0o700)
        # FileNotFoundError: a concurrent stale-version cleanup already removed the tree;
        # the rebuild that follows is all that is needed.
        with suppress(FileNotFoundError):
            os.rename(cached_version_path, aside_dir / "snapshot")

    def _validate_materialized_version(
        self,
        *,
        version_path: Path,
        version: str,
        manifest_ref: BundleManifestRef | None,
        check_content: bool = True,
        allow_cached_bytecode: bool = False,
    ) -> None:
        if manifest_ref:
            if manifest_ref.version != version:
                raise BundleManifestError(
                    f"Bundle manifest reference contains version {manifest_ref.version!r}, "
                    f"expected {version!r}"
                )
            self._validate_snapshot_for_ref(
                manifest_ref,
                version_path,
                check_content=check_content,
                allow_cached_bytecode=allow_cached_bytecode,
            )
            return

        snapshot_manifest = self._read_local_snapshot_manifest(version_path)
        self._validate_snapshot_manifest(snapshot_manifest, expected_version=version)
        self._validate_snapshot_files(
            snapshot_manifest,
            version_path,
            check_content=check_content,
            allow_cached_bytecode=allow_cached_bytecode,
        )

    def _ensure_current_manifest_ref(self) -> BundleManifestRef:
        # Only a cheap read of the release reference. Snapshot materialization and full
        # checksum validation happen in initialize()/refresh(); ``path`` can still do the
        # required structural check of a cache copy that has a validation marker.
        if self._current_manifest_ref is None:
            self._current_manifest_ref = self._read_current_manifest_ref()
        return self._current_manifest_ref

    def _read_current_manifest_ref(self) -> BundleManifestRef:
        payload = self._store.read_ref(
            missing_message=(
                f"Bundle '{self.name}' manifest reference file {self.manifest_ref_path} is missing. "
                "Publish the bundle or restore the release reference before refreshing this bundle."
            ),
            invalid_message=f"Bundle manifest reference is not valid JSON: {self.manifest_ref_path}",
        )
        return self._build_manifest_ref_from_payload(payload, source=str(self.manifest_ref_path))

    def _build_manifest_ref_from_payload(self, payload: dict[str, Any], source: str) -> BundleManifestRef:
        schema_version = payload.get("schema_version")
        if schema_version != MANIFEST_SCHEMA_VERSION:
            raise BundleManifestError(
                f"Bundle manifest reference {source} has unsupported schema_version {schema_version!r}"
            )

        bundle_name = payload.get("bundle_name")
        if bundle_name != self.name:
            raise BundleManifestError(
                f"Bundle manifest reference {source} is for bundle {bundle_name!r}, expected {self.name!r}"
            )

        version = payload.get("version")
        if not isinstance(version, str) or not version:
            raise BundleManifestError(f"Bundle manifest reference {source} does not contain a version")
        version = validate_bundle_version(version, source=f"manifest reference {source} version")

        backend = payload.get("backend")
        if not isinstance(backend, dict) or backend.get("type") != FILESYSTEM_ARTIFACT_BACKEND_TYPE:
            raise BundleManifestError(f"Bundle manifest reference {source} is not for a local backend")

        manifest_data = payload.get("manifest")
        if not isinstance(manifest_data, dict):
            raise BundleManifestError(f"Bundle manifest reference {source} does not contain manifest data")

        manifest_path = manifest_data.get("path")
        manifest_sha256 = manifest_data.get("sha256")
        if not isinstance(manifest_path, str):
            raise BundleManifestError(
                f"Bundle manifest reference {source} does not contain a valid manifest path"
            )
        if not isinstance(manifest_sha256, str) or not manifest_sha256:
            raise BundleManifestError(f"Bundle manifest reference {source} does not contain manifest sha256")
        self._validate_manifest_ref_path(manifest_path)

        file_count = payload.get("file_count")
        total_size = payload.get("total_size")
        # bool is an int subclass and True == 1, so it would slip through every check below.
        if not isinstance(file_count, int) or isinstance(file_count, bool) or file_count < 0:
            raise BundleManifestError(
                f"Bundle manifest reference {source} does not contain a valid file_count"
            )
        if not isinstance(total_size, int) or isinstance(total_size, bool) or total_size < 0:
            raise BundleManifestError(
                f"Bundle manifest reference {source} does not contain a valid total_size"
            )

        ref_payload = build_ref_payload(
            bundle_name=bundle_name,
            version=version,
            backend={"type": FILESYSTEM_ARTIFACT_BACKEND_TYPE},
            manifest_sha256=manifest_sha256,
            file_count=file_count,
            total_size=total_size,
        )
        return BundleManifestRef(
            version=version,
            ref_payload=ref_payload,
            manifest_sha256=manifest_sha256,
        )

    @staticmethod
    def _validate_manifest_ref_path(manifest_path: str) -> None:
        relative_path = validate_bundle_relative_path(manifest_path)
        if relative_path.as_posix() != MANIFEST_FILE_NAME:
            raise BundleManifestError(
                f"Bundle manifest reference must point to {MANIFEST_FILE_NAME!r}, got {manifest_path!r}"
            )

    def _validate_snapshot_for_ref(
        self,
        manifest_ref: BundleManifestRef,
        version_path: Path,
        *,
        check_content: bool = True,
        allow_cached_bytecode: bool = False,
    ) -> None:
        snapshot_manifest = self._read_local_snapshot_manifest(version_path)
        self._validate_snapshot_manifest(snapshot_manifest, expected_version=manifest_ref.version)
        expected_file_count = manifest_ref.ref_payload["file_count"]
        if snapshot_manifest["file_count"] != expected_file_count:
            raise BundleManifestError(
                f"Bundle manifest file_count {snapshot_manifest['file_count']!r} does not match "
                f"manifest reference {expected_file_count!r}"
            )
        expected_total_size = manifest_ref.ref_payload["total_size"]
        if snapshot_manifest["total_size"] != expected_total_size:
            raise BundleManifestError(
                f"Bundle manifest total_size {snapshot_manifest['total_size']!r} does not match "
                f"manifest reference {expected_total_size!r}"
            )
        verify_bundle_version_manifest(snapshot_manifest, manifest_ref.manifest_sha256)
        self._validate_snapshot_files(
            snapshot_manifest,
            version_path,
            check_content=check_content,
            allow_cached_bytecode=allow_cached_bytecode,
        )

    def _validate_snapshot_manifest(self, manifest: dict[str, Any], *, expected_version: str) -> None:
        validate_bundle_version_manifest_structure(
            manifest,
            expected_version=expected_version,
            expected_bundle_name=self.name,
            expected_backend_type=FILESYSTEM_ARTIFACT_BACKEND_TYPE,
        )

    def _validate_snapshot_files(
        self,
        snapshot_manifest: dict[str, Any],
        version_path: Path,
        *,
        check_content: bool = True,
        allow_cached_bytecode: bool = False,
    ) -> None:
        # check_content=False keeps the structural checks (path safety, symlinks, file
        # types, unexpected files) but skips the per-file hashing pass.
        expected_paths = {file_info["path"] for file_info in snapshot_manifest["files"]}
        observed_paths = {
            path.relative_to(version_path).as_posix()
            for path in self._iter_snapshot_files(
                version_path, cached_source_paths=expected_paths if allow_cached_bytecode else None
            )
        }
        unexpected_paths = sorted(observed_paths - expected_paths)
        if unexpected_paths:
            raise BundleManifestError(
                f"Bundle snapshot {version_path} contains files not present in the manifest: "
                f"{', '.join(unexpected_paths)}"
            )

        for file_info in snapshot_manifest["files"]:
            relative_path = validate_bundle_relative_path(file_info["path"])
            source = version_path / relative_path
            ensure_destination_is_within_root(version_path, source)
            if source.is_symlink():
                raise BundleManifestError(
                    f"Bundle snapshot file {file_info['path']!r} is a symlink; symlinks are not "
                    "supported in manifest bundle snapshots"
                )
            try:
                file_stat = source.stat()
            except FileNotFoundError:
                raise BundleManifestError(
                    f"Bundle snapshot {version_path} is missing manifest entry {file_info['path']!r}"
                ) from None
            if not stat.S_ISREG(file_stat.st_mode):
                raise BundleManifestError(f"Bundle snapshot file {file_info['path']!r} is not a regular file")
            if not check_content:
                continue
            file_sha256, size = compute_file_sha256(source)
            executable = bool(stat.S_IMODE(file_stat.st_mode) & 0o111)
            if (
                size != file_info["size"]
                or file_sha256 != file_info["sha256"]
                or executable != file_info["executable"]
            ):
                raise BundleManifestError(
                    f"Bundle snapshot file {file_info['path']!r} does not match local snapshot manifest"
                )

    @staticmethod
    def _iter_snapshot_files(
        snapshot_root: Path, *, cached_source_paths: set[str] | None = None
    ) -> Iterator[Path]:
        def raise_walk_error(error: OSError) -> None:
            raise BundleManifestError(
                f"Bundle snapshot {snapshot_root} changed or became unreadable while validating: "
                f"{error.filename}"
            ) from error

        for dirpath, dirnames, filenames in os.walk(
            snapshot_root,
            followlinks=False,
            onerror=raise_walk_error,
        ):
            dirpath_path = Path(dirpath)
            relative_dirpath = dirpath_path.relative_to(snapshot_root).as_posix()
            if dirpath_path.is_symlink():
                raise BundleManifestError(
                    f"Bundle snapshot {snapshot_root} contains symlinked directory "
                    f"{relative_dirpath!r}; symlinks are not supported in manifest bundle snapshots"
                )

            for dirname in dirnames:
                path = dirpath_path / dirname
                if path.is_symlink():
                    relative_path = path.relative_to(snapshot_root).as_posix()
                    raise BundleManifestError(
                        f"Bundle snapshot {snapshot_root} contains symlinked directory "
                        f"{relative_path!r}; symlinks are not supported in manifest bundle snapshots"
                    )
                if not path.is_dir():
                    relative_path = path.relative_to(snapshot_root).as_posix()
                    raise BundleManifestError(
                        f"Bundle snapshot {snapshot_root} contains non-directory entry "
                        f"{relative_path!r} where a directory was expected"
                    )
            dirnames[:] = sorted(dirnames)
            for filename in sorted(filenames):
                path = Path(dirpath) / filename
                relative_path = path.relative_to(snapshot_root).as_posix()
                is_cached_bytecode = cached_source_paths is not None and _is_cached_bytecode_file(
                    Path(relative_path), cached_source_paths
                )
                if path.is_symlink():
                    raise BundleManifestError(
                        f"Bundle snapshot {snapshot_root} contains symlinked file "
                        f"{relative_path!r}; symlinks are not supported in manifest bundle snapshots"
                    )
                try:
                    file_stat = path.stat()
                except FileNotFoundError as e:
                    if is_cached_bytecode:
                        # Python replaces its temporary bytecode file atomically.
                        continue
                    # Not BundleManifestSourceChangedError: snapshots are immutable, so a file
                    # vanishing from one is damage or a concurrent removal, not a source edit.
                    raise BundleManifestError(
                        f"Bundle snapshot file disappeared during validation: {relative_path}"
                    ) from e
                if not stat.S_ISREG(file_stat.st_mode):
                    raise BundleManifestError(
                        f"Bundle snapshot {snapshot_root} contains non-regular file {relative_path!r}"
                    )
                if relative_path == MANIFEST_FILE_NAME or is_cached_bytecode:
                    continue
                yield path

    @staticmethod
    def _read_local_snapshot_manifest(version_path: Path) -> dict[str, Any]:
        manifest_file = version_path / MANIFEST_FILE_NAME
        missing_message = (
            f"Bundle snapshot at {version_path} does not contain {MANIFEST_FILE_NAME}. "
            "Publish or restore the immutable snapshot before updating the manifest reference."
        )
        if not manifest_file.exists():
            raise BundleManifestNotFoundError(missing_message)
        if manifest_file.is_symlink():
            raise BundleManifestError(
                f"Bundle snapshot manifest {manifest_file} is a symlink; symlinks are not supported "
                "in manifest bundle snapshots"
            )
        if not manifest_file.is_file():
            raise BundleManifestError(f"Bundle snapshot manifest {manifest_file} is not a regular file")
        return read_json_file(
            manifest_file,
            missing_message=missing_message,
            invalid_message=f"Bundle snapshot manifest is not valid JSON: {manifest_file}",
        )

    @property
    def path(self) -> Path:
        if self.version:
            version = validate_bundle_version(self.version, source="pinned version")
            return self.versions_dir / version
        with _reraise_oserror_as_manifest_error():
            manifest_ref = self._ensure_current_manifest_ref()
            version = manifest_ref.version
            current_version_path = self.versions_dir / version
            if self._has_validated_cache(version, manifest_ref=manifest_ref):
                return current_version_path

            # Never another version's tree: callers record get_current_version() against
            # whatever they parse here.
            if current_version_path.exists():
                raise BundleManifestError(
                    f"Bundle '{self.name}' version '{version}' has an unvalidated or structurally "
                    f"invalid cache copy at {current_version_path}. Initialize or refresh the bundle "
                    "to rebuild it before use."
                )
            return current_version_path

    def _remove_orphaned_validation_markers(self) -> None:
        """
        Reap validation markers whose version dir is gone.

        Airflow's stale-version cleanup removes only the version dir and its tracking file;
        without this sweep every version ever materialized would leak one marker file.
        """
        try:
            entries = list(self.versions_dir.iterdir())
        except OSError:
            return
        for entry in entries:
            version = _get_marker_file_version(entry)
            if version is None:
                continue
            if not (self.versions_dir / version).exists():
                with suppress(OSError):
                    entry.unlink()


def publish_prepared_manifest_dag_bundle(
    *,
    bundle: ManifestDagBundleBase,
    prepared_source: PreparedPublishSource,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """Publish one prepared local tree as an immutable manifest-backed snapshot."""
    source_path = prepared_source.root
    bundle._store.validate_source_paths(source_path, cache_versions_dir=bundle.versions_dir)
    if not prepared_source.source_snapshot.files and not bundle.allow_empty_source:
        raise BundleManifestError(
            f"Refusing to explicitly publish empty source tree {source_path} for bundle "
            f"'{bundle.name}'; set allow_empty_source=True if removing every Dag is intended"
        )
    bundle._store.prepare_publish_areas()
    with bundle.acquire_publication_lock():
        if expected_current_version is not None:
            expected_current_version = validate_bundle_version(
                expected_current_version, source="expected current version"
            )
            try:
                current_version = bundle._read_current_manifest_ref().version
            except BundleManifestNotFoundError as e:
                raise BundleManifestReferenceChangedError(
                    f"Bundle '{bundle.name}' has no published version to compare with "
                    "expected_current_version. Omit it for the initial publication."
                ) from e
            if current_version != expected_current_version:
                raise BundleManifestReferenceChangedError(
                    f"Bundle '{bundle.name}' manifest reference changed: expected "
                    f"{expected_current_version!r}, found {current_version!r}"
                )

        manifest_result = build_bundle_version_manifest(
            bundle_name=bundle.name,
            root=source_path,
            backend_type=FILESYSTEM_ARTIFACT_BACKEND_TYPE,
            source_snapshot=prepared_source.source_snapshot,
        )
        return _publish_manifest_result(
            bundle=bundle,
            manifest_result=manifest_result,
            prepared_source=prepared_source,
        )


def _publish_manifest_result(
    *,
    bundle: ManifestDagBundleBase,
    manifest_result: BundleVersionManifest,
    prepared_source: PreparedPublishSource,
) -> BundlePublishResult:
    """Publish a precomputed manifest while the caller holds the publication lock."""
    ref_payload = dict(manifest_result.ref_payload)
    manifest_ref = bundle._build_manifest_ref_from_payload(ref_payload, source="publisher")
    version_path = bundle._store.snapshot_path(manifest_result.version)

    bundle._store.sweep_publish_temps()
    created_snapshot = bundle._store.publish_snapshot(
        manifest_result.version,
        manifest=manifest_result.manifest,
        source_root=prepared_source.root,
        validate_existing=lambda tree: bundle._validate_snapshot_for_ref(manifest_ref, tree),
    )

    bundle._confirm_publish_source(prepared_source)
    bundle._store.write_ref(ref_payload)

    return BundlePublishResult(
        bundle_name=bundle.name,
        version=manifest_result.version,
        ref_payload=ref_payload,
        version_path=version_path,
        manifest_ref_path=bundle.manifest_ref_path,
        manifest_sha256=manifest_ref.manifest_sha256,
        file_count=ref_payload["file_count"],
        total_size=ref_payload["total_size"],
        created_snapshot=created_snapshot,
    )


def _is_cached_bytecode_file(path: Path, source_paths: set[str]) -> bool:
    """Recognize Python bytecode (including atomic-write temps) for manifest-listed sources."""
    if path.parent.name != "__pycache__":
        return False
    suffix = path.suffix.removeprefix(".")
    if suffix.isascii() and suffix.isdecimal():
        path = path.with_suffix("")
    if path.suffix != ".pyc":
        return False
    stem, separator, tag = path.stem.rpartition(".")
    if tag.startswith("opt-"):
        if not tag.removeprefix("opt-").isalnum():
            return False
        stem, separator, tag = stem.rpartition(".")
    return bool(separator and tag) and (path.parent.parent / f"{stem}.py").as_posix() in source_paths


def _get_marker_file_version(path: Path) -> str | None:
    """Return the version a ``.{version}.validated`` marker file certifies, or None."""
    name = path.name
    if not (name.startswith(".") and name.endswith(".validated")):
        return None
    version = name[1 : -len(".validated")]
    if not is_valid_bundle_version(version):
        return None
    if path.is_symlink() or not path.is_file():
        return None
    return version
