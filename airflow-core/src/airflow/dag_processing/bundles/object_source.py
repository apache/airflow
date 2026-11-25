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
Shared read-only object-store source machinery for manifest Dag bundles.

``ObjectStoreSourceDagBundleBase`` handles source observation, the disposable
local mirror and its state file, mirror safety validation, and the publication
confirmation flow.
A concrete adapter supplies the storage client, the object listing, the
deployment-marker read, and the pinned download.

"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import tempfile
from abc import abstractmethod
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.dag_processing.bundles._filesystem import (
    ensure_destination_is_within_root,
    remove_bundle_tree_forcefully,
)
from airflow.dag_processing.bundles.artifact_store import ArtifactStoreBusyError
from airflow.dag_processing.bundles.filesystem_artifact_store import _write_json_atomically
from airflow.dag_processing.bundles.manifest import (
    IGNORED_DIR_NAMES,
    BundleManifestError,
    BundleManifestSourceChangedError,
    BundleSourceSnapshot,
    collect_bundle_source_snapshot,
    compute_file_sha256,
    is_ignored_bundle_file_name,
)
from airflow.dag_processing.bundles.manifest_bundle import (
    BundleManifestRef,
    ManifestDagBundleBase,
    PreparedPublishSource,
    publish_prepared_manifest_dag_bundle,
)

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.manifest_bundle import BundlePublishResult

DEFAULT_MAX_FILE_COUNT = 10_000
DEFAULT_MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024
DEFAULT_MAX_TOTAL_SIZE_BYTES = 1024 * 1024 * 1024

log = logging.getLogger(__name__)

_STORE_BACKEND_DESCRIPTIONS = {
    "local": "a filesystem path",
    "s3": "an s3:// URL",
    "gcs": "a gs:// URL",
}


@dataclass(frozen=True)
class ObjectSourceObservation:
    """One canonical observation of an object-store source folder."""

    entries: tuple[Any, ...]
    inventory_signature: str
    candidate_signature: str
    marker_signature: str | None


class ObjectStoreSourceDagBundleBase(ManifestDagBundleBase):
    """
    Mirror a mutable object-store folder into local staging and publish snapshots.

    The remote folder is never parsed or executed from directly. Concrete
    adapters define the class attributes below and implement the abstract
    storage operations; every safety property (path validation, mirror
    quarantine, before/after observation comparison, size and count limits)
    lives here so it cannot drift between adapters.
    """

    #: Release metadata ``source.type`` and confirmed-source key component.
    _source_type: str
    #: Human label used in error messages, e.g. ``"S3"``.
    _source_label: str
    #: Message noun for remote object identifiers, e.g. ``"object key"``.
    _remote_name_noun: str
    #: URL scheme of the source, e.g. ``"s3"``.
    _source_url_scheme: str
    #: Directory name of the local mirror below the bundle base dir.
    _mirror_dir_name: str
    #: File name of the mirror state document below the bundle base dir.
    _mirror_state_file_name: str
    #: Schema version embedded in observation signature payloads.
    _observation_schema_version: int
    #: Schema version of the mirror state document.
    _mirror_state_schema_version: int
    #: Observation container class returned by ``_collect_source_observation``.
    _observation_class: type[ObjectSourceObservation]
    #: Requirement wording for a marker key of the wrong type.
    _marker_key_requirement: str
    #: Requirement wording for an unsafe marker key.
    _marker_key_safe_requirement: str
    #: ``store_backend`` values this adapter may publish to and consume from.
    #: Cross-cloud pairings (a GCS source with an S3 published_root, or the
    #: reverse) are rejected at construction.
    _compatible_store_backends: tuple[str, ...]

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str = "",
        deployment_marker_key: str | None = None,
        max_file_count: int = DEFAULT_MAX_FILE_COUNT,
        max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
        max_total_size_bytes: int = DEFAULT_MAX_TOTAL_SIZE_BYTES,
        auto_publish: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not isinstance(bucket_name, str) or not bucket_name:
            raise TypeError("bucket_name must be a non-empty string")
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a string")
        if deployment_marker_key is not None:
            if not isinstance(deployment_marker_key, str) or not deployment_marker_key:
                raise TypeError(f"deployment_marker_key must be {self._marker_key_requirement}")
            _validate_marker_relative_key(
                deployment_marker_key,
                requirement=self._marker_key_safe_requirement,
            )
        _validate_positive_limit("max_file_count", max_file_count)
        _validate_positive_limit("max_file_size_bytes", max_file_size_bytes)
        _validate_positive_limit("max_total_size_bytes", max_total_size_bytes)
        if not isinstance(auto_publish, bool):
            raise TypeError("auto_publish must be a boolean")

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.deployment_marker_key = deployment_marker_key
        self.max_file_count = max_file_count
        self.max_file_size_bytes = max_file_size_bytes
        self.max_total_size_bytes = max_total_size_bytes
        self.auto_publish = auto_publish
        store_backend = self._store.backend_type
        if store_backend not in self._compatible_store_backends:
            supported = " or ".join(
                _STORE_BACKEND_DESCRIPTIONS.get(backend, backend)
                for backend in self._compatible_store_backends
            )
            raise TypeError(
                f"{type(self).__name__} does not support the configured published_root "
                f"{self._configured_published_root!r} ({store_backend} store backend). Cross-cloud "
                f"publication is not supported; use {supported}."
            )
        self._normalized_prefix = prefix.rstrip("/")
        self._marker_remote_name = (
            _join_object_name(self._normalized_prefix, deployment_marker_key)
            if deployment_marker_key is not None
            else None
        )
        self._source_mirror_dir = self.base_dir / self._mirror_dir_name
        self._mirror_state_path = self.base_dir / self._mirror_state_file_name
        self._source_configuration_checked = False

    @abstractmethod
    def _get_source_client(self) -> Any:
        """Return the storage client, translating construction failures."""

    @abstractmethod
    def _get_source_endpoint(self, client: Any) -> str | None:
        """Return the endpoint identity component of the source identity."""

    @abstractmethod
    def _list_source_objects(self, client: Any) -> Iterator[Any]:
        """Yield one observation per listed remote object, translating errors."""

    @abstractmethod
    def _read_deployment_marker(self, client: Any) -> Any:
        """Return the marker observation, or None when no marker is configured."""

    @abstractmethod
    def _download_entry(self, client: Any, *, entry: Any, tmp_path: Path) -> None:
        """Download one observed object into ``tmp_path``."""

    @abstractmethod
    def _validate_source_configuration(self, client: Any) -> None:
        """Raise if the configured bucket or prefix is unusable."""

    def _prepare_artifact_store(self, client: Any, observation: ObjectSourceObservation) -> None:
        """Supply optional transport hints to a concrete artifact store."""

    @property
    def has_publish_source(self) -> bool:
        return self.auto_publish

    @property
    def publish_source_description(self) -> str:
        suffix = f"/{self.prefix}" if self.prefix else ""
        return f"{self._source_url_scheme}://{self.bucket_name}{suffix}"

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__}(name={self.name!r}, bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, version={self.version!r})>"
        )

    def prepare_publish_source(self) -> PreparedPublishSource:
        self._check_source_configuration()
        client = self._get_source_client()
        observation = self._collect_source_observation(client)
        source_identity = self._get_source_identity(client)
        snapshot, file_hashes = self._synchronize_mirror(
            client=client,
            source_identity=source_identity,
            observation=observation,
        )
        release_source_metadata: dict[str, str] = {
            "type": self._source_type,
            "identity": source_identity,
            "observation": observation.inventory_signature,
        }
        if observation.marker_signature is not None:
            release_source_metadata["deployment_marker"] = observation.marker_signature
        self._prepare_artifact_store(client, observation)
        return PreparedPublishSource(
            root=self._source_mirror_dir,
            source_snapshot=snapshot,
            source_type=self._source_type,
            source_identity=source_identity,
            source_signature=observation.candidate_signature,
            file_sha256_by_path=file_hashes,
            confirmation_data=observation,
            release_source_metadata=release_source_metadata,
        )

    def _publish_from_source_if_ready(
        self,
        current_ref: BundleManifestRef | None,
    ) -> BundleManifestRef | None:
        # Keep one host's mutable mirror stable through hashing, snapshot copy, and
        # the final remote confirmation. The artifact store coordinates publication
        # across hosts.
        with ExitStack() as locks:
            try:
                locks.enter_context(self.lock(wait=False))
            except BlockingIOError as e:
                raise ArtifactStoreBusyError(
                    f"Another process holds the source mirror lock for '{self.name}'"
                ) from e
            return super()._publish_from_source_if_ready(current_ref)

    def confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        expected = prepared.confirmation_data
        if not isinstance(expected, self._observation_class):
            raise BundleManifestError(
                f"Prepared {self._source_label} source does not contain a remote observation"
            )
        current_snapshot = collect_bundle_source_snapshot(prepared.root)
        if current_snapshot.signature != prepared.source_snapshot.signature:
            raise BundleManifestSourceChangedError(
                f"{self._source_label} mirror changed while publishing bundle '{self.name}'"
            )
        current = self._collect_source_observation(self._get_source_client())
        if current != expected:
            raise BundleManifestSourceChangedError(
                f"{self._source_label} source changed while publishing bundle '{self.name}'"
            )

    def validate_release_transition(
        self,
        *,
        current_ref: BundleManifestRef | None,
        prepared: PreparedPublishSource,
    ) -> None:
        if current_ref is None:
            return
        current_source = current_ref.ref_payload.get("source")
        prepared_source = prepared.release_source_metadata
        if not isinstance(current_source, dict) or not isinstance(prepared_source, dict):
            return
        if current_source.get("type") == self._source_type and current_source.get(
            "identity"
        ) != prepared_source.get("identity"):
            raise BundleManifestError(
                f"Current release for bundle '{self.name}' records a different {self._source_label} source"
            )
        if self.deployment_marker_key is None:
            return
        if current_source.get("observation") != prepared_source.get("observation") and current_source.get(
            "deployment_marker"
        ) == prepared_source.get("deployment_marker"):
            raise BundleManifestSourceChangedError(
                f"{self._source_label} objects for bundle '{self.name}' changed "
                "without a new deployment marker"
            )

    def _check_source_configuration(self) -> None:
        if self._source_configuration_checked:
            return
        self._validate_source_configuration(self._get_source_client())
        self._source_configuration_checked = True

    def _get_source_identity(self, client: Any) -> str:
        payload = {
            "schema_version": self._observation_schema_version,
            "endpoint": self._get_source_endpoint(client),
            "bucket": self.bucket_name,
            "prefix": self._normalized_prefix,
        }
        return _compute_json_sha256(payload)

    def _collect_source_observation(self, client: Any) -> ObjectSourceObservation:
        entries: list[Any] = []
        total_size = 0
        marker_before = self._read_deployment_marker(client)
        for observation in self._list_source_objects(client):
            if observation.remote_name == self._marker_remote_name:
                continue
            if _is_ignored_relative_path(observation.relative_path):
                continue
            if observation.size > self.max_file_size_bytes:
                raise BundleManifestError(
                    f"{self._source_label} object {observation.remote_name!r} has size "
                    f"{observation.size}, which exceeds "
                    f"max_file_size_bytes={self.max_file_size_bytes}"
                )
            if len(entries) >= self.max_file_count:
                raise BundleManifestError(
                    f"{self._source_label} source for bundle '{self.name}' exceeds "
                    f"max_file_count={self.max_file_count}"
                )
            total_size += observation.size
            if total_size > self.max_total_size_bytes:
                raise BundleManifestError(
                    f"{self._source_label} source for bundle '{self.name}' has total size "
                    f"{total_size}, which exceeds "
                    f"max_total_size_bytes={self.max_total_size_bytes}"
                )
            entries.append(observation)

        entries.sort(key=lambda entry: entry.relative_path)
        _validate_inventory_paths(entries, label=self._source_label)
        marker_after = self._read_deployment_marker(client)
        if marker_before != marker_after:
            raise BundleManifestSourceChangedError(
                f"{self._source_label} deployment marker changed while observing bundle '{self.name}'"
            )

        inventory_signature = _compute_json_sha256(
            {
                "schema_version": self._observation_schema_version,
                "files": [entry.build_signature_record() for entry in entries],
            }
        )
        marker_signature = (
            _compute_json_sha256(
                {
                    "schema_version": self._observation_schema_version,
                    "marker": marker_after.build_signature_record(),
                }
            )
            if marker_after is not None
            else None
        )
        candidate_signature = _compute_json_sha256(
            {
                "schema_version": self._observation_schema_version,
                "inventory": inventory_signature,
                "deployment_marker": marker_signature,
            }
        )
        return self._observation_class(
            entries=tuple(entries),
            inventory_signature=inventory_signature,
            candidate_signature=candidate_signature,
            marker_signature=marker_signature,
        )

    def _synchronize_mirror(
        self,
        *,
        client: Any,
        source_identity: str,
        observation: ObjectSourceObservation,
    ) -> tuple[BundleSourceSnapshot, dict[str, str]]:
        state = self._read_mirror_state()
        state_entries = _get_state_entries_by_path(
            state,
            source_identity=source_identity,
            schema_version=self._mirror_state_schema_version,
        )
        mirror_changed = False
        if state_entries is None or not self._is_mirror_tree_safe():
            self._reset_mirror()
            state_entries = {}
            mirror_changed = True
        else:
            self._source_mirror_dir.mkdir(parents=True, exist_ok=True)

        verify_reused_content = not self._can_skip_reused_content_check(
            source_identity=source_identity,
            observation=observation,
        )
        expected_paths = {entry.relative_path for entry in observation.entries}
        observed_paths = self._get_observed_mirror_files()
        self._normalize_mirror_directories()
        for stale_path in sorted(observed_paths - expected_paths, reverse=True):
            (self._source_mirror_dir / stale_path).unlink()
            mirror_changed = True
        self._remove_empty_mirror_directories()

        local_records: list[dict[str, Any]] = []
        file_sha256_by_path: dict[str, str] = {}
        downloaded_files = 0
        downloaded_bytes = 0
        for entry in observation.entries:
            destination = self._source_mirror_dir / entry.relative_path
            ensure_destination_is_within_root(self._source_mirror_dir, destination)
            old_record = state_entries.get(entry.relative_path, {})
            reused = _can_reuse_mirror_file(destination, entry=entry, state=old_record)
            verified_reused_file: tuple[str, int] | None = None
            if reused and verify_reused_content:
                verified_reused_file = compute_file_sha256(destination)
                expected_sha256 = old_record["local_sha256"].removeprefix("sha256:")
                if verified_reused_file[0] != expected_sha256:
                    reused = False
                    verified_reused_file = None
            if not reused:
                self._download_object(client, entry=entry, destination=destination)
                downloaded_files += 1
                downloaded_bytes += entry.size
                mirror_changed = True
            destination.chmod(0o644)
            if reused:
                if verified_reused_file is not None:
                    file_sha256, actual_size = verified_reused_file
                else:
                    file_sha256 = old_record["local_sha256"].removeprefix("sha256:")
                    actual_size = destination.stat().st_size
            else:
                file_sha256, actual_size = compute_file_sha256(destination)
            if actual_size != entry.size:
                raise BundleManifestSourceChangedError(
                    f"Downloaded {self._source_label} object {entry.remote_name!r} has "
                    f"size {actual_size}, expected {entry.size}"
                )
            local_records.append(
                {
                    **entry.build_signature_record(),
                    "local_mtime_ns": destination.stat().st_mtime_ns,
                    "local_sha256": f"sha256:{file_sha256}",
                }
            )
            file_sha256_by_path[entry.relative_path] = file_sha256

        self._normalize_mirror_directories()
        if mirror_changed:
            confirmed = self._collect_source_observation(client)
            if confirmed != observation:
                raise BundleManifestSourceChangedError(
                    f"{self._source_label} source changed while synchronizing bundle '{self.name}'"
                )
        self._validate_mirror_against_inventory(observation)
        snapshot = collect_bundle_source_snapshot(self._source_mirror_dir)
        _write_json_atomically(
            self._mirror_state_path,
            {
                "schema_version": self._mirror_state_schema_version,
                "source_identity": source_identity,
                "source_signature": observation.candidate_signature,
                "files": local_records,
            },
        )
        if downloaded_files:
            log.info(
                "Synchronized %s bundle mirror. bundle=%s downloaded_files=%d downloaded_bytes=%d",
                self._source_label,
                self.name,
                downloaded_files,
                downloaded_bytes,
            )
        return snapshot, file_sha256_by_path

    def _can_skip_reused_content_check(
        self,
        *,
        source_identity: str,
        observation: ObjectSourceObservation,
    ) -> bool:
        confirmed_version = self._confirmed_source_version
        return (
            self._confirmed_source_key
            == (self._source_type, source_identity, observation.candidate_signature)
            and confirmed_version is not None
            and self._store.snapshot_exists(confirmed_version)
        )

    def _read_mirror_state(self) -> dict[str, Any] | None:
        try:
            payload = json.loads(self._mirror_state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError, OSError):
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _reset_mirror(self) -> None:
        if self._source_mirror_dir.exists() or self._source_mirror_dir.is_symlink():
            remove_bundle_tree_forcefully(self._source_mirror_dir)
        self._source_mirror_dir.mkdir(parents=True, mode=0o755)

    def _is_mirror_tree_safe(self) -> bool:
        if not self._source_mirror_dir.exists():
            return False
        if self._source_mirror_dir.is_symlink() or not self._source_mirror_dir.is_dir():
            return False
        try:
            self._get_observed_mirror_files()
        except BundleManifestError:
            return False
        return True

    def _get_observed_mirror_files(self) -> set[str]:
        observed: set[str] = set()
        if not self._source_mirror_dir.exists():
            return observed
        for dirpath, dirnames, filenames in os.walk(self._source_mirror_dir, followlinks=False):
            root = Path(dirpath)
            for dirname in dirnames:
                path = root / dirname
                if path.is_symlink() or not path.is_dir():
                    raise BundleManifestError(f"{self._source_label} mirror contains unsafe directory {path}")
            for filename in filenames:
                path = root / filename
                if path.is_symlink():
                    raise BundleManifestError(f"{self._source_label} mirror contains symlink {path}")
                try:
                    file_mode = path.stat().st_mode
                except FileNotFoundError as e:
                    raise BundleManifestSourceChangedError(
                        f"{self._source_label} mirror file disappeared during validation: {path}"
                    ) from e
                if not stat.S_ISREG(file_mode):
                    raise BundleManifestError(f"{self._source_label} mirror contains non-regular file {path}")
                observed.add(path.relative_to(self._source_mirror_dir).as_posix())
        return observed

    def _download_object(
        self,
        client: Any,
        *,
        entry: Any,
        destination: Path,
    ) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            dir=destination.parent,
        )
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            self._download_entry(client, entry=entry, tmp_path=tmp_path)
            if tmp_path.is_symlink() or not tmp_path.is_file():
                raise BundleManifestError(
                    f"{self._source_label} download for object {entry.remote_name!r} "
                    "did not produce a regular file"
                )
            os.replace(tmp_path, destination)
        finally:
            with suppress(OSError):
                tmp_path.unlink(missing_ok=True)

    def _validate_mirror_against_inventory(self, observation: ObjectSourceObservation) -> None:
        expected = {entry.relative_path: entry.size for entry in observation.entries}
        observed = self._get_observed_mirror_files()
        if observed != set(expected):
            raise BundleManifestSourceChangedError(
                f"{self._source_label} mirror file set does not match the remote "
                f"inventory for bundle '{self.name}'"
            )
        for relative_path, expected_size in expected.items():
            if (self._source_mirror_dir / relative_path).stat().st_size != expected_size:
                raise BundleManifestSourceChangedError(
                    f"{self._source_label} mirror file {relative_path!r} has the wrong size"
                )

    def _remove_empty_mirror_directories(self) -> None:
        for dirpath, _, _ in os.walk(self._source_mirror_dir, topdown=False):
            path = Path(dirpath)
            if path == self._source_mirror_dir:
                continue
            with suppress(OSError):
                path.rmdir()

    def _normalize_mirror_directories(self) -> None:
        self._normalize_mirror_directory(self._source_mirror_dir)
        for dirpath, dirnames, _ in os.walk(self._source_mirror_dir, followlinks=False):
            self._normalize_mirror_directory(Path(dirpath))
            for dirname in dirnames:
                self._normalize_mirror_directory(Path(dirpath) / dirname)

    def _normalize_mirror_directory(self, path: Path) -> None:
        try:
            mode = path.lstat().st_mode
        except OSError as e:
            raise BundleManifestError(
                f"Could not inspect {self._source_label} mirror directory {path}"
            ) from e
        if not stat.S_ISDIR(mode):
            raise BundleManifestError(f"{self._source_label} mirror contains unsafe directory {path}")

        try:
            directory_fd = os.open(
                path,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
        except OSError as e:
            raise BundleManifestError(
                f"Could not open {self._source_label} mirror directory safely: {path}"
            ) from e
        try:
            if not stat.S_ISDIR(os.fstat(directory_fd).st_mode):
                raise BundleManifestError(f"{self._source_label} mirror contains unsafe directory {path}")
            os.fchmod(directory_fd, 0o755)
        except OSError as e:
            raise BundleManifestError(
                f"Could not normalize {self._source_label} mirror directory permissions: {path}"
            ) from e
        finally:
            os.close(directory_fd)

    @contextmanager
    def _translate_source_error(self, operation: str) -> Iterator[None]:
        try:
            yield
        except BundleManifestError:
            raise
        except Exception as e:
            raise BundleManifestError(
                f"Could not {operation} for {self._source_label} source {self.publish_source_description}"
            ) from e


def publish_manifest_object_store_dag_bundle(
    *,
    bundle: ObjectStoreSourceDagBundleBase,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """Publish the configured object-store source as an immutable snapshot."""
    if bundle.auto_publish:
        raise BundleManifestError(
            f"Bundle '{bundle.name}' has auto_publish enabled. Set auto_publish=False "
            f"before using the explicit {bundle._source_label} publisher."
        )
    if bundle.version:
        raise BundleManifestError(
            f"Cannot explicitly publish pinned bundle '{bundle.name}' at version {bundle.version!r}"
        )

    # The mirror is mutable host-local staging. Keep the Airflow bundle lock until
    # publication finishes its final local and remote source confirmation.
    with bundle.lock():
        prepared = bundle.prepare_publish_source()
        return publish_prepared_manifest_dag_bundle(
            bundle=bundle,
            prepared_source=prepared,
            expected_current_version=expected_current_version,
        )


def _is_ignored_relative_path(path: str) -> bool:
    parts = path.split("/")
    return any(part in IGNORED_DIR_NAMES for part in parts[:-1]) or is_ignored_bundle_file_name(parts[-1])


def _validate_marker_relative_key(key: str, *, requirement: str) -> None:
    if key.startswith("/") or key.endswith("/") or "\\" in key:
        raise TypeError(f"deployment_marker_key must be {requirement}")
    parts = key.split("/")
    if any(
        not part
        or part in {".", ".."}
        or any(ord(character) < 32 or ord(character) == 127 for character in part)
        for part in parts
    ):
        raise TypeError(f"deployment_marker_key must be {requirement}")


def _validate_positive_limit(name: str, value: Any) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise TypeError(f"{name} must be a positive integer")


def _join_object_name(prefix: str, name: str) -> str:
    return f"{prefix}/{name}" if prefix else name


def _get_relative_object_path(
    *,
    name: str,
    normalized_prefix: str,
    label: str,
    noun: str,
) -> str | None:
    if (
        "\x00" in name
        or "\\" in name
        or any(ord(character) < 32 or ord(character) == 127 for character in name)
    ):
        raise BundleManifestError(f"{label} source contains unsafe {noun} {name!r}")
    prefix_with_separator = f"{normalized_prefix}/" if normalized_prefix else ""
    if normalized_prefix:
        if name == normalized_prefix or name == prefix_with_separator:
            return None
        if not name.startswith(prefix_with_separator):
            raise BundleManifestError(
                f"{label} {noun} {name!r} is outside normalized prefix {normalized_prefix!r}"
            )
        relative_path = name[len(prefix_with_separator) :]
    else:
        relative_path = name
    if relative_path.endswith("/"):
        directory_path = relative_path[:-1]
        if not directory_path:
            return None
        parts = directory_path.split("/")
        if any(not part or part in {".", ".."} for part in parts):
            raise BundleManifestError(f"{label} source contains unsafe directory marker {name!r}")
        return None
    parts = relative_path.split("/")
    if (
        not relative_path
        or relative_path.startswith("/")
        or any(not part or part in {".", ".."} for part in parts)
    ):
        raise BundleManifestError(f"{label} source contains unsafe {noun} {name!r}")
    return "/".join(parts)


def _validate_inventory_paths(entries: list[Any], *, label: str) -> None:
    seen: set[str] = set()
    for path in sorted(entry.relative_path for entry in entries):
        if path in seen:
            raise BundleManifestError(f"{label} source contains duplicate path {path!r}")
        parts = path.split("/")
        for index in range(1, len(parts)):
            parent = "/".join(parts[:index])
            if parent in seen:
                raise BundleManifestError(f"{label} source path {path!r} collides with file {parent!r}")
        seen.add(path)


def _get_state_entries_by_path(
    state: dict[str, Any] | None,
    *,
    source_identity: str,
    schema_version: int,
) -> dict[str, dict[str, Any]] | None:
    if (
        state is None
        or state.get("schema_version") != schema_version
        or state.get("source_identity") != source_identity
        or not isinstance(state.get("source_signature"), str)
        or not isinstance(state.get("files"), list)
    ):
        return None
    result: dict[str, dict[str, Any]] = {}
    for item in state["files"]:
        if not isinstance(item, dict):
            return None
        path = item.get("path")
        if not isinstance(path, str) or not path or path in result:
            return None
        result[path] = item
    return result


def _can_reuse_mirror_file(
    path: Path,
    *,
    entry: Any,
    state: dict[str, Any] | None,
) -> bool:
    if state is None or path.is_symlink():
        return False
    try:
        file_stat = path.stat()
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(file_stat.st_mode):
        return False
    local_sha256 = state.get("local_sha256")
    return (
        all(state.get(field) == value for field, value in entry.build_signature_record().items())
        and entry.size == file_stat.st_size
        and state.get("local_mtime_ns") == file_stat.st_mtime_ns
        and isinstance(local_sha256, str)
        and len(local_sha256) == 71
        and local_sha256.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in local_sha256[7:])
    )


def _compute_json_sha256(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(serialized).hexdigest()}"


__all__ = [
    "DEFAULT_MAX_FILE_COUNT",
    "DEFAULT_MAX_FILE_SIZE_BYTES",
    "DEFAULT_MAX_TOTAL_SIZE_BYTES",
    "ObjectSourceObservation",
    "ObjectStoreSourceDagBundleBase",
    "publish_manifest_object_store_dag_bundle",
]
