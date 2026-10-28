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
"""Shared-filesystem artifact store for manifest-backed Dag bundles."""

from __future__ import annotations

import logging
import os
import shutil
import stat
import tempfile
import time
from contextlib import contextmanager, suppress
from fcntl import LOCK_EX, LOCK_NB, LOCK_UN, flock
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.dag_processing.bundles._filesystem import (
    ensure_destination_is_within_root,
    fsync_directory,
    fsync_file,
    fsync_tree_directories,
    get_path_mode,
    get_temp_snapshot_prefix,
    raise_walk_error,
    read_json_file,
    remove_bundle_tree_forcefully,
    remove_orphaned_temp_snapshots,
)
from airflow.dag_processing.bundles.artifact_store import (
    ArtifactStore,
    BundleManifestNotFoundError,
    BundleManifestRefDurabilityError,
)
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestSourceChangedError,
    _raise_source_access_error,
    compute_file_sha256,
    serialize_bundle_version_manifest,
    validate_bundle_relative_path,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

log = logging.getLogger(__name__)

DEFAULT_PUBLICATION_LOCK_TIMEOUT = 600.0
_LOCK_POLL_INTERVAL = 1.0
_LOCK_WAIT_LOG_INTERVAL = 30.0


class FilesystemArtifactStore(ArtifactStore):
    """
    Artifact store for a ``published_root`` on a filesystem shared by publishers and Airflow.

    Layout under ``published_root``: ``refs/<bundle>/latest.json`` (release reference),
    ``versions/<bundle>/<version>/`` (snapshots) and ``_locks/<bundle>.lock`` (publication lock).

    Publishers need write access to the publication directories and read/write access to
    the lock file. For different OS users, provision these with a shared publisher group.

    Publishers on different hosts serialize through ``flock`` on the lock file, so the
    filesystem must enforce ``flock`` across hosts: NFSv4 does, NFSv3 only with the lock
    manager (lockd) running, and FUSE or object-store mounts often accept ``flock`` without
    enforcing it, which leaves concurrent publishers unserialized.

    Airflow never deletes published snapshots: a Dag run can request any version it pinned,
    for example on a retry, rerun or callback. Snapshots are read-only and may be owned by
    the publisher's OS user, so to prune one that no run can still request, remove it as
    that user after adding the owner write bit (``chmod -R u+w``).

    :param lock_timeout: Seconds to wait for another publisher to release the lock before
        failing. The lock is held while a whole snapshot is materialized, so allow for the
        largest bundle on the slowest shared filesystem.
    """

    def __init__(
        self,
        *,
        bundle_name: str,
        published_root: Path,
        lock_timeout: float = DEFAULT_PUBLICATION_LOCK_TIMEOUT,
    ) -> None:
        self.bundle_name = bundle_name
        self._root = published_root
        self._ref_path = published_root / "refs" / bundle_name / "latest.json"
        self._snapshots_root = published_root / "versions" / bundle_name
        self._lock_path = published_root / "_locks" / f"{bundle_name}.lock"
        self._lock_timeout = lock_timeout

    @property
    def root(self) -> str:
        return str(self._root)

    @property
    def ref_path(self) -> str:
        return str(self._ref_path)

    @property
    def snapshots_root(self) -> str:
        return str(self._snapshots_root)

    def snapshot_path(self, version: str) -> str:
        return str(self._snapshots_root / version)

    def read_ref(self) -> dict[str, Any]:
        return read_json_file(
            self._ref_path,
            missing_message=f"Bundle '{self.bundle_name}' has no release reference at {self._ref_path}",
            invalid_message=(
                f"Bundle '{self.bundle_name}' release reference {self._ref_path} is not a JSON object"
            ),
        )

    def write_ref(self, payload: dict[str, Any]) -> None:
        with _raise_storage_error(f"write release reference {self._ref_path}"):
            _write_json_atomically(self._ref_path, payload)

    @contextmanager
    def publication_guard(self):
        """Serialize publishers through a lock stored with the authoritative snapshots."""
        with _raise_storage_error(f"open publication lock {self._lock_path}"):
            _ensure_public_dir(self._lock_path.parent)
            # Linux NFS implements flock with fcntl locks, which require a writable
            # descriptor for an exclusive lock.
            try:
                fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o664)
            except PermissionError as e:
                raise BundleManifestError(
                    f"Cannot open publication lock {self._lock_path}: publishers need read/write "
                    "access. Set its group to a shared publisher group and grant group read/write "
                    "access (chmod 0664), and ensure the lock directory is writable."
                ) from e
        with os.fdopen(fd, "r+b") as lock_file:
            # os.open masks the mode with the umask; repair it whenever this publisher
            # owns the file so other members of the publisher group can acquire it.
            with _raise_storage_error(f"set permissions on publication lock {self._lock_path}"):
                try:
                    os.fchmod(fd, stat.S_IMODE(os.fstat(fd).st_mode) | 0o664)
                except OSError:
                    if os.fstat(fd).st_uid == os.geteuid():
                        raise
            self._acquire_publication_lock(lock_file)
            # Unlock explicitly, not only through close(): a forked child sharing this open
            # file description would keep holding the lock after this process closes it.
            try:
                with _raise_storage_error(f"remove leftovers of failed publications under {self._root}"):
                    self._remove_publish_leftovers()
                yield
            except BaseException:
                try:
                    flock(lock_file, LOCK_UN)
                except OSError:
                    # The publication's own error tells callers how to recover; keep it.
                    log.warning("Cannot release publication lock %s", self._lock_path, exc_info=True)
                raise
            with _raise_storage_error(f"release publication lock {self._lock_path}"):
                flock(lock_file, LOCK_UN)

    def _acquire_publication_lock(self, lock_file) -> None:
        deadline = time.monotonic() + self._lock_timeout
        next_log_time = time.monotonic()
        while True:
            try:
                flock(lock_file, LOCK_EX | LOCK_NB)
            except BlockingIOError:
                pass
            except OSError as e:
                raise BundleManifestError(
                    f"Cannot lock publication lock {self._lock_path}: {e}. The filesystem under "
                    "published_root must support flock across hosts; NFSv3 needs lockd."
                ) from e
            else:
                return
            now = time.monotonic()
            if now >= deadline:
                raise BundleManifestError(
                    f"Timed out after {self._lock_timeout:g}s waiting for publication lock "
                    f"{self._lock_path}; another publisher still holds it."
                )
            if now >= next_log_time:
                log.info("Waiting for publication lock %s held by another publisher", self._lock_path)
                next_log_time = now + _LOCK_WAIT_LOG_INTERVAL
            time.sleep(min(_LOCK_POLL_INTERVAL, deadline - now))

    def prepare_publish_areas(self) -> None:
        with _raise_storage_error(f"create publication directories under {self._root}"):
            _ensure_public_dir(self._snapshots_root)
            _ensure_public_dir(self._ref_path.parent)

    def snapshot_exists(self, version: str) -> bool:
        with _raise_storage_error(f"check published snapshot {self._snapshots_root / version}"):
            mode = get_path_mode(self._snapshots_root / version)
            return mode is not None and stat.S_ISDIR(mode)

    def fetch_snapshot(
        self,
        version: str,
        destination: Path,
        *,
        structural_validator: Callable[[Path], None],
    ) -> None:
        published_version_path = self._snapshots_root / version
        with _raise_storage_error(f"fetch published snapshot {published_version_path}"):
            published_mode = get_path_mode(published_version_path)
            if published_mode is None or not stat.S_ISDIR(published_mode):
                raise BundleManifestNotFoundError(
                    f"Bundle '{self.bundle_name}' version '{version}' is not published at "
                    f"{published_version_path}. Publish or restore the immutable snapshot before "
                    "running pinned work."
                )
            if destination.exists() and any(destination.iterdir()):
                raise ValueError(f"Snapshot fetch destination {destination} must be absent or empty")
            structural_validator(published_version_path)
            shutil.copytree(
                published_version_path, destination, copy_function=shutil.copy2, dirs_exist_ok=True
            )

    def publish_snapshot(
        self,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
    ) -> bool:
        version_path = self._snapshots_root / manifest["version"]
        with _raise_storage_error(f"publish snapshot {version_path}"):
            version_mode = get_path_mode(version_path)
            if version_mode is None:
                created = _materialize_manifest_snapshot(
                    source_path=source_root,
                    manifest=manifest,
                    versions_dir=self._snapshots_root,
                    version_path=version_path,
                )
            elif not stat.S_ISDIR(version_mode):
                raise BundleManifestError(
                    f"Bundle snapshot path exists but is not a directory: {version_path}"
                )
            else:
                created = False
            if not created:
                validate_existing(version_path)
            # An earlier attempt may have committed the snapshot and failed before this sync.
            fsync_directory(self._snapshots_root)
            return created

    def _remove_publish_leftovers(self) -> None:
        remove_orphaned_temp_snapshots(self._snapshots_root)
        _remove_orphaned_atomic_temp_files(self._ref_path)


@contextmanager
def _raise_storage_error(action: str) -> Iterator[None]:
    """
    Re-raise an ``OSError`` from published-artifact storage as a bundle error.

    A missing artifact becomes ``BundleManifestNotFoundError``; any other failure is a
    ``BundleManifestError``, never ``BundleManifestSourceChangedError``, which publishers
    treat as a benign concurrent edit of the source and retry quietly.
    """
    try:
        yield
    except BundleManifestError:
        raise
    except FileNotFoundError as e:
        raise BundleManifestNotFoundError(f"Cannot {action}: {e}") from e
    except OSError as e:
        raise BundleManifestError(f"Cannot {action}: {e}") from e


def _materialize_manifest_snapshot(
    *,
    source_path: Path,
    manifest: dict[str, Any],
    versions_dir: Path,
    version_path: Path,
) -> bool:
    """Commit the snapshot; return False if the rename failed but the version exists and needs validation."""
    version = manifest["version"]
    tmp_path = Path(
        tempfile.mkdtemp(
            prefix=get_temp_snapshot_prefix(version),
            dir=versions_dir,
        )
    )
    try:
        for file_info in manifest["files"]:
            relative_path = validate_bundle_relative_path(file_info["path"])
            source = source_path / relative_path
            destination = tmp_path / relative_path
            ensure_destination_is_within_root(tmp_path, destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                # Not copy2: copystat would carry over source ACLs and extended attributes,
                # which the read-only chmod before commit does not clear.
                shutil.copy(source, destination)
            except OSError as e:
                # Fast-copy failures name both files and can come from either side, so only
                # an error naming the source alone is attributed to it.
                if e.filename == os.fspath(source) and e.filename2 is None:
                    _raise_source_access_error(
                        e, subject=f"file {file_info['path']!r}", phase="materializing the snapshot"
                    )
                raise
            # Executable bit is part of the content address; verify before commit.
            destination_sha256, destination_size = compute_file_sha256(destination)
            destination_executable = bool(stat.S_IMODE(destination.stat().st_mode) & 0o111)
            if (
                destination_sha256 != file_info["sha256"]
                or destination_size != file_info["size"]
                or destination_executable != file_info["executable"]
            ):
                raise BundleManifestSourceChangedError(
                    f"Bundle source changed while materializing bundle version {version}"
                )

        _write_manifest_file(manifest, tmp_path / MANIFEST_FILE_NAME)
        _set_snapshot_permissions(tmp_path)
        fsync_tree_directories(tmp_path)
        try:
            os.replace(tmp_path, version_path)
        except OSError:
            # Renaming onto another publisher's committed tree fails with a
            # platform-specific errno, and NFS can report failure after the rename
            # completed, so check for the result instead.
            if not version_path.is_dir():
                raise
        else:
            return True
    except Exception:
        # Best effort only: a cleanup failure must not mask the original error.
        with suppress(OSError):
            remove_bundle_tree_forcefully(tmp_path)
        raise
    with suppress(OSError):
        remove_bundle_tree_forcefully(tmp_path)
    return False


def _remove_orphaned_atomic_temp_files(destination_path: Path) -> None:
    if not destination_path.parent.exists():
        return
    prefix = f".{destination_path.name}."
    for path in destination_path.parent.iterdir():
        if not path.name.startswith(prefix) or path.is_symlink() or not path.is_file():
            continue
        with suppress(OSError):
            path.unlink()


def _write_json_atomically(destination_path: Path, payload: dict[str, Any]) -> None:
    _ensure_public_dir(destination_path.parent)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{destination_path.name}.",
        dir=destination_path.parent,
    )
    tmp_path = Path(tmp_name)
    version = payload.get("version")
    ref_version = version if isinstance(version, str) else None
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(serialize_bundle_version_manifest(payload))
            file.flush()
            # mkstemp creates the file owner-only; consumers can run as a different OS user.
            os.fchmod(file.fileno(), 0o644)
            os.fsync(file.fileno())
        try:
            os.replace(tmp_path, destination_path)
        except OSError as replace_error:
            # NFS can report failure after completing the rename. The temporary file
            # still existing is the only proof the reference was not replaced.
            try:
                tmp_path.lstat()
            except FileNotFoundError:
                pass
            except OSError as e:
                raise BundleManifestRefDurabilityError(
                    ref_path=str(destination_path), version=ref_version
                ) from e
            else:
                raise replace_error
        try:
            fsync_directory(destination_path.parent)
        except OSError as e:
            raise BundleManifestRefDurabilityError(ref_path=str(destination_path), version=ref_version) from e
    except Exception:
        with suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise


def _ensure_public_dir(path: Path) -> None:
    """
    Create ``path`` with world-traversable permissions.

    Every directory this call creates -- including ancestors above ``published_root`` -- is
    chmodded 0755. Pre-existing directories (for example an admin-provisioned
    ``published_root`` the publisher does not own) keep their permissions and remain the
    deployer's contract.
    """
    missing: list[Path] = []
    current = Path(os.path.abspath(path))
    while not current.exists() and current.parent != current:
        missing.append(current)
        current = current.parent
    # A previous attempt may have stopped after mkdir but before syncing its parent.
    fsync_directory(current)
    if current.parent != current:
        fsync_directory(current.parent)
    for created in reversed(missing):
        try:
            created.mkdir()
        except FileExistsError:
            mode = get_path_mode(created)
            if mode is None or not stat.S_ISDIR(mode):
                raise
            created_here = False
        else:
            created_here = True
        try:
            created.chmod(0o755)
        except OSError:
            if created_here:
                # Retries preserve pre-existing permissions, so undo incomplete creation.
                try:
                    created.rmdir()
                    fsync_directory(created.parent)
                except OSError:
                    log.warning(
                        "Could not clean up publication directory %s after failed permission setup; "
                        "inspect its permissions before retrying",
                        created,
                        exc_info=True,
                    )
                raise
            # Another publisher may own the directory and apply its permissions itself.
            if created.stat().st_uid == os.geteuid():
                raise
        fsync_directory(created)
        fsync_directory(created.parent)


def _write_manifest_file(manifest: dict[str, Any], manifest_file: Path) -> None:
    manifest_file.write_bytes(serialize_bundle_version_manifest(manifest))


def _set_snapshot_permissions(snapshot_path: Path) -> None:
    # Snapshots must be read-only and world-readable: the publisher and the Airflow
    # components reading published_root may run as different OS users.
    directories: list[Path] = []
    for dirpath, _, filenames in os.walk(snapshot_path, onerror=raise_walk_error):
        directories.append(Path(dirpath))
        for filename in filenames:
            path = Path(dirpath) / filename
            if path.is_symlink():
                continue
            executable = stat.S_IMODE(path.stat().st_mode) & 0o111
            path.chmod(0o555 if executable else 0o444)
            # One sync after chmod makes the data and the final mode durable together.
            fsync_file(path)
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        directory.chmod(0o555)
