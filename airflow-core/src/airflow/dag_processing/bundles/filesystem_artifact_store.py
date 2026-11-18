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
"""Shared-filesystem artifact store for manifest-backed Dag bundles, and its filesystem helpers."""

from __future__ import annotations

import json
import logging
import os
import shutil
import stat
import tempfile
from contextlib import contextmanager, suppress
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.dag_processing.bundles.artifact_store import (
    ArtifactStore,
    BundleManifestNotFoundError,
    BundleManifestRefDurabilityError,
)
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    SHA256_VERSION_PREFIX,
    BundleManifestError,
    BundleManifestSourceChangedError,
    compute_file_sha256,
    is_sha256_hex,
    serialize_bundle_version_manifest,
    validate_bundle_relative_path,
)

if TYPE_CHECKING:
    from collections.abc import Callable

log = logging.getLogger(__name__)


class FilesystemArtifactStore(ArtifactStore):
    """
    Artifact store for a ``published_root`` on a filesystem shared by publishers and Airflow.

    Layout under ``published_root``: ``refs/<bundle>/latest.json`` (release reference),
    ``versions/<bundle>/<version>/`` (snapshots) and ``_locks/<bundle>.lock`` (publication lock).

    Publishers need write access to the publication directories and read/write access to
    the lock file. For different OS users, provision these with a shared publisher group.
    """

    def __init__(self, *, bundle_name: str, published_root: Path, cache_root: Path) -> None:
        self.bundle_name = bundle_name
        if paths_overlap(published_root, cache_root):
            raise ValueError(
                "published_root must not overlap Airflow's bundle cache. Configure separate "
                "authoritative publication and dag_bundle_storage_path locations."
            )
        self._root = published_root
        self._ref_path = published_root / "refs" / bundle_name / "latest.json"
        self._snapshots_root = published_root / "versions" / bundle_name
        self.lock_path = published_root / "_locks" / f"{bundle_name}.lock"

    @property
    def root(self) -> Path:
        return self._root

    @property
    def ref_path(self) -> Path:
        return self._ref_path

    @property
    def snapshots_root(self) -> Path:
        return self._snapshots_root

    def snapshot_path(self, version: str) -> Path:
        return self._snapshots_root / version

    def read_ref(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        return read_json_file(
            self._ref_path, missing_message=missing_message, invalid_message=invalid_message
        )

    def write_ref(self, payload: dict[str, Any]) -> None:
        _write_json_atomically(self._ref_path, payload)

    @contextmanager
    def publication_guard(self):
        """Serialize publishers through a lock stored with the authoritative snapshots."""
        _ensure_public_dir(self.lock_path.parent)
        # Linux NFS implements flock with fcntl locks, which require a writable
        # descriptor for an exclusive lock.
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o664)
        except PermissionError as e:
            raise BundleManifestError(
                f"Cannot open publication lock {self.lock_path}: publishers need read/write "
                "access. Set its group to a shared publisher group and grant group read/write "
                "access (chmod 0664), and ensure the lock directory is writable."
            ) from e
        with os.fdopen(fd, "r+b") as lock_file:
            # os.open masks the mode with the umask; repair it whenever this publisher
            # owns the file so other members of the publisher group can acquire it.
            with suppress(OSError):
                os.fchmod(fd, stat.S_IMODE(os.fstat(fd).st_mode) | 0o664)
            flock(lock_file, LOCK_EX)
            try:
                yield
            finally:
                flock(lock_file, LOCK_UN)

    def prepare_publish_areas(self) -> None:
        _ensure_public_dir(self._snapshots_root)
        _ensure_public_dir(self._ref_path.parent)

    def validate_source_paths(self, source_path: Path, *, cache_versions_dir: Path) -> None:
        _validate_publish_paths(
            source_path=source_path,
            published_root=self._root,
            versions_dir=cache_versions_dir,
        )

    def snapshot_exists(self, version: str) -> bool:
        return (self._snapshots_root / version).is_dir()

    def fetch_snapshot(
        self,
        version: str,
        destination: Path,
        *,
        structural_validator: Callable[[Path], None],
    ) -> None:
        published_version_path = self._snapshots_root / version
        if not published_version_path.is_dir():
            raise BundleManifestNotFoundError(
                f"Bundle '{self.bundle_name}' version '{version}' is not published at "
                f"{published_version_path}. Publish or restore the immutable snapshot before "
                "running pinned work."
            )
        structural_validator(published_version_path)
        shutil.copytree(published_version_path, destination, copy_function=shutil.copy2, dirs_exist_ok=True)

    def publish_snapshot(
        self,
        version: str,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
    ) -> bool:
        version_path = self._snapshots_root / version
        if version_path.exists():
            if not version_path.is_dir():
                raise FileExistsError(f"Bundle snapshot path exists but is not a directory: {version_path}")
            validate_existing(version_path)
            return False
        _materialize_manifest_snapshot(
            source_path=source_root,
            manifest=manifest,
            versions_dir=self._snapshots_root,
            version_path=version_path,
        )
        return True

    def sweep_publish_temps(self) -> None:
        remove_orphaned_temp_snapshots(self._snapshots_root)
        _remove_orphaned_atomic_temp_files(self._ref_path)


def read_json_file(path: Path, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
    """Read a JSON object from a published document, raising ``missing_message`` if it is absent."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise BundleManifestNotFoundError(missing_message) from e
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise BundleManifestError(invalid_message) from e

    if not isinstance(payload, dict):
        raise BundleManifestError(invalid_message)
    return payload


def ensure_destination_is_within_root(root: Path, destination: Path) -> None:
    try:
        destination.resolve().relative_to(root.resolve())
    except ValueError:
        raise BundleManifestError(
            f"Bundle manifest path {destination} resolves outside snapshot root {root}"
        ) from None


def _make_tree_writable(path: Path) -> None:
    directories: list[Path] = []
    for dirpath, _, filenames in os.walk(path):
        directory = Path(dirpath)
        directories.append(directory)
        for filename in filenames:
            child = directory / filename
            if child.is_symlink():
                continue
            try:
                child.chmod(stat.S_IMODE(child.stat().st_mode) | 0o200)
            except OSError:
                continue

    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        try:
            directory.chmod(stat.S_IMODE(directory.stat().st_mode) | 0o700)
        except OSError:
            continue


def remove_bundle_tree_forcefully(path: Path) -> None:
    """Remove a bundle tree, adding write permissions first when a read-only snapshot blocks removal."""
    if path.is_symlink() or not path.is_dir():
        path.unlink(missing_ok=True)
        return
    try:
        shutil.rmtree(path)
    except OSError:
        _make_tree_writable(path)
        shutil.rmtree(path)


def _materialize_manifest_snapshot(
    *,
    source_path: Path,
    manifest: dict[str, Any],
    versions_dir: Path,
    version_path: Path,
) -> None:
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
            shutil.copy2(source, destination)
            fsync_file(destination)
            # Hash the copy inline: this verifies both the copy and the source against
            # the manifest without a second full walk over the materialized tree. The
            # executable bit is part of the content address, so it must be checked here
            # too -- failing after os.replace would leave a published snapshot whose modes
            # contradict its own manifest, permanently blocking that content version.
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
        _fsync_tree_directories(tmp_path)
        os.replace(tmp_path, version_path)
        fsync_directory(versions_dir)
    except Exception:
        # Best effort only: a cleanup failure must not mask the original error.
        with suppress(OSError):
            remove_bundle_tree_forcefully(tmp_path)
        raise


def get_temp_snapshot_prefix(version: str) -> str:
    return f".{version}."


def remove_orphaned_temp_snapshots(versions_dir: Path) -> None:
    if not versions_dir.exists():
        return

    removed_orphans = False
    for path in versions_dir.iterdir():
        if not _is_temp_snapshot_dir(path):
            continue
        try:
            remove_bundle_tree_forcefully(path)
        except OSError:
            # One undeletable orphan (e.g. owned by another OS user) must not block
            # materialization of valid versions.
            log.warning("Could not remove orphaned temp snapshot %s", path, exc_info=True)
            continue
        removed_orphans = True

    if removed_orphans:
        fsync_directory(versions_dir)


def _is_temp_snapshot_dir(path: Path) -> bool:
    if path.is_symlink() or not path.is_dir():
        return False

    name = path.name
    if not name.startswith(f".{SHA256_VERSION_PREFIX}"):
        return False

    digest, separator, suffix = name.removeprefix(f".{SHA256_VERSION_PREFIX}").partition(".")
    return separator == "." and bool(suffix) and is_sha256_hex(digest)


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
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(serialize_bundle_version_manifest(payload))
            file.flush()
            # mkstemp creates the file owner-only; consumers can run as a different OS user.
            os.fchmod(file.fileno(), 0o644)
            os.fsync(file.fileno())
        os.replace(tmp_path, destination_path)
        try:
            fsync_directory(destination_path.parent)
        except OSError as e:
            version = payload.get("version")
            raise BundleManifestRefDurabilityError(
                ref_path=destination_path, version=version if isinstance(version, str) else None
            ) from e
    except Exception:
        # missing_ok: an exists()/unlink() pair could itself raise here (e.g. an external
        # tmp sweeper won the race) and mask the original publish error.
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
        created.mkdir(exist_ok=True)
        # A concurrent publisher may have created (and own) the directory; it applies
        # the same permissions itself.
        with suppress(OSError):
            created.chmod(0o755)
        fsync_directory(created)
        fsync_directory(created.parent)


def _validate_publish_paths(
    *,
    source_path: Path,
    published_root: Path,
    versions_dir: Path,
) -> None:
    if _is_same_or_nested_path(published_root, source_path):
        raise ValueError(
            "published_root must not be inside source_path. Keep published snapshots "
            "outside the Dag source tree before publishing a manifest bundle."
        )

    if _is_same_or_nested_path(source_path, published_root):
        raise ValueError(
            "source_path must not be inside published_root. Keep the Dag source tree "
            "outside the published snapshots before publishing a manifest bundle."
        )

    if paths_overlap(source_path, versions_dir):
        raise ValueError(
            "source_path and versions_dir must not overlap. Keep Airflow's bundle cache outside "
            "the Dag source tree before publishing a manifest bundle."
        )


def _is_same_or_nested_path(path: Path, root: Path) -> bool:
    absolute_path = Path(os.path.abspath(path))
    absolute_root = Path(os.path.abspath(root))
    return absolute_path.is_relative_to(absolute_root) or path.resolve(strict=False).is_relative_to(
        root.resolve(strict=False)
    )


def paths_overlap(first: Path, second: Path) -> bool:
    return _is_same_or_nested_path(first, second) or _is_same_or_nested_path(second, first)


def fsync_file(path: Path) -> None:
    with path.open("rb") as file:
        os.fsync(file.fileno())


def fsync_directory(path: Path) -> None:
    flags = getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, os.O_RDONLY | flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_tree_directories(root: Path) -> None:
    directories = [Path(dirpath) for dirpath, _, _ in os.walk(root)]
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        fsync_directory(directory)


def fsync_tree(root: Path, *, ignore_file: Callable[[Path], bool] | None = None) -> None:
    """Make regular files and directory entries durable, excluding files matched by ``ignore_file``."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            if ignore_file is not None and ignore_file(path):
                continue
            if not path.is_symlink():
                fsync_file(path)
    _fsync_tree_directories(root)


def _write_manifest_file(manifest: dict[str, Any], manifest_file: Path) -> None:
    with manifest_file.open("wb") as file:
        file.write(serialize_bundle_version_manifest(manifest))
        file.flush()
        os.fsync(file.fileno())


def _set_snapshot_permissions(snapshot_path: Path) -> None:
    # Snapshots must be read-only and world-readable: the publisher and the Airflow
    # components reading published_root may run as different OS users.
    directories: list[Path] = []
    for dirpath, _, filenames in os.walk(snapshot_path):
        directories.append(Path(dirpath))
        for filename in filenames:
            path = Path(dirpath) / filename
            if path.is_symlink():
                continue
            executable = stat.S_IMODE(path.stat().st_mode) & 0o111
            path.chmod(0o555 if executable else 0o444)
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        directory.chmod(0o555)


def set_cache_tree_permissions(cache_path: Path) -> None:
    # Cache copies keep files read-only (content drift stays detectable) but leave
    # directories writable: Airflow's stale-bundle cleanup removes cached versions
    # with a plain shutil.rmtree, which fails on read-only directories.
    for dirpath, _, filenames in os.walk(cache_path):
        directory = Path(dirpath)
        for filename in filenames:
            path = directory / filename
            if path.is_symlink():
                continue
            executable = stat.S_IMODE(path.stat().st_mode) & 0o111
            path.chmod(0o555 if executable else 0o444)
        directory.chmod(0o755)
