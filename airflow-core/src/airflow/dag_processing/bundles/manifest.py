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

import hashlib
import json
import os
import stat
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

from airflow.exceptions import AirflowException

MANIFEST_FILE_NAME = ".airflow-bundle-manifest.json"
IGNORED_DIR_NAMES = frozenset({".git", "__pycache__"})
# ".git" as a file name covers git-worktree checkouts, where .git is a pointer file.
IGNORED_FILE_NAMES = frozenset({MANIFEST_FILE_NAME, ".git"})
IGNORED_FILE_SUFFIXES = frozenset({".pyc"})
MANIFEST_SCHEMA_VERSION = 1
# "sha256-" (not "sha256:") keeps a version usable as a path segment: Airflow builds
# cache and tracking paths from the raw value.
SHA256_VERSION_PREFIX = "sha256-"


class BundleManifestError(AirflowException):
    """
    Base class for bundle manifest errors.

    Subclasses ``AirflowException`` because every Dag bundle entry point Airflow calls
    is expected to fail with one, so a bundle that cannot be read is reported per
    bundle rather than escaping as an unrelated error type.
    """


class BundleManifestSourceChangedError(BundleManifestError):
    """Raised when bundle source files change while a manifest is being built."""


@dataclass(frozen=True)
class BundleSourceFile:
    """A source file and the stat metadata used to detect source changes during publishing."""

    path: Path
    relative_path: str
    size: int
    mtime_ns: int
    ctime_ns: int
    mode: int

    def build_signature_record(self) -> dict[str, Any]:
        return {
            "path": self.relative_path,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "ctime_ns": self.ctime_ns,
            "mode": self.mode,
        }


@dataclass(frozen=True)
class BundleSourceSnapshot:
    """A deterministic snapshot of the source tree metadata."""

    root: Path
    files: tuple[BundleSourceFile, ...]
    signature: str


def is_ignored_bundle_file_name(file_name: str) -> bool:
    """Return whether manifest collection excludes one file, judged by its name alone."""
    return file_name in IGNORED_FILE_NAMES or PurePosixPath(file_name).suffix in IGNORED_FILE_SUFFIXES


@dataclass(frozen=True)
class BundleVersionManifest:
    """Full manifest plus the compact release-reference payload written to latest.json."""

    version: str
    manifest: dict[str, Any]
    ref_payload: dict[str, Any]
    source_snapshot: BundleSourceSnapshot


def _raise_source_access_error(error: OSError, *, subject: str, phase: str) -> NoReturn:
    """
    Re-raise an OSError from the bundle source as the matching manifest error.

    A missing entry means the source really did change under us, which a publisher
    treats as "nothing stable to publish yet, try the next pass". Any other OSError --
    a permission problem, a stale network handle -- is a standing fault, so it must not
    be reported as a benign concurrent edit and then quietly retried forever.
    """
    if isinstance(error, FileNotFoundError):
        raise BundleManifestSourceChangedError(
            f"Bundle source {subject} disappeared while {phase}"
        ) from error
    raise BundleManifestError(f"Bundle source {subject} became unreadable while {phase}") from error


def _raise_source_walk_error(error: OSError) -> NoReturn:
    _raise_source_access_error(
        error, subject=f"directory {error.filename}", phase="collecting manifest metadata"
    )


def _iter_manifest_file_paths(root: Path) -> Iterator[tuple[Path, os.stat_result]]:
    for dirpath, dirnames, filenames in os.walk(
        root,
        followlinks=False,
        onerror=_raise_source_walk_error,
    ):
        retained_dirnames: list[str] = []
        for dirname in sorted(dirnames):
            if dirname in IGNORED_DIR_NAMES:
                continue
            path = Path(dirpath) / dirname
            try:
                file_stat = path.lstat()
            except OSError as e:
                _raise_source_access_error(
                    e, subject=f"directory {path}", phase="collecting manifest metadata"
                )
            if stat.S_ISLNK(file_stat.st_mode):
                raise BundleManifestError(f"Bundle source contains symlinked directory: {path}")
            if not stat.S_ISDIR(file_stat.st_mode):
                raise BundleManifestError(f"Bundle source contains non-directory entry: {path}")
            retained_dirnames.append(dirname)
        dirnames[:] = retained_dirnames

        for filename in sorted(filenames):
            if is_ignored_bundle_file_name(filename):
                continue
            path = Path(dirpath) / filename
            try:
                file_stat = path.lstat()
            except OSError as e:
                _raise_source_access_error(e, subject=f"file {path}", phase="collecting manifest metadata")
            if stat.S_ISLNK(file_stat.st_mode):
                raise BundleManifestError(f"Bundle source contains symlinked file: {path}")
            if not stat.S_ISREG(file_stat.st_mode):
                raise BundleManifestError(f"Bundle source contains non-regular file: {path}")
            yield path, file_stat


def _build_source_file(root: Path, path: Path, file_stat: os.stat_result) -> BundleSourceFile:
    return BundleSourceFile(
        path=path,
        relative_path=path.relative_to(root).as_posix(),
        size=file_stat.st_size,
        mtime_ns=file_stat.st_mtime_ns,
        ctime_ns=file_stat.st_ctime_ns,
        mode=stat.S_IMODE(file_stat.st_mode),
    )


def compute_file_sha256(path: Path) -> tuple[str, int]:
    """Compute the sha256 hex digest and byte size of a file."""
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def compute_bundle_version(files: Sequence[dict[str, Any]]) -> str:
    """
    Compute the content-addressed bundle version from manifest file entries.

    Only the identity fields (path, sha256, size, executable) participate, so the
    version stays stable if file entries ever gain extra metadata.
    """
    try:
        payload = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "files": [
                {
                    "path": file_info["path"],
                    "sha256": file_info["sha256"],
                    "size": file_info["size"],
                    "executable": file_info["executable"],
                }
                for file_info in files
            ],
        }
    except (KeyError, TypeError) as e:
        raise BundleManifestError(
            "Bundle manifest file entries must each contain path, sha256, size and executable"
        ) from e
    return f"{SHA256_VERSION_PREFIX}{hashlib.sha256(_serialize_manifest_payload(payload)).hexdigest()}"


def is_sha256_hex(value: str) -> bool:
    """Return whether a string is exactly 64 lowercase hex characters."""
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def is_valid_bundle_version(version: str) -> bool:
    """Return whether a string has the exact shape ``compute_bundle_version`` produces."""
    digest = version.removeprefix(SHA256_VERSION_PREFIX)
    return digest != version and is_sha256_hex(digest)


def validate_bundle_version(version: str, *, source: str) -> str:
    """
    Return ``version`` if it is a well-formed bundle version, else raise.

    Versions arrive from published documents and from operator-pinned configuration, and
    Airflow joins them into cache and tracking paths, so they are checked before use.
    """
    if not is_valid_bundle_version(version):
        raise BundleManifestError(f"Bundle {source} is not a valid sha256 version: {version!r}")
    return version


def validate_bundle_relative_path(relative_path: str) -> Path:
    """
    Return ``relative_path`` as a ``Path`` if it is safe to join onto a bundle root, else raise.

    Manifest paths are written by whoever published the bundle, so a consumer must not
    join one onto a local root before rejecting absolute paths, parent traversal, the
    non-normalized spellings (``a//b``, ``a/./b``, trailing slash) that would resolve
    somewhere other than where the manifest says, and control characters that no
    filesystem can represent. A backslash is an ordinary character in a POSIX file
    name, so it is accepted -- rejecting it would make a legitimate Dag file
    publishable but never materializable.

    This checks the path in isolation. Whether the manifest may legitimately name the
    path at all -- that it is not the manifest file itself, not a duplicate, and not
    excluded from collection -- is the caller's decision.
    """
    if any(character < " " for character in relative_path):
        raise BundleManifestError(f"Bundle manifest contains unsafe relative path: {relative_path!r}")
    path = Path(relative_path)
    if (
        path.is_absolute()
        or any(segment in {"", ".", ".."} for segment in relative_path.split("/"))
        or path.as_posix() != relative_path
    ):
        raise BundleManifestError(f"Bundle manifest contains unsafe relative path: {relative_path!r}")
    return path


def _serialize_manifest_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def serialize_bundle_version_manifest(manifest: dict[str, Any]) -> bytes:
    """Serialize a bundle manifest deterministically for storage and verification."""
    return _serialize_manifest_payload(manifest)


def _validate_bundle_root(root: Path) -> Path:
    # Unlike a file vanishing mid-walk, a root we never saw is a configured path rather
    # than a concurrent edit, so none of these are reported as a changed source: a typo
    # in the configuration must not look like something worth quietly retrying.
    root = Path(root)
    try:
        root_stat = root.stat()
    except FileNotFoundError as e:
        raise BundleManifestError(f"Bundle source path does not exist: {root}") from e
    except OSError as e:
        raise BundleManifestError(f"Bundle source path is unreadable: {root}") from e
    if not stat.S_ISDIR(root_stat.st_mode):
        raise BundleManifestError(f"Bundle source path is not a directory: {root}")
    return root


def collect_bundle_source_snapshot(root: Path) -> BundleSourceSnapshot:
    """Collect deterministic source-file metadata without reading file contents."""
    root = _validate_bundle_root(root)
    files = [_build_source_file(root, path, file_stat) for path, file_stat in _iter_manifest_file_paths(root)]
    files.sort(key=lambda source_file: source_file.relative_path)
    signature_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "files": [file.build_signature_record() for file in files],
    }
    signature = f"sha256:{hashlib.sha256(_serialize_manifest_payload(signature_payload)).hexdigest()}"
    return BundleSourceSnapshot(root=root, files=tuple(files), signature=signature)


def _ensure_source_file_unchanged(source_file: BundleSourceFile) -> None:
    try:
        current_stat = source_file.path.lstat()
    except OSError as e:
        _raise_source_access_error(
            e, subject=f"file {source_file.relative_path}", phase="verifying manifest metadata"
        )

    current_metadata = (
        current_stat.st_size,
        current_stat.st_mtime_ns,
        current_stat.st_ctime_ns,
        stat.S_IMODE(current_stat.st_mode),
    )
    expected_metadata = (
        source_file.size,
        source_file.mtime_ns,
        source_file.ctime_ns,
        source_file.mode,
    )
    if current_metadata != expected_metadata:
        raise BundleManifestSourceChangedError(
            f"Bundle source file changed while building manifest: {source_file.relative_path}"
        )


def build_ref_payload(
    *,
    bundle_name: str,
    version: str,
    backend: dict[str, Any],
    manifest_sha256: str,
    file_count: int,
    total_size: int,
) -> dict[str, Any]:
    """Build the compact release-reference payload that defines the ``latest.json`` format."""
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "bundle_name": bundle_name,
        "version": version,
        "backend": backend,
        "manifest": {
            "path": MANIFEST_FILE_NAME,
            "sha256": manifest_sha256,
        },
        "file_count": file_count,
        "total_size": total_size,
    }


def _build_ref_payload_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    manifest_sha256 = hashlib.sha256(serialize_bundle_version_manifest(manifest)).hexdigest()
    return build_ref_payload(
        bundle_name=manifest["bundle_name"],
        version=manifest["version"],
        backend=manifest["backend"],
        manifest_sha256=f"sha256:{manifest_sha256}",
        file_count=manifest["file_count"],
        total_size=manifest["total_size"],
    )


def build_bundle_version_manifest(
    *,
    bundle_name: str,
    root: Path,
    backend_type: str,
    source_snapshot: BundleSourceSnapshot | None = None,
    precomputed_file_sha256: Mapping[str, str] | None = None,
    file_version_ids: Mapping[str, str] | None = None,
) -> BundleVersionManifest:
    """
    Build a deterministic content manifest for a materialized Dag bundle root.

    ``file_version_ids`` records the backend's own identifier for each stored object, such
    as an S3 ``VersionId``, so a consumer can fetch the exact object a release was built
    from rather than whatever currently sits at the key. It is backend bookkeeping, not
    content, so it stays out of ``compute_bundle_version`` -- republishing identical files
    to a versioned bucket mints new object versions and must still yield one bundle version.
    """
    source_snapshot = source_snapshot or collect_bundle_source_snapshot(root)
    root = _validate_bundle_root(root)
    if source_snapshot.root != root:
        raise ValueError("source_snapshot root does not match manifest root")
    expected_paths = {source_file.relative_path for source_file in source_snapshot.files}
    if precomputed_file_sha256 is not None and set(precomputed_file_sha256) != expected_paths:
        raise BundleManifestError("Precomputed file hashes do not match the bundle source snapshot")
    if file_version_ids is not None:
        if set(file_version_ids) != expected_paths:
            raise BundleManifestError("File version ids do not match the bundle source snapshot")
        for relative_path, version_id in sorted(file_version_ids.items()):
            if not isinstance(version_id, str) or not version_id:
                raise BundleManifestError(f"File version id is invalid for {relative_path!r}")

    files: list[dict[str, Any]] = []
    total_size = 0
    for source_file in source_snapshot.files:
        if precomputed_file_sha256 is None:
            try:
                file_digest, _ = compute_file_sha256(source_file.path)
            except OSError as e:
                _raise_source_access_error(
                    e, subject=f"file {source_file.relative_path}", phase="building manifest"
                )
        else:
            file_digest = precomputed_file_sha256[source_file.relative_path]
            if not is_sha256_hex(file_digest):
                raise BundleManifestError(
                    f"Precomputed file hash is invalid for {source_file.relative_path!r}"
                )
        _ensure_source_file_unchanged(source_file)
        file_info = {
            "path": source_file.relative_path,
            "sha256": file_digest,
            "size": source_file.size,
            "executable": bool(source_file.mode & 0o111),
        }
        if file_version_ids is not None:
            file_info["version_id"] = file_version_ids[source_file.relative_path]
        files.append(file_info)
        total_size += source_file.size

    version = compute_bundle_version(files)
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "bundle_name": bundle_name,
        "version": version,
        "backend": {"type": backend_type},
        "file_count": len(files),
        "total_size": total_size,
        "files": files,
    }
    return BundleVersionManifest(
        version=version,
        manifest=manifest,
        ref_payload=_build_ref_payload_from_manifest(manifest),
        source_snapshot=source_snapshot,
    )


def _validate_manifest_file_entry(file_info: Any) -> str:
    if not isinstance(file_info, dict):
        raise BundleManifestError("Bundle manifest file entries must be objects")
    relative_path = file_info.get("path")
    if not isinstance(relative_path, str):
        raise BundleManifestError("Bundle manifest file entry does not contain a path")
    validate_bundle_relative_path(relative_path)
    if relative_path == MANIFEST_FILE_NAME:
        raise BundleManifestError(f"Bundle manifest must not list {MANIFEST_FILE_NAME!r} as a bundle file")

    digest = file_info.get("sha256")
    if not isinstance(digest, str) or not is_sha256_hex(digest):
        raise BundleManifestError(f"Bundle manifest entry {relative_path!r} does not contain a valid sha256")
    size = file_info.get("size")
    # bool is an int subclass and True == 1, so it would slip through the checks below.
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise BundleManifestError(f"Bundle manifest entry {relative_path!r} does not contain a valid size")
    if not isinstance(file_info.get("executable"), bool):
        raise BundleManifestError(
            f"Bundle manifest entry {relative_path!r} does not contain a valid executable flag"
        )
    version_id = file_info.get("version_id")
    if version_id is not None and (not isinstance(version_id, str) or not version_id):
        raise BundleManifestError(
            f"Bundle manifest entry {relative_path!r} does not contain a valid version id"
        )
    return relative_path


def validate_bundle_version_manifest_structure(
    manifest: Any,
    *,
    expected_version: str | None,
    expected_bundle_name: str | None = None,
    expected_backend_type: str | None = None,
) -> None:
    """
    Check that a manifest is well formed and that its version commits to its file entries.

    Re-deriving the version from the entries binds the two together, but that is only as
    strong as the version it is compared with: a publisher who edits the entries can
    recompute the version as well, so a manifest checked against nothing but itself is
    merely self-consistent. Pass ``expected_version`` from somewhere the publisher does not
    control -- an operator's pin, or a version Airflow has already recorded -- to make this
    an integrity check. It is required so that passing ``None`` is a deliberate statement
    that no such version is available.
    """
    if not isinstance(manifest, dict):
        raise BundleManifestError("Bundle manifest must be an object")

    schema_version = manifest.get("schema_version")
    if schema_version != MANIFEST_SCHEMA_VERSION:
        raise BundleManifestError(f"Bundle manifest has unsupported schema_version {schema_version!r}")

    bundle_name = manifest.get("bundle_name")
    if not isinstance(bundle_name, str) or not bundle_name:
        raise BundleManifestError("Bundle manifest does not contain a bundle name")
    if expected_bundle_name is not None and bundle_name != expected_bundle_name:
        raise BundleManifestError(
            f"Bundle manifest is for bundle {bundle_name!r}, expected {expected_bundle_name!r}"
        )

    version = manifest.get("version")
    if not isinstance(version, str):
        raise BundleManifestError("Bundle manifest does not contain a version")
    validate_bundle_version(version, source="manifest version")
    if expected_version is not None and version != expected_version:
        raise BundleManifestError(
            f"Bundle manifest contains version {version!r}, expected {expected_version!r}"
        )

    backend = manifest.get("backend")
    if not isinstance(backend, dict) or not isinstance(backend.get("type"), str):
        raise BundleManifestError("Bundle manifest does not contain a backend type")
    if expected_backend_type is not None and backend["type"] != expected_backend_type:
        # Worded to match the manifest bundle's existing "not for a local backend" checks.
        raise BundleManifestError(
            f"Bundle manifest is not for a {expected_backend_type} backend: got {backend['type']!r}"
        )

    files = manifest.get("files")
    if not isinstance(files, list):
        raise BundleManifestError("Bundle manifest files must be a list")

    seen_paths: set[str] = set()
    paths: list[str] = []
    total_size = 0
    for file_info in files:
        relative_path = _validate_manifest_file_entry(file_info)
        if relative_path in seen_paths:
            raise BundleManifestError(f"Bundle manifest contains duplicate path {relative_path!r}")
        seen_paths.add(relative_path)
        paths.append(relative_path)
        total_size += file_info["size"]
    # The version hashes entries in order, so without a fixed order the same files could
    # be published under several versions.
    if paths != sorted(paths):
        raise BundleManifestError("Bundle manifest file entries are not sorted by path")

    if manifest.get("file_count") != len(files):
        raise BundleManifestError(
            f"Bundle manifest file_count {manifest.get('file_count')!r} does not match its "
            f"{len(files)} file entries"
        )
    if manifest.get("total_size") != total_size:
        raise BundleManifestError(
            f"Bundle manifest total_size {manifest.get('total_size')!r} does not match its "
            f"entries, which total {total_size}"
        )

    computed_version = compute_bundle_version(files)
    if version != computed_version:
        raise BundleManifestError(
            f"Bundle manifest version {version!r} does not match its contents, "
            f"which hash to {computed_version!r}"
        )


def verify_bundle_version_manifest(manifest: dict[str, Any], expected_sha256: str) -> None:
    """
    Check that a manifest serializes to ``expected_sha256``.

    This proves only that the manifest is the document the digest was taken from, which
    says nothing about the contents when the digest came from the same publisher. Pair it
    with ``validate_bundle_version_manifest_structure``, which carries its own caveat.
    """
    actual_sha256 = hashlib.sha256(serialize_bundle_version_manifest(manifest)).hexdigest()
    expected_sha256 = expected_sha256.removeprefix("sha256:")
    if actual_sha256 != expected_sha256:
        raise BundleManifestError(
            f"Bundle manifest digest mismatch: expected sha256:{expected_sha256}, got sha256:{actual_sha256}"
        )
