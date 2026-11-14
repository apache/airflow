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
Filesystem helpers shared by the filesystem artifact store and the manifest bundle's local cache.

Not an extension point: code outside ``airflow.dag_processing.bundles`` must reach published
artifacts through ``ArtifactStore``.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import stat
from pathlib import Path
from typing import Any, NoReturn

from airflow.dag_processing.bundles.artifact_store import BundleManifestNotFoundError
from airflow.dag_processing.bundles.manifest import (
    SHA256_VERSION_PREFIX,
    BundleManifestError,
    is_sha256_hex,
)

log = logging.getLogger(__name__)


def read_json_file(path: Path, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
    """Read a JSON object from a published document, raising ``missing_message`` if it is absent."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise BundleManifestNotFoundError(missing_message) from e
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise BundleManifestError(invalid_message) from e
    except OSError as e:
        raise BundleManifestError(f"Cannot read {path}: {e}") from e

    if not isinstance(payload, dict):
        raise BundleManifestError(invalid_message)
    return payload


def get_path_mode(path: Path) -> int | None:
    """
    Return the ``st_mode`` of ``path``, or ``None`` if it does not exist.

    Use instead of ``Path.exists()`` or ``Path.is_dir()`` on published artifacts: from
    Python 3.14 those return False on any ``OSError``, so a permission or I/O failure
    would read as a missing artifact.
    """
    try:
        return path.stat().st_mode
    except FileNotFoundError:
        return None


def raise_walk_error(error: OSError) -> NoReturn:
    """``os.walk`` ``onerror`` handler; by default ``os.walk`` skips directories it cannot list."""
    raise error


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


def fsync_tree_directories(root: Path) -> None:
    directories = [Path(dirpath) for dirpath, _, _ in os.walk(root, onerror=raise_walk_error)]
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        fsync_directory(directory)
