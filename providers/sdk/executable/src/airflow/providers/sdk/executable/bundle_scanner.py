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
"""
Scan directories for native executable Airflow SDK bundles.

Each executable bundle is expected to have a sidecar metadata file
(``airflow-metadata.yaml``) that declares the bundle's DAG IDs.  Detection
is based on file executability and the presence of this metadata file.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import NamedTuple

import yaml

_METADATA_FILENAME = "airflow-metadata.yaml"


class ResolvedExecutableBundle(NamedTuple):
    """A resolved native executable DAG bundle: everything needed to start the bundle process."""

    executable_path: str


class BundleScanner:
    """
    Locate Airflow native executable bundles inside a directory tree.

    Supports two directory layouts:

    - **Nested** — each immediate subdirectory of *bundles_dir* is a bundle home
      containing the executable and its ``airflow-metadata.yaml``.
    - **Flat** — *bundles_dir* itself contains the executables and metadata files.

    Within a bundle home, the scanner looks for files that are executable
    (``os.access(path, os.X_OK)``) and have a corresponding metadata file.
    """

    def __init__(self, bundles_dir: Path) -> None:
        self._bundles_dir = bundles_dir

    def resolve(self, dag_id: str) -> ResolvedExecutableBundle:
        """
        Find the bundle whose metadata YAML lists *dag_id*.

        :raises FileNotFoundError: if no matching bundle is found.
        """
        for bundle_home in self._candidate_homes():
            executables = _executable_files(bundle_home)
            if not executables:
                continue

            metadata_path = bundle_home / _METADATA_FILENAME
            dag_ids = _read_metadata_dag_ids(metadata_path)
            if dag_id in dag_ids and executables:
                return ResolvedExecutableBundle(executable_path=str(executables[0].resolve()))

        raise FileNotFoundError(
            f"No executable bundle containing dag_id={dag_id!r} found in {self._bundles_dir}"
        )

    @staticmethod
    def resolve_executable(path: Path) -> str | None:
        """
        Validate that *path* is a valid Airflow executable bundle.

        Returns the executable path string if valid (the file is executable
        and has a companion ``airflow-metadata.yaml``), ``None`` otherwise.
        """
        resolved = path.resolve()

        if resolved.is_file() and os.access(resolved, os.X_OK):
            # Check for metadata in the same directory
            metadata_path = resolved.parent / _METADATA_FILENAME
            if metadata_path.is_file():
                dag_ids = _read_metadata_dag_ids(metadata_path)
                if dag_ids:
                    return str(resolved)

        # path might be a directory containing the executable and metadata
        if resolved.is_dir():
            metadata_path = resolved / _METADATA_FILENAME
            if metadata_path.is_file():
                executables = _executable_files(resolved)
                if executables:
                    return str(executables[0].resolve())

        return None

    def _candidate_homes(self) -> list[Path]:
        """Return normalised bundle-home directories to inspect."""
        candidates: list[Path] = []

        # Each subdirectory is a potential bundle home (nested layout).
        if self._bundles_dir.is_dir():
            for child in sorted(self._bundles_dir.iterdir()):
                if child.is_dir():
                    candidates.append(child)

        # The directory itself (flat layout).
        candidates.append(self._bundles_dir)
        return candidates


def _executable_files(directory: Path) -> list[Path]:
    """List all executable files in *directory*, sorted by name."""
    if not directory.is_dir():
        return []
    return sorted(
        p
        for p in directory.iterdir()
        if p.is_file() and os.access(p, os.X_OK) and p.name != _METADATA_FILENAME
    )


def _read_metadata_dag_ids(metadata_path: Path) -> set[str]:
    """Parse dag IDs from an ``airflow-metadata.yaml`` file."""
    if not metadata_path.is_file():
        return set()
    try:
        with open(metadata_path) as f:
            data = yaml.safe_load(f)
    except (OSError, yaml.YAMLError):
        return set()
    if not isinstance(data, dict) or "dags" not in data:
        return set()
    return set(data["dags"].keys())


def read_source_code(executable_path: Path) -> str | None:
    """
    Read source code from a sidecar file alongside the executable.

    Looks for common source file patterns in the same directory:
    ``main.go``, ``main.rs``, ``<name>.go``, ``<name>.rs``, or a generic
    ``source`` file.  Returns ``None`` if no source file is found.
    """
    parent = executable_path.parent
    stem = executable_path.stem

    candidates = [
        parent / f"{stem}.go",
        parent / "main.go",
        parent / f"{stem}.rs",
        parent / "main.rs",
        parent / "source",
    ]

    for candidate in candidates:
        if candidate.is_file():
            try:
                return candidate.read_text()
            except OSError:
                continue

    return None
