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
Scan directories for Airflow Java SDK bundle JARs.

Mirrors the Java SDK's ``BundleScanner`` — checks each JAR's manifest for
``Airflow-Java-SDK-Metadata``, reads the embedded metadata YAML, and
resolves the main class and classpath needed to launch the bundle process.
"""

from __future__ import annotations

import email
import os
import zipfile
from pathlib import Path
from typing import NamedTuple

import yaml

MANIFEST_PATH = "META-INF/MANIFEST.MF"
METADATA_MANIFEST_KEY = "Airflow-Java-SDK-Metadata"
SDK_VERSION_MANIFEST_KEY = "Airflow-Java-SDK-Version"
DAG_CODE_MANIFEST_KEY = "Airflow-Java-SDK-Dag-Code"
MAIN_CLASS_MANIFEST_KEY = "Main-Class"


class ResolvedJarBundle(NamedTuple):
    """A resolved Java DAG bundle: everything needed to start the bundle process."""

    main_class: str
    classpath: str


class BundleScanner:
    """
    Locate Airflow Java SDK bundles inside a directory tree.

    Supports two directory layouts:

    - **Nested** - each immediate subdirectory of *bundles_dir* is a bundle home.
    - **Flat** — *bundles_dir* itself contains the bundle JARs.

    Within a bundle home the JVM convention of a ``lib/`` subdirectory for
    dependency JARs is respected automatically.
    """

    def __init__(self, bundles_dir: Path) -> None:
        self._bundles_dir = bundles_dir

    def resolve(self, dag_id: str) -> ResolvedJarBundle:
        """
        Find the bundle whose metadata YAML lists *dag_id*.

        :raises FileNotFoundError: if no matching bundle is found.
        """
        for bundle_home in self._candidate_homes():
            jars = _jar_files(bundle_home)
            if not jars:
                continue

            for jar_path in jars:
                result = _read_bundle_jar(jar_path)
                if result is None:
                    continue
                main_class, dag_ids = result
                if dag_id in dag_ids:
                    classpath = os.pathsep.join(str(j.resolve()) for j in jars)
                    return ResolvedJarBundle(main_class=main_class, classpath=classpath)

        raise FileNotFoundError(f"No JAR bundle containing dag_id={dag_id!r} found in {self._bundles_dir}")

    @staticmethod
    def resolve_jar(jar_path: Path) -> str:
        """
        Read ``Main-Class`` from a single bundle JAR, validating SDK attributes.

        :raises FileNotFoundError: if the JAR is not a valid Airflow Java SDK bundle.
        """
        result = _read_bundle_jar(jar_path)
        if result is None:
            raise FileNotFoundError(
                f"Not a valid Airflow Java SDK bundle: {jar_path} "
                f"(requires {METADATA_MANIFEST_KEY} and {MAIN_CLASS_MANIFEST_KEY})"
            )
        return result[0]

    def _candidate_homes(self) -> list[Path]:
        """Return normalised bundle-home directories to inspect."""
        candidates: list[Path] = []

        # Each subdirectory is a potential bundle home (nested layout).
        if self._bundles_dir.is_dir():
            for child in sorted(self._bundles_dir.iterdir()):
                if child.is_dir():
                    candidates.append(_normalize_bundle_home(child))

        # The directory itself (flat layout).
        candidates.append(_normalize_bundle_home(self._bundles_dir))
        return candidates


def _jar_files(directory: Path) -> list[Path]:
    """List all ``.jar`` files in *directory*, sorted by name."""
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.iterdir() if p.is_file() and p.suffix == ".jar")


def _normalize_bundle_home(path: Path) -> Path:
    """
    Normalize a bundle path to the directory containing JARs.

    Handles the common JVM distribution layout where dependency JARs
    live in a ``lib/`` subdirectory (Gradle ``application`` plugin,
    Maven Assembly, sbt-native-packager, etc.).

    - If *path* points to a JAR file, use its parent directory.
    - If the directory has a ``lib/`` subdirectory containing JARs, use that.
    - Otherwise, return the directory as-is.
    """
    normalized = path.resolve()
    if normalized.is_file() and normalized.suffix == ".jar":
        return normalized.parent
    lib = normalized / "lib"
    if lib.is_dir() and any(p.suffix == ".jar" for p in lib.iterdir()):
        return lib
    return normalized


def _read_bundle_jar(jar_path: Path) -> tuple[str, set[str]] | None:
    """
    Read ``Main-Class`` and dag IDs from a JAR's manifest and embedded metadata.

    Returns ``(main_class, dag_ids)`` when the JAR carries valid
    ``Airflow-Java-SDK-Metadata`` and ``Main-Class`` manifest attributes
    and the referenced metadata YAML contains at least one dag ID.
    Returns ``None`` otherwise.
    """
    try:
        with zipfile.ZipFile(jar_path) as zf:
            try:
                with zf.open(MANIFEST_PATH) as f:
                    manifest = email.message_from_binary_file(f)
            except KeyError:
                return None

            metadata_file = manifest.get(METADATA_MANIFEST_KEY)
            if not metadata_file:
                return None

            main_class = manifest.get(MAIN_CLASS_MANIFEST_KEY)
            if not main_class:
                return None

            try:
                with zf.open(metadata_file) as f:
                    content = f.read().decode()
            except KeyError:
                return None
    except zipfile.BadZipFile:
        return None

    dag_ids = _parse_dag_ids_from_metadata(content)
    if not dag_ids:
        return None

    return main_class, dag_ids


def read_dag_code(jar_path: Path) -> str | None:
    """
    Read the DAG source code embedded in a JAR bundle.

    Returns the source code string when the JAR carries a valid
    ``Airflow-Java-SDK-Dag-Code`` manifest attribute pointing to an
    embedded source file.  Returns ``None`` otherwise.
    """
    try:
        with zipfile.ZipFile(jar_path) as zf:
            try:
                with zf.open(MANIFEST_PATH) as f:
                    manifest = email.message_from_binary_file(f)
            except KeyError:
                return None

            dag_code_path = manifest.get(DAG_CODE_MANIFEST_KEY)
            if not dag_code_path:
                return None

            try:
                with zf.open(dag_code_path) as f:
                    return f.read().decode()
            except KeyError:
                return None
    except zipfile.BadZipFile:
        return None


def _parse_dag_ids_from_metadata(yaml_content: str) -> set[str]:
    """Parse dag IDs from an ``airflow-metadata.yaml`` content string."""
    data = yaml.safe_load(yaml_content)
    if not isinstance(data, dict) or "dags" not in data:
        return set()
    return set(data["dags"].keys())
