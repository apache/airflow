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
"""The Dag importer of :class:`~airflow.sdk.coordinators.java.JavaCoordinator`."""

from __future__ import annotations

import json
import zipfile
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.coordinators.java._jar_manifest import MAIN_CLASS, SOURCES, read_main_attributes
from airflow.sdk.importers.base import DagSourceCode

if TYPE_CHECKING:
    from airflow.sdk.coordinators.java.coordinator import JavaCoordinator
    from airflow.sdk.importers.base import DagDefinition

_SOURCES_DIR: Final = "META-INF/airflow/sources/"
_MAX_SOURCE_BYTES: Final = 1024 * 1024
_NO_SOURCE: Final = (
    "// This JAR embeds no Dag source. Build it with the Airflow Java SDK Gradle plugin to show the\n"
    "// source here.\n"
)
_SOURCE_TOO_LARGE: Final = "// This Dag source file is over 1 MiB, so it is not shown.\n"


class JavaDagImporter(CoordinatorDagImporter):
    """
    Claim the native Dags of Java bundle JARs.

    A :class:`~airflow.sdk.coordinators.java.JavaCoordinator` parses them. Only a JAR whose manifest sets
    ``Main-Class`` (matching the coordinator's ``main_class`` if set) is parsed.
    """

    coordinator_classpath: ClassVar[str] = "airflow.sdk.coordinators.java.JavaCoordinator"
    artifact_suffix: ClassVar[str] = ".jar"
    supported_extensions = [".jar"]

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        """
        Return whether the JAR sets ``Main-Class``, matching the parsing coordinator's ``main_class`` if set.

        ``safe_mode`` does not apply, because a JAR without ``Main-Class`` cannot run at all. A JAR that
        cannot be read is kept, so that parsing it reports the error. So is every JAR when no coordinator
        can parse the bundle, so that each parse reports why.
        """
        try:
            with definition.as_file() as path, zipfile.ZipFile(path) as zf:
                attributes = read_main_attributes(zf) or {}
        except (OSError, zipfile.BadZipFile):
            return True
        if not (main_class := attributes.get(MAIN_CLASS)):
            return False
        try:
            wanted = cast("JavaCoordinator", self.get_parsing_coordinator()).main_class
        except Exception:
            return True
        return not wanted or main_class == wanted

    def get_source_code(self, definition: DagDefinition, dag_id: str | None = None) -> DagSourceCode:
        """Return the entrypoint source the JAR embeds, or a notice when it embeds none."""
        with definition.as_file() as path, zipfile.ZipFile(path) as zf:
            info = _find_source_entry(zf)
            if info is None:
                return DagSourceCode(source_code=_NO_SOURCE, language="java")
            if info.file_size > _MAX_SOURCE_BYTES:
                return DagSourceCode(source_code=_SOURCE_TOO_LARGE, language="java")
            source = zf.read(info).decode("utf-8", errors="replace")
        return DagSourceCode(source_code=source or _NO_SOURCE, language="java")


def _find_source_entry(zf: zipfile.ZipFile, dag_id: str | None = None) -> zipfile.ZipInfo | None:
    """
    Return the JAR entry of the source embedded for *dag_id*, or ``None`` when there is none.

    The Gradle plugin packs each Dag's source file once, maps every Java-declared Dag to its file in
    ``dag_source_paths``, and always packs the entrypoint (the ``Main-Class`` source). A Dag that is
    not mapped, or no *dag_id*, falls back to the entrypoint.
    """
    index = _read_source_index(zf)
    paths = index.get("dag_source_paths")
    source_path = paths.get(dag_id) if dag_id is not None and isinstance(paths, dict) else None
    if not isinstance(source_path, str):
        source_path = index.get("entrypoint_path")
    if not isinstance(source_path, str):
        return None
    try:
        return zf.getinfo(_SOURCES_DIR + source_path)
    except KeyError:
        return None


def _read_source_index(zf: zipfile.ZipFile) -> dict[str, Any]:
    if not (entry := (read_main_attributes(zf) or {}).get(SOURCES)):
        return {}
    try:
        info = zf.getinfo(entry)
    except KeyError:
        return {}
    if info.file_size > _MAX_SOURCE_BYTES:
        return {}
    try:
        index = json.loads(zf.read(info))
    except ValueError:
        return {}
    return index if isinstance(index, dict) else {}
