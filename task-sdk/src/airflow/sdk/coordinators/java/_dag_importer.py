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

import zipfile
from typing import TYPE_CHECKING, ClassVar, Final

from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.coordinators.java._jar_manifest import DAG_CODE, MAIN_CLASS, read_main_attributes
from airflow.sdk.importers.base import DagSourceCode

if TYPE_CHECKING:
    from airflow.sdk.coordinators.java.coordinator import JavaCoordinator
    from airflow.sdk.importers.base import DagDefinition

_MAX_SOURCE_BYTES: Final = 1024 * 1024
_NO_SOURCE: Final = (
    "// This JAR embeds no Dag source. Build it with the Airflow Java SDK Gradle plugin, or set\n"
    "// airflowBundle.dagSource, to show the source here.\n"
)
_SOURCE_TOO_LARGE: Final = "// The Dag source this JAR embeds is over 1 MiB, so it is not shown.\n"


class JavaDagImporter(CoordinatorDagImporter):
    """
    Parse the native Dags of Java bundle JARs with the coordinator's JVM.

    Only a JAR whose manifest sets ``Main-Class`` (matching the coordinator's ``main_class`` if set) is
    parsed.
    """

    artifact_suffix: ClassVar[str] = ".jar"
    supported_extensions = [".jar"]
    coordinator: JavaCoordinator

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        """
        Return whether the JAR sets ``Main-Class``, matching the coordinator's ``main_class`` if that is set.

        ``safe_mode`` does not apply, because a JAR without ``Main-Class`` cannot run at all. A JAR that
        cannot be read is kept, so that parsing it reports the error.
        """
        try:
            with definition.as_file() as path, zipfile.ZipFile(path) as zf:
                attributes = read_main_attributes(zf) or {}
        except (OSError, zipfile.BadZipFile):
            return True
        if not (main_class := attributes.get(MAIN_CLASS)):
            return False
        return not self.coordinator.main_class or main_class == self.coordinator.main_class

    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        """Return the Dag source the JAR embeds, or a placeholder when it embeds none."""
        with definition.as_file() as path, zipfile.ZipFile(path) as zf:
            if (info := _find_source_entry(zf)) is None:
                return DagSourceCode(source_code=_NO_SOURCE, language="java")
            if info.file_size > _MAX_SOURCE_BYTES:
                return DagSourceCode(source_code=_SOURCE_TOO_LARGE, language="java")
            source = zf.read(info).decode("utf-8", errors="replace")
        return DagSourceCode(source_code=source or _NO_SOURCE, language="java")


def _find_source_entry(zf: zipfile.ZipFile) -> zipfile.ZipInfo | None:
    if not (entry := (read_main_attributes(zf) or {}).get(DAG_CODE)):
        return None
    try:
        return zf.getinfo(entry)
    except KeyError:
        return None
