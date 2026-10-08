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
"""The Dag importer of :class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator`."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Final

from airflow.sdk._shared.module_loading.file_discovery import find_path_from_directory
from airflow.sdk.configuration import conf
from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.coordinators.executable._bundle_reader import (
    read_bundle_entrypoint_source,
    read_bundle_language,
    read_bundle_source,
)
from airflow.sdk.coordinators.executable.coordinator import FOOTER_MAGIC
from airflow.sdk.importers.base import DagSourceCode, FilesystemDagDefinition, get_file_suffix

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.importers.base import DagDefinition

_NO_SOURCE: Final = "// Source code is not available: the bundle embeds no source.\n"
_DEFAULT_LANGUAGE: Final = "text"


class ExecutableDagImporter(CoordinatorDagImporter):
    """
    Claim the native Dags of executable bundles, such as the ones the Go SDK packs.

    An :class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator` parses them. A bundle binary has
    no file extension, and the ``AFBNDL01`` magic that ends the file identifies it when the bundle is scanned.
    """

    coordinator_classpath: ClassVar[str] = "airflow.sdk.coordinators.executable.ExecutableCoordinator"
    artifact_suffix = ""
    supported_extensions: list[str] = []

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        return get_file_suffix(definition) == ""

    def list_dag_definitions(
        self, bundle: BaseDagBundle, *, safe_mode: bool = True
    ) -> Iterator[FilesystemDagDefinition]:
        root = Path(bundle.path)
        if root.is_file():
            paths: Iterator[Path] = iter([root])
        else:
            ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
            paths = (Path(p) for p in find_path_from_directory(root, ".airflowignore", ignore_file_syntax))
        for path in paths:
            if path.is_file() and self.can_handle(path):
                definition = FilesystemDagDefinition(path=path)
                if self.might_contain_dag(definition, safe_mode):
                    yield definition

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        """
        Return whether the file ends with the bundle trailer.

        ``safe_mode`` does not apply: whether a bundle defines Dags is known only by running it, and a
        bundle that only registers task handlers is parsed too. A bundle that fails verification is kept,
        so that parsing it reports why.
        """
        try:
            with definition.as_file() as path, open(path, "rb") as bundle_file:
                bundle_file.seek(-len(FOOTER_MAGIC), os.SEEK_END)
                return bundle_file.read(len(FOOTER_MAGIC)) == FOOTER_MAGIC
        except OSError:
            return False

    def get_source_code(self, definition: DagDefinition, dag_id: str | None = None) -> DagSourceCode:
        """
        Return the embedded source of *dag_id*'s own file, or a notice when the bundle embeds none.

        Without *dag_id*, or for one the bundle maps to no file, this is the entrypoint source.
        """
        with definition.as_file() as path:
            source = (
                read_bundle_source(path, dag_id)
                if dag_id is not None
                else read_bundle_entrypoint_source(path)
            )
            language = read_bundle_language(path)
        return DagSourceCode(source_code=source or _NO_SOURCE, language=language or _DEFAULT_LANGUAGE)
