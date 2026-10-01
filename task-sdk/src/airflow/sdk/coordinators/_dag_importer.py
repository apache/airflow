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
"""The Dag importer a coordinator hands out for its native Dag files."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from airflow.sdk.importers.base import (
    AbstractDagImporter,
    DagImportError,
    DagImportResult,
    FilesystemDagDefinition,
    find_file_dag_definitions,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
    from airflow.sdk.importers.base import DagDefinition


class CoordinatorDagImporter(AbstractDagImporter[FilesystemDagDefinition]):
    """
    Claim the native Dag files of a coordinator's artifacts, which the coordinator's runtime parses.

    The Dag processor does not call :meth:`import_definition`: it runs the runtime itself and stores
    the Dags the runtime serialized. A Dag bag, such as a CLI command's, reports such a file as an
    import error.

    Subclasses set :attr:`artifact_suffix` and :attr:`supported_extensions`, and implement
    :meth:`get_source_code`.
    """

    artifact_suffix: ClassVar[str]
    """The file name suffix of the artifacts this importer claims, such as ``.min.mjs``."""

    supported_extensions: list[str]
    """
    The extensions a registry routes to this importer, such as ``[".mjs"]`` for ``.min.mjs`` artifacts.

    A registry routes by the last suffix alone.
    """

    def __init__(self, *, coordinator: SubprocessCoordinator) -> None:
        self.coordinator = coordinator

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        return str(definition).endswith(self.artifact_suffix)

    def list_dag_definitions(
        self, bundle: BaseDagBundle, *, safe_mode: bool = True
    ) -> Iterator[FilesystemDagDefinition]:
        for definition in find_file_dag_definitions(bundle.path, self.supported_extensions):
            if definition.path.name.endswith(self.artifact_suffix) and self.might_contain_dag(
                definition, safe_mode
            ):
                yield definition

    def import_definition(
        self, definition: FilesystemDagDefinition, bundle: BaseDagBundle
    ) -> DagImportResult:
        """Report that only the Dag processor parses *definition*."""
        return DagImportResult(
            definition=definition,
            errors=[
                DagImportError(
                    source_reference=repr(definition),
                    message="A native Lang-SDK Dag is parsed only by the Dag processor",
                )
            ],
        )
