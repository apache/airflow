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
"""The Dag importers of the native Dag files that a coordinator's runtime parses."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Final, cast

from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.execution_time.coordinator import CoordinatorManager, get_coordinator_manager
from airflow.sdk.importers.base import (
    AbstractDagImporter,
    FilesystemDagDefinition,
    find_file_dag_definitions,
    get_importer_registry,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
    from airflow.sdk.importers.base import DagDefinition

COORDINATOR_DAG_IMPORTERS: Final[tuple[str, ...]] = ()
"""
The classpaths of the :class:`CoordinatorDagImporter` subclasses a Dag bundle's registry may hold.

An importer is registered in a bundle when ``[sdk] coordinators`` has a coordinator of its
:attr:`~CoordinatorDagImporter.coordinator_classpath` class.
"""


class CoordinatorDagImporter(AbstractDagImporter[FilesystemDagDefinition]):
    """
    Claim the native Dag files of one runtime, which that runtime's coordinator parses.

    The importer belongs to one Dag bundle and finds its coordinator in ``[sdk] coordinators`` when it
    needs one: the only coordinator of :attr:`coordinator_classpath`'s class, or the one
    ``[sdk] dag_bundle_to_coordinator`` picks for the bundle when there are several.

    Subclasses set :attr:`coordinator_classpath`, :attr:`artifact_suffix` and
    :attr:`supported_extensions`, and implement :meth:`import_definition` and :meth:`get_source_code`.
    """

    coordinator_classpath: ClassVar[str]
    """
    The classpath of the coordinator class that parses and runs this importer's files.

    A coordinator is of this class when its class is that class or a subclass of it.
    """

    artifact_suffix: ClassVar[str]
    """The file name suffix of the artifacts this importer claims, such as ``.min.mjs``."""

    supported_extensions: list[str]
    """
    The extensions a registry routes to this importer, such as ``[".mjs"]`` for ``.min.mjs`` artifacts.

    A registry routes by the last suffix alone.
    """

    def __init__(self, *, bundle_name: str) -> None:
        self.bundle_name = bundle_name

    def get_parsing_coordinator(self) -> SubprocessCoordinator:
        """
        Return the coordinator that parses this importer's files in its Dag bundle.

        :raises InvalidCoordinatorError: when no single coordinator can parse them, or the one that can
            cannot be built.
        """
        manager = get_coordinator_manager()
        key = manager.get_dag_parsing_coordinator_key(self.coordinator_classpath, self.bundle_name)
        return cast("SubprocessCoordinator", manager.get_coordinator(key))

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


def build_coordinator_dag_importers(
    manager: CoordinatorManager, bundle_name: str
) -> list[CoordinatorDagImporter]:
    """
    Build the coordinator Dag importers of *bundle_name*, one for each runtime that has a coordinator.

    No coordinator is built, and nothing is imported when *manager* has no coordinators.
    """
    if not manager.has_coordinators():
        return []
    importer_classes = [import_string(classpath) for classpath in COORDINATOR_DAG_IMPORTERS]
    return [
        importer_class(bundle_name=bundle_name)
        for importer_class in importer_classes
        if manager.get_coordinator_keys_for_class(importer_class.coordinator_classpath)
    ]


def find_claiming_importer(
    path: str | os.PathLike[str], bundle_name: str | None
) -> CoordinatorDagImporter | None:
    """
    Return the coordinator Dag importer that claims *path* in *bundle_name*, or ``None`` if there is none.

    The importer builds no coordinator.

    :raises RuntimeError: when the bundle's coordinator Dag importers could not be built.
    """
    registry = get_importer_registry(bundle_name)
    if (error := registry.coordinator_importer_error) is not None:
        raise RuntimeError(
            f"Cannot build the coordinator Dag importers of Dag bundle {bundle_name!r}"
        ) from error
    file = Path(path)
    importer = registry.get_importer(file)
    return importer if isinstance(importer, CoordinatorDagImporter) and importer.can_handle(file) else None
