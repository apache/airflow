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

import structlog

from airflow.sdk.exceptions import AirflowConfigException
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

    from structlog.typing import FilteringBoundLogger

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
    from airflow.sdk.importers.base import DagDefinition

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.dag_importer")


def _get_import_timeout(definition: FilesystemDagDefinition) -> float | None:
    """Return the parse timeout for *definition*, as ``PythonDagImporter`` does; ``None`` means none."""
    try:
        from airflow import settings  # noqa: SDK002

        timeout = settings.get_dagbag_import_timeout(repr(definition))
    except (ImportError, AttributeError):
        timeout = 30.0
    if not isinstance(timeout, (int, float)):
        raise AirflowConfigException(f"Value ({timeout}) from get_dagbag_import_timeout must be int or float")
    return timeout if timeout > 0 else None


class CoordinatorDagImporter(AbstractDagImporter[FilesystemDagDefinition]):
    """
    Import the native Dags of a coordinator's artifacts by running the coordinator's runtime.

    The Dag processor does not call :meth:`import_definition`: it runs the runtime itself and stores
    the Dags the runtime serialized. This method serves a Dag bag, such as a CLI command's, and returns
    each Dag as a ``SerializedLangSDKDAG``, the ``SerializedDAG`` the scheduler loads. Its tasks run only through the coordinator,
    never in Python.

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
        from airflow.dag_processing.lang_sdk_processor import LangSDKDagFileProcessorProcess  # noqa: SDK002
        from airflow.serialization.serialized_objects import DagSerialization  # noqa: SDK002

        source_reference = repr(definition)
        bundle_path = bundle.path or definition.path.parent
        result = DagImportResult(definition=definition)
        try:
            parsing_result = LangSDKDagFileProcessorProcess.run(
                path=definition.path,
                bundle_path=bundle_path,
                bundle_name=bundle.name,
                dag_file_rel_path=definition.get_relative_loc(bundle_path),
                timeout=_get_import_timeout(definition),
                logger=log,
            )
        except TimeoutError as e:
            result.errors.append(
                DagImportError(source_reference=source_reference, message=str(e), error_type="timeout")
            )
            return result

        relative_loc = definition.get_relative_loc(bundle_path)
        for key, message in (parsing_result.import_errors or {}).items():
            result.errors.append(
                DagImportError(
                    source_reference=source_reference,
                    message=message if key == relative_loc else f"{key}: {message}",
                )
            )
        # The runtime process validated each Dag, and moved one that fails into import_errors.
        result.dags.extend(DagSerialization.from_dict(dag.data) for dag in parsing_result.serialized_dags)
        return result
