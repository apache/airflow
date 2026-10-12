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
AIP-85 YAML/JSON Dag importer.

This discovers ``.yaml`` and ``.yml`` Dag files in a bundle, and builds the Dag
objects they describe. It is not turned on by default; enable with::

    [dag_processor]
    dag_importer_configs = [{"classpath": "airflow.sdk.importers.yaml_importer.importer.YamlDagImporter"}]
"""

from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING

from airflow.sdk.importers.base import (
    AbstractDagImporter,
    DagImportError,
    DagImportResult,
    DagSourceCode,
    FileDagDefinition,
    _normalize_extensions,
    find_file_dag_definitions,
    get_file_suffix,
)
from airflow.sdk.importers.yaml_importer.builder import DagBuilder
from airflow.sdk.importers.yaml_importer.parser import YamlDagParseError, parse_documents

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.importers.base import DagDefinition

log = logging.getLogger(__name__)


class YamlDagImporter(AbstractDagImporter[FileDagDefinition]):
    """Importer for YAML/JSON DAG sources. Opt-in via ``dag_importer_configs`` (not a default)."""

    supported_extensions = [".yaml", ".yml"]

    def __init__(self, extensions: list[str] | None = None) -> None:
        if extensions is not None:
            self.supported_extensions = _normalize_extensions(extensions)

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        suffix = get_file_suffix(definition)
        return suffix in self.supported_extensions if suffix else False

    def list_dag_definitions(
        self,
        bundle: BaseDagBundle,
        *,
        safe_mode: bool = True,
    ) -> Iterator[FileDagDefinition | DagImportError]:
        for definition in find_file_dag_definitions(bundle.path, self.supported_extensions):
            if self.might_contain_dag(definition, safe_mode):
                yield definition
            else:
                log.debug("Skipping %r: no '$schema' marker found", definition)

    def import_definition(
        self,
        definition: FileDagDefinition,
        bundle: BaseDagBundle,
    ) -> DagImportResult:
        result = DagImportResult(definition=definition)
        try:
            source = definition.read_text(encoding="utf-8")
        except Exception as exc:
            result.errors.append(
                DagImportError(
                    source_reference=repr(definition),
                    message=str(exc),
                    error_type="import",
                    stacktrace=traceback.format_exc(),
                )
            )
            return result

        # A parse failure fails the whole file (like a Python module that won't import); a build
        # failure is per-document so the file's other DAGs still load.
        try:
            documents = list(parse_documents(source, source=repr(definition)))
        except YamlDagParseError as exc:
            result.errors.append(
                DagImportError(source_reference=repr(definition), message=str(exc), error_type="parse")
            )
            return result

        for document in documents:
            try:
                dag = DagBuilder(definition.sibling).build(document)
            except Exception as exc:
                result.errors.append(
                    DagImportError(
                        source_reference=f"{definition!r} (dag_id={document.dag_id!r})",
                        message=str(exc),
                        error_type="import",
                        stacktrace=traceback.format_exc(),
                    )
                )
                continue
            dag.bundle_name = bundle.name
            dag.fileloc = repr(definition)
            dag.relative_fileloc = definition.get_relative_loc(bundle.path)
            result.dags.append(dag)
            log.debug("Built DAG %s from %r", dag.dag_id, definition)

        return result

    def get_source_code(self, definition: DagDefinition, dag_id: str | None = None) -> DagSourceCode:
        return DagSourceCode(source_code=definition.read_text(encoding="utf-8"), language="yaml")

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        """Cheap sniff: a DAG document declares a ``$schema``. Keep the file if the read fails."""
        if not safe_mode:
            return True
        try:
            return b"$schema" in definition.read_bytes()
        except OSError:
            return True
