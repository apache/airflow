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
AIP-85 YAML Dag importer.

This discovers ``.yaml`` and ``.yml`` Dag files in a bundle, and builds the Dag
objects they describe. It is not turned on by default; enable with::

    [dag_processor]
    dag_importer_configs = [{"classpath": "airflow.sdk.importers.yaml_importer.YamlDagImporter"}]
"""

from __future__ import annotations

import logging
import re
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
from airflow.sdk.importers.yaml_importer.builder import DagBuilder, YamlDagBuildError
from airflow.sdk.importers.yaml_importer.migrator import DAG_SCHEMA_PREFIX
from airflow.sdk.importers.yaml_importer.parser import YamlDagParseError, parse_documents

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk import DAG
    from airflow.sdk.importers.base import DagDefinition

log = logging.getLogger(__name__)

# Sniff a file for the canonical $schema declaration. The URL is matched only as
# a valid YAML or JSON key-value format (e.g. $schema: <url>) to reduce false
# positives. The migrator's public prefix is reused to keep checks in sync.
_DAG_SCHEMA_SNIFF_RE = re.compile(rb'\$schema["\']?\s*:\s*["\']?' + re.escape(DAG_SCHEMA_PREFIX.encode()))


class YamlDagImporter(AbstractDagImporter[FileDagDefinition]):
    """Importer for YAML DAG sources. Opt-in via ``dag_importer_configs`` (not a default)."""

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
        ref = repr(definition)
        try:
            source = definition.read_text(encoding="utf-8")
        except Exception as exc:
            result.errors.append(
                DagImportError(
                    source_reference=ref,
                    message=str(exc),
                    error_type="import",
                    stacktrace=traceback.format_exc(),
                )
            )
            return result

        # A parse failure fails the whole file (like a Python module that won't
        # import). All documents' failures are reported together.
        try:
            documents = list(parse_documents(source, source=ref))
        except YamlDagParseError as exc:
            result.errors.append(DagImportError(source_reference=ref, message=str(exc), error_type="parse"))
            return result

        dags: dict[str, DAG] = {}
        build_errors: list[Exception] = []
        with definition.import_context():
            for document in documents:
                try:
                    dag = DagBuilder(definition=definition, bundle=bundle).build(document)
                    if dag.dag_id in dags:
                        raise YamlDagBuildError(f"duplicate dag_id {dag.dag_id!r} across documents")
                except Exception as exc:
                    exc.add_note(f"dag_id={document.dag_id!r}")
                    build_errors.append(exc)
                else:
                    dags[dag.dag_id] = dag

        if build_errors:
            summary = f"{len(build_errors)} of {len(documents)} document(s) failed to build"
            detail = "\n".join(f"{err.__notes__[-1]}: {err}" for err in build_errors)
            stacktrace = "\n".join(
                "".join(traceback.format_exception(type(err), err, err.__traceback__)) for err in build_errors
            )
            result.errors.append(
                DagImportError(
                    source_reference=ref,
                    message=f"{summary}:\n{detail}",
                    error_type="import",
                    stacktrace=stacktrace,
                )
            )
            return result

        result.dags.extend(dags.values())
        for dag in dags.values():
            log.debug("Built Dag %s from %r", dag.dag_id, definition)
        return result

    def get_source_code(self, definition: DagDefinition, dag_id: str | None = None) -> DagSourceCode:
        return DagSourceCode(source_code=definition.read_text(encoding="utf-8"), language="yaml")

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        """
        Cheap snif ensuring the file declares a reasonable ``$schema``.

        This matches the URL as the ``$schema`` value, and skips a file that
        does not have such a key or only mentions the URL in another field. A
        malformed Dag may still reach the parser and get a clear error, rather
        than silently disappearing.
        """
        if not safe_mode:
            return True
        try:
            data = definition.read_bytes()
        except OSError:
            return True
        return _DAG_SCHEMA_SNIFF_RE.search(data) is not None
