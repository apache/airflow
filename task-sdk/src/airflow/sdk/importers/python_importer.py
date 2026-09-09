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
"""Python DAG importer - imports DAGs from Python files."""

from __future__ import annotations

import functools
import importlib.machinery
import importlib.util
import logging
import os
import sys
import traceback
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.sdk._shared.module_loading.dag_file import get_unique_dag_module_name, might_contain_dag
from airflow.sdk.configuration import conf
from airflow.sdk.definitions._internal.contextmanager import DagContext
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.execution_time.timeout import timeout
from airflow.sdk.importers.base import (
    AbstractDagImporter,
    DagDefinition,
    DagImportError,
    DagImportResult,
    DagImportWarning,
    DagSourceCode,
    _normalize_extensions,
    find_file_dag_definitions,
    get_file_suffix,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

    from airflow.dag_processing.bundles.base import BaseDagBundle

log = logging.getLogger(__name__)


class PythonDagImporter(AbstractDagImporter):
    """
    Importer for Python DAG files.

    This is the default importer registered with the DagImporterRegistry. It handles
    .py files containing Python DAGs.
    """

    supported_extensions = [".py"]

    def __init__(self, extensions: list[str] | None = None) -> None:
        if extensions is not None:
            self.supported_extensions = _normalize_extensions(extensions)

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        """Check if this importer can handle the given definition based on file extension."""
        suffix = get_file_suffix(definition)
        return suffix in self.supported_extensions if suffix else False

    def list_dag_definitions(
        self,
        bundle: BaseDagBundle,
        *,
        safe_mode: bool = True,
    ) -> Iterator[DagDefinition]:
        """List Python DAG definitions in a bundle matching supported extensions."""
        yield from find_file_dag_definitions(bundle.path, self.supported_extensions)

    def import_definition(
        self,
        definition: DagDefinition,
        bundle: BaseDagBundle,
        *,
        safe_mode: bool = True,
    ) -> DagImportResult:
        """
        Import DAGs from a Python DAG definition.

        :param definition: The definition to import from.
        :param bundle: The DAG bundle containing the definition.
        :param safe_mode: If True, skip files that don't appear to contain DAGs.
        :return: DagImportResult with imported DAGs and any errors.
        """
        result = DagImportResult(definition=definition)
        DagContext.autoregistered_dags.clear()
        captured_warnings: list[warnings.WarningMessage] = []

        try:
            with warnings.catch_warnings(record=True) as captured_warnings:
                with definition.as_file() as local_path:
                    filepath = os.fspath(local_path)
                    modules = self._load_modules_from_file(
                        filepath,
                        safe_mode,
                        result,
                        bundle=bundle,
                    )
        except AirflowConfigException:
            # Configuration errors (e.g., invalid timeout type) should propagate
            raise
        except Exception as e:
            result.errors.append(
                DagImportError(
                    source_reference=repr(definition),
                    message=str(e),
                    error_type="import",
                    stacktrace=traceback.format_exc(),
                )
            )
            return result

        for warn_msg in captured_warnings:
            category = warn_msg.category.__name__
            if (module := warn_msg.category.__module__) != "builtins":
                category = f"{module}.{category}"
            result.warnings.append(
                DagImportWarning(
                    source_reference=repr(definition),
                    message=str(warn_msg.message),
                    warning_type=category,
                    line_number=warn_msg.lineno,
                )
            )

        self._process_modules(
            modules,
            result,
            bundle=bundle,
        )

        return result

    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        """Retrieve the raw source code for the Python definition."""
        return DagSourceCode(
            source_code=definition.read_text(encoding="utf-8"),
            language="python",
        )

    def might_contain_dag(self, file_path: str | Path, safe_mode: bool = True) -> bool:
        """Check whether a file might contain Airflow DAGs according to safe mode heuristics."""
        if not safe_mode:
            return True
        return might_contain_dag(str(file_path), safe_mode)

    def _load_modules_from_file(
        self,
        filepath: str,
        safe_mode: bool,
        result: DagImportResult,
        bundle: BaseDagBundle,
    ) -> list[ModuleType]:
        definition = result.definition

        if not self.might_contain_dag(filepath, safe_mode):
            log.debug("File %s assumed to contain no DAGs. Skipping.", filepath)
            if definition is not None:
                result.skipped_definitions.append(definition)
            return []

        log.debug("Importing %s (bundle: %s)", filepath, bundle.name)
        mod_name = get_unique_dag_module_name(filepath)

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        DagContext.current_autoregister_module_name = mod_name

        def parse(mod_name: str, filepath: str) -> list[ModuleType]:
            try:
                loader = importlib.machinery.SourceFileLoader(mod_name, filepath)
                spec = importlib.util.spec_from_loader(mod_name, loader)
                new_module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
                sys.modules[spec.name] = new_module  # type: ignore[union-attr]
                loader.exec_module(new_module)
                return [new_module]
            except KeyboardInterrupt:
                sys.modules.pop(mod_name, None)
                raise
            except BaseException as e:
                sys.modules.pop(mod_name, None)
                DagContext.autoregistered_dags.clear()
                log.exception("Failed to import: %s", filepath)
                if self._dagbag_import_error_tracebacks:
                    stacktrace = traceback.format_exc(limit=-self._dagbag_import_error_traceback_depth)
                else:
                    stacktrace = None
                result.errors.append(
                    DagImportError(
                        source_reference=repr(definition),
                        message=str(e),
                        error_type="import",
                        stacktrace=stacktrace,
                    )
                )
                return []

        dagbag_import_timeout: float
        try:
            from airflow import settings  # noqa: SDK002

            dagbag_import_timeout = settings.get_dagbag_import_timeout(filepath)
        except (ImportError, AttributeError):
            dagbag_import_timeout = 30.0

        if not isinstance(dagbag_import_timeout, (int, float)):
            raise AirflowConfigException(
                f"Value ({dagbag_import_timeout}) from get_dagbag_import_timeout must be int or float"
            )

        if dagbag_import_timeout <= 0:
            return parse(mod_name, filepath)

        timeout_msg = (
            f"DagBag import timeout for {filepath} after {dagbag_import_timeout}s.\n"
            "Please take a look at these docs to improve your DAG import time:\n"
            "* https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code\n"
            "* https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#reducing-dag-complexity"
        )
        with timeout(seconds=dagbag_import_timeout, error_message=timeout_msg):
            return parse(mod_name, filepath)

    def _process_modules(
        self,
        mods: list[Any],
        result: DagImportResult,
        bundle: BaseDagBundle,
    ) -> None:
        """Extract DAG objects from modules. Validation happens in bag_dag()."""
        top_level_dags: set[tuple[DAG, Any]] = {
            (o, m) for m in mods for o in m.__dict__.values() if isinstance(o, DAG)
        }
        top_level_dags.update(DagContext.autoregistered_dags)

        DagContext.current_autoregister_module_name = None
        DagContext.autoregistered_dags.clear()

        for dag, _mod in top_level_dags:
            dag.bundle_name = bundle.name
            dag.fileloc = repr(result.definition)
            if result.definition is not None:
                dag.relative_fileloc = result.definition.get_relative_loc(bundle.path)
            result.dags.append(dag)
            log.debug("Found DAG %s", dag.dag_id)

    @functools.cached_property
    def _dagbag_import_error_tracebacks(self) -> bool:
        return conf.getboolean("core", "dagbag_import_error_tracebacks")

    @functools.cached_property
    def _dagbag_import_error_traceback_depth(self) -> int:
        return conf.getint("core", "dagbag_import_error_traceback_depth")
