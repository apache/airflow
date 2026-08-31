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

import importlib.machinery
import importlib.util
import logging
import os
import sys
import traceback
import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.base import BaseDagBundle

from airflow.sdk.importers.base import (
    AbstractDagImporter,
    DagDefinition,
    DagImportError,
    DagImportResult,
    DagImportWarning,
    DagSourceCode,
)
from airflow.sdk.execution_time.timeout import timeout

if TYPE_CHECKING:
    from types import ModuleType

    from airflow.sdk import DAG

log = logging.getLogger(__name__)


class PythonDagImporter(AbstractDagImporter):
    """
    Importer for Python DAG files.

    This is the default importer registered with the DagImporterRegistry. It handles
    .py files containing Python DAGs.
    """

    supported_extensions = [".py"]

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
        :param safe_mode: If True, skip files that don't appear to contain DAGs.
        :return: DagImportResult with imported DAGs and any errors.
        """
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        result = DagImportResult(definition=definition)

        # Clear any autoregistered dags from previous imports
        DagContext.autoregistered_dags.clear()

        # Capture warnings during import
        captured_warnings: list[warnings.WarningMessage] = []

        try:
            with warnings.catch_warnings(record=True) as captured_warnings:
                with definition.as_file() as local_path:
                    filepath = os.fspath(local_path)
                    modules = self._load_modules_from_file(filepath, safe_mode, result, bundle)
        except TypeError:
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

        # Convert captured warnings to DagImportWarning
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

        # Process imported modules to extract DAGs
        self._process_modules(modules, result, bundle)

        return result

    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        """Retrieve the raw source code for the Python definition."""
        from airflow.sdk.importers.base import DagSourceCode

        return DagSourceCode(
            source_code=definition.read_text(encoding="utf-8"),
            language="python",
        )

    def _load_modules_from_file(
        self, filepath: str, safe_mode: bool, result: DagImportResult, bundle: BaseDagBundle
    ) -> list[ModuleType]:
        from airflow import settings
        from airflow.sdk._shared.module_loading.dag_file import get_unique_dag_module_name, might_contain_dag
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        definition = result.definition

        if not might_contain_dag(filepath, safe_mode):
            log.debug("File %s assumed to contain no DAGs. Skipping.", filepath)
            result.skipped_definitions.append(definition)
            return []

        log.debug("Importing %s (bundle: %s)", filepath, bundle.name)
        mod_name = get_unique_dag_module_name(filepath)

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        DagContext.current_autoregister_module_name = mod_name

        def parse(mod_name: str, filepath: str) -> list[ModuleType]:
            from airflow.configuration import conf

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
                if conf and conf.getboolean("core", "dagbag_import_error_tracebacks"):
                    stacktrace = traceback.format_exc(
                        limit=-conf.getint("core", "dagbag_import_error_traceback_depth")
                    )
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

        if settings is not None:
            dagbag_import_timeout = settings.get_dagbag_import_timeout(filepath)
        else:
            dagbag_import_timeout = 30.0

        if not isinstance(dagbag_import_timeout, (int, float)):
            raise TypeError(
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
        from airflow.sdk import DAG
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        top_level_dags: set[tuple[DAG, Any]] = {
            (o, m) for m in mods for o in m.__dict__.values() if isinstance(o, DAG)
        }
        top_level_dags.update(DagContext.autoregistered_dags)

        DagContext.current_autoregister_module_name = None
        DagContext.autoregistered_dags.clear()

        for dag, _mod in top_level_dags:
            dag.bundle_name = bundle.name
            dag.fileloc = repr(result.definition)
            dag.relative_fileloc = result.definition.get_relative_loc(bundle.path)
            result.dags.append(dag)
            log.debug("Found DAG %s", dag.dag_id)
