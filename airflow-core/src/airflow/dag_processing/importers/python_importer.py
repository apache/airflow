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

import contextlib
import importlib
import importlib.machinery
import importlib.util
import logging
import os
import signal
import sys
import traceback
import warnings
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow import settings
from airflow._shared.module_loading.file_discovery import find_path_from_directory
from airflow.configuration import conf
from airflow.dag_processing.importers.base import (
    AbstractDagImporter,
    DagImportError,
    DagImportResult,
    DagImportWarning,
)
from airflow.utils.docs import get_docs_url
from airflow.utils.file import get_unique_dag_module_name, might_contain_dag

if TYPE_CHECKING:
    from types import ModuleType

    from airflow.sdk import DAG

log = logging.getLogger(__name__)


@contextlib.contextmanager
def _timeout(seconds: float = 1, error_message: str = "Timeout"):
    """Context manager for timing out operations."""
    error_message = error_message + ", PID: " + str(os.getpid())

    def handle_timeout(signum, frame):
        log.error("Process timed out, PID: %s", str(os.getpid()))
        from airflow.sdk.exceptions import AirflowTaskTimeout

        raise AirflowTaskTimeout(error_message)

    try:
        try:
            signal.signal(signal.SIGALRM, handle_timeout)
            signal.setitimer(signal.ITIMER_REAL, seconds)
        except ValueError:
            log.warning("timeout can't be used in the current context", exc_info=True)
        yield
    finally:
        with contextlib.suppress(ValueError):
            signal.setitimer(signal.ITIMER_REAL, 0)


class PythonDagImporter(AbstractDagImporter):
    """
    Importer for Python DAG files and zip archives containing Python DAGs.

    This is the default importer registered with the DagImporterRegistry. It handles:
    - .py files: Standard Python DAG files
    - .zip files: ZIP archives containing Python DAG files

    Note: The .zip extension is exclusively owned by this importer. If you need to
    support other file formats inside ZIP archives (e.g., YAML), you would need to
    either extend this importer or create a composite importer that delegates based
    on the contents of the archive.
    """

    @classmethod
    def supported_extensions(cls) -> list[str]:
        """Return file extensions handled by this importer (.py and .zip)."""
        return [".py", ".zip"]

    def list_dag_files(
        self,
        directory: str | os.PathLike[str],
        safe_mode: bool = True,
    ) -> Iterator[str]:
        """
        List Python DAG files in a directory.

        Handles both .py files and .zip archives containing Python DAGs.
        Respects .airflowignore files in the directory tree.
        """
        ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")

        for file_path in find_path_from_directory(directory, ".airflowignore", ignore_file_syntax):
            path = Path(file_path)
            try:
                if path.is_file() and (path.suffix.lower() == ".py" or zipfile.is_zipfile(path)):
                    if might_contain_dag(file_path, safe_mode):
                        yield file_path
            except Exception:
                log.exception("Error while examining %s", file_path)

    def import_file(
        self,
        file_path: str | Path,
        *,
        bundle_path: Path | None = None,
        bundle_name: str | None = None,
        safe_mode: bool = True,
    ) -> DagImportResult:
        """
        Import DAGs from a Python file or zip archive.

        :param file_path: Path to the Python file to import.
        :param bundle_path: Path to the bundle root.
        :param bundle_name: Name of the bundle.
        :param safe_mode: If True, skip files that don't appear to contain DAGs.
        :return: DagImportResult with imported DAGs and any errors.
        """
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        filepath = str(file_path)
        relative_path = self.get_relative_path(filepath, bundle_path)
        result = DagImportResult(file_path=relative_path)

        if not os.path.isfile(filepath):
            result.errors.append(
                DagImportError(
                    file_path=relative_path,
                    message=f"File not found: {filepath}",
                    error_type="file_not_found",
                )
            )
            return result

        # Clear any autoregistered dags from previous imports
        DagContext.autoregistered_dags.clear()

        # Capture warnings during import
        captured_warnings: list[warnings.WarningMessage] = []

        try:
            with warnings.catch_warnings(record=True) as captured_warnings:
                if filepath.endswith(".py") or not zipfile.is_zipfile(filepath):
                    modules = self._load_modules_from_file(filepath, safe_mode, result)
                else:
                    modules = self._load_modules_from_zip(filepath, safe_mode, result)
        except TypeError:
            # Configuration errors (e.g., invalid timeout type) should propagate
            raise
        except Exception as e:
            result.errors.append(
                DagImportError(
                    file_path=relative_path,
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
                    file_path=warn_msg.filename,
                    message=str(warn_msg.message),
                    warning_type=category,
                    line_number=warn_msg.lineno,
                )
            )

        # Process imported modules to extract DAGs
        self._process_modules(filepath, modules, bundle_name, bundle_path, result)

        return result

    def _load_modules_from_file(
        self, filepath: str, safe_mode: bool, result: DagImportResult
    ) -> list[ModuleType]:
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        def sigsegv_handler(signum, frame):
            msg = f"Received SIGSEGV signal while processing {filepath}."
            log.error(msg)
            result.errors.append(
                DagImportError(
                    file_path=result.file_path,
                    message=msg,
                    error_type="segfault",
                )
            )

        try:
            signal.signal(signal.SIGSEGV, sigsegv_handler)
        except ValueError:
            log.warning("SIGSEGV signal handler registration failed. Not in the main thread")

        if not might_contain_dag(filepath, safe_mode):
            log.debug("File %s assumed to contain no DAGs. Skipping.", filepath)
            result.skipped_files.append(filepath)
            return []

        log.debug("Importing %s", filepath)
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
                raise
            except BaseException as e:
                DagContext.autoregistered_dags.clear()
                log.exception("Failed to import: %s", filepath)
                if conf.getboolean("core", "dagbag_import_error_tracebacks"):
                    stacktrace = traceback.format_exc(
                        limit=-conf.getint("core", "dagbag_import_error_traceback_depth")
                    )
                else:
                    stacktrace = None
                result.errors.append(
                    DagImportError(
                        file_path=result.file_path,
                        message=str(e),
                        error_type="import",
                        stacktrace=stacktrace,
                    )
                )
                return []

        dagbag_import_timeout = settings.get_dagbag_import_timeout(filepath)

        if not isinstance(dagbag_import_timeout, (int, float)):
            raise TypeError(
                f"Value ({dagbag_import_timeout}) from get_dagbag_import_timeout must be int or float"
            )

        if dagbag_import_timeout <= 0:
            return parse(mod_name, filepath)

        timeout_msg = (
            f"DagBag import timeout for {filepath} after {dagbag_import_timeout}s.\n"
            "Please take a look at these docs to improve your DAG import time:\n"
            f"* {get_docs_url('best-practices.html#top-level-python-code')}\n"
            f"* {get_docs_url('best-practices.html#reducing-dag-complexity')}"
        )
        with _timeout(dagbag_import_timeout, error_message=timeout_msg):
            return parse(mod_name, filepath)

    def _load_modules_from_zip(
        self, filepath: str, safe_mode: bool, result: DagImportResult
    ) -> list[ModuleType]:
        """Load Python modules from a zip archive."""
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        mods: list[ModuleType] = []
        with zipfile.ZipFile(filepath) as current_zip_file:
            for zip_info in current_zip_file.infolist():
                zip_path = Path(zip_info.filename)
                if zip_path.suffix not in [".py", ".pyc"] or len(zip_path.parts) > 1:
                    continue

                if zip_path.stem == "__init__":
                    log.warning("Found %s at root of %s", zip_path.name, filepath)

                log.debug("Reading %s from %s", zip_info.filename, filepath)

                if not might_contain_dag(zip_info.filename, safe_mode, current_zip_file):
                    result.skipped_files.append(f"{filepath}:{zip_info.filename}")
                    continue

                mod_name = zip_path.stem
                if mod_name in sys.modules:
                    del sys.modules[mod_name]

                DagContext.current_autoregister_module_name = mod_name
                try:
                    sys.path.insert(0, filepath)
                    current_module = importlib.import_module(mod_name)
                    mods.append(current_module)
                except Exception as e:
                    DagContext.autoregistered_dags.clear()
                    fileloc = os.path.join(filepath, zip_info.filename)
                    log.exception("Failed to import: %s", fileloc)
                    if conf.getboolean("core", "dagbag_import_error_tracebacks"):
                        stacktrace = traceback.format_exc(
                            limit=-conf.getint("core", "dagbag_import_error_traceback_depth")
                        )
                    else:
                        stacktrace = None
                    result.errors.append(
                        DagImportError(
                            file_path=fileloc,  # Use the file path inside the ZIP
                            message=str(e),
                            error_type="import",
                            stacktrace=stacktrace,
                        )
                    )
                finally:
                    if sys.path[0] == filepath:
                        del sys.path[0]
        return mods

    def _process_modules(
        self,
        filepath: str,
        mods: list[Any],
        bundle_name: str | None,
        bundle_path: Path | None,
        result: DagImportResult,
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

        for dag, mod in top_level_dags:
            dag.fileloc = mod.__file__
            relative_fileloc = self.get_relative_path(dag.fileloc, bundle_path)
            dag.relative_fileloc = relative_fileloc

            result.dags.append(dag)
            log.debug("Found DAG %s", dag.dag_id)
