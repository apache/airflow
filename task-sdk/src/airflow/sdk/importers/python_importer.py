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
import importlib.abc
import importlib.util
import logging
import marshal
import signal
import sys
import traceback
import types
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
    FileDagDefinition,
    _normalize_extensions,
    find_file_dag_definitions,
    get_file_suffix,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002

log = logging.getLogger(__name__)


class _DefinitionSourceLoader(importlib.abc.SourceLoader):
    """
    A SourceLoader that executes a DagDefinition straight from its bytes.

    It needs no file on disk: :meth:`.get_data`` returns the definition's
    source, and :meth:`get_filename` reports the definition's repr, so
    ``__file__`` and tracebacks stay meaningful.

    Bytecode caching is left disabled (the inherited ``path_stats`` raises
    ``OSError``) since it's not particularly useful in dag processors.
    """

    def __init__(self, definition: DagDefinition) -> None:
        self._definition = definition

    def get_filename(self, fullname: str) -> str:
        return repr(self._definition)

    def get_data(self, path: str) -> bytes:
        # The machinery only asks for get_filename(), i.e. the module's own
        # source. Any other path is a sibling-resource request this bytes-backed
        # loader can't serve, so fail loud instead of returning the DAG source.
        if path != self.get_filename(path):
            raise FileNotFoundError(path)
        return self._definition.read_bytes()


class _DefinitionBytecodeLoader(importlib.abc.Loader):
    """
    Execute a definition's compiled bytecode (``.pyc``) straight from its bytes.

    The bytes-based counterpart of :class:`importlib.machinery.SourcelessFileLoader`
    (which is file-backed); reading through the definition keeps archive members from
    being extracted just to run them.
    """

    def __init__(self, definition: DagDefinition) -> None:
        self._definition = definition

    def get_filename(self, fullname: str) -> str:
        return repr(self._definition)

    def is_package(self, fullname: str) -> bool:
        # importlib's file loaders report an ``__init__`` module as a package. Without this a
        # sourceless Dag package loses ``__path__`` and its relative imports fail.
        return Path(self.get_filename(fullname)).stem == "__init__"

    def get_code(self, fullname: str) -> types.CodeType:
        data = self._definition.read_bytes()
        if len(data) < 16 or data[:4] != importlib.util.MAGIC_NUMBER:
            raise ImportError(f"Incompatible or corrupt bytecode for {self._definition!r}")
        if int.from_bytes(data[4:8], "little") & ~0b11:  # Reject undefined PEP 552 flag bits.
            raise ImportError(f"Invalid bytecode flags for {self._definition!r}")
        if not isinstance(code := marshal.loads(data[16:]), types.CodeType):
            raise ImportError(f"Bytecode for {self._definition!r} does not contain a code object")
        return code

    def exec_module(self, module: types.ModuleType) -> None:
        exec(self.get_code(module.__name__), module.__dict__)


class PythonDagImporter(AbstractDagImporter[FileDagDefinition]):
    """
    Importer for Python DAG sources.

    This is the default importer registered with the DagImporterRegistry for
    ``.py`` and ``.pyc`` files. The importer can import both from plain files
    from the local filesystem, or members inside a zip archive.
    """

    supported_extensions = [".py", ".pyc"]

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
    ) -> Iterator[FileDagDefinition | DagImportError]:
        """
        List Python DAG files in a bundle matching supported extensions.

        A lightweight content sniff (``might_contain_dag``) is applied here so files that
        clearly hold no DAG never become definitions -- keeping the discovered set (and the
        eventual parse-process count) close to the number of real DAG files. Zip members are
        discovered by :class:`..zip_importer.ZipImporter`, not here.
        """
        if not bundle.path.is_dir():
            return
        for definition in find_file_dag_definitions(bundle.path, self.supported_extensions):
            if self.might_contain_dag(definition, safe_mode):
                yield definition

    def import_definition(
        self,
        definition: FileDagDefinition,
        bundle: BaseDagBundle,
    ) -> DagImportResult:
        """Import DAGs from a Python DAG definition."""
        result = DagImportResult(definition=definition)
        DagContext.autoregistered_dags.clear()
        captured_warnings: list[warnings.WarningMessage] = []

        try:
            with warnings.catch_warnings(record=True) as captured_warnings:
                modules = self._load_modules(definition, result, bundle=bundle)
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

        self._process_modules(modules, result, bundle=bundle)
        return result

    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        """Retrieve the raw source code for the Python definition."""
        if get_file_suffix(definition) == ".pyc":
            return DagSourceCode(
                source_code="# Sourceless bytecode (.pyc) — source code not available\n",
                language="python",
            )
        return DagSourceCode(source_code=definition.read_text(encoding="utf-8"), language="python")

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        """Sniff a Python DAG source's bytes for the Airflow/DAG markers."""
        return might_contain_dag(definition, safe_mode=safe_mode, conf=conf)

    def _load_modules(
        self,
        definition: FileDagDefinition,
        result: DagImportResult,
        bundle: BaseDagBundle,
    ) -> list[types.ModuleType]:
        def _handle_sigsegv(signum, frame):
            msg = f"Received SIGSEGV signal while processing {definition!r}."
            log.error(msg)
            result.errors.append(
                DagImportError(source_reference=repr(definition), message=msg, error_type="segfault")
            )

        try:
            signal.signal(signal.SIGSEGV, _handle_sigsegv)
        except (ValueError, AttributeError):
            log.warning("SIGSEGV signal handler registration failed. Not in the main thread")

        log.debug("Importing %r (bundle: %s)", definition, bundle.name)
        mod_name = get_unique_dag_module_name(repr(definition))

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        DagContext.current_autoregister_module_name = mod_name

        def parse() -> list[types.ModuleType]:
            try:
                with definition.import_context():
                    loader: importlib.abc.Loader
                    if get_file_suffix(definition) == ".pyc":
                        loader = _DefinitionBytecodeLoader(definition)
                    else:
                        loader = _DefinitionSourceLoader(definition)
                    spec = importlib.util.spec_from_loader(mod_name, loader)
                    new_module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
                    sys.modules[mod_name] = new_module
                    loader.exec_module(new_module)
                    return [new_module]
            except KeyboardInterrupt:
                sys.modules.pop(mod_name, None)
                raise
            except BaseException as e:
                sys.modules.pop(mod_name, None)
                DagContext.autoregistered_dags.clear()
                log.exception("Failed to import: %r", definition)
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

            dagbag_import_timeout = settings.get_dagbag_import_timeout(repr(definition))
        except (ImportError, AttributeError):
            dagbag_import_timeout = 30.0

        if not isinstance(dagbag_import_timeout, (int, float)):
            raise AirflowConfigException(
                f"Value ({dagbag_import_timeout}) from get_dagbag_import_timeout must be int or float"
            )

        if dagbag_import_timeout <= 0:
            return parse()

        timeout_msg = (
            f"DagBag import timeout for {definition!r} after {dagbag_import_timeout}s.\n"
            "Please take a look at these docs to improve your DAG import time:\n"
            "* https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code\n"
            "* https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#reducing-dag-complexity"
        )
        with timeout(seconds=dagbag_import_timeout, error_message=timeout_msg):
            return parse()

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
