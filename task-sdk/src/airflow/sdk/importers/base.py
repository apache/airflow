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
"""Abstract base class for DAG importers."""

from __future__ import annotations

import contextlib
import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.sdk._shared.module_loading.file_discovery import find_path_from_directory

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.base import BaseDagBundle
    from airflow.sdk import DAG

log = logging.getLogger(__name__)


class DagDefinition(ABC):
    """Abstract base class for a DAG source definition."""

    @property
    @abstractmethod
    def freshness_token(self) -> str:
        """Opaque, generalized token representing the current state of the source."""

    @abstractmethod
    def get_relative_loc(self, root: Path | None = None) -> str:
        """Get relative location of the definition to a root directory."""

    @abstractmethod
    def read_bytes(self) -> bytes:
        """Read and return the content of the resource as bytes."""

    def read_text(self, encoding: str = "utf-8") -> str:
        """Read and return the content of the resource as a string."""
        return self.read_bytes().decode(encoding)

    @abstractmethod
    def as_file(self) -> contextlib.AbstractContextManager[Path]:
        """
        Return a context manager yielding a Path pointing to a local file.

        For file-backed resources, this is the actual file path.
        For others, a temp file is created and cleaned up.
        """

    @abstractmethod
    def __repr__(self) -> str:
        """Return string representation used by import error and warning objects."""


@dataclass
class FileDagDefinition(DagDefinition):
    """A DAG definition backed by a file on the local filesystem."""

    path: Path

    @property
    def freshness_token(self) -> str:
        try:
            stat = self.path.stat()
            return f"{stat.st_mtime_ns}-{stat.st_size}"
        except OSError:
            return ""

    def get_relative_loc(self, root: Path | None = None) -> str:
        if root is None:
            return str(self.path)
        try:
            return str(self.path.relative_to(root))
        except ValueError:
            return str(self.path)

    def read_bytes(self) -> bytes:
        return self.path.read_bytes()

    @contextlib.contextmanager
    def as_file(self) -> Iterator[Path]:
        yield self.path

    def __repr__(self) -> str:
        return str(self.path)


@dataclass
class DagImportError:
    """Structured error information for DAG import failures."""

    source_reference: str
    message: str
    error_type: str = "import"
    line_number: int | None = None
    column_number: int | None = None
    context: str | None = None
    suggestion: str | None = None
    stacktrace: str | None = None

    def format_message(self) -> str:
        """Format the error as a human-readable string."""
        parts = [f"Error in {self.source_reference}"]
        if self.line_number is not None:
            loc = f"line {self.line_number}"
            if self.column_number is not None:
                loc += f", column {self.column_number}"
            parts.append(f"Location: {loc}")
        parts.append(f"Error ({self.error_type}): {self.message}")
        if self.context:
            parts.append(f"Context:\n{self.context}")
        if self.suggestion:
            parts.append(f"Suggestion: {self.suggestion}")
        return "\n".join(parts)


@dataclass
class DagImportWarning:
    """Warning information for non-fatal issues during DAG import."""

    source_reference: str
    message: str
    warning_type: str = "import"
    line_number: int | None = None
    context: dict[str, Any] | None = None


@dataclass
class DagImportResult:
    """Result of importing DAGs from a definition."""

    definition: DagDefinition | None = None
    dags: list[DAG] = field(default_factory=list)
    errors: list[DagImportError] = field(default_factory=list)
    skipped_definitions: list[DagDefinition] = field(default_factory=list)
    warnings: list[DagImportWarning] = field(default_factory=list)
    dependencies: list[DagDefinition] = field(default_factory=list)


@dataclass
class DagSourceCode:
    """Raw source code and its language identifier for a DAG definition."""

    source_code: str
    language: str


class AbstractDagImporter(ABC):
    """Abstract base class for DAG importers."""

    @property
    @abstractmethod
    def supported_extensions(self) -> list[str]:
        """Return file extensions this importer handles (e.g., ['.py', '.zip'])."""

    @abstractmethod
    def import_definition(
        self,
        definition: DagDefinition,
        bundle: BaseDagBundle,
        *,
        safe_mode: bool = True,
    ) -> DagImportResult:
        """Import DAGs from a DAG definition."""

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        """Check if this importer can handle the given definition."""
        path = (
            definition
            if isinstance(definition, (str, Path))
            else getattr(definition, "path", getattr(definition, "file_path", None))
        )
        return Path(path).suffix.lower() in self.supported_extensions if path else False

    def list_dag_definitions(
        self,
        bundle: BaseDagBundle,
        safe_mode: bool = True,
    ) -> Iterator[DagDefinition]:
        """
        List DAG definitions in a bundle that this importer can handle.

        Override this method to customize definition discovery for your importer.
        The default implementation finds files matching supported_extensions
        and respects .airflowignore files.
        """
        try:
            from airflow.configuration import conf

            ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
        except ImportError:
            ignore_file_syntax = "glob"

        supported_exts = [ext.lower() for ext in self.supported_extensions]

        for file_path in find_path_from_directory(bundle.path, ".airflowignore", ignore_file_syntax):
            path = Path(file_path)

            if not path.is_file():
                continue

            # Check if this importer handles this file extension
            if path.suffix.lower() not in supported_exts:
                continue

            yield FileDagDefinition(path=path)

    @abstractmethod
    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        """Retrieve the raw source code and its language identifier for the specified DAG definition."""


class DagImporterRegistry:
    """
    Registry for DAG importers. Singleton that manages importers by file extension.

    Each file extension can only be handled by one importer at a time. If multiple
    importers claim the same extension, the last registered one wins and a warning
    is logged. The built-in PythonDagImporter handles .py and ZipImporter handles .zip files.
    """

    _importers: dict[str, AbstractDagImporter]

    def __init__(self, register_defaults: bool = True):
        self._importers = {}
        if register_defaults:
            self._register_default_importers()

    def _register_default_importers(self) -> None:
        from airflow.sdk.importers.python_importer import PythonDagImporter
        from airflow.sdk.importers.zip_importer import ZipImporter

        self.register(PythonDagImporter())
        self.register(ZipImporter())

    def register(self, importer: AbstractDagImporter) -> None:
        """
        Register an importer for its supported extensions.

        Each extension can only have one importer. If an extension is already registered,
        the new importer will override it and a warning will be logged.
        """
        for ext in importer.supported_extensions:
            ext_lower = ext.lower()
            if ext_lower in self._importers:
                existing = self._importers[ext_lower]
                log.warning(
                    "Extension '%s' already registered by %s, overriding with %s",
                    ext,
                    type(existing).__name__,
                    type(importer).__name__,
                )
            self._importers[ext_lower] = importer

    def _get_suffix(self, definition: DagDefinition | str | Path) -> str | None:
        path = (
            definition
            if isinstance(definition, (str, Path))
            else getattr(definition, "path", getattr(definition, "file_path", None))
        )
        return Path(path).suffix.lower() if path else None

    def get_importer(self, definition: DagDefinition | str | Path) -> AbstractDagImporter | None:
        """Get the appropriate importer for a definition or file, or None if unsupported."""
        suffix = self._get_suffix(definition)
        if suffix and suffix in self._importers:
            return self._importers[suffix]

        for importer in set(self._importers.values()):
            if importer.can_handle(definition):
                return importer
        return None

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        """Check if any registered importer can handle this definition/file."""
        return self.get_importer(definition) is not None

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (for testing)."""
        global _global_registry
        with _global_registry_lock:
            _global_registry = None


_global_registry: DagImporterRegistry | None = None
_global_registry_lock = threading.Lock()


def get_importer_registry() -> DagImporterRegistry:
    """Get the global importer registry instance."""
    global _global_registry
    with _global_registry_lock:
        if _global_registry is None:
            _global_registry = DagImporterRegistry()
        return _global_registry
