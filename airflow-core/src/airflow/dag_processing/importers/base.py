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

import logging
import os
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from airflow._shared.module_loading.file_discovery import find_path_from_directory
from airflow.configuration import conf
from airflow.utils.file import might_contain_dag

if TYPE_CHECKING:
    from airflow.sdk import DAG

log = logging.getLogger(__name__)


@dataclass
class DagImportError:
    """Structured error information for DAG import failures."""

    file_path: str
    message: str
    error_type: str = "import"
    line_number: int | None = None
    column_number: int | None = None
    context: str | None = None
    suggestion: str | None = None
    stacktrace: str | None = None

    def format_message(self) -> str:
        """Format the error as a human-readable string."""
        parts = [f"Error in {self.file_path}"]
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

    file_path: str
    message: str
    warning_type: str = "general"
    line_number: int | None = None


@dataclass
class DagImportResult:
    """Result of importing DAGs from a file."""

    file_path: str
    dags: list[DAG] = field(default_factory=list)
    errors: list[DagImportError] = field(default_factory=list)
    skipped_files: list[str] = field(default_factory=list)
    warnings: list[DagImportWarning] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Return True if no fatal errors occurred."""
        return len(self.errors) == 0


class AbstractDagImporter(ABC):
    """Abstract base class for DAG importers."""

    @classmethod
    @abstractmethod
    def supported_extensions(cls) -> list[str]:
        """Return file extensions this importer handles (e.g., ['.py', '.zip'])."""

    @abstractmethod
    def import_file(
        self,
        file_path: str | Path,
        *,
        bundle_path: Path | None = None,
        bundle_name: str | None = None,
        safe_mode: bool = True,
    ) -> DagImportResult:
        """Import DAGs from a file."""

    def can_handle(self, file_path: str | Path) -> bool:
        """Check if this importer can handle the given file."""
        path = Path(file_path) if isinstance(file_path, str) else file_path
        return path.suffix.lower() in self.supported_extensions()

    def get_relative_path(self, file_path: str | Path, bundle_path: Path | None) -> str:
        """Get the relative file path from the bundle root."""
        if bundle_path is None:
            return str(file_path)
        try:
            return str(Path(file_path).relative_to(bundle_path))
        except ValueError:
            return str(file_path)

    def list_dag_files(
        self,
        directory: str | os.PathLike[str],
        safe_mode: bool = True,
    ) -> Iterator[str]:
        """
        List DAG files in a directory that this importer can handle.

        Override this method to customize file discovery for your importer.
        The default implementation finds files matching supported_extensions()
        and respects .airflowignore files.

        :param directory: Directory to search for DAG files
        :param safe_mode: Whether to use heuristics to filter non-DAG files
        :return: Iterator of file paths
        """
        ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
        supported_exts = [ext.lower() for ext in self.supported_extensions()]

        for file_path in find_path_from_directory(directory, ".airflowignore", ignore_file_syntax):
            path = Path(file_path)

            if not path.is_file():
                continue

            # Check if this importer handles this file extension
            if path.suffix.lower() not in supported_exts:
                continue

            # Apply safe_mode heuristic if enabled
            if safe_mode and not might_contain_dag(file_path, safe_mode):
                continue

            yield file_path


class DagImporterRegistry:
    """
    Registry for DAG importers. Singleton that manages importers by file extension.

    Each file extension can only be handled by one importer at a time. If multiple
    importers claim the same extension, the last registered one wins and a warning
    is logged. The built-in PythonDagImporter handles .py and .zip extensions.
    """

    _instance: DagImporterRegistry | None = None
    _importers: dict[str, AbstractDagImporter]
    _lock = threading.Lock()

    def __new__(cls) -> DagImporterRegistry:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._importers = {}
                cls._instance._register_default_importers()
        return cls._instance

    def _register_default_importers(self) -> None:
        from airflow.dag_processing.importers.python_importer import PythonDagImporter

        self.register(PythonDagImporter())

    def register(self, importer: AbstractDagImporter) -> None:
        """
        Register an importer for its supported extensions.

        Each extension can only have one importer. If an extension is already registered,
        the new importer will override it and a warning will be logged.
        """
        for ext in importer.supported_extensions():
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

    def get_importer(self, file_path: str | Path) -> AbstractDagImporter | None:
        """Get the appropriate importer for a file, or None if unsupported."""
        path = Path(file_path) if isinstance(file_path, str) else file_path
        return self._importers.get(path.suffix.lower())

    def can_handle(self, file_path: str | Path) -> bool:
        """Check if any registered importer can handle this file."""
        return self.get_importer(file_path) is not None

    def supported_extensions(self) -> list[str]:
        """Return all registered file extensions."""
        return list(self._importers.keys())

    def list_dag_files(
        self,
        directory: str | os.PathLike[str],
        safe_mode: bool = True,
    ) -> list[str]:
        """
        List all DAG files in a directory using all registered importers.

        If directory is actually a file, returns that file if any importer can handle it.

        :param directory: Directory (or file) to search for DAG files
        :param safe_mode: Whether to use heuristics to filter non-DAG files
        :return: List of file paths (deduplicated)
        """
        path = Path(directory)

        # If it's a file, just return it if we can handle it
        if path.is_file():
            if self.can_handle(path):
                return [str(path)]
            return []

        if not path.is_dir():
            return []

        seen_files: set[str] = set()
        file_paths: list[str] = []

        for importer in set(self._importers.values()):
            for file_path in importer.list_dag_files(directory, safe_mode):
                if file_path not in seen_files:
                    seen_files.add(file_path)
                    file_paths.append(file_path)

        return file_paths

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (for testing)."""
        cls._instance = None


def get_importer_registry() -> DagImporterRegistry:
    """Get the global importer registry instance."""
    return DagImporterRegistry()
