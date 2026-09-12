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

import functools
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow._shared.module_loading.file_discovery import find_path_from_directory
from airflow.configuration import conf
from airflow.utils.file import might_contain_dag

if TYPE_CHECKING:
    from airflow.sdk import DAG
    from airflow.typing_compat import Self

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
        return not self.errors


def _normalize_extensions(extensions: Iterable[str]) -> list[str]:
    """Normalize file extensions to lowercase with leading dot."""
    return [ext.lower() if ext.startswith(".") else f".{ext.lower()}" for ext in extensions]


class AbstractDagImporter(ABC):
    """Abstract base class for DAG importers."""

    def __init__(self, extensions: list[str] | None = None) -> None:
        self._configured_extensions = _normalize_extensions(extensions) if extensions is not None else None

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

    def get_supported_extensions(self) -> list[str]:
        """Return active supported extensions (configured extensions if set, else class defaults)."""
        if self._configured_extensions is not None:
            return self._configured_extensions
        if callable(self.supported_extensions):
            return self.supported_extensions()
        return self.supported_extensions

    def set_configured_extensions(self, extensions: list[str]) -> None:
        """Assign configured extensions to this importer instance."""
        self._configured_extensions = _normalize_extensions(extensions)

    def can_handle(self, file_path: str | Path) -> bool:
        """Check if this importer can handle the given file."""
        path = Path(file_path) if isinstance(file_path, str) else file_path
        return path.suffix.lower() in self.get_supported_extensions()

    def might_contain_dag(self, file_path: str | Path, safe_mode: bool = True) -> bool:
        """
        Check whether a file might contain Airflow DAGs according to safe mode heuristics.

        Custom importers can override this to implement format-specific heuristics.
        """
        if not safe_mode:
            return True
        return might_contain_dag(str(file_path), safe_mode)

    def list_dag_files(
        self,
        directory: str | os.PathLike[str],
        safe_mode: bool = True,
    ) -> Iterator[str]:
        """
        List DAG files in a directory that this importer can handle.

        :param directory: Directory to search for DAG files
        :param safe_mode: Whether to use heuristics to filter non-DAG files
        :return: Iterator of file paths
        """
        ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
        supported_exts = [ext.lower() for ext in self.get_supported_extensions()]

        for file_path in find_path_from_directory(directory, ".airflowignore", ignore_file_syntax):
            path = Path(file_path)

            if not path.is_file():
                continue
            if path.suffix.lower() not in supported_exts:
                continue
            if not self.might_contain_dag(file_path, safe_mode):
                continue

            yield file_path

    def get_relative_path(self, file_path: str | Path, bundle_path: Path | None) -> str:
        """Get the relative file path from the bundle root."""
        if bundle_path is None:
            return str(file_path)
        try:
            return str(Path(file_path).relative_to(bundle_path))
        except ValueError:
            return str(file_path)


@dataclass(frozen=True)
class _ImporterSpec:
    """Declarative specification for a DAG importer."""

    classpath: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    extensions: list[str] | None = None
    context: str = "importer configuration"


def _parse_importer_specs(configs: Any, context: str) -> list[_ImporterSpec]:
    from airflow.exceptions import AirflowConfigException

    if not isinstance(configs, list):
        raise AirflowConfigException(
            f"Invalid importer configuration for {context}: expected a list of dictionaries."
        )
    specs: list[_ImporterSpec] = []
    for item in configs:
        if not isinstance(item, dict):
            raise AirflowConfigException(
                f"Invalid importer configuration for {context}: each entry must be a dictionary."
            )
        classpath = item.get("classpath")
        if not classpath:
            raise AirflowConfigException(
                f"Missing required 'classpath' in importer configuration for {context}."
            )
        kwargs = item.get("kwargs", {})
        if not isinstance(kwargs, dict):
            raise AirflowConfigException(
                f"Field 'kwargs' must be a dictionary in importer configuration for {context}."
            )
        extensions = item.get("extensions")
        if extensions is not None:
            if not isinstance(extensions, list) or any(not isinstance(ext, str) for ext in extensions):
                raise AirflowConfigException(
                    f"Field 'extensions' must be a list of strings in importer configuration for {context}."
                )
            extensions = _normalize_extensions(extensions)
        specs.append(
            _ImporterSpec(
                classpath=classpath,
                kwargs=kwargs,
                extensions=extensions,
                context=context,
            )
        )
    return specs


class DagImporterRegistry:
    """
    Registry for DAG importers that manages importers by file extension.

    Each file extension can only be handled by one importer at a time. If multiple
    importers claim the same extension, the last registered one wins and a warning
    is logged. The built-in PythonDagImporter handles .py and .zip extensions.
    """

    _importers: dict[str, AbstractDagImporter]
    _importer_specs: dict[str, _ImporterSpec]

    def __init__(self, register_defaults: bool = True):
        self._importers = {}
        self._importer_specs = {}
        if register_defaults:
            self._register_default_importers()

    def _register_default_importers(self) -> None:
        from airflow.dag_processing.importers.python_importer import PythonDagImporter

        self.register(importer=PythonDagImporter())

    @staticmethod
    def _instantiate_spec(spec: _ImporterSpec) -> AbstractDagImporter:
        from airflow._shared.module_loading import import_string
        from airflow.exceptions import AirflowConfigException

        try:
            importer_class = import_string(spec.classpath)
            importer = importer_class(**spec.kwargs)
        except Exception as err:
            raise AirflowConfigException(
                f"Failed to load DAG importer '{spec.classpath}' for {spec.context}: {err}"
            ) from err

        if not isinstance(importer, AbstractDagImporter):
            raise AirflowConfigException(
                f"Configured DAG importer {type(importer).__module__}."
                f"{type(importer).__qualname__} for {spec.context} must inherit "
                "from AbstractDagImporter."
            )

        if spec.extensions is not None:
            importer.set_configured_extensions(spec.extensions)
        return importer

    def _warn_and_evict_extension(self, ext: str, new_name: str) -> None:
        if ext in self._importers or ext in self._importer_specs:
            existing = self._importers.get(ext)
            existing_name = type(existing).__name__ if existing else self._importer_specs[ext].classpath
            log.warning(
                "Extension '%s' already registered by %s, overriding with %s",
                ext,
                existing_name,
                new_name,
            )
            self._importers.pop(ext, None)
            self._importer_specs.pop(ext, None)

    def register(self, importer: AbstractDagImporter, extensions: list[str] | None = None) -> None:
        """
        Register an importer for its supported extensions.

        Each extension can only have one importer. If an extension is already registered,
        the new importer will override it and a warning will be logged.
        """
        if extensions is None:
            extensions = importer.get_supported_extensions()
        normalized_extensions = _normalize_extensions(extensions)
        importer.set_configured_extensions(normalized_extensions)

        for ext_lower in normalized_extensions:
            self._warn_and_evict_extension(ext_lower, type(importer).__name__)
            self._importers[ext_lower] = importer

    def register_specs(self, configs: list[dict[str, Any]], context: str) -> None:
        """Register importer specifications from configuration dictionaries."""
        for spec in _parse_importer_specs(configs, context=context):
            if spec.extensions is None:
                self.register(self._instantiate_spec(spec))
                continue

            for ext_lower in spec.extensions:
                self._warn_and_evict_extension(ext_lower, spec.classpath)
                self._importer_specs[ext_lower] = spec

    @classmethod
    def from_config(cls, bundle_name: str | None = None) -> Self:
        """Create and configure a DagImporterRegistry with 3-tier precedence."""
        from airflow.exceptions import AirflowConfigException

        registry = cls(register_defaults=True)

        global_importers = conf.getjson("dag_processor", "dag_importer_configs", fallback=None)
        if global_importers:
            if not isinstance(global_importers, list):
                raise AirflowConfigException(
                    "Section `dag_processor` key `dag_importer_configs` must be a list "
                    f"but got {global_importers.__class__.__name__}"
                )
            registry.register_specs(global_importers, context="global configuration")

        if bundle_name:
            bundle_importers = cls._get_bundle_importers_config(bundle_name)
            if bundle_importers:
                registry.register_specs(bundle_importers, context=f"bundle '{bundle_name}'")

        return registry

    @staticmethod
    def _get_bundle_importers_config(bundle_name: str) -> list[dict[str, Any]] | None:
        """Retrieve importer configs for a specific bundle from the bundle config snapshot."""
        from airflow.dag_processing.bundles.manager import load_bundle_config_snapshot

        for cfg in load_bundle_config_snapshot().configs:
            if cfg.name == bundle_name:
                return cfg.importers
        return None

    def get_importer(self, file_path: str | Path) -> AbstractDagImporter | None:
        """Get the appropriate importer for a file, or None if unsupported."""
        ext = (Path(file_path) if isinstance(file_path, str) else file_path).suffix.lower()

        if ext in self._importers:
            return self._importers[ext]

        if ext in self._importer_specs:
            spec = self._importer_specs[ext]
            importer = self._instantiate_spec(spec)
            for e, s in list(self._importer_specs.items()):
                if s is spec:
                    self._importers[e] = importer
                    del self._importer_specs[e]
            return importer

        return None

    def can_handle(self, file_path: str | Path) -> bool:
        """Check if any registered importer can handle this file."""
        ext = (Path(file_path) if isinstance(file_path, str) else file_path).suffix.lower()
        return ext in self._importers or ext in self._importer_specs

    def supported_extensions(self) -> list[str]:
        """Return all registered file extensions."""
        return sorted(set(self._importers) | set(self._importer_specs))

    def list_dag_files(
        self,
        directory: str | os.PathLike[str],
        safe_mode: bool = True,
    ) -> list[str]:
        """
        List all DAG files in a directory using all registered importers.

        Performs a single filesystem traversal, matching files against registered
        importers by extension and delegating safe-mode validation to the importer.

        :param directory: Directory (or file) to search for DAG files
        :param safe_mode: Whether to use heuristics to filter non-DAG files
        :return: List of file paths (deduplicated)
        """
        path = Path(directory)
        if path.is_file():
            importer = self.get_importer(path)
            if importer and importer.might_contain_dag(path, safe_mode):
                return [str(path)]
            return []

        if not path.is_dir():
            return []

        ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
        file_paths: list[str] = []

        for file_path in find_path_from_directory(directory, ".airflowignore", ignore_file_syntax):
            p = Path(file_path)
            try:
                if not p.is_file():
                    continue

                importer = self.get_importer(p)
                if importer is None:
                    continue

                if importer.might_contain_dag(file_path, safe_mode):
                    file_paths.append(file_path)
            except Exception:
                log.exception("Error while examining %s", file_path)

        return file_paths

    @classmethod
    def reset(cls) -> None:
        """Reset the cached importer registries (for testing)."""
        reset_importer_registry()


@functools.cache
def get_importer_registry(bundle_name: str | None = None) -> DagImporterRegistry:
    """Get the cached DagImporterRegistry instance for global or bundle scope."""
    return DagImporterRegistry.from_config(bundle_name=bundle_name)


def reset_importer_registry() -> None:
    """Reset cached importer registries."""
    get_importer_registry.cache_clear()
    try:
        from airflow.dag_processing.bundles.manager import load_bundle_config_snapshot

        load_bundle_config_snapshot.cache_clear()  # type: ignore[attr-defined]
    except (ImportError, AttributeError):
        pass
