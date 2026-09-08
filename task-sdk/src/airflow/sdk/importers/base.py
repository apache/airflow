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
import functools
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.sdk._shared.module_loading.file_discovery import find_path_from_directory
from airflow.sdk.configuration import conf
from airflow.sdk.exceptions import AirflowConfigException

if TYPE_CHECKING:
    from typing_extensions import Self

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

    @property
    def success(self) -> bool:
        """Return True if no fatal errors occurred."""
        return not self.errors


@dataclass
class DagSourceCode:
    """Raw source code and its language identifier for a DAG definition."""

    source_code: str
    language: str


def _normalize_extensions(extensions: Iterable[str]) -> list[str]:
    """Normalize file extensions to lowercase with leading dot."""
    return [ext.lower() if ext.startswith(".") else f".{ext.lower()}" for ext in extensions]


def _get_importer_extensions(importer: Any) -> list[str]:
    """Extract supported extensions from an importer via duck typing."""
    exts = getattr(importer, "supported_extensions", None)
    if callable(exts):
        return _normalize_extensions(exts())
    if exts is not None:
        return _normalize_extensions(exts)
    return []


class AbstractDagImporter(ABC):
    """Abstract base class for DAG importers."""

    @abstractmethod
    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        """Check if this importer can handle the given definition."""

    @abstractmethod
    def import_definition(
        self,
        definition: DagDefinition,
        *,
        bundle_path: Path | None = None,
        bundle_name: str | None = None,
        safe_mode: bool = True,
    ) -> DagImportResult:
        """Import DAGs from a DAG definition."""

    @abstractmethod
    def list_dag_definitions(
        self,
        bundle_name: str,
        bundle_path: Path,
        *,
        safe_mode: bool = True,
    ) -> Iterator[DagDefinition]:
        """List DAG definitions in a bundle that this importer can handle."""

    @abstractmethod
    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        """Retrieve the raw source code and its language identifier for the specified DAG definition."""


def get_file_suffix(definition: DagDefinition | str | Path) -> str | None:
    """Extract lowercase file suffix from a definition, path, or filename."""
    path = (
        definition
        if isinstance(definition, (str, Path))
        else getattr(definition, "path", getattr(definition, "file_path", None))
    )
    return Path(path).suffix.lower() if path else None


def find_file_dag_definitions(
    bundle_path: Path,
    supported_extensions: Iterable[str],
) -> Iterator[DagDefinition]:
    """Find file DAG definitions in a bundle matching given extensions and respecting .airflowignore."""
    ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
    supported_exts = _normalize_extensions(supported_extensions)

    for file_path in find_path_from_directory(bundle_path, ".airflowignore", ignore_file_syntax):
        path = Path(file_path)

        if not path.is_file():
            continue

        if path.suffix.lower() not in supported_exts:
            continue

        yield FileDagDefinition(path=path)


@dataclass(frozen=True)
class _ImporterSpec:
    """Declarative specification for a DAG importer."""

    classpath: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    extensions: list[str] | None = None
    context: str = "importer configuration"


def _parse_importer_specs(configs: Any, context: str) -> list[_ImporterSpec]:
    from airflow.sdk.exceptions import AirflowConfigException

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
    Registry for DAG importers. Manages importers by file extension and generic definition.

    Each file extension can only be handled by one importer at a time. If multiple
    importers claim the same extension, the last registered one wins and a warning
    is logged. The built-in PythonDagImporter handles .py and ZipImporter handles .zip files.
    """

    _extension_importers: dict[str, AbstractDagImporter]
    _extension_specs: dict[str, _ImporterSpec]
    _ordered_importers: list[AbstractDagImporter]

    def __init__(self, register_defaults: bool = True) -> None:
        self._extension_importers = {}
        self._extension_specs = {}
        self._ordered_importers = []
        if register_defaults:
            self._register_default_importers()

    @classmethod
    def from_config(cls, bundle_name: str | None = None) -> Self:
        """Create and configure a DagImporterRegistry with 3-tier precedence."""
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

    def register(self, importer: AbstractDagImporter, extensions: list[str] | None = None) -> None:
        """
        Register an importer.

        Each extension can only have one importer. If an extension is already registered,
        the new importer will override it and a warning will be logged.
        """
        if importer not in self._ordered_importers:
            self._ordered_importers.append(importer)

        if extensions is None:
            extensions = _get_importer_extensions(importer)

        if extensions:
            normalized_extensions = _normalize_extensions(extensions)
            if hasattr(importer, "supported_extensions"):
                with contextlib.suppress(AttributeError, TypeError):
                    importer.supported_extensions = normalized_extensions
            for ext_lower in normalized_extensions:
                self._warn_and_evict_extension(ext_lower, type(importer).__name__)
                self._extension_importers[ext_lower] = importer

    def register_specs(self, configs: list[dict[str, Any]], context: str) -> None:
        """Register importer specifications from configuration dictionaries."""
        for spec in _parse_importer_specs(configs, context=context):
            if spec.extensions is None:
                self.register(self._instantiate_spec(spec))
                continue

            for ext_lower in spec.extensions:
                self._warn_and_evict_extension(ext_lower, spec.classpath)
                self._extension_specs[ext_lower] = spec

    def get_importer(self, definition: DagDefinition | str | Path) -> AbstractDagImporter | None:
        """Get the appropriate importer for a definition or file, or None if unsupported."""
        suffix = self._get_suffix(definition)
        if suffix:
            if suffix in self._extension_importers:
                return self._extension_importers[suffix]

            if suffix in self._extension_specs:
                spec = self._extension_specs[suffix]
                importer = self._instantiate_spec(spec)
                for e, s in list(self._extension_specs.items()):
                    if s is spec:
                        self._extension_importers[e] = importer
                        del self._extension_specs[e]
                if importer not in self._ordered_importers:
                    self._ordered_importers.append(importer)
                return importer

        for importer in reversed(self._ordered_importers):
            if importer.can_handle(definition):
                return importer
        return None

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        """Check if any registered importer can handle this definition/file."""
        suffix = self._get_suffix(definition)
        if suffix and (suffix in self._extension_importers or suffix in self._extension_specs):
            return True
        return any(importer.can_handle(definition) for importer in reversed(self._ordered_importers))

    def supported_extensions(self) -> list[str]:
        """Return all registered file extensions."""
        return sorted(set(self._extension_importers) | set(self._extension_specs))

    @classmethod
    def reset(cls) -> None:
        """Reset the cached importer registries (for testing)."""
        reset_importer_registry()

    def _register_default_importers(self) -> None:
        from airflow.sdk.importers.python_importer import PythonDagImporter
        from airflow.sdk.importers.zip_importer import ZipImporter

        self.register(PythonDagImporter())
        self.register(ZipImporter())

    @staticmethod
    def _instantiate_spec(spec: _ImporterSpec) -> AbstractDagImporter:
        from airflow.sdk._shared.module_loading import import_string
        from airflow.sdk.exceptions import AirflowConfigException

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

        if spec.extensions is not None and hasattr(importer, "supported_extensions"):
            with contextlib.suppress(AttributeError, TypeError):
                importer.supported_extensions = spec.extensions
        return importer

    def _warn_and_evict_extension(self, ext: str, new_name: str) -> None:
        if ext in self._extension_importers or ext in self._extension_specs:
            existing = self._extension_importers.get(ext)
            existing_name = type(existing).__name__ if existing else self._extension_specs[ext].classpath
            log.warning(
                "Extension '%s' already registered by %s, overriding with %s",
                ext,
                existing_name,
                new_name,
            )
            self._extension_importers.pop(ext, None)
            self._extension_specs.pop(ext, None)

    @staticmethod
    def _get_bundle_importers_config(bundle_name: str) -> list[dict[str, Any]] | None:
        """Retrieve importer configs for a specific bundle from configuration."""
        bundle_config_list = conf.getjson("dag_processor", "dag_bundle_config_list", fallback=None)
        if isinstance(bundle_config_list, list):
            for item in bundle_config_list:
                if isinstance(item, dict) and item.get("name") == bundle_name:
                    return item.get("importers")
        return None

    def _get_suffix(self, definition: DagDefinition | str | Path) -> str | None:
        return get_file_suffix(definition)


@functools.cache
def get_importer_registry(bundle_name: str | None = None) -> DagImporterRegistry:
    """Get the cached DagImporterRegistry instance for global or bundle scope."""
    return DagImporterRegistry.from_config(bundle_name=bundle_name)


def reset_importer_registry() -> None:
    """Reset cached importer registries."""
    get_importer_registry.cache_clear()
