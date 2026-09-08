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
"""Zip archive DAG importer."""

from __future__ import annotations

import contextlib
import logging
import sys
import tempfile
import threading
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.importers.base import (
    AbstractDagImporter,
    DagDefinition,
    DagImporterRegistry,
    DagImportError,
    DagImportResult,
    DagSourceCode,
    _get_importer_extensions,
    _normalize_extensions,
    _parse_importer_specs,
    find_file_dag_definitions,
    get_file_suffix,
)

log = logging.getLogger(__name__)

_sys_path_lock = threading.Lock()


@contextlib.contextmanager
def _temporary_sys_path(path: str) -> Iterator[None]:
    """Safely prepend a path to sys.path with synchronization and restoration."""
    with _sys_path_lock:
        already_present = path in sys.path
        if not already_present:
            sys.path.insert(0, path)
        try:
            yield
        finally:
            if not already_present:
                with contextlib.suppress(ValueError):
                    sys.path.remove(path)


@dataclass
class ZipFileDagDefinition(DagDefinition):
    """A DAG definition backed by a file inside a ZIP archive."""

    zip_path: Path
    file_path: str
    _content: bytes | None = field(default=None, repr=False, compare=False)
    _temp_path: Path | None = field(default=None, repr=False, compare=False)

    @property
    def freshness_token(self) -> str:
        try:
            stat = self.zip_path.stat()
            return f"{stat.st_mtime_ns}-{stat.st_size}-{self.file_path}"
        except OSError:
            return ""

    def get_relative_loc(self, root: Path | None = None) -> str:
        if root is None:
            return f"{self.zip_path}:{self.file_path}"
        try:
            rel_zip = self.zip_path.relative_to(root)
            return f"{rel_zip}:{self.file_path}"
        except ValueError:
            return f"{self.zip_path}:{self.file_path}"

    def read_bytes(self) -> bytes:
        if self._content is None:
            if self._temp_path is not None and self._temp_path.exists():
                self._content = self._temp_path.read_bytes()
            else:
                with zipfile.ZipFile(self.zip_path) as z:
                    self._content = z.read(self.file_path)
        return self._content

    @contextlib.contextmanager
    def as_file(self) -> Iterator[Path]:
        if self._temp_path is not None and self._temp_path.exists():
            yield self._temp_path
            return

        suffix = Path(self.file_path).suffix
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            f.write(self.read_bytes())
            temp_path = Path(f.name)
        try:
            yield temp_path
        finally:
            with contextlib.suppress(OSError):
                temp_path.unlink()

    def __repr__(self) -> str:
        return f"{self.zip_path}:{self.file_path}"


class ZipImporter(AbstractDagImporter):
    """Composite importer responsible for routing archive members to internal importers."""

    supported_extensions = [".zip"]

    def __init__(
        self,
        internal_importers: dict[str, AbstractDagImporter | dict[str, Any]]
        | list[dict[str, Any]]
        | None = None,
        extensions: list[str] | None = None,
    ) -> None:
        if extensions is not None:
            self.supported_extensions = _normalize_extensions(extensions)
        self._internal_extension_importers: dict[str, AbstractDagImporter] = {}
        self._ordered_internal_importers: list[AbstractDagImporter] = []

        if internal_importers is None:
            from airflow.sdk.importers.python_importer import PythonDagImporter

            self._register_internal(PythonDagImporter())
        elif isinstance(internal_importers, list):
            specs = _parse_importer_specs(internal_importers, context="internal_importers of ZipImporter")
            for spec in specs:
                importer = DagImporterRegistry._instantiate_spec(spec)
                self._register_internal(importer, extensions=spec.extensions)
        elif isinstance(internal_importers, dict):
            for ext, cfg in internal_importers.items():
                if isinstance(cfg, AbstractDagImporter):
                    self._register_internal(cfg, extensions=[ext])
                elif isinstance(cfg, dict):
                    specs = _parse_importer_specs(
                        [cfg], context=f"internal_importers configuration for extension '{ext}'"
                    )
                    importer = DagImporterRegistry._instantiate_spec(specs[0])
                    self._register_internal(importer, extensions=specs[0].extensions or [ext])
                else:
                    raise AirflowConfigException(
                        f"Invalid internal importer configuration for extension '{ext}': "
                        f"expected AbstractDagImporter or dictionary, got {type(cfg).__name__}."
                    )
        else:
            raise AirflowConfigException(
                f"Field 'internal_importers' must be a list or dictionary, got {type(internal_importers).__name__}."
            )

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        """Check if this importer can handle the given definition based on file extension."""
        suffix = get_file_suffix(definition)
        return suffix in self.supported_extensions if suffix else False

    def import_definition(
        self,
        definition: DagDefinition,
        *,
        bundle_path: Path | None = None,
        bundle_name: str | None = None,
        safe_mode: bool = True,
    ) -> DagImportResult:
        """Import DAGs from a ZIP archive DAG definition by routing internal files."""
        result = DagImportResult(definition=definition)

        with definition.as_file() as local_zip_path:
            with tempfile.TemporaryDirectory(prefix="airflow_zip_") as temp_dir:
                temp_dir_path = Path(temp_dir)
                extracted_members: list[tuple[str, Path, AbstractDagImporter]] = []

                try:
                    with zipfile.ZipFile(local_zip_path) as z:
                        for member_name in z.namelist():
                            if member_name.startswith("__MACOSX/") or member_name.endswith("/"):
                                continue
                            # ZipSlip prevention: check for directory traversal attempts
                            target_path = (temp_dir_path / member_name).resolve()
                            if not target_path.is_relative_to(temp_dir_path.resolve()):
                                log.warning(
                                    "Skipping zip member %r in %s: directory traversal patterns detected",
                                    member_name,
                                    local_zip_path,
                                )
                                continue
                            importer = self._get_internal_importer(member_name)
                            if importer is not None:
                                extracted_path = Path(z.extract(member_name, path=temp_dir_path))
                                extracted_members.append((member_name, extracted_path, importer))
                except Exception as e:
                    result.errors.append(
                        DagImportError(
                            source_reference=definition.get_relative_loc(bundle_path),
                            message=f"Failed to read ZIP archive: {e}",
                            error_type="zip_read_error",
                        )
                    )
                    return result

                with _temporary_sys_path(str(temp_dir_path)):
                    for member_name, extracted_file, importer in extracted_members:
                        nested_def = ZipFileDagDefinition(
                            zip_path=local_zip_path,
                            file_path=member_name,
                            _temp_path=extracted_file,
                        )
                        if not importer.can_handle(nested_def):
                            continue

                        member_result = importer.import_definition(
                            definition=nested_def,
                            bundle_path=bundle_path,
                            bundle_name=bundle_name,
                            safe_mode=safe_mode,
                        )

                        result.dags.extend(member_result.dags)
                        result.errors.extend(member_result.errors)
                        result.warnings.extend(member_result.warnings)
                        result.skipped_definitions.extend(member_result.skipped_definitions)
                        result.dependencies.extend(member_result.dependencies)

        return result

    def list_dag_definitions(
        self,
        bundle_name: str,
        bundle_path: Path,
        *,
        safe_mode: bool = True,
    ) -> Iterator[DagDefinition]:
        """List zip archive DAG definitions in a bundle matching supported extensions."""
        yield from find_file_dag_definitions(bundle_path, self.supported_extensions)

    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        if isinstance(definition, ZipFileDagDefinition):
            importer = self._get_internal_importer(definition.file_path)
            if importer is not None:
                return importer.get_source_code(definition)
            raise ValueError(f"No internal importer registered for zip member {definition.file_path}")

        # If definition is the zip archive itself, route to code member(s)
        with definition.as_file() as local_zip_path:
            with zipfile.ZipFile(local_zip_path) as z:
                candidates = [
                    name
                    for name in z.namelist()
                    if self._get_internal_importer(name) is not None
                    and not name.startswith("__MACOSX")
                    and ".." not in Path(name).parts
                    and not Path(name).is_absolute()
                ]
            if not candidates:
                raise ValueError(f"No code files found inside ZIP archive {definition}")
            if len(candidates) == 1:
                nested_def = ZipFileDagDefinition(zip_path=local_zip_path, file_path=candidates[0])
                importer = self._get_internal_importer(candidates[0])
                if importer is not None:
                    return importer.get_source_code(nested_def)
                raise ValueError(f"No internal importer registered for zip member {candidates[0]}")

            parts = []
            primary_language = "text"
            for name in candidates:
                nested_def = ZipFileDagDefinition(zip_path=local_zip_path, file_path=name)
                importer = self._get_internal_importer(name)
                if importer is not None:
                    res = importer.get_source_code(nested_def)
                    primary_language = res.language
                    parts.append(f"# --- {name} ---\n{res.source_code}")
            return DagSourceCode(source_code="\n\n".join(parts), language=primary_language)

    def _register_internal(self, importer: AbstractDagImporter, extensions: list[str] | None = None) -> None:
        if importer not in self._ordered_internal_importers:
            self._ordered_internal_importers.append(importer)
        exts = extensions if extensions is not None else _get_importer_extensions(importer)
        if exts:
            normalized = _normalize_extensions(exts)
            if hasattr(importer, "supported_extensions"):
                with contextlib.suppress(AttributeError, TypeError):
                    importer.supported_extensions = normalized
            for ext in normalized:
                self._internal_extension_importers[ext] = importer

    def _get_internal_importer(self, member_name: str) -> AbstractDagImporter | None:
        suffix = get_file_suffix(member_name)
        if suffix and suffix in self._internal_extension_importers:
            return self._internal_extension_importers[suffix]
        for importer in reversed(self._ordered_internal_importers):
            if importer.can_handle(member_name):
                return importer
        return None
