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
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.base import BaseDagBundle

from airflow.sdk.importers.base import (
    AbstractDagImporter,
    DagDefinition,
    DagImportError,
    DagImportResult,
    DagSourceCode,
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

    def __init__(self, internal_importers: dict[str, Any] | None = None):
        super().__init__()
        self._internal_importers: dict[str, AbstractDagImporter] = {}
        if internal_importers is None:
            from airflow.sdk.importers.python_importer import PythonDagImporter

            self._internal_importers[".py"] = PythonDagImporter()
        else:
            from airflow.sdk._shared.module_loading import import_string

            for ext, cfg in internal_importers.items():
                ext_lower = ext if ext.startswith(".") else f".{ext}"
                if isinstance(cfg, AbstractDagImporter):
                    self._internal_importers[ext_lower.lower()] = cfg
                elif isinstance(cfg, dict) and "classpath" in cfg:
                    importer_class = import_string(cfg["classpath"])
                    self._internal_importers[ext_lower.lower()] = importer_class(**cfg.get("kwargs", {}))

    def import_definition(
        self,
        definition: DagDefinition,
        bundle: BaseDagBundle,
        *,
        safe_mode: bool = True,
    ) -> DagImportResult:
        """Import DAGs from a ZIP archive DAG definition by routing internal files."""
        result = DagImportResult(definition=definition)

        with definition.as_file() as local_zip_path:
            try:
                valid_members: dict[str, bytes] = {}
                with zipfile.ZipFile(local_zip_path) as z:
                    for member_name in z.namelist():
                        # ZipSlip prevention: check for directory traversal attempts
                        if ".." in Path(member_name).parts or Path(member_name).is_absolute():
                            log.warning(
                                "Skipping zip member %r in %s: directory traversal patterns detected",
                                member_name,
                                local_zip_path,
                            )
                            continue
                        suffix_lower = Path(member_name).suffix.lower()
                        if suffix_lower in self._internal_importers:
                            valid_members[member_name] = z.read(member_name)
            except Exception as e:
                result.errors.append(
                    DagImportError(
                        source_reference=definition.get_relative_loc(bundle.path),
                        message=f"Failed to read ZIP archive: {e}",
                        error_type="zip_read_error",
                    )
                )
                return result

            # Extract matching members into a single temp directory to avoid per-file churn
            with tempfile.TemporaryDirectory(prefix="airflow_zip_") as temp_dir:
                temp_dir_path = Path(temp_dir)
                for member_name, content in valid_members.items():
                    extracted_file = temp_dir_path / member_name
                    extracted_file.parent.mkdir(parents=True, exist_ok=True)
                    extracted_file.write_bytes(content)

                with _temporary_sys_path(str(temp_dir_path)):
                    for member_name, content in valid_members.items():
                        extracted_file = temp_dir_path / member_name
                        suffix_lower = extracted_file.suffix.lower()

                        nested_def = ZipFileDagDefinition(
                            zip_path=local_zip_path,
                            file_path=member_name,
                            _content=content,
                            _temp_path=extracted_file,
                        )

                        importer = self._internal_importers[suffix_lower]
                        if not importer.can_handle(nested_def):
                            continue

                        member_result = importer.import_definition(
                            definition=nested_def,
                            bundle=bundle,
                            safe_mode=safe_mode,
                        )

                        result.dags.extend(member_result.dags)
                        result.errors.extend(member_result.errors)
                        result.warnings.extend(member_result.warnings)
                        result.skipped_definitions.extend(member_result.skipped_definitions)
                        result.dependencies.extend(member_result.dependencies)

        return result

    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        if isinstance(definition, ZipFileDagDefinition):
            suffix = Path(definition.file_path).suffix.lower()
            if suffix in self._internal_importers:
                return self._internal_importers[suffix].get_source_code(definition)
            raise ValueError(f"No internal importer registered for zip member {definition.file_path}")

        # If definition is the zip archive itself, route to code member(s)
        with definition.as_file() as local_zip_path:
            with zipfile.ZipFile(local_zip_path) as z:
                candidates = [
                    name
                    for name in z.namelist()
                    if Path(name).suffix.lower() in self._internal_importers
                    and not name.startswith("__MACOSX")
                    and ".." not in Path(name).parts
                    and not Path(name).is_absolute()
                ]
            if not candidates:
                raise ValueError(f"No code files found inside ZIP archive {definition}")
            if len(candidates) == 1:
                nested_def = ZipFileDagDefinition(zip_path=local_zip_path, file_path=candidates[0])
                importer = self._internal_importers[Path(candidates[0]).suffix.lower()]
                return importer.get_source_code(nested_def)

            parts = []
            primary_language = "python"
            for name in candidates:
                nested_def = ZipFileDagDefinition(zip_path=local_zip_path, file_path=name)
                importer = self._internal_importers[Path(name).suffix.lower()]
                res = importer.get_source_code(nested_def)
                primary_language = res.language
                parts.append(f"# --- {name} ---\n{res.source_code}")
            return DagSourceCode(source_code="\n\n".join(parts), language=primary_language)
