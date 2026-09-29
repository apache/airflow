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
import os
import sys
import tempfile
import threading
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.importers.base import (
    AbstractDagImporter,
    DagDefinition,
    DagImporterRegistry,
    DagImportError,
    DagImportResult,
    DagSourceCode,
    FileDagDefinition,
    _get_importer_extensions,
    _normalize_extensions,
    _parse_importer_specs,
    find_file_dag_definitions,
    get_file_suffix,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002

log = logging.getLogger(__name__)

_sys_path_lock = threading.RLock()


@dataclass
class ZipMemberDagDefinition(FileDagDefinition):
    """A DAG definition backed by a file inside a ZIP archive."""

    zip_path: Path
    file_path: str
    _content: bytes | None = field(default=None, repr=False, compare=False)

    @property
    def freshness_token(self) -> str:
        try:
            stat = self.zip_path.stat()
        except OSError:
            return ""
        return f"{stat.st_mtime_ns}-{stat.st_size}-{self.file_path}"

    def get_relative_loc(self, root: Path | None = None) -> str:
        if root is not None:
            with contextlib.suppress(ValueError):
                return str(self.zip_path.relative_to(root).joinpath(self.file_path))
        return str(self.zip_path.joinpath(self.file_path))

    @property
    def suffix(self) -> str:
        return os.path.splitext(self.file_path)[-1].lower()

    @contextlib.contextmanager
    def import_context(self) -> Generator[None, None, None]:
        path = str(self.zip_path)
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

    def read_bytes(self) -> bytes:
        if self._content is None:
            with zipfile.ZipFile(self.zip_path) as z:
                self._content = z.read(self.file_path)
        return self._content

    @contextlib.contextmanager
    def as_file(self) -> Generator[Path, None, None]:
        # Keep the member's file name: importers may derive identity from it, such as a Dag id.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir, Path(self.file_path).name)
            temp_path.write_bytes(self.read_bytes())
            yield temp_path

    def __repr__(self) -> str:
        return str(self.zip_path.joinpath(self.file_path))


class ZipImporter(AbstractDagImporter[ZipMemberDagDefinition]):
    """Composite importer responsible for routing archive members to internal importers."""

    supported_extensions = [".zip"]

    def __init__(
        self,
        internal_importers: dict[str, AbstractDagImporter[Any] | dict[str, Any]]
        | list[dict[str, Any]]
        | None = None,
        extensions: list[str] | None = None,
    ) -> None:
        if extensions is not None:
            self.supported_extensions = _normalize_extensions(extensions)
        self._internal_extension_importers: dict[str, AbstractDagImporter[Any]] = {}
        self._ordered_internal_importers: list[AbstractDagImporter[Any]] = []

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

    def list_dag_definitions(
        self,
        bundle: BaseDagBundle,
        *,
        safe_mode: bool = True,
    ) -> Iterator[ZipMemberDagDefinition | DagImportError]:
        """
        List importable members across the bundle's zip archives.

        Each member is yielded as a plain ZipMemberDagDefinition; import_definition
        re-resolves the internal importer from the member's extension. An archive or a
        member that cannot be read is reported as a DagImportError and the remaining
        members are still yielded.
        """
        for archive in find_file_dag_definitions(bundle.path, self.supported_extensions):
            try:
                zip_file = zipfile.ZipFile(archive.path)
            except Exception as e:
                log.warning("Cannot read ZIP archive %s: %s", archive.path, e)
                yield DagImportError(
                    source_reference=archive.get_relative_loc(bundle.path),
                    message=f"Failed to read ZIP archive: {e}",
                    error_type="zip_read_error",
                )
                continue

            with zip_file:
                for member, importer in self._iter_supported_members(archive.path, zip_file.namelist()):
                    if safe_mode:
                        try:
                            # Read through the open archive: reopening it per member re-parses the
                            # central directory and makes discovery quadratic in the member count.
                            member._content = zip_file.read(member.file_path)
                            if not importer.might_contain_dag(member, safe_mode):
                                continue
                        except Exception as e:
                            # One unreadable member must not end discovery for the rest of the archive.
                            log.warning(
                                "Cannot read zip member %s of %s: %s", member.file_path, archive.path, e
                            )
                            yield DagImportError(
                                source_reference=member.get_relative_loc(bundle.path),
                                message=f"Failed to read ZIP member: {e}",
                                error_type="zip_read_error",
                            )
                            continue
                    yield member

    def _iter_supported_members(
        self, zip_path: Path, member_names: list[str]
    ) -> Iterator[tuple[ZipMemberDagDefinition, AbstractDagImporter[Any]]]:
        """Yield the archive's importable members, source preferred over bytecode."""
        candidates: list[tuple[str, AbstractDagImporter[Any]]] = []
        for member_name in member_names:
            if member_name.endswith("/") or member_name.startswith("__MACOSX/"):
                continue
            # ZipSlip defence: reject traversal or absolute member names.
            member_path = Path(member_name)
            if member_path.is_absolute() or ".." in member_path.parts:
                log.warning(
                    "Skipping zip member %r in %s: directory traversal patterns detected",
                    member_name,
                    zip_path,
                )
                continue
            if "__pycache__" in member_path.parts:
                continue
            if (importer := self._get_internal_importer(member_name)) is not None:
                candidates.append((member_name, importer))

        # A .pyc is kept only when no supported .py sibling survived the same filtering, so an
        # unsupported source never hides a genuinely sourceless module. Suffixes are compared
        # lowercased, so `dag.PY` and `dag.pyc` still pair up.
        source_stems = {os.path.splitext(name)[0] for name, _ in candidates if get_file_suffix(name) == ".py"}
        for member_name, importer in candidates:
            if get_file_suffix(member_name) == ".pyc" and os.path.splitext(member_name)[0] in source_stems:
                continue
            yield ZipMemberDagDefinition(zip_path=zip_path, file_path=member_name), importer

    def import_definition(
        self,
        definition: ZipMemberDagDefinition,
        bundle: BaseDagBundle,
    ) -> DagImportResult:
        """
        Import a single archive member.

        The internal importer is resolved from the member's extension; the member's
        ``import_context`` places the archive on ``sys.path`` so imports between members
        resolve via ``zipimport``.
        """
        importer = self._get_internal_importer(definition.file_path)
        if importer is None:
            result = DagImportResult(definition=definition)
            result.errors.append(
                DagImportError(
                    source_reference=definition.get_relative_loc(bundle.path),
                    message=f"No internal importer registered for zip member {definition.file_path}",
                    error_type="import",
                )
            )
            return result
        return importer.import_definition(definition, bundle)

    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        """
        Return the source of a single archive member.

        A zip is treated as a directory of DAG files: each member is its own
        source unit, so this renders exactly the member named by ``definition``
        through its file-type internal importer. The archive as a whole has no
        source, the same way a directory does not; passing an archive-level
        definition here is a category error and raises.
        """
        importer = self._get_internal_importer(definition)
        if importer is None:
            raise ValueError(f"No internal importer to read source for {definition!r}")
        return importer.get_source_code(definition)

    def _register_internal(
        self, importer: AbstractDagImporter[Any], extensions: list[str] | None = None
    ) -> None:
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

    def _get_internal_importer(self, member: DagDefinition | str) -> AbstractDagImporter | None:
        suffix = get_file_suffix(member)
        if suffix and suffix in self._internal_extension_importers:
            return self._internal_extension_importers[suffix]
        for importer in reversed(self._ordered_internal_importers):
            if importer.can_handle(member):
                return importer
        return None
