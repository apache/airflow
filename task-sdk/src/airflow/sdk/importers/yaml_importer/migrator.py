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
"""
Schema version resolution and forward migration to the head shape.

A document with an older ``$schema`` is migrated to the head shape by walking
the Cadwyn version bundle and applying each ``VersionChange``'s forward request
converters (`@convert_request_to_next_version_for(DagDocument)`), oldest to
newest.

Only whole-document request converters keyed on `DagDocument` are applied; a
bundle carrying any other cadwyn instruction is rejected at construction rather
than migrating a document with half its changes applied. (See documentation on
:meth:`DagDocumentMigrator._reject_unsupported_instructions` for rationale.)
"""

from __future__ import annotations

import copy
import functools
import re
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk.importers.yaml_importer.models import DagDocument
from airflow.sdk.importers.yaml_importer.versions import get_bundle

if TYPE_CHECKING:
    from cadwyn import VersionBundle

_SCHEMA_URL = "https://airflow.apache.org/schemas/dag/{date}.json"
_VERSION_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def schema_url(date: str) -> str:
    """Build the canonical ``$schema`` URL for a published version *date*."""
    return _SCHEMA_URL.format(date=date)


def version_from_schema(schema: str) -> str | None:
    """Read the version (``YYYY-MM-DD``) token out of a ``$schema`` URL, ignoring the host."""
    match = _VERSION_RE.search(schema or "")
    return match.group(0) if match else None


@attrs.define
class _RequestInfo:
    """
    Duck-type stand-in for Cadwyn's ``RequestInfo``.

    ``cadwyn.structure.data._AlterDataInstruction.__call__`` only reads
    and writes ``info.body``; the by-schema transformers we drive never
    touch FastAPI's Request/Response. Passing this minimal object lets
    us run cadwyn's migrations from a pure in-process code path with no
    HTTP stack.
    """

    body: dict[str, Any]


def _calculate_version_values(migrator: DagDocumentMigrator) -> frozenset[str]:
    return frozenset(v.value for v in migrator._bundle.versions)


@attrs.define
class DagDocumentMigrator:
    """YAML Dag document migrator; pins each document to its ``$schema`` version."""

    _bundle: VersionBundle
    _version_values: frozenset[str] = attrs.field(
        init=False,
        default=attrs.Factory(_calculate_version_values, takes_self=True),
    )

    def __attrs_post_init__(self) -> None:
        self._reject_unsupported_instructions()

    def _reject_unsupported_instructions(self) -> None:
        """
        Fail if the bundle carries unsupported migrations.

        Only forward request converters keyed on :class:`DagDocument` may run.
        This is due to difficulties posed by things like templates and XCom,
        which can present in nested locations inside task arguments. Technically
        we could just ban model-level migration only for them, but that's
        considered unnecessarily complicated since you can already do all
        per-model-level changes on :class:`DagDocument`. We can relax this
        restriction in the future if the ergonomics prove to be suboptimal.
        """
        unsupported = (
            "alter_endpoint_instructions",
            "alter_enum_instructions",
            "alter_request_by_path_instructions",
            "alter_response_by_path_instructions",
            "alter_response_by_schema_instructions",
            "alter_schema_instructions",
        )
        for version in self._bundle.versions:
            for change in version.changes:
                name = type(change).__name__
                other_models = set(change.alter_request_by_schema_instructions) - {DagDocument}
                if other_models:
                    raise RuntimeError(
                        f"{name}: request converters for {sorted(m.__name__ for m in other_models)} "
                        f"are not applied; only whole-document {DagDocument.__name__} converters "
                        f"are supported"
                    )
                if used := [b for b in unsupported if getattr(change, b, None)]:
                    raise RuntimeError(
                        f"{name}: unsupported cadwyn instructions {used}; only "
                        f"{DagDocument.__name__} request converters are applied"
                    )

    def resolve_version(self, version: str | None) -> str:
        """Validate *version* is a published version in the bundle."""
        if version is None or version not in self._version_values:
            raise ValueError(f"$schema version {version!r} is not valid")
        return version

    def resolve_and_migrate(self, body: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve *body*'s ``$schema`` version and migrate it to the head shape.

        The version token is read from the ``$schema`` URL. Every valid
        (published) version stays in the bundle and is migrated forward. An
        unknown version is rejected.

        :raises ValueError: if the ``$schema`` version is not valid.
        :return: The migrated result. If *body* is already at head, it is
            returned as-is; otherwise a migrated copy is returned.
        """
        source_version = self.resolve_version(version_from_schema(body["$schema"]))
        if source_version == self._bundle.versions[0].value:
            return body
        info = _RequestInfo(copy.deepcopy(dict(body)))
        for version in self._bundle.reversed_versions:
            if version.value <= source_version:
                continue
            for change in version.changes:
                for instruction in change.alter_request_by_schema_instructions.get(DagDocument, ()):
                    instruction(info)  # type: ignore[arg-type]
        return info.body


@functools.cache
def get_migrator() -> DagDocumentMigrator:
    """Get the process-wide migrator bound to the format's version bundle."""
    return DagDocumentMigrator(get_bundle())
