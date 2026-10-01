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
`compatibility_date` resolution and forward migration to the head shape.

A document authored at an older `compatibility_date` is migrated up to the current head shape
*before* validation, by walking the Cadwyn version bundle and applying each `VersionChange`'s
forward request converters (`@convert_request_to_next_version_for(DagDocument)`), oldest to
newest. This mirrors the exec-API / supervisor-schema `SchemaVersionMigrator`, trimmed to the
one direction a document format needs (author's version -> head), with the bundle and its
version list encapsulated in the migrator rather than exposed as module-level helpers.

With a single published version there is nothing to migrate; the machinery is here so a future
version bump only has to add a `VersionChange` carrying a converter — no parser changes.
"""

from __future__ import annotations

import copy
import functools
import warnings
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk.importers.yaml_importer.models import DagDocument
from airflow.sdk.importers.yaml_importer.versions import get_bundle

if TYPE_CHECKING:
    from cadwyn import VersionBundle


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


@attrs.define
class DagDocumentMigrator:
    """YAML Dag document migrator against ``compatibility_date``."""

    _bundle: VersionBundle

    def resolve_and_migrate(self, body: dict[str, Any], *, source: str) -> dict[str, Any]:
        """
        Resolve *body*'s ``compatibility_date``.

        A warning is emitted for a date outside the published range (a future
        date, or one older than the earliest published version); an in-between
        date resolves silently to the newest applicable ruleset.

        :return: The migrated result. If *body* is already at head, it is
            returned as-is; otherwise a migrated copy is returned.
        """
        versions = self._bundle.versions
        date = body["compatibility_date"]
        source_version = self._resolve_source_version(date)
        if date > versions[0].value or date < versions[-1].value:
            warnings.warn(
                f"{source}: compatibility_date {date!r} is not a known version "
                f"{[v.value for v in versions]}; using the newest applicable ruleset {source_version!r}",
                stacklevel=2,
            )
        return self._migrate_to_head(body, source_version)

    def _resolve_source_version(self, date: str) -> str:
        """
        Resolve the version a document dated *date* should be migrated from.

        This is inspired by Cloudflare Worker's ``compatibility_date``. The
        newest published version whose date is ``<= date``, clamped to the
        published range (future -> head; older than the earliest published
        version -> that earliest version).
        """
        versions = self._bundle.versions
        if date >= (newest := versions[0].value):
            return newest
        if date < (oldest := versions[-1].value):
            return oldest
        return next(v.value for v in versions if v.value <= date)

    def _migrate_to_head(self, body: dict[str, Any], source_version: str) -> dict[str, Any]:
        """
        Migrate a raw document *body* from *source_version* to the head shape.

        This applies the forward request converters for :class:`DagDocument`
        from the version after *source_version* through head, in order.

        :return: The migrated result. If *body* is already at head, it is
            returned as-is; otherwise a migrated copy is returned.
        """
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
