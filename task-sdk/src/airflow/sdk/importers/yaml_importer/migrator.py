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
`$schema` version resolution and forward migration to the head shape.

A document pinned to an older `$schema` version is migrated up to the current head shape
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
import re
import warnings
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


@attrs.define
class DagDocumentMigrator:
    """YAML Dag document migrator; pins each document to its ``$schema`` version."""

    _bundle: VersionBundle

    def resolve_and_migrate(self, body: dict[str, Any], *, source: str) -> dict[str, Any]:
        """
        Resolve *body*'s ``$schema`` version and migrate it to the head shape.

        The version token is read from the ``$schema`` URL (the host is ignored). It is an
        exact pin: a known version uses its own ruleset; an unknown one (e.g. newer than this
        importer) resolves to the latest ruleset with a warning, never a hard failure.

        :return: The migrated result. If *body* is already at head, it is
            returned as-is; otherwise a migrated copy is returned.
        """
        known = [v.value for v in self._bundle.versions]  # newest-first
        date = version_from_schema(body["$schema"])
        if date in known:
            source_version = date
        else:
            source_version = known[0]  # unknown version -> latest ruleset we have
            warnings.warn(
                f"{source}: $schema version {date!r} is not a known version {known}; "
                f"using the latest ruleset {source_version!r}",
                stacklevel=2,
            )
        return self._migrate_to_head(body, source_version)

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
