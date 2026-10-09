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

"""Parse a YAML/JSON DAG file into validated :class:`~.models.DagDocument` objects."""

from __future__ import annotations

import collections.abc
import datetime
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import ValidationError

from airflow.sdk.importers.yaml_importer import migrator
from airflow.sdk.importers.yaml_importer.models import DagDocument

if TYPE_CHECKING:
    from collections.abc import Hashable, Iterator
    from typing import IO

    from yaml.nodes import MappingNode


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """
    A ``SafeLoader`` that rejects duplicate mapping keys.

    PyYAML keeps the last value for a repeated key, so a second ``tasks:`` block
    or a second ``with:`` or ``needs:`` on a task would silently drop the first.
    This can be a footgun for a hand-edited format, so we raise a
    ``ConstructorError`` instead of silently shadowing.
    """

    def construct_mapping(self, node: MappingNode, deep: bool = False) -> dict[Hashable, Any]:
        seen: set[Hashable] = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, collections.abc.Hashable):
                continue
            if key not in seen:
                seen.add(key)
                continue
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        return super().construct_mapping(node, deep=deep)


class YamlDagParseError(ValueError):
    """A document could not be parsed or did not conform to the format."""


def _resolve_and_migrate(raw: dict[str, Any], *, source: str) -> dict[str, Any]:
    """Resolve the ``$schema`` version and migrate to the head shape."""
    if not isinstance(raw, collections.abc.Mapping):
        raise YamlDagParseError(f"{source}: a DAG document must be a mapping, got {type(raw).__name__}")
    if not (schema := raw.get("$schema")):
        raise YamlDagParseError(f"{source}: missing required key '$schema'")
    if isinstance(schema, datetime.date):  # Smartly treat an unquoted date as version.
        raw = {**raw, "$schema": migrator.schema_url(schema.strftime(r"%Y-%m-%d"))}
    elif not isinstance(schema, str):
        raise YamlDagParseError(
            f"{source}: '$schema' must be a string, got {type(schema).__name__} ({schema!r})"
        )
    try:
        return migrator.get_migrator().resolve_and_migrate(raw)
    except ValueError as exc:  # unresolvable $schema version; bundle-config errors are not ValueError
        raise YamlDagParseError(f"{source}: {exc}") from exc


def parse_documents(stream: str | IO[str], *, source: str = "<string>") -> Iterator[DagDocument]:
    """Parse each document of *stream* into a :class:`DagDocument`."""
    try:
        for pos, raw in enumerate(yaml.load_all(stream, _UniqueKeySafeLoader)):
            if raw is None:  # empty document between --- separators
                continue
            where = source if pos == 0 else f"{source}[doc {pos}]"
            migrated = _resolve_and_migrate(raw, source=where)
            try:
                yield DagDocument.model_validate(migrated)
            except ValidationError as exc:
                raise YamlDagParseError(f"{where}: {exc}") from exc
    except yaml.YAMLError as exc:  # a syntax error surfaced while streaming a document
        raise YamlDagParseError(f"{source}: invalid YAML: {exc}") from exc
