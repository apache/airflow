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
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import ValidationError

from airflow.sdk.importers.yaml_importer import migrator
from airflow.sdk.importers.yaml_importer.models import DagDocument

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import IO


class YamlDagParseError(ValueError):
    """A document could not be parsed or did not conform to the format."""


def _resolve_and_migrate(raw: dict[str, Any], *, source: str) -> dict[str, Any]:
    """Resolve the ``$schema`` version and migrate to the head shape."""
    if not isinstance(raw, collections.abc.Mapping):
        raise YamlDagParseError(f"{source}: a DAG document must be a mapping, got {type(raw).__name__}")
    if not raw.get("$schema"):
        raise YamlDagParseError(f"{source}: missing required key '$schema'")
    try:
        return migrator.get_migrator().resolve_and_migrate(raw)
    except ValueError as exc:  # unresolvable $schema version; bundle-config errors are not ValueError
        raise YamlDagParseError(f"{source}: {exc}") from exc


def parse_documents(stream: str | IO[str], *, source: str = "<string>") -> Iterator[DagDocument]:
    """Parse each document of *stream* into a :class:`DagDocument`."""
    try:
        for pos, raw in enumerate(yaml.safe_load_all(stream)):
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
