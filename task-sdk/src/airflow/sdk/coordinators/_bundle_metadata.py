#
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
"""Bundle-metadata helpers shared by the subprocess coordinators."""

from __future__ import annotations

import os
import pathlib
from typing import Any

import attrs
import yaml

from airflow.sdk.execution_time.schema import get_schema_version_migrator


def convert_roots(
    value: None | os.PathLike[str] | pathlib.Path | list[os.PathLike[str] | pathlib.Path],
) -> list[pathlib.Path]:
    """Normalize a coordinator's root-directories kwarg into a list of expanded paths."""
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike, pathlib.Path)):
        return [pathlib.Path(value).expanduser()]
    return [pathlib.Path(v).expanduser() for v in value]


def validate_schema_version(instance, _, value) -> str:
    """Attrs validator resolving a bundle's supervisor schema version to a known one."""
    return get_schema_version_migrator().resolve_version(str(value))


@attrs.define
class ResolvedBundle:
    """A located bundle whose supervisor schema version has been resolved."""

    path: pathlib.Path
    schema_version: str = attrs.field(validator=validate_schema_version)


def parse_metadata_mapping(content: str | bytes, *, source: str) -> dict[str, Any]:
    """
    Parse *content* as the ``airflow-metadata.yaml`` mapping.

    Raises ``ValueError`` on undecodable, unparsable, or non-mapping content;
    *source* names the metadata's origin in the error message.
    """
    try:
        data = yaml.safe_load(content.decode("utf-8") if isinstance(content, bytes) else content)
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError(f"cannot parse {source}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"{source} must contain a mapping")
    return data


def extract_supervisor_schema_version(metadata: dict[str, Any]) -> str:
    """Return ``sdk.supervisor_schema_version`` from bundle metadata, raising ``ValueError`` if absent."""
    sdk = metadata.get("sdk")
    if not isinstance(sdk, dict):
        raise ValueError("missing sdk metadata mapping")

    value = sdk.get("supervisor_schema_version")
    if not isinstance(value, str) or not value:
        raise ValueError("missing or invalid sdk.supervisor_schema_version")
    return value
