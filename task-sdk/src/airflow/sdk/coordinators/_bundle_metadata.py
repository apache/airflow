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

import pathlib
import stat
from typing import TYPE_CHECKING, Any

import structlog
import yaml

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from structlog.typing import FilteringBoundLogger

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators")


def walk_files(
    roots: Iterable[pathlib.Path], *, match: Callable[[pathlib.Path], bool]
) -> Iterator[pathlib.Path]:
    """
    Yield the regular files under *roots* that satisfy *match*, descending into directories.

    Roots are visited in order and each directory's entries sorted, so what a coordinator lists or
    reads from a bundle does not depend on filesystem ordering.
    """
    yield from _walk_files(roots, match, set())


def _walk_files(
    items: Iterable[pathlib.Path],
    match: Callable[[pathlib.Path], bool],
    seen_dirs: set[tuple[int, int]],
) -> Iterator[pathlib.Path]:
    for item in items:
        try:
            file_info = item.stat()
        except OSError:
            # A broken symlink or unreadable parent must not abort the scan.
            # The caller reports a genuinely missing artifact once every root is searched.
            continue
        if stat.S_ISDIR(file_info.st_mode):
            # Dedupe by identity so a symlink loop cannot recurse until the stack is exhausted.
            key = (file_info.st_dev, file_info.st_ino)
            if key in seen_dirs:
                log.debug("Skipping already-visited directory", path=item)
                continue
            seen_dirs.add(key)
            yield from _walk_files(_sorted_children(item), match, seen_dirs)
        elif stat.S_ISREG(file_info.st_mode) and match(item):
            yield item


def _sorted_children(directory: pathlib.Path) -> list[pathlib.Path]:
    # iterdir() is lazy, so an unreadable directory raises only once iteration starts.
    try:
        return sorted(directory.iterdir())
    except OSError:
        return []


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
