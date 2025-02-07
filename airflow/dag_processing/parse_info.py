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

from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path


@dataclass(frozen=True)
class ParseBundleInfo:
    """
    In-parsing time context about bundle being processed.

    :param name: Bundle name.
    :param root_path: Root path of the bundle version.
    :param version: Bundle version.
    """

    name: str
    root_path: Path | str = field(compare=False)
    version: str | None = None


@dataclass(frozen=True)
class ParseFileInfo:
    """
    Information about a DAG file at parse time with fixed version.

    :param rel_path: Relative path of a file within a bundle.
    :param bundle: Bundle information.
    """

    rel_path: Path | str
    bundle: ParseBundleInfo

    @property
    def absolute_path(self) -> Path:
        return Path(self.bundle.root_path) / Path(self.rel_path)

    @cached_property
    def entrypoint(self) -> DagEntrypoint:
        return DagEntrypoint(rel_path=Path(self.rel_path), bundle_name=self.bundle.name)


@dataclass(frozen=True)
class DagEntrypoint:
    """
    DAG file entrypoint identifier.

    Fully identifies an entrypoint for potential DAG files within an Airflow deployment and other Airflow entities related to it (import errors, warnings, etc.).

    :param rel_path: Relative path of an entrypoint file within a bundle.
    :param bundle_name: Name of the bundle.
    """

    rel_path: Path
    bundle_name: str
