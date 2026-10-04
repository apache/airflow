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
"""The Dag importer that parses native TypeScript Dags from packed bundles."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.coordinators.node._bundle_reader import (
    BUNDLE_SUFFIX,
    has_bundle_layout_prefix,
    read_bundle_entrypoint_source,
)
from airflow.sdk.importers.base import DagSourceCode

if TYPE_CHECKING:
    from airflow.sdk.importers.base import DagDefinition

_NO_SOURCE: Final = "// Source code is not available: the bundle embeds no entrypoint source.\n"


class NodeDagImporter(CoordinatorDagImporter):
    """
    Claim the native Dags of packed ``*.min.mjs`` TypeScript bundles.

    A :class:`~airflow.sdk.coordinators.node.NodeCoordinator` parses them.
    """

    coordinator_classpath: ClassVar[str] = "airflow.sdk.coordinators.node.NodeCoordinator"
    artifact_suffix = BUNDLE_SUFFIX
    supported_extensions = [".mjs"]

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        # The header identifies a packed bundle rather than guessing at content, so safe_mode keeps it.
        with definition.as_file() as path:
            try:
                return has_bundle_layout_prefix(path)
            except OSError:
                # Keep the file, so parsing it records why it cannot be read.
                return True

    def get_source_code(self, definition: DagDefinition) -> DagSourceCode:
        """Return the embedded entrypoint source of the bundle, or a notice when it embeds none."""
        with definition.as_file() as path:
            source = read_bundle_entrypoint_source(path)
        return DagSourceCode(source_code=source or _NO_SOURCE, language="typescript")
