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
"""Local Python and ZIP discovery for the executor parsing prototype."""

from __future__ import annotations

import hashlib
from pathlib import Path

from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.dag_processing.executor_worker import compute_source_revision
from airflow.dag_processing.orchestrator import DiscoveredDefinition
from airflow.sdk.importers.base import DagImportError
from airflow.sdk.importers.python_importer import PythonDagImporter
from airflow.sdk.importers.zip_importer import ZipImporter, ZipMemberDagDefinition


def discover_python_bundle(root: Path, *, bundle_name: str = "poc") -> list[DiscoveredDefinition]:
    """Trusted local SDK discovery, run outside step(); fail without applying a partial inventory."""
    root = root.resolve(strict=True)
    bundle = LocalDagBundle(name=bundle_name, path=str(root))
    definitions = []
    archives: dict[Path, str] = {}
    for importer in (PythonDagImporter(), ZipImporter()):
        for definition in importer.list_dag_definitions(bundle, safe_mode=False):
            if isinstance(definition, DagImportError):
                raise ValueError(f"Discovery failed: {definition.format_message()}")
            archive = definition.zip_path if isinstance(definition, ZipMemberDagDefinition) else None
            path = archive or root / definition.get_relative_loc(root)
            if not path.resolve(strict=True).is_relative_to(root):
                raise ValueError("Discovered source escapes the bundle")
            if archive is not None and archive not in archives:
                archives[archive] = compute_source_revision(archive)
            definitions.append(
                DiscoveredDefinition(
                    relative_path=definition.get_relative_loc(root),
                    source_revision=hashlib.sha256(definition.read_bytes()).hexdigest(),
                    archive_path=archive.relative_to(root).as_posix() if archive else None,
                    archive_revision=archives[archive] if archive else None,
                )
            )
    if any(compute_source_revision(path) != revision for path, revision in archives.items()):
        raise ValueError("Archive changed during discovery")
    return definitions
