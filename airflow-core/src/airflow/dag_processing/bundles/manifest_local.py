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
"""Local source adapter for manifest-backed Dag bundles."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from airflow.dag_processing.bundles.manifest import (
    BundleManifestError,
    BundleManifestSourceChangedError,
    collect_bundle_source_snapshot,
)
from airflow.dag_processing.bundles.manifest_bundle import (
    ManifestDagBundleBase,
    PreparedPublishSource,
    publish_prepared_manifest_dag_bundle,
)

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.manifest_bundle import BundlePublishResult


class ManifestLocalDagBundle(ManifestDagBundleBase):
    """
    Manifest-backed Dag bundle whose snapshots are published from a local directory.

    Publish with ``publish_manifest_local_dag_bundle``; the bundle serves the release that
    publication recorded, never the mutable source directory itself.
    """

    def _confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        current_snapshot = collect_bundle_source_snapshot(prepared.root)
        if current_snapshot.signature != prepared.source_snapshot.signature:
            raise BundleManifestSourceChangedError(
                "Bundle source changed while publishing the bundle snapshot"
            )


def publish_manifest_local_dag_bundle(
    *,
    bundle: ManifestLocalDagBundle,
    source_path: str | Path,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """
    Publish a local source tree as an immutable manifest-backed snapshot.

    :param expected_current_version: Publish only if this is still the current release, so
        that a publisher working from a stale view cannot overwrite a newer release.
    """
    if bundle.version:
        raise BundleManifestError(
            f"Cannot explicitly publish pinned bundle '{bundle.name}' at version {bundle.version!r}"
        )
    source_path = Path(source_path)
    return publish_prepared_manifest_dag_bundle(
        bundle=bundle,
        prepared_source=PreparedPublishSource(
            root=source_path, source_snapshot=collect_bundle_source_snapshot(source_path)
        ),
        expected_current_version=expected_current_version,
    )
