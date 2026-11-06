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

import hashlib
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

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

    Publish with ``publish_manifest_local_dag_bundle``, or set ``source_path`` to have every
    unpinned refresh publish it automatically. Either way the bundle serves the release that
    publication recorded, never the mutable source directory itself.

    :param source_path: Local directory to publish automatically once its metadata has
        remained unchanged for ``source_stability_seconds``. A bundle with ``source_path``
        cannot be published explicitly.
    """

    def __init__(self, *, source_path: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.source_path = Path(source_path) if source_path else None
        if self.source_path is not None:
            try:
                self._store.validate_source_paths(self.source_path, cache_versions_dir=self.versions_dir)
            except ValueError as e:
                # TypeError, not ValueError: callback bundle preparation treats a ValueError
                # from bundle construction as "bundle no longer configured".
                raise TypeError(str(e)) from e

    @property
    def _has_publish_source(self) -> bool:
        return self.source_path is not None

    @property
    def _publish_source_description(self) -> str:
        return str(self.source_path) if self.source_path is not None else "<none>"

    def _prepare_publish_source(self) -> PreparedPublishSource:
        if self.source_path is None:
            raise BundleManifestError("Automatic local publication requires source_path")
        return _prepare_local_source(self.source_path)

    def _confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        current_snapshot = collect_bundle_source_snapshot(prepared.root)
        if current_snapshot.signature != prepared.source_snapshot.signature:
            raise BundleManifestSourceChangedError(
                "Bundle source changed while publishing the bundle snapshot"
            )


def _get_local_source_identity(source_path: Path) -> str:
    normalized = os.path.abspath(source_path)
    return f"sha256:{hashlib.sha256(normalized.encode()).hexdigest()}"


def _prepare_local_source(source_path: Path) -> PreparedPublishSource:
    return PreparedPublishSource(
        root=source_path,
        source_snapshot=collect_bundle_source_snapshot(source_path),
        source_type="local",
        source_identity=_get_local_source_identity(source_path),
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
    if bundle.source_path is not None:
        raise BundleManifestError(
            f"Bundle '{bundle.name}' has source_path configured for automatic publication. "
            "Remove source_path before publishing it explicitly."
        )
    if bundle.version:
        raise BundleManifestError(
            f"Cannot explicitly publish pinned bundle '{bundle.name}' at version {bundle.version!r}"
        )
    return publish_prepared_manifest_dag_bundle(
        bundle=bundle,
        prepared_source=_prepare_local_source(Path(source_path)),
        expected_current_version=expected_current_version,
    )
