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
"""Tests for the artifact store seam of manifest-backed Dag bundles."""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest

from airflow.dag_processing.bundles.artifact_store import (
    ArtifactStore,
    ArtifactStoreConflictError,
    BundleManifestNotFoundError,
    PublishedSnapshot,
)
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    build_bundle_version_manifest,
    collect_bundle_source_snapshot,
    serialize_bundle_version_manifest,
    validate_bundle_relative_path,
)
from airflow.dag_processing.bundles.manifest_bundle import (
    BundleManifestReferenceChangedError,
    BundlePublishResult,
    ManifestDagBundleBase,
    PreparedPublishSource,
    publish_prepared_manifest_dag_bundle,
)
from airflow.dag_processing.bundles.manifest_local import ManifestLocalDagBundle

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = pytest.mark.usefixtures("isolate_bundle_storage")

BUNDLE_NAME = "manifest-memory"
DAG_FILE = "dags/example.py"


@dataclass
class _VersionedBucket:
    """What a versioned object store holds: object versions, snapshot manifests and the reference."""

    objects: dict[str, dict[str, bytes]] = field(default_factory=dict)
    manifests: dict[str, bytes] = field(default_factory=dict)
    ref: bytes | None = None
    ref_generation: int = 0


class _InMemoryArtifactStore(ArtifactStore):
    """
    Store with no published root that fetches files by object version id.

    Its guard is not a lock: entering it resets the compare-and-swap baseline, so ``write_ref``
    is conditioned on the reference read inside the guard, or on the current one when there
    was no such read.
    """

    backend_type = "memory"

    def __init__(self, *, bundle_name: str, bucket: _VersionedBucket) -> None:
        self.bundle_name = bundle_name
        self._bucket = bucket
        self._ref_baseline: int | None = None

    @property
    def root(self) -> str:
        return f"memory://{self.bundle_name}"

    @property
    def ref_path(self) -> str:
        return f"{self.root}/latest.json"

    @property
    def snapshots_root(self) -> str:
        return f"{self.root}/versions"

    def snapshot_path(self, version: str) -> str:
        return f"{self.snapshots_root}/{version}"

    def read_ref(self) -> dict[str, Any]:
        # Set even when there is no reference, so the next write requires it to stay absent.
        self._ref_baseline = self._bucket.ref_generation
        if self._bucket.ref is None:
            raise BundleManifestNotFoundError(f"Bundle '{self.bundle_name}' has no release reference")
        return json.loads(self._bucket.ref)

    def write_ref(self, payload: dict[str, Any]) -> None:
        content = serialize_bundle_version_manifest(payload)
        baseline = self._bucket.ref_generation if self._ref_baseline is None else self._ref_baseline
        if self._bucket.ref_generation != baseline and self._bucket.ref != content:
            raise ArtifactStoreConflictError(f"Another publisher updated {self.ref_path}")
        self._bucket.ref = content
        self._bucket.ref_generation += 1

    @contextmanager
    def publication_guard(self):
        self._ref_baseline = None
        yield

    def prepare_publish_areas(self) -> None:
        pass

    def snapshot_exists(self, version: str) -> bool:
        return version in self._bucket.manifests

    def fetch_snapshot(
        self, version: str, destination: Path, *, structural_validator: Callable[[Path], None]
    ) -> None:
        if version not in self._bucket.manifests:
            raise BundleManifestNotFoundError(f"Bundle version {version!r} is not published")
        manifest_bytes = self._bucket.manifests[version]
        for file_info in json.loads(manifest_bytes)["files"]:
            path = destination / validate_bundle_relative_path(file_info["path"])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(self._bucket.objects[file_info["path"]][file_info["version_id"]])
            if file_info["executable"]:
                path.chmod(0o755)
        (destination / MANIFEST_FILE_NAME).write_bytes(manifest_bytes)

    def publish_snapshot(
        self,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
    ) -> PublishedSnapshot:
        version = manifest["version"]
        if version not in self._bucket.manifests:
            for file_info in manifest["files"]:
                object_versions = self._bucket.objects.setdefault(file_info["path"], {})
                object_versions[file_info["version_id"]] = (source_root / file_info["path"]).read_bytes()
        # The manifest commits the snapshot: written last, and only if absent.
        manifest_bytes = serialize_bundle_version_manifest(manifest)
        committed = self._bucket.manifests.setdefault(version, manifest_bytes)
        return PublishedSnapshot(manifest=json.loads(committed), created=committed is manifest_bytes)


class _MemoryManifestDagBundle(ManifestDagBundleBase):
    def __init__(self, *, bucket: _VersionedBucket, **kwargs) -> None:
        self.bucket = bucket
        super().__init__(**kwargs)

    def create_artifact_store(self) -> ArtifactStore:
        return _InMemoryArtifactStore(bundle_name=self.name, bucket=self.bucket)

    def confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        pass


def _write_source(root: Path, content: str = "print('dag')") -> Path:
    path = root / DAG_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return root


def _publish(
    bundle: ManifestDagBundleBase,
    source: Path,
    version_id: str,
    *,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    return publish_prepared_manifest_dag_bundle(
        bundle=bundle,
        prepared_source=PreparedPublishSource(
            root=source,
            source_snapshot=collect_bundle_source_snapshot(source),
            file_version_ids={DAG_FILE: version_id},
        ),
        expected_current_version=expected_current_version,
    )


class TestStoreWithoutPublishedRoot:
    def test_create_artifact_store_runs_after_base_initialization(self):
        bundle = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=_VersionedBucket())

        assert bundle.published_root == f"memory://{BUNDLE_NAME}"

    def test_publishes_and_serves_a_release_with_object_version_ids(self, tmp_path):
        bucket = _VersionedBucket()
        bundle = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=bucket)

        result = _publish(bundle, _write_source(tmp_path / "source"), "object-version-1")
        bundle.refresh()

        committed = json.loads(bucket.manifests[result.version])
        assert committed["backend"] == {"type": "memory"}
        assert [file_info["version_id"] for file_info in committed["files"]] == ["object-version-1"]
        assert json.loads(bucket.ref) == result.ref_payload
        assert (bundle.path / DAG_FILE).read_text() == "print('dag')"

    def test_publish_still_rejects_a_source_overlapping_the_cache(self, tmp_path):
        bundle = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=_VersionedBucket())
        source = _write_source(bundle.versions_dir / "publisher-source")

        with pytest.raises(ValueError, match="source_path and versions_dir must not overlap"):
            _publish(bundle, source, "object-version-1")

    def test_republish_with_new_object_version_ids_keeps_the_committed_manifest(self, tmp_path):
        bucket = _VersionedBucket()
        bundle = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=bucket)
        source = _write_source(tmp_path / "source")
        first = _publish(bundle, source, "object-version-1")

        second = _publish(bundle, source, "object-version-2")
        bundle.refresh()

        assert second.version == first.version
        assert second.created_snapshot is False
        assert second.ref_payload == first.ref_payload
        assert json.loads(bucket.ref) == first.ref_payload
        assert (bundle.path / DAG_FILE).read_text() == "print('dag')"

    def test_publish_rejects_a_committed_manifest_for_another_version(self, tmp_path):
        bucket = _VersionedBucket()
        bundle = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=bucket)
        source = _write_source(tmp_path / "source")
        version = build_bundle_version_manifest(
            bundle_name=BUNDLE_NAME, root=source, backend_type="memory"
        ).version
        other_manifest = build_bundle_version_manifest(
            bundle_name=BUNDLE_NAME,
            root=_write_source(tmp_path / "other-source", "print('other')"),
            backend_type="memory",
            file_version_ids={DAG_FILE: "object-version-0"},
        ).manifest
        # The store hands back whatever is committed under the version without checking it.
        bucket.manifests[version] = serialize_bundle_version_manifest(other_manifest)

        with pytest.raises(BundleManifestError, match="Bundle manifest contains version"):
            _publish(bundle, source, "object-version-1")

        assert bucket.ref is None

    def test_concurrent_reference_update_is_reported_as_a_reference_change(self, tmp_path):
        bucket = _VersionedBucket()
        bundle = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=bucket)
        rival = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=bucket)
        first = _publish(bundle, _write_source(tmp_path / "source"), "object-version-1")
        rival_source = _write_source(tmp_path / "rival-source", "print('rival')")
        rival_results: list[BundlePublishResult] = []

        def publish_rival_first(prepared: PreparedPublishSource) -> None:
            rival_results.append(_publish(rival, rival_source, "object-version-2"))

        with (
            mock.patch.object(
                bundle, "confirm_publish_source", autospec=True, side_effect=publish_rival_first
            ),
            pytest.raises(
                BundleManifestReferenceChangedError,
                match="another publisher updated the release concurrently",
            ) as raised,
        ):
            _publish(
                bundle,
                _write_source(tmp_path / "next-source", "print('next')"),
                "object-version-3",
                expected_current_version=first.version,
            )

        assert isinstance(raised.value.__cause__, ArtifactStoreConflictError)
        assert json.loads(bucket.ref) == rival_results[0].ref_payload


class TestFilesystemStoreRepublish:
    def test_existing_snapshot_is_checked_against_its_version_not_the_new_manifest(self, tmp_path):
        published_root = tmp_path / "published"
        published_root.mkdir()
        bundle = ManifestLocalDagBundle(name=BUNDLE_NAME, published_root=str(published_root))
        source = _write_source(tmp_path / "source")
        first = _publish(bundle, source, "object-version-1")

        second = _publish(bundle, source, "object-version-2")

        assert second.created_snapshot is False
        assert second.ref_payload == first.ref_payload
        committed = json.loads((Path(first.version_path) / MANIFEST_FILE_NAME).read_text())
        assert [file_info["version_id"] for file_info in committed["files"]] == ["object-version-1"]
