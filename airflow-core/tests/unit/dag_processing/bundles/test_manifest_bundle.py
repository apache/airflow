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
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
import time_machine

from airflow.dag_processing.bundles.artifact_store import (
    ArtifactStore,
    ArtifactStoreConflictError,
    BundleManifestNotFoundError,
    PublishedSnapshot,
)
from airflow.dag_processing.bundles.base import BundleNotReadyError
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestSourceChangedError,
    build_bundle_version_manifest,
    collect_bundle_source_snapshot,
    serialize_bundle_version_manifest,
    validate_bundle_relative_path,
)
from airflow.dag_processing.bundles.manifest_bundle import (
    AUTO_PUBLISH_STATE_SCHEMA_VERSION,
    BundleManifestRef,
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
    """What a versioned object store holds: object versions, snapshot manifests and the two documents."""

    objects: dict[str, dict[str, bytes]] = field(default_factory=dict)
    manifests: dict[str, bytes] = field(default_factory=dict)
    ref: bytes | None = None
    ref_generation: int = 0
    state: bytes | None = None
    state_generation: int = 0


class _InMemoryArtifactStore(ArtifactStore):
    """
    Store with no published root that fetches files by object version id.

    Its guard is not a lock: entering it resets the compare-and-swap baselines, so ``write_ref``
    and ``write_state`` are conditioned on the document read inside the guard, or on the current
    one when there was no such read.
    """

    backend_type = "memory"

    def __init__(self, *, bundle_name: str, bucket: _VersionedBucket) -> None:
        self.bundle_name = bundle_name
        self._bucket = bucket
        self._ref_baseline: int | None = None
        self._state_baseline: int | None = None

    @property
    def ref_locator(self) -> str:
        return f"memory://{self.bundle_name}/latest.json"

    @property
    def state_locator(self) -> str:
        return f"memory://{self.bundle_name}/auto-publish.json"

    def snapshot_locator(self, version: str) -> str:
        return f"memory://{self.bundle_name}/versions/{version}"

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
            raise ArtifactStoreConflictError(f"Another publisher updated {self.ref_locator}")
        self._bucket.ref = content
        self._bucket.ref_generation += 1

    def read_state(self) -> dict[str, Any]:
        # Set even when there is no state, so the next write requires it to stay absent.
        self._state_baseline = self._bucket.state_generation
        if self._bucket.state is None:
            raise BundleManifestNotFoundError(
                f"Bundle '{self.bundle_name}' has no automatic-publication state"
            )
        return json.loads(self._bucket.state)

    def write_state(self, payload: dict[str, Any]) -> None:
        content = serialize_bundle_version_manifest(payload)
        baseline = self._bucket.state_generation if self._state_baseline is None else self._state_baseline
        if self._bucket.state_generation != baseline and self._bucket.state != content:
            raise ArtifactStoreConflictError(f"Another publisher updated {self.state_locator}")
        self._bucket.state = content
        self._bucket.state_generation += 1

    @contextmanager
    def publication_guard(self, *, wait: bool = True):
        self._ref_baseline = None
        self._state_baseline = None
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


class _AutoPublishingMemoryManifestDagBundle(_MemoryManifestDagBundle):
    """Publishes its local ``source`` automatically, recording one object version id per file."""

    def __init__(self, *, source: Path, **kwargs) -> None:
        self.source = source
        super().__init__(**kwargs)

    @property
    def has_publish_source(self) -> bool:
        return True

    @property
    def publish_source_description(self) -> str:
        return str(self.source)

    def prepare_publish_source(self) -> PreparedPublishSource:
        return PreparedPublishSource(
            root=self.source,
            source_snapshot=collect_bundle_source_snapshot(self.source),
            file_version_ids={DAG_FILE: "object-version-auto"},
            source_type="memory-test",
            source_identity=f"sha256:{self.source.name}",
        )


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

        assert bundle._store.bundle_name == BUNDLE_NAME

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

    def test_automatic_publication_records_object_version_ids(self, tmp_path):
        bucket = _VersionedBucket()
        bundle = _AutoPublishingMemoryManifestDagBundle(
            name=BUNDLE_NAME,
            bucket=bucket,
            source=_write_source(tmp_path / "source"),
            source_stability_seconds=0,
        )

        bundle.initialize()

        committed = json.loads(bucket.manifests[bundle.get_current_version().version])
        assert [file_info["version_id"] for file_info in committed["files"]] == ["object-version-auto"]
        assert (bundle.path / DAG_FILE).read_text() == "print('dag')"


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
        committed = json.loads(
            (published_root / "versions" / BUNDLE_NAME / first.version / MANIFEST_FILE_NAME).read_text()
        )
        assert [file_info["version_id"] for file_info in committed["files"]] == ["object-version-1"]


class TestAutomaticPublicationConflicts:
    """A store without a lock reports a lost race as a conflict; the publisher must reconcile it."""

    def test_publisher_follows_a_release_that_won_the_reference_race(self, tmp_path, caplog):
        bucket = _VersionedBucket()
        _publish(
            _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=bucket),
            _write_source(tmp_path / "released"),
            "object-version-1",
        )
        bundle = _AutoPublishingMemoryManifestDagBundle(
            name=BUNDLE_NAME,
            bucket=bucket,
            source=_write_source(tmp_path / "source", "print('next')"),
            source_stability_seconds=0,
        )
        rival = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=bucket)
        rival_source = _write_source(tmp_path / "rival-source", "print('rival')")
        rival_results: list[BundlePublishResult] = []
        publish_snapshot = bundle._store.publish_snapshot

        def publish_rival_first(**kwargs):
            rival_results.append(_publish(rival, rival_source, "object-version-2"))
            return publish_snapshot(**kwargs)

        with mock.patch.object(
            bundle._store, "publish_snapshot", autospec=True, side_effect=publish_rival_first
        ):
            bundle.refresh()

        assert json.loads(bucket.ref) == rival_results[0].ref_payload
        assert bundle.get_current_version().version == rival_results[0].version
        assert (bundle.path / DAG_FILE).read_text() == "print('rival')"
        assert {
            "event": "Another publisher updated the release concurrently; following its release. "
            f"bundle={BUNDLE_NAME}",
            "log_level": "info",
        } in caplog

    @pytest.mark.parametrize(
        ("winner_overrides", "expected_error", "match"),
        [
            pytest.param({}, None, None, id="same-observation"),
            pytest.param(
                {"source_identity": "sha256:other"},
                BundleManifestError,
                "do not use the same memory-test source",
                id="other-source",
            ),
            pytest.param(
                {"source_signature": "sha256:newer"},
                BundleManifestSourceChangedError,
                "candidate changed",
                id="other-signature",
            ),
        ],
    )
    def test_candidate_race_lost_to_another_replica(self, tmp_path, winner_overrides, expected_error, match):
        bucket = _VersionedBucket()
        bundle = _AutoPublishingMemoryManifestDagBundle(
            name=BUNDLE_NAME,
            bucket=bucket,
            source=_write_source(tmp_path / "source"),
            source_stability_seconds=30,
        )
        prepared = bundle.prepare_publish_source()

        def build_state(**overrides) -> dict[str, Any]:
            return {
                "schema_version": AUTO_PUBLISH_STATE_SCHEMA_VERSION,
                "bundle_name": BUNDLE_NAME,
                "source_type": prepared.source_type,
                "source_identity": prepared.source_identity,
                "source_signature": prepared.source_snapshot.signature,
                "first_observed_at": 90,
                **overrides,
            }

        def write_state(payload: dict[str, Any]) -> None:
            bucket.state = serialize_bundle_version_manifest(payload)
            bucket.state_generation += 1

        write_state(build_state(source_signature="sha256:earlier", first_observed_at=10))
        winner = build_state(**winner_overrides)

        # Confirming the source falls between the guard's state read and the candidate write.
        with (
            mock.patch.object(
                bundle,
                "confirm_publish_source",
                autospec=True,
                side_effect=lambda prepared: write_state(winner),
            ),
            time_machine.travel(100, tick=False),
        ):
            if expected_error is None:
                assert bundle._get_auto_publish_candidate_readiness(prepared) == (False, 20)
            else:
                with pytest.raises(expected_error, match=match):
                    bundle._get_auto_publish_candidate_readiness(prepared)

        assert json.loads(bucket.state) == winner

    def test_store_that_misreports_a_reference_conflict_is_reported(self, tmp_path):
        bucket = _VersionedBucket()
        _publish(
            _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=bucket),
            _write_source(tmp_path / "released"),
            "object-version-1",
        )
        bundle = _AutoPublishingMemoryManifestDagBundle(
            name=BUNDLE_NAME,
            bucket=bucket,
            source=_write_source(tmp_path / "source", "print('next')"),
            source_stability_seconds=0,
        )

        with (
            mock.patch.object(
                bundle._store,
                "write_ref",
                autospec=True,
                side_effect=ArtifactStoreConflictError("lost the race"),
            ),
            pytest.raises(BundleManifestError, match="release reference .* is unchanged; the store's"),
        ):
            bundle._publish_from_source_if_ready(bundle._read_current_manifest_ref_or_none())

    def test_store_that_misreports_a_state_conflict_is_reported(self, tmp_path):
        bundle = _AutoPublishingMemoryManifestDagBundle(
            name=BUNDLE_NAME,
            bucket=_VersionedBucket(),
            source=_write_source(tmp_path / "source"),
            source_stability_seconds=30,
        )

        with (
            mock.patch.object(
                bundle._store,
                "write_state",
                autospec=True,
                side_effect=ArtifactStoreConflictError("lost the race"),
            ),
            pytest.raises(
                BundleManifestError, match="automatic-publication state .* is unchanged; the store's"
            ),
        ):
            bundle._get_auto_publish_candidate_readiness(bundle.prepare_publish_source())

    def test_first_publication_whose_winning_release_vanished_reports_not_ready(self, tmp_path):
        bucket = _VersionedBucket()
        bundle = _AutoPublishingMemoryManifestDagBundle(
            name=BUNDLE_NAME,
            bucket=bucket,
            source=_write_source(tmp_path / "source"),
            source_stability_seconds=30,
        )
        prepared = bundle.prepare_publish_source()
        # A candidate that has been stable for long enough, so that publication goes ahead.
        bucket.state = serialize_bundle_version_manifest(
            {
                "schema_version": AUTO_PUBLISH_STATE_SCHEMA_VERSION,
                "bundle_name": BUNDLE_NAME,
                "source_type": prepared.source_type,
                "source_identity": prepared.source_identity,
                "source_signature": prepared.source_snapshot.signature,
                "first_observed_at": 0,
            }
        )
        winner = BundleManifestRef(version="sha256-" + "0" * 64, ref_payload={}, manifest_sha256="0")

        with (
            mock.patch.object(
                bundle._store, "write_ref", autospec=True, side_effect=ArtifactStoreConflictError("lost")
            ),
            # Read by refresh, inside the guard before the write, after the conflict, and again
            # by the handler that follows the winner, which is gone by then.
            mock.patch.object(
                bundle,
                "_read_current_manifest_ref_or_none",
                autospec=True,
                side_effect=[None, None, winner, None],
            ),
            pytest.raises(
                BundleNotReadyError, match="updated the release reference .* concurrently"
            ) as raised,
        ):
            bundle.initialize()

        assert raised.value.retry_after == 0
        assert isinstance(raised.value.__cause__, ArtifactStoreConflictError)


class TestAutomaticPublicationSource:
    @pytest.mark.parametrize("missing", ["source_type", "source_identity"])
    def test_publish_source_must_name_its_type_and_identity(self, tmp_path, missing):
        bundle = _AutoPublishingMemoryManifestDagBundle(
            name=BUNDLE_NAME,
            bucket=_VersionedBucket(),
            source=_write_source(tmp_path / "source"),
            source_stability_seconds=0,
        )
        prepared = replace(bundle.prepare_publish_source(), **{missing: ""})

        with (
            mock.patch.object(bundle, "prepare_publish_source", autospec=True, return_value=prepared),
            pytest.raises(BundleManifestError, match="without a source_type and source_identity"),
        ):
            bundle.initialize()

        assert bundle.bucket.state is None

    def test_bundle_without_publish_source_has_none_to_prepare(self):
        bundle = _MemoryManifestDagBundle(name=BUNDLE_NAME, bucket=_VersionedBucket())

        assert bundle.has_publish_source is False
        with pytest.raises(NotImplementedError, match="has no source for automatic publication"):
            bundle.prepare_publish_source()
