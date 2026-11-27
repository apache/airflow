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
from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from airflow.dag_processing.bundles.artifact_store import (
    ArtifactStoreConflictError,
    BundleManifestNotFoundError,
    BundleManifestRefDurabilityError,
)
from airflow.dag_processing.bundles.database_artifact_store import DatabaseArtifactStore
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    build_bundle_version_manifest,
    build_ref_payload_from_manifest,
    collect_bundle_source_snapshot,
)
from airflow.dag_processing.bundles.manifest_bundle import (
    BundleManifestReferenceChangedError,
    BundlePublishResult,
    ManifestDagBundleBase,
    PreparedPublishSource,
    publish_prepared_manifest_dag_bundle,
)
from airflow.models.dag_bundle_manifest import DagBundleManifest, DagBundleManifestRef

pytestmark = [pytest.mark.db_test, pytest.mark.usefixtures("isolate_bundle_storage", "clear_manifest_tables")]

BUNDLE_NAME = "manifest-database"
BACKEND_TYPE = "versioned-objects"
DAG_FILE = "dags/example.py"


@pytest.fixture
def clear_manifest_tables(session):
    def clear():
        session.execute(delete(DagBundleManifestRef))
        session.execute(delete(DagBundleManifest))
        session.commit()

    clear()
    yield
    clear()


class _VersionedObjects:
    """Object versions of a versioned bucket, keyed by path and then by version id."""

    def __init__(self) -> None:
        self.objects: dict[str, dict[str, bytes]] = {}

    def put(self, source_root: Path, relative_path: str, version_id: str) -> None:
        self.objects.setdefault(relative_path, {})[version_id] = (source_root / relative_path).read_bytes()

    def fetch_file(self, relative_path: str, version_id: str | None, destination: Path) -> None:
        if version_id is None:
            raise AssertionError(f"Manifest has no version id for {relative_path}")
        destination.write_bytes(self.objects[relative_path][version_id])


class _DatabaseManifestDagBundle(ManifestDagBundleBase):
    def __init__(self, *, objects: _VersionedObjects, **kwargs) -> None:
        self.objects = objects
        super().__init__(**kwargs)

    def create_artifact_store(self) -> DatabaseArtifactStore:
        return DatabaseArtifactStore(
            bundle_name=self.name,
            backend_type=BACKEND_TYPE,
            fetch_file=self.objects.fetch_file,
            version_data=self.version_data,
        )

    def confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        pass


def _write_source(root: Path, content: str = "print('dag')") -> Path:
    path = root / DAG_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return root


def _publish(
    bundle: _DatabaseManifestDagBundle,
    source: Path,
    version_id: str,
    *,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    bundle.objects.put(source, DAG_FILE, version_id)
    return publish_prepared_manifest_dag_bundle(
        bundle=bundle,
        prepared_source=PreparedPublishSource(
            root=source,
            source_snapshot=collect_bundle_source_snapshot(source),
            file_version_ids={DAG_FILE: version_id},
        ),
        expected_current_version=expected_current_version,
    )


def _build_manifest(source: Path, version_id: str) -> dict[str, Any]:
    return build_bundle_version_manifest(
        bundle_name=BUNDLE_NAME,
        root=source,
        backend_type=BACKEND_TYPE,
        file_version_ids={DAG_FILE: version_id},
    ).manifest


def _make_store(objects: _VersionedObjects | None = None, **kwargs) -> DatabaseArtifactStore:
    return DatabaseArtifactStore(
        bundle_name=BUNDLE_NAME,
        backend_type=BACKEND_TYPE,
        fetch_file=(objects or _VersionedObjects()).fetch_file,
        **kwargs,
    )


class TestPublishAndServe:
    def test_publishes_to_the_database_and_serves_the_release(self, tmp_path, session):
        bundle = _DatabaseManifestDagBundle(name=BUNDLE_NAME, objects=_VersionedObjects())

        result = _publish(bundle, _write_source(tmp_path / "source"), "object-version-1")
        bundle.refresh()

        stored = session.scalar(select(DagBundleManifest.manifest))
        assert [file_info["version_id"] for file_info in stored["files"]] == ["object-version-1"]
        assert session.scalar(select(DagBundleManifestRef.payload)) == result.ref_payload
        assert session.scalar(select(DagBundleManifestRef.version)) == result.version
        assert result.created_snapshot is True
        assert (bundle.path / DAG_FILE).read_text() == "print('dag')"

    def test_replacing_the_reference_updates_its_version_column(self, tmp_path, session):
        bundle = _DatabaseManifestDagBundle(name=BUNDLE_NAME, objects=_VersionedObjects())
        first = _publish(bundle, _write_source(tmp_path / "first"), "object-version-1")

        second = _publish(
            bundle,
            _write_source(tmp_path / "second", "print('second')"),
            "object-version-2",
            expected_current_version=first.version,
        )

        assert second.version != first.version
        assert session.scalar(select(DagBundleManifestRef.version)) == second.version

    def test_pinned_bundle_materializes_from_version_data_without_the_database(self, tmp_path, session):
        objects = _VersionedObjects()
        result = _publish(
            _DatabaseManifestDagBundle(name=BUNDLE_NAME, objects=objects),
            _write_source(tmp_path / "source"),
            "object-version-1",
        )
        manifest = session.scalar(select(DagBundleManifest.manifest))
        pinned = _DatabaseManifestDagBundle(
            name=BUNDLE_NAME, objects=objects, version=result.version, version_data=manifest
        )

        with mock.patch(
            "airflow.dag_processing.bundles.database_artifact_store.create_session",
            side_effect=AssertionError("a pinned worker has no database access"),
        ):
            pinned.initialize()

        assert (pinned.path / DAG_FILE).read_text() == "print('dag')"

    def test_pinned_bundle_without_version_data_reads_the_database(self, tmp_path):
        objects = _VersionedObjects()
        result = _publish(
            _DatabaseManifestDagBundle(name=BUNDLE_NAME, objects=objects),
            _write_source(tmp_path / "source"),
            "object-version-1",
        )
        pinned = _DatabaseManifestDagBundle(name=BUNDLE_NAME, objects=objects, version=result.version)

        pinned.initialize()

        assert (pinned.path / DAG_FILE).read_text() == "print('dag')"

    def test_republish_with_new_object_version_ids_keeps_the_committed_manifest(self, tmp_path, session):
        bundle = _DatabaseManifestDagBundle(name=BUNDLE_NAME, objects=_VersionedObjects())
        source = _write_source(tmp_path / "source")
        first = _publish(bundle, source, "object-version-1")

        second = _publish(bundle, source, "object-version-2")

        assert second.version == first.version
        assert second.created_snapshot is False
        stored = session.scalar(select(DagBundleManifest.manifest))
        assert [file_info["version_id"] for file_info in stored["files"]] == ["object-version-1"]

    def test_concurrent_reference_update_is_reported_as_a_reference_change(self, tmp_path, session):
        objects = _VersionedObjects()
        bundle = _DatabaseManifestDagBundle(name=BUNDLE_NAME, objects=objects)
        rival = _DatabaseManifestDagBundle(name=BUNDLE_NAME, objects=objects)
        first = _publish(bundle, _write_source(tmp_path / "source"), "object-version-1")
        rival_source = _write_source(tmp_path / "rival-source", "print('rival')")
        rival_results: list[BundlePublishResult] = []

        def publish_rival_first(prepared: PreparedPublishSource) -> None:
            rival_results.append(_publish(rival, rival_source, "object-version-2"))

        with (
            mock.patch.object(
                bundle, "confirm_publish_source", autospec=True, side_effect=publish_rival_first
            ),
            pytest.raises(BundleManifestReferenceChangedError) as raised,
        ):
            _publish(
                bundle,
                _write_source(tmp_path / "next-source", "print('next')"),
                "object-version-3",
                expected_current_version=first.version,
            )

        assert isinstance(raised.value.__cause__, ArtifactStoreConflictError)
        assert session.scalar(select(DagBundleManifestRef.payload)) == rival_results[0].ref_payload


class TestWriteRef:
    @pytest.mark.parametrize("rival_has_published_before", [False, True], ids=["first-release", "update"])
    def test_rival_write_after_the_guarded_read_conflicts(self, tmp_path, rival_has_published_before):
        store, rival = _make_store(), _make_store()
        ours = build_ref_payload_from_manifest(_build_manifest(_write_source(tmp_path / "ours"), "v1"))
        theirs = build_ref_payload_from_manifest(
            _build_manifest(_write_source(tmp_path / "theirs", "print('theirs')"), "v2")
        )
        if rival_has_published_before:
            with rival.publication_guard():
                rival.write_ref(theirs)

        with store.publication_guard():
            with contextlib.suppress(BundleManifestNotFoundError):
                store.read_ref()
            with rival.publication_guard():
                rival.write_ref({**theirs, "file_count": 99})
            with pytest.raises(ArtifactStoreConflictError):
                store.write_ref(ours)

        assert store.read_ref() == {**theirs, "file_count": 99}

    def test_rival_writing_identical_content_is_not_a_conflict(self, tmp_path):
        store, rival = _make_store(), _make_store()
        payload = build_ref_payload_from_manifest(_build_manifest(_write_source(tmp_path), "v1"))

        with store.publication_guard():
            with pytest.raises(BundleManifestNotFoundError):
                store.read_ref()
            with rival.publication_guard():
                rival.write_ref(payload)
            store.write_ref(payload)
            store.write_ref({**payload, "file_count": 2})

        assert store.read_ref() == {**payload, "file_count": 2}

    @pytest.mark.parametrize(
        ("payload", "expected_version"),
        [
            pytest.param({"version": "sha256:" + "0" * 64}, "sha256:" + "0" * 64, id="string-version"),
            pytest.param({"version": 1}, None, id="non-string-version"),
        ],
    )
    def test_uncertain_commit_raises_durability_error(self, payload, expected_version):
        store = _make_store()
        session = mock.MagicMock(spec=Session)
        session.commit.side_effect = OperationalError("COMMIT", {}, Exception("connection lost"))

        with (
            mock.patch.object(store, "_read_ref_row", autospec=True, return_value=None),
            mock.patch(
                "airflow.dag_processing.bundles.database_artifact_store.create_session",
                return_value=mock.MagicMock(__enter__=mock.Mock(return_value=session)),
            ),
            pytest.raises(BundleManifestRefDurabilityError) as raised,
        ):
            store.write_ref(payload)

        assert raised.value.version == expected_version


class TestStoreReads:
    def test_read_ref_without_a_reference_raises_not_found(self):
        with pytest.raises(BundleManifestNotFoundError, match="has no release reference"):
            _make_store().read_ref()

    def test_read_ref_rejects_a_payload_that_is_not_an_object(self, session):
        session.add(
            DagBundleManifestRef(bundle_name=BUNDLE_NAME, payload=["not", "an", "object"], revision=1)
        )
        session.commit()

        with pytest.raises(BundleManifestError, match="is not a JSON object"):
            _make_store().read_ref()

    @pytest.mark.backend("sqlite")  # The other backends reject invalid JSON on insert.
    def test_read_ref_reports_stored_text_that_is_not_json_as_a_bundle_error(self, session):
        session.execute(
            text(
                "INSERT INTO dag_bundle_manifest_ref (bundle_name, payload, revision, updated_at) "
                "VALUES (:name, 'not json', 1, CURRENT_TIMESTAMP)"
            ),
            {"name": BUNDLE_NAME},
        )
        session.commit()

        with pytest.raises(BundleManifestError, match="Cannot read release reference") as raised:
            _make_store().read_ref()

        assert isinstance(raised.value.__cause__, json.JSONDecodeError)

    def test_database_error_is_reported_as_a_bundle_error(self):
        with (
            mock.patch(
                "airflow.dag_processing.bundles.database_artifact_store.create_session",
                side_effect=OperationalError("SELECT", {}, Exception("database is down")),
            ),
            pytest.raises(BundleManifestError, match="Cannot read release reference"),
        ):
            _make_store().read_ref()

    def test_snapshot_exists(self, tmp_path):
        store = _make_store()
        manifest = _build_manifest(_write_source(tmp_path), "v1")

        assert store.snapshot_exists(manifest["version"]) is False
        store.publish_snapshot(manifest=manifest, source_root=tmp_path, validate_existing=mock.Mock())
        assert store.snapshot_exists(manifest["version"]) is True


class TestFetchSnapshot:
    def test_fetches_files_with_executable_bits_and_the_manifest(self, tmp_path):
        source = _write_source(tmp_path / "source")
        (source / "run.sh").write_text("echo hi")
        (source / "run.sh").chmod(0o755)
        objects = _VersionedObjects()
        objects.put(source, DAG_FILE, "v1")
        objects.put(source, "run.sh", "v1")
        manifest = build_bundle_version_manifest(
            bundle_name=BUNDLE_NAME,
            root=source,
            backend_type=BACKEND_TYPE,
            file_version_ids={DAG_FILE: "v1", "run.sh": "v1"},
        ).manifest
        destination = tmp_path / "destination"

        _make_store(objects, version_data=manifest).fetch_snapshot(
            manifest["version"], destination, structural_validator=mock.Mock()
        )

        assert (destination / DAG_FILE).read_text() == "print('dag')"
        assert (destination / "run.sh").stat().st_mode & 0o111
        assert not (destination / DAG_FILE).stat().st_mode & 0o111
        assert (destination / MANIFEST_FILE_NAME).is_file()

    def test_version_data_for_another_version_is_not_used(self, tmp_path):
        manifest = _build_manifest(_write_source(tmp_path), "v1")

        with pytest.raises(BundleManifestNotFoundError, match="is not published"):
            _make_store(version_data=manifest).fetch_snapshot(
                "sha256:" + "0" * 64, tmp_path / "destination", structural_validator=mock.Mock()
            )

    def test_rejects_a_manifest_that_does_not_hash_to_the_version(self, tmp_path):
        manifest = _build_manifest(_write_source(tmp_path), "v1")
        tampered = {**manifest, "files": [{**manifest["files"][0], "size": 1}], "total_size": 1}

        with pytest.raises(BundleManifestError, match="does not match its contents"):
            _make_store(version_data=tampered).fetch_snapshot(
                manifest["version"], tmp_path / "destination", structural_validator=mock.Mock()
            )

    def test_reports_a_failed_file_fetch_as_a_bundle_error(self, tmp_path):
        manifest = _build_manifest(_write_source(tmp_path / "source"), "v1")
        fetch_file = mock.Mock(side_effect=KeyError("v1"))
        store = DatabaseArtifactStore(
            bundle_name=BUNDLE_NAME, backend_type=BACKEND_TYPE, fetch_file=fetch_file, version_data=manifest
        )

        with pytest.raises(BundleManifestError, match=r"Cannot fetch 'dags/example.py'.*'v1'") as raised:
            store.fetch_snapshot(
                manifest["version"], tmp_path / "destination", structural_validator=mock.Mock()
            )

        assert isinstance(raised.value.__cause__, KeyError)

    def test_rejects_a_non_empty_destination(self, tmp_path):
        manifest = _build_manifest(_write_source(tmp_path / "source"), "v1")
        destination = tmp_path / "destination"
        destination.mkdir()
        (destination / "leftover").touch()

        with pytest.raises(ValueError, match="must be absent or empty"):
            _make_store(version_data=manifest).fetch_snapshot(
                manifest["version"], destination, structural_validator=mock.Mock()
            )
