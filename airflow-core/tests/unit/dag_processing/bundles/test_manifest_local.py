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

import gc
import importlib
import json
import os
import shutil
import stat
import sys
from importlib.util import cache_from_source
from pathlib import Path
from unittest import mock

import pytest

from airflow.dag_processing.bundles import (
    filesystem_artifact_store as store_module,
    manifest_bundle as bundle_module,
)
from airflow.dag_processing.bundles.artifact_store import BundleManifestNotFoundError
from airflow.dag_processing.bundles.base import (
    BundleUsageTrackingManager,
    BundleVersion,
    get_bundle_tracking_file,
)
from airflow.dag_processing.bundles.filesystem_artifact_store import remove_bundle_tree_forcefully
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    MANIFEST_SCHEMA_VERSION,
    BundleManifestError,
    BundleManifestSourceChangedError,
    build_bundle_version_manifest,
    serialize_bundle_version_manifest,
)
from airflow.dag_processing.bundles.manifest_bundle import BundleManifestReferenceChangedError
from airflow.dag_processing.bundles.manifest_local import (
    ManifestLocalDagBundle,
    publish_manifest_local_dag_bundle,
)
from airflow.exceptions import AirflowException

from tests_common.test_utils.config import conf_vars

BUNDLE_NAME = "manifest-local"


def _write_file(root: Path, relative_path: str, content: str) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _write_manifest_ref(manifest_ref_path: Path | str, ref_payload: dict) -> None:
    manifest_ref_path = Path(manifest_ref_path)
    manifest_ref_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_ref_path.write_bytes(serialize_bundle_version_manifest(ref_payload))


def _add_write_bits(path: Path) -> None:
    write_bits = 0o700 if path.is_dir() else 0o600
    path.chmod(stat.S_IMODE(path.stat().st_mode) | write_bits)


def _create_symlink_or_skip(target: Path, link: Path, *, target_is_directory: bool = False) -> None:
    try:
        link.symlink_to(target, target_is_directory=target_is_directory)
    except OSError as e:
        pytest.skip(f"Symlinks are not supported in this test environment: {e}")


def _make_bundle(tmp_path: Path, **kwargs) -> ManifestLocalDagBundle:
    return ManifestLocalDagBundle(name=BUNDLE_NAME, published_root=str(tmp_path / "published"), **kwargs)


def _publish(bundle: ManifestLocalDagBundle, source: Path, *, materialize_snapshot: bool = True):
    if materialize_snapshot:
        return publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

    result = build_bundle_version_manifest(bundle_name=bundle.name, root=source, backend_type="local")
    _write_manifest_ref(bundle.manifest_ref_path, result.ref_payload)
    return result


def _published_bundle(tmp_path: Path, **bundle_kwargs):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    bundle = _make_bundle(tmp_path, **bundle_kwargs)
    return bundle, source, _publish(bundle, source)


def _call_entry_point(bundle: ManifestLocalDagBundle, entry_point: str):
    if entry_point == "path":
        return bundle.path
    return getattr(bundle, entry_point)()


@pytest.fixture(autouse=True)
def _isolate_bundle_storage(tmp_path):
    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        # The tracking folder is resolved once at import time, not from the config above.
        mock.patch(
            "airflow.dag_processing.bundles.base.STALE_BUNDLE_TRACKING_FOLDER", tmp_path / "bundles/_tracking"
        ),
    ):
        yield


class TestConstruction:
    def test_supports_versioning(self):
        assert ManifestLocalDagBundle.supports_versioning is True

    @pytest.mark.parametrize("published_root", [None, ""])
    def test_requires_published_root(self, published_root):
        # TypeError, not ValueError: callback bundle preparation treats a ValueError from
        # bundle construction as "bundle no longer configured", silently dropping callbacks.
        with pytest.raises(TypeError, match="published_root must be provided"):
            ManifestLocalDagBundle(name=BUNDLE_NAME, published_root=published_root)

    @pytest.mark.parametrize(
        "published_root", ["s3://bucket/releases", "s3:/bucket/releases", "gs://bucket", "s3:releases"]
    )
    def test_rejects_url_shaped_published_root(self, published_root):
        with pytest.raises(TypeError, match="Unsupported published_root scheme"):
            ManifestLocalDagBundle(name=BUNDLE_NAME, published_root=published_root)

    @pytest.mark.parametrize("allow_empty_source", ["true", 1, None])
    def test_rejects_non_boolean_allow_empty_source(self, tmp_path, allow_empty_source):
        with pytest.raises(TypeError, match="allow_empty_source must be a boolean"):
            _make_bundle(tmp_path, allow_empty_source=allow_empty_source)

    def test_published_root_derives_authoritative_paths(self, tmp_path):
        bundle = _make_bundle(tmp_path)

        assert bundle.published_root == tmp_path / "published"
        assert bundle.manifest_ref_path == tmp_path / "published/refs/manifest-local/latest.json"
        assert bundle.published_versions_dir == tmp_path / "published/versions/manifest-local"
        assert bundle.publication_lock_path == tmp_path / "published/_locks/manifest-local.lock"

    @pytest.mark.parametrize("published_root_position", ["inside", "ancestor"])
    def test_rejects_published_root_overlapping_airflow_cache(self, tmp_path, published_root_position):
        cache_root = tmp_path / "cache"
        published_root = cache_root / "published" if published_root_position == "inside" else tmp_path

        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(cache_root)}),
            pytest.raises(TypeError, match="must not overlap Airflow's bundle cache"),
        ):
            ManifestLocalDagBundle(name=BUNDLE_NAME, published_root=str(published_root))


class TestConsumer:
    def test_get_current_version_returns_bundle_version_from_ref(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)

        # The version string is the whole contract: no metadata flows through Airflow.
        assert bundle.get_current_version() == BundleVersion(version=published.version, data=None)

    def test_pinned_get_current_version_does_not_read_ref(self, tmp_path):
        version = f"sha256-{'a' * 64}"
        bundle = _make_bundle(tmp_path, version=version)

        assert bundle.get_current_version() == BundleVersion(version=version)

    def test_path_and_get_current_version_do_not_materialize_snapshots(self, tmp_path):
        missing_version = f"sha256-{'0' * 64}"
        _write_manifest_ref(
            tmp_path / "published/refs/manifest-local/latest.json",
            {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "bundle_name": BUNDLE_NAME,
                "version": missing_version,
                "backend": {"type": "local"},
                "manifest": {"path": MANIFEST_FILE_NAME, "sha256": f"sha256:{'1' * 64}"},
                "file_count": 0,
                "total_size": 0,
            },
        )
        bundle = _make_bundle(tmp_path)

        with mock.patch.object(
            ManifestLocalDagBundle, "_materialize_cached_version", autospec=True
        ) as mock_materialize:
            assert bundle.get_current_version().version == missing_version
            assert bundle.path == bundle.versions_dir / missing_version
        mock_materialize.assert_not_called()

        with pytest.raises(BundleManifestNotFoundError, match="not published"):
            bundle.initialize()

    def test_path_loads_materialized_snapshot_before_refresh(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)

        assert bundle._current_manifest_ref is None
        assert bundle.path == bundle.versions_dir / published.version

    def test_refresh_uses_materialized_snapshot_for_latest_path(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        _write_file(source, "dags/__pycache__/ignored.pyc", "compiled")
        bundle = _make_bundle(tmp_path)
        published = _publish(bundle, source)

        bundle.refresh()

        assert bundle.path == bundle.versions_dir / published.version
        assert bundle.path != source
        assert (bundle.path / "dags/example.py").read_text() == "print('dag')"
        assert not (bundle.path / "dags/example.py").stat().st_mode & 0o200
        assert stat.S_IMODE(bundle.path.stat().st_mode) == 0o755
        assert stat.S_IMODE((bundle.path / "dags").stat().st_mode) == 0o755
        assert not (bundle.path / "dags/__pycache__/ignored.pyc").exists()
        snapshot_manifest = json.loads((bundle.path / MANIFEST_FILE_NAME).read_text())
        assert snapshot_manifest["version"] == published.version
        assert [file_info["path"] for file_info in snapshot_manifest["files"]] == ["dags/example.py"]

    def test_initialize_materializes_current_release(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)

        bundle.initialize()

        assert bundle.is_initialized
        assert (bundle.versions_dir / published.version / "dags/example.py").read_text() == "print('dag')"

    def test_source_changes_without_ref_update_do_not_affect_get_current_version(self, tmp_path):
        bundle, source, published = _published_bundle(tmp_path)

        _write_file(source, "dags/example.py", "print('changed')")
        _write_file(source, "dags/new.py", "print('new')")
        fresh_bundle = _make_bundle(tmp_path)

        assert fresh_bundle.get_current_version().version == published.version
        fresh_bundle.refresh()
        assert fresh_bundle.path == fresh_bundle.versions_dir / published.version
        assert (fresh_bundle.path / "dags/example.py").read_text() == "print('dag')"
        assert not (fresh_bundle.path / "dags/new.py").exists()

    def test_ref_update_changes_bundle_version_when_snapshot_exists(self, tmp_path):
        bundle, source, first = _published_bundle(tmp_path)
        bundle.refresh()

        _write_file(source, "dags/example.py", "print('changed')")
        second = _publish(bundle, source)
        bundle.refresh()

        assert second.version != first.version
        assert bundle.get_current_version().version == second.version
        assert (bundle.path / "dags/example.py").read_text() == "print('changed')"

    def test_ref_update_before_snapshot_materialization_raises_clear_error(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        bundle = _make_bundle(tmp_path)
        published = _publish(bundle, source, materialize_snapshot=False)

        with pytest.raises(BundleManifestError, match="not published"):
            bundle.refresh()

        assert not (bundle.versions_dir / published.version).exists()

    @mock.patch.object(
        bundle_module, "compute_file_sha256", autospec=True, side_effect=bundle_module.compute_file_sha256
    )
    @mock.patch.object(
        ManifestLocalDagBundle,
        "_validate_snapshot_files",
        autospec=True,
        side_effect=ManifestLocalDagBundle._validate_snapshot_files,
    )
    def test_refresh_rechecks_structure_without_rehashing_validated_cache(
        self, mock_validate, mock_hash, tmp_path
    ):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        mock_validate.reset_mock()
        mock_hash.reset_mock()

        bundle.refresh()

        expected_path = bundle.versions_dir / published.version
        for _ in range(3):
            assert bundle.path == expected_path
        mock_validate.assert_called_once_with(
            bundle, mock.ANY, expected_path, check_content=False, allow_cached_bytecode=True
        )
        mock_hash.assert_not_called()

    def test_refresh_sweeps_orphaned_temp_snapshots_from_cache(self, tmp_path):
        bundle, _, _ = _published_bundle(tmp_path)
        bundle.versions_dir.mkdir(parents=True, exist_ok=True)
        orphan = bundle.versions_dir / f".sha256-{'a' * 64}.orphan"
        _write_file(orphan, "dags/leftover.py", "print('leftover')")

        bundle.refresh()

        assert not orphan.exists()
        assert (bundle.path / "dags/example.py").read_text() == "print('dag')"

    @mock.patch.object(
        store_module,
        "remove_bundle_tree_forcefully",
        autospec=True,
        side_effect=PermissionError("operation not permitted"),
    )
    def test_refresh_survives_undeletable_orphan_temp_snapshot(self, mock_remove, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.versions_dir.mkdir(parents=True, exist_ok=True)
        orphan = bundle.versions_dir / f".sha256-{'a' * 64}.orphan"
        _write_file(orphan, "dags/leftover.py", "print('leftover')")

        bundle.refresh()

        mock_remove.assert_called_once_with(orphan)
        assert orphan.exists()
        assert (bundle.versions_dir / published.version).is_dir()

    def test_orphaned_validation_markers_are_reaped(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.versions_dir.mkdir(parents=True, exist_ok=True)
        orphan_marker = bundle.versions_dir / f".sha256-{'b' * 64}.validated"
        orphan_marker.write_text(f"sha256-{'b' * 64}")

        bundle.refresh()

        assert not orphan_marker.exists()
        # The live version's marker must survive the sweep.
        assert bundle._get_validation_marker_path(published.version).exists()

    def test_cache_tree_and_marker_are_fsynced_in_durability_order(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        cached_version_path = bundle.versions_dir / published.version
        marker_path = bundle._get_validation_marker_path(published.version)
        calls: list[tuple[str, Path]] = []
        real_fsync_file = store_module.fsync_file
        real_fsync_directory = store_module.fsync_directory
        real_replace = os.replace

        def record_fsync_file(path):
            calls.append(("fsync_file", Path(path)))
            real_fsync_file(path)

        def record_fsync_directory(path):
            calls.append(("fsync_directory", Path(path)))
            real_fsync_directory(path)

        def record_replace(source_path, destination_path):
            if Path(destination_path) == cached_version_path:
                calls.append(("replace_cache", Path(destination_path)))
            real_replace(source_path, destination_path)

        with (
            mock.patch.object(store_module, "fsync_file", side_effect=record_fsync_file),
            mock.patch.object(bundle_module, "fsync_file", side_effect=record_fsync_file),
            mock.patch.object(bundle_module, "fsync_directory", side_effect=record_fsync_directory),
            mock.patch.object(bundle_module.os, "replace", side_effect=record_replace),
        ):
            bundle.refresh()

        replace_index = calls.index(("replace_cache", cached_version_path))
        marker_fsync_index = calls.index(("fsync_file", marker_path))
        cache_file_fsync_indices = [
            index
            for index, (operation, path) in enumerate(calls)
            if operation == "fsync_file" and path != marker_path
        ]
        versions_dir_fsync_indices = [
            index for index, call in enumerate(calls) if call == ("fsync_directory", bundle.versions_dir)
        ]

        assert cache_file_fsync_indices
        assert max(cache_file_fsync_indices) < replace_index
        assert any(replace_index < index < marker_fsync_index for index in versions_dir_fsync_indices)
        assert any(index > marker_fsync_index for index in versions_dir_fsync_indices)
        assert marker_path.read_text() == published.version


class TestPinned:
    def test_pinned_bundle_uses_materialized_snapshot(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        pinned_bundle = _make_bundle(tmp_path, version=published.version)

        pinned_bundle.initialize()

        assert pinned_bundle.get_current_version().version == published.version
        assert pinned_bundle.path == bundle.versions_dir / published.version
        assert (pinned_bundle.path / "dags/example.py").read_text() == "print('dag')"

    def test_pinned_bundle_rematerializes_deleted_cache(self, tmp_path):
        _, _, published = _published_bundle(tmp_path)
        pinned_bundle = _make_bundle(tmp_path, version=published.version)
        pinned_bundle.initialize()
        remove_bundle_tree_forcefully(pinned_bundle.path)

        fresh_pinned_bundle = _make_bundle(tmp_path, version=published.version)
        fresh_pinned_bundle.initialize()

        assert published.version_path.exists()
        assert (fresh_pinned_bundle.path / "dags/example.py").read_text() == "print('dag')"

    def test_pinned_bundle_rebuilds_corrupt_cache(self, tmp_path):
        _, _, published = _published_bundle(tmp_path)
        pinned_bundle = _make_bundle(tmp_path, version=published.version)
        pinned_bundle.initialize()
        cached_dag = pinned_bundle.path / "dags/example.py"
        _add_write_bits(cached_dag)
        cached_dag.write_text("print('corrupt')")

        # A host whose validation state was invalidated must detect the corruption.
        pinned_bundle._get_validation_marker_path(published.version).unlink()
        fresh_pinned_bundle = _make_bundle(tmp_path, version=published.version)
        fresh_pinned_bundle.initialize()

        assert cached_dag.read_text() == "print('dag')"
        assert (published.version_path / "dags/example.py").read_text() == "print('dag')"

    def test_validation_marker_lets_fresh_bundle_skip_hashing(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        assert bundle._get_validation_marker_path(published.version).exists()

        fresh_bundle = _make_bundle(tmp_path, version=published.version)
        # The marker skips only the hashing pass; the structural pass still runs so
        # that truncated or mutated cache trees are detected and rebuilt.
        with mock.patch.object(
            bundle_module,
            "compute_file_sha256",
            side_effect=AssertionError("marker must skip the hashing pass"),
        ):
            fresh_bundle.initialize()
        assert (fresh_bundle.path / "dags/example.py").read_text() == "print('dag')"

    def test_missing_pinned_snapshot_raises_clear_error(self, tmp_path):
        bundle = _make_bundle(tmp_path, version=f"sha256-{'0' * 64}")

        with pytest.raises(BundleManifestNotFoundError, match="is not published"):
            bundle.initialize()

    @pytest.mark.parametrize("entry_point", ["initialize", "path"])
    def test_pinned_bundle_rejects_invalid_version_before_path_lookup(self, tmp_path, entry_point):
        bundle = _make_bundle(tmp_path, version="../outside")

        with pytest.raises(BundleManifestError, match="pinned version is not a valid sha256 version"):
            _call_entry_point(bundle, entry_point)

    def test_pinned_refresh_is_not_supported(self, tmp_path):
        bundle = _make_bundle(tmp_path, version=f"sha256-{'0' * 64}")

        with pytest.raises(ValueError, match="Refreshing a specific bundle version is not supported"):
            bundle.refresh()

    @pytest.mark.skipif(os.geteuid() == 0, reason="permission checks are bypassed as root")
    def test_pinned_initialize_wraps_permission_error(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()

        pinned = _make_bundle(tmp_path, version=published.version)
        storage_root = tmp_path / "bundles"
        storage_root.chmod(0o000)
        try:
            with pytest.raises(BundleManifestError) as excinfo:
                pinned.initialize()
        finally:
            storage_root.chmod(0o755)
        assert isinstance(excinfo.value.__cause__, PermissionError)


class TestPublish:
    def test_publish_and_refresh_support_nested_paths_before_root_files(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "z.py", "print('root')")
        _write_file(source, "a/example.py", "print('nested')")
        bundle = _make_bundle(tmp_path)

        published = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)
        snapshot_manifest = json.loads(
            (published.version_path / MANIFEST_FILE_NAME).read_text(encoding="utf-8")
        )
        bundle.refresh()

        assert [file_info["path"] for file_info in snapshot_manifest["files"]] == ["a/example.py", "z.py"]
        assert (bundle.path / "a/example.py").read_text() == "print('nested')"
        assert (bundle.path / "z.py").read_text() == "print('root')"

    def test_publish_is_idempotent(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        bundle = _make_bundle(tmp_path)

        first = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)
        second = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        assert first.version == second.version
        assert first.created_snapshot is True
        assert second.created_snapshot is False
        assert json.loads(bundle.manifest_ref_path.read_text()) == second.ref_payload
        assert not stat.S_IMODE(second.version_path.stat().st_mode) & 0o222
        assert not (bundle.versions_dir / second.version).exists()

    def test_republish_rejects_damaged_existing_snapshot(self, tmp_path):
        bundle, source, published = _published_bundle(tmp_path)
        _add_write_bits(published.version_path / "dags/example.py")
        _write_file(published.version_path, "dags/example.py", "print('drift')")

        with pytest.raises(BundleManifestError, match="does not match local snapshot manifest"):
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

    def test_explicit_publish_rejects_empty_source_by_default(self, tmp_path):
        source = tmp_path / "source"
        source.mkdir()
        bundle = _make_bundle(tmp_path)

        with pytest.raises(BundleManifestError, match="explicitly publish empty source tree"):
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        assert not bundle.manifest_ref_path.exists()

        allowed = _make_bundle(tmp_path, allow_empty_source=True)
        published = publish_manifest_local_dag_bundle(bundle=allowed, source_path=source)

        assert published.file_count == 0
        assert json.loads(allowed.manifest_ref_path.read_text())["version"] == published.version

    def test_explicit_publish_rejects_pinned_bundle(self, tmp_path):
        bundle = _make_bundle(tmp_path, version=f"sha256-{'a' * 64}")

        with pytest.raises(BundleManifestError, match="Cannot explicitly publish pinned bundle"):
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=tmp_path / "explicit-source")

    def test_publish_rejects_stale_expected_current_version(self, tmp_path):
        source = tmp_path / "source"
        dag_file = _write_file(source, "dags/example.py", "print('first')")
        bundle = _make_bundle(tmp_path)

        first = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)
        dag_file.write_text("print('second')")
        second = publish_manifest_local_dag_bundle(
            bundle=bundle, source_path=source, expected_current_version=first.version
        )
        dag_file.write_text("print('stale')")

        with pytest.raises(
            BundleManifestReferenceChangedError,
            match=f"expected {first.version!r}, found {second.version!r}",
        ):
            publish_manifest_local_dag_bundle(
                bundle=bundle, source_path=source, expected_current_version=first.version
            )

        assert json.loads(bundle.manifest_ref_path.read_text())["version"] == second.version

    def test_first_publish_rejects_expected_current_version_with_clear_error(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        bundle = _make_bundle(tmp_path)

        with pytest.raises(BundleManifestReferenceChangedError, match="no published version to compare"):
            publish_manifest_local_dag_bundle(
                bundle=bundle, source_path=source, expected_current_version=f"sha256-{'0' * 64}"
            )

        assert not bundle.manifest_ref_path.exists()

    def test_publish_rejects_invalid_expected_current_version(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        bundle = _make_bundle(tmp_path)

        with pytest.raises(BundleManifestError, match="expected current version is not a valid sha256"):
            publish_manifest_local_dag_bundle(
                bundle=bundle, source_path=source, expected_current_version="v1"
            )

    def test_publish_rejects_file_added_during_publication(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        real_materialize = store_module._materialize_manifest_snapshot

        def materialize_and_add_file(**kwargs):
            real_materialize(**kwargs)
            _write_file(source, "dags/added.py", "print('added')")

        bundle = _make_bundle(tmp_path)
        with (
            mock.patch.object(
                store_module, "_materialize_manifest_snapshot", side_effect=materialize_and_add_file
            ),
            pytest.raises(BundleManifestSourceChangedError, match="changed while publishing"),
        ):
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        assert not bundle.manifest_ref_path.exists()

    def test_publish_rejects_published_root_inside_source(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        bundle = ManifestLocalDagBundle(name=BUNDLE_NAME, published_root=str(source / "published"))

        with pytest.raises(ValueError, match="published_root must not be inside source_path"):
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        assert not bundle.manifest_ref_path.exists()

    def test_publish_rejects_source_inside_published_root(self, tmp_path):
        published_root = tmp_path / "published"
        source = published_root / "refs" / BUNDLE_NAME
        _write_file(source, "dags/example.py", "print('dag')")
        bundle = ManifestLocalDagBundle(name=BUNDLE_NAME, published_root=str(published_root))

        with pytest.raises(ValueError, match="source_path must not be inside published_root"):
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        assert not bundle.manifest_ref_path.exists()

    def test_publish_rejects_versions_dir_inside_source(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        storage_path = source / ".airflow-bundles"

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(storage_path)}):
            bundle = _make_bundle(tmp_path)

            with pytest.raises(ValueError, match="source_path and versions_dir must not overlap"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        assert not bundle.manifest_ref_path.exists()
        assert not storage_path.exists()

    def test_publish_rejects_source_inside_versions_dir(self, tmp_path):
        bundle = _make_bundle(tmp_path)
        source = bundle.versions_dir / "publisher-source"
        dag_file = _write_file(source, "dags/example.py", "print('dag')")

        with pytest.raises(ValueError, match="source_path and versions_dir must not overlap"):
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        assert dag_file.exists()
        assert not bundle.manifest_ref_path.exists()

    def test_publish_fsyncs_versions_dir_before_ref_write(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        calls = []
        real_write_json_atomically = store_module._write_json_atomically

        def record_fsync_directory(path):
            calls.append(("fsync_directory", Path(path)))

        def record_write_json_atomically(destination_path, payload):
            calls.append(("write_json", Path(destination_path)))
            real_write_json_atomically(destination_path, payload)

        bundle = _make_bundle(tmp_path)
        with (
            mock.patch.object(store_module, "fsync_directory", side_effect=record_fsync_directory),
            mock.patch.object(
                store_module, "_write_json_atomically", side_effect=record_write_json_atomically
            ),
        ):
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        versions_dir_fsync = ("fsync_directory", bundle.published_versions_dir)
        ref_write = ("write_json", bundle.manifest_ref_path)
        assert versions_dir_fsync in calls
        assert ref_write in calls
        assert calls.index(versions_dir_fsync) < calls.index(ref_write)


class TestValidation:
    def test_refresh_rejects_missing_manifest_ref(self, tmp_path):
        bundle = _make_bundle(tmp_path)

        with pytest.raises(BundleManifestNotFoundError, match="manifest reference file"):
            bundle.refresh()

    def test_refresh_rejects_invalid_json_ref(self, tmp_path):
        bundle = _make_bundle(tmp_path)
        bundle.manifest_ref_path.parent.mkdir(parents=True)
        bundle.manifest_ref_path.write_text("{not-json")

        with pytest.raises(BundleManifestError, match="not valid JSON"):
            bundle.refresh()

    @pytest.mark.parametrize("entry_point", ["path", "get_current_version", "refresh", "initialize"])
    def test_entry_points_reject_non_utf8_manifest_ref(self, tmp_path, entry_point):
        bundle = _make_bundle(tmp_path)
        bundle.manifest_ref_path.parent.mkdir(parents=True)
        bundle.manifest_ref_path.write_bytes(b"\xff")

        with pytest.raises(BundleManifestError, match="not valid JSON") as excinfo:
            _call_entry_point(bundle, entry_point)

        assert isinstance(excinfo.value.__cause__, UnicodeDecodeError)

    @pytest.mark.parametrize("entry_point", ["path", "get_current_version", "refresh", "initialize"])
    def test_entry_points_raise_airflow_exception_when_ref_missing(self, tmp_path, entry_point):
        bundle = _make_bundle(tmp_path)

        with pytest.raises(BundleManifestError, match="manifest reference file") as excinfo:
            _call_entry_point(bundle, entry_point)
        assert isinstance(excinfo.value, AirflowException)

    @pytest.mark.parametrize(
        ("mutate", "match"),
        [
            (lambda payload: payload.update({"schema_version": 2}), "unsupported schema_version"),
            (lambda payload: payload.update({"bundle_name": "other"}), "expected 'manifest-local'"),
            (lambda payload: payload.update({"version": None}), "does not contain a version"),
            (lambda payload: payload.update({"version": "../outside"}), "valid sha256 version"),
            (lambda payload: payload.update({"version": "sha256-bad"}), "valid sha256 version"),
            (lambda payload: payload.update({"version": f"sha256-{'A' * 64}"}), "valid sha256 version"),
            (lambda payload: payload.update({"backend": {"type": "s3"}}), "not for a local backend"),
            (lambda payload: payload.update({"manifest": None}), "does not contain manifest data"),
            (
                lambda payload: payload.update({"manifest": {"path": None, "sha256": "x"}}),
                "valid manifest path",
            ),
            (
                lambda payload: payload.update({"manifest": {"path": MANIFEST_FILE_NAME, "sha256": ""}}),
                "does not contain manifest sha256",
            ),
            (
                lambda payload: payload.update({"manifest": {"path": "../manifest.json", "sha256": "x"}}),
                "unsafe",
            ),
            (
                lambda payload: payload.update({"manifest": {"path": "/tmp/manifest.json", "sha256": "x"}}),
                "unsafe",
            ),
            (
                lambda payload: payload.update(
                    {"manifest": {"path": "nested/.airflow-bundle-manifest.json", "sha256": "x"}}
                ),
                "must point",
            ),
            (lambda payload: payload.update({"file_count": -1}), "valid file_count"),
            (lambda payload: payload.update({"file_count": True}), "valid file_count"),
            (lambda payload: payload.update({"total_size": -1}), "valid total_size"),
            (lambda payload: payload.update({"total_size": True}), "valid total_size"),
        ],
    )
    def test_refresh_rejects_invalid_ref_payload(self, tmp_path, mutate, match):
        bundle, _, published = _published_bundle(tmp_path)
        payload = dict(published.ref_payload)
        payload["backend"] = dict(payload["backend"])
        payload["manifest"] = dict(payload["manifest"])
        mutate(payload)
        _write_manifest_ref(bundle.manifest_ref_path, payload)

        with pytest.raises(BundleManifestError, match=match):
            bundle.refresh()

    @pytest.mark.parametrize(
        ("field", "match"),
        [("file_count", "file_count 1 does not match manifest reference 2"), ("total_size", "total_size")],
    )
    def test_refresh_rejects_ref_counts_that_disagree_with_snapshot(self, tmp_path, field, match):
        bundle, _, published = _published_bundle(tmp_path)
        payload = dict(published.ref_payload)
        payload[field] += 1
        _write_manifest_ref(bundle.manifest_ref_path, payload)

        with pytest.raises(BundleManifestError, match=match):
            bundle.refresh()

    @pytest.mark.parametrize(
        ("mutate", "match"),
        [
            (lambda manifest: manifest.update({"schema_version": 2}), "unsupported schema_version"),
            (lambda manifest: manifest.update({"bundle_name": "other"}), "expected 'manifest-local'"),
            (lambda manifest: manifest.update({"backend": {"type": "s3"}}), "not for a local backend"),
            (lambda manifest: manifest.update({"file_count": 99}), "file_count"),
            (lambda manifest: manifest.update({"total_size": 99}), "total_size"),
            (lambda manifest: manifest["files"].append(dict(manifest["files"][0])), "duplicate path"),
            (lambda manifest: manifest["files"][0].update({"path": "../evil.py"}), "unsafe relative path"),
            (
                lambda manifest: manifest["files"][0].update({"path": "dags//example.py"}),
                "unsafe relative path",
            ),
            (
                lambda manifest: manifest["files"][0].update({"path": "dags/./example.py"}),
                "unsafe relative path",
            ),
        ],
    )
    def test_refresh_rejects_corrupt_snapshot_manifest(self, tmp_path, mutate, match):
        bundle, _, published = _published_bundle(tmp_path)
        manifest_path = published.version_path / MANIFEST_FILE_NAME
        manifest = json.loads(manifest_path.read_text())
        mutate(manifest)
        _add_write_bits(manifest_path)
        manifest_path.write_text(json.dumps(manifest))

        with pytest.raises(BundleManifestError, match=match):
            bundle.refresh()

    def test_refresh_rejects_corrupt_snapshot_manifest_json(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        manifest_path = published.version_path / MANIFEST_FILE_NAME
        _add_write_bits(manifest_path)
        manifest_path.write_text("{not-json")

        with pytest.raises(BundleManifestError, match="snapshot manifest is not valid JSON"):
            bundle.refresh()

    def test_refresh_rejects_missing_snapshot_manifest(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        manifest_path = published.version_path / MANIFEST_FILE_NAME
        _add_write_bits(manifest_path.parent)
        manifest_path.unlink()

        with pytest.raises(BundleManifestNotFoundError, match=MANIFEST_FILE_NAME):
            bundle.refresh()

    def test_refresh_rejects_symlinked_snapshot_manifest(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        manifest_path = published.version_path / MANIFEST_FILE_NAME
        real_manifest = tmp_path / "real-manifest.json"
        shutil.copy(manifest_path, real_manifest)
        _add_write_bits(manifest_path.parent)
        manifest_path.unlink()
        _create_symlink_or_skip(real_manifest, manifest_path)

        with pytest.raises(BundleManifestError, match="snapshot manifest .* is a symlink"):
            bundle.refresh()

    def test_refresh_rejects_snapshot_manifest_digest_mismatch(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        payload = dict(published.ref_payload)
        payload["manifest"] = dict(payload["manifest"])
        payload["manifest"]["sha256"] = "sha256:bad"
        _write_manifest_ref(bundle.manifest_ref_path, payload)

        with pytest.raises(BundleManifestError, match="digest mismatch"):
            bundle.refresh()

    def test_refresh_rejects_snapshot_file_content_drift(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        _add_write_bits(published.version_path / "dags/example.py")
        _write_file(published.version_path, "dags/example.py", "print('drift')")

        with pytest.raises(BundleManifestError, match="does not match local snapshot manifest"):
            bundle.refresh()

    def test_refresh_rejects_unexpected_snapshot_files(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        _add_write_bits(published.version_path / "dags")
        _write_file(published.version_path, "dags/unexpected.py", "print('extra')")

        with pytest.raises(BundleManifestError, match="files not present in the manifest"):
            bundle.refresh()

    def test_refresh_rejects_unexpected_snapshot_file_under_ignored_directory(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        snapshot_dags_dir = published.version_path / "dags"
        _add_write_bits(snapshot_dags_dir)
        _write_file(snapshot_dags_dir, "__pycache__/unexpected.py", "print('extra')")

        with pytest.raises(BundleManifestError, match="files not present in the manifest"):
            bundle.refresh()

    def test_refresh_rejects_symlinked_snapshot_directory(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        symlink_target = tmp_path / "outside"
        _write_file(symlink_target, "outside.py", "print('outside')")
        snapshot_dags_dir = published.version_path / "dags"
        _add_write_bits(snapshot_dags_dir)
        _create_symlink_or_skip(symlink_target, snapshot_dags_dir / "linked", target_is_directory=True)

        with pytest.raises(BundleManifestError, match="symlinked directory"):
            bundle.refresh()

    def test_refresh_rejects_symlinked_snapshot_file(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        symlink_target = tmp_path / "outside.py"
        symlink_target.write_text("print('outside')")
        snapshot_dags_dir = published.version_path / "dags"
        _add_write_bits(snapshot_dags_dir)
        _create_symlink_or_skip(symlink_target, snapshot_dags_dir / "linked.py")

        with pytest.raises(BundleManifestError, match="symlinked file"):
            bundle.refresh()

    def test_refresh_rejects_non_regular_snapshot_file(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        snapshot_dags_dir = published.version_path / "dags"
        _add_write_bits(snapshot_dags_dir)
        try:
            os.mkfifo(snapshot_dags_dir / "fifo.py")
        except OSError as e:
            pytest.skip(f"mkfifo is not supported in this test environment: {e}")

        with pytest.raises(BundleManifestError, match="non-regular file"):
            bundle.refresh()

    def test_snapshot_validation_rejects_walk_error(self, tmp_path):
        snapshot_root = tmp_path / "snapshot"
        snapshot_root.mkdir()

        def fail_walk(*args, onerror, **kwargs):
            onerror(PermissionError(13, "Permission denied", str(snapshot_root / "unreadable")))

        with (
            mock.patch.object(bundle_module.os, "walk", side_effect=fail_walk),
            pytest.raises(BundleManifestError, match="changed or became unreadable"),
        ):
            list(ManifestLocalDagBundle._iter_snapshot_files(snapshot_root))

    def test_snapshot_file_vanishing_during_validation_is_not_a_source_change(self, tmp_path):
        snapshot_root = tmp_path / "snapshot"
        snapshot_root.mkdir()

        with (
            mock.patch.object(
                bundle_module.os, "walk", autospec=True, return_value=[(str(snapshot_root), [], ["gone.py"])]
            ),
            pytest.raises(BundleManifestError, match="disappeared during validation") as excinfo,
        ):
            list(ManifestLocalDagBundle._iter_snapshot_files(snapshot_root))
        assert not isinstance(excinfo.value, BundleManifestSourceChangedError)


class TestCacheRecovery:
    @pytest.mark.parametrize("has_validation_marker", [True, False])
    def test_helper_import_bytecode_does_not_invalidate_cached_snapshot(
        self, tmp_path, monkeypatch, has_validation_marker
    ):
        source = tmp_path / "source"
        helper_name = "manifest_local_bundle_helper"
        _write_file(source, f"{helper_name}.py", "VALUE = 'helper'\n")
        bundle = _make_bundle(tmp_path)
        published = _publish(bundle, source)
        bundle.refresh()
        cached_helper = bundle.path / f"{helper_name}.py"
        tracking_file = get_bundle_tracking_file(bundle_name=bundle.name, version=published.version)
        tracking_inode = tracking_file.stat().st_ino
        monkeypatch.syspath_prepend(str(bundle.path))
        # Breeze disables bytecode generation; production helper imports normally enable it.
        monkeypatch.setattr(sys, "dont_write_bytecode", False)
        monkeypatch.setattr(sys, "pycache_prefix", None)
        try:
            helper = importlib.import_module(helper_name)
            assert helper.VALUE == "helper"
        finally:
            sys.modules.pop(helper_name, None)
        cached_bytecode = Path(cache_from_source(str(cached_helper)))
        assert cached_bytecode.exists()
        remove_bundle_tree_forcefully(published.version_path)
        if not has_validation_marker:
            bundle._get_validation_marker_path(published.version).unlink()

        fresh = _make_bundle(tmp_path, version=published.version)
        fresh.initialize()

        assert cached_helper.read_text() == "VALUE = 'helper'\n"
        assert cached_bytecode.exists()
        assert tracking_file.stat().st_ino == tracking_inode

    @pytest.mark.parametrize("location", ["cache", "published"])
    @pytest.mark.parametrize(
        ("source_file", "bytecode_file"),
        [
            ("dags/example.py", "dags/__pycache__/example.cpython-313.pyc"),
            ("dags/example.py", "dags/__pycache__/example.cpython-313.opt-1.pyc"),
            ("dags/example.py", "dags/__pycache__/example.cpython-313.opt-O2.pyc"),
            ("dags/example.py", "dags/__pycache__/example.pypy310.pyc"),
            ("dags/example.helper.py", "dags/__pycache__/example.helper.cpython-313.pyc"),
            ("dags/example.py", "dags/__pycache__/example.cpython-313.pyc.123456789"),
        ],
    )
    def test_runtime_bytecode_is_accepted_only_in_cache(self, tmp_path, location, source_file, bytecode_file):
        source = tmp_path / "source"
        _write_file(source, source_file, "VALUE = 'helper'\n")
        bundle = _make_bundle(tmp_path)
        published = _publish(bundle, source)
        if location == "cache":
            bundle.refresh()
            snapshot_path = bundle.path
        else:
            snapshot_path = published.version_path
            _add_write_bits(snapshot_path / Path(source_file).parent)
        bytecode_path = _write_file(snapshot_path, bytecode_file, "bytecode")

        if location == "cache":
            remove_bundle_tree_forcefully(published.version_path)
            fresh = _make_bundle(tmp_path, version=published.version)
            fresh.initialize()
            assert (fresh.path / source_file).read_text() == "VALUE = 'helper'\n"
            assert bytecode_path.exists()
        else:
            with pytest.raises(BundleManifestError, match="files not present in the manifest"):
                bundle.refresh()

    @pytest.mark.parametrize(
        ("relative_path", "entry_type"),
        [
            ("dags/__pycache__/orphan.cpython-313.pyc", "file"),
            ("dags/__pycache__/example.pyc", "file"),
            ("dags/__pycache__/example.cpython-313.pyc.tmp", "file"),
            ("dags/__pycache__/nested/example.cpython-313.pyc", "file"),
            ("dags/example.cpython-313.pyc", "file"),
            ("dags/__pycache__/example.cpython-313.opt-.pyc", "file"),
            ("dags/__pycache__/example.cpython-313.py", "file"),
            ("dags/__pycache__/example.cpython-313.pyc", "symlink"),
            ("dags/__pycache__/example.cpython-313.pyc", "fifo"),
        ],
    )
    def test_bytecode_tolerance_does_not_hide_invalid_cache_entries(
        self, tmp_path, relative_path, entry_type
    ):
        bundle, _, _ = _published_bundle(tmp_path)
        bundle.refresh()
        extra_path = bundle.path / relative_path
        extra_path.parent.mkdir(parents=True, exist_ok=True)
        if entry_type == "symlink":
            target = _write_file(tmp_path, "outside.pyc", "bytecode")
            _create_symlink_or_skip(target, extra_path)
        elif entry_type == "fifo":
            try:
                os.mkfifo(extra_path)
            except OSError as e:
                pytest.skip(f"mkfifo is not supported in this test environment: {e}")
        else:
            extra_path.write_text("unexpected")

        fresh = _make_bundle(tmp_path)

        with pytest.raises(BundleManifestError, match="structurally invalid cache copy"):
            _ = fresh.path

    @mock.patch.object(bundle_module.os, "walk", autospec=True)
    def test_cache_validation_tolerates_bytecode_temp_removed_after_directory_scan(self, mock_walk, tmp_path):
        cache_dir = tmp_path / "__pycache__"
        cache_dir.mkdir()
        mock_walk.return_value = [
            (str(tmp_path), ["__pycache__"], []),
            (str(cache_dir), [], ["example.cpython-313.pyc.123456789"]),
        ]

        assert (
            list(ManifestLocalDagBundle._iter_snapshot_files(tmp_path, cached_source_paths={"example.py"}))
            == []
        )

    def test_marker_does_not_certify_truncated_cache_tree(self, tmp_path):
        # Simulates stale cleanup's shutil.rmtree being interrupted partway: the version
        # dir still exists (truncated) and the marker survives. The structural pass behind
        # the marker must detect this and rebuild.
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        cached_dag = bundle.versions_dir / published.version / "dags/example.py"
        cached_dag.unlink()

        _make_bundle(tmp_path).refresh()

        assert cached_dag.read_text() == "print('dag')"

    def test_marker_does_not_certify_mutated_cache_tree(self, tmp_path):
        # Cache dirs are owner-writable (a stale-cleanup requirement), so Dag code could
        # inject files; the structural pass behind the marker must catch it.
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        injected = bundle.versions_dir / published.version / "dags/generated.py"
        injected.write_text("print('injected')")

        _make_bundle(tmp_path).refresh()

        assert not injected.exists()

    def test_path_does_not_serve_another_version_before_materialization(self, tmp_path):
        # get_current_version() already reports the new release, so serving the previous
        # cached tree here would pair that version with another version's code.
        bundle, source, first = _published_bundle(tmp_path)
        bundle.refresh()
        _write_file(source, "dags/example.py", "print('dag v2')")
        second = _publish(bundle, source)

        fresh = _make_bundle(tmp_path)

        assert fresh.get_current_version().version == second.version
        assert fresh.path == fresh.versions_dir / second.version
        assert not fresh.path.exists()
        assert (fresh.versions_dir / first.version).is_dir()

    def test_path_rejects_truncated_current_cache(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        (bundle.versions_dir / published.version / "dags/example.py").unlink()

        fresh = _make_bundle(tmp_path)

        with pytest.raises(BundleManifestError, match="structurally invalid cache copy"):
            _ = fresh.path

    @conf_vars(
        {
            ("dag_processor", "stale_bundle_cleanup_min_versions"): "0",
            ("dag_processor", "stale_bundle_cleanup_age_threshold"): "60",
        }
    )
    def test_corrupt_cache_rebuild_restores_tracking_lock(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()

        tracking_file = get_bundle_tracking_file(bundle_name=bundle.name, version=published.version)
        assert tracking_file.exists()

        cached_dag = bundle.versions_dir / published.version / "dags/example.py"
        _add_write_bits(cached_dag)
        cached_dag.write_text("corrupted!!")
        bundle._get_validation_marker_path(published.version).unlink()

        fresh = _make_bundle(tmp_path)
        fresh.refresh()

        assert tracking_file.exists()
        _set_tracking_timestamp(fresh, published.version, "2000-01-01T00:00:00+00:00")
        BundleUsageTrackingManager()._remove_stale_bundle_versions_for_bundle(bundle_name=bundle.name)
        assert (fresh.versions_dir / published.version / "dags/example.py").read_text() == "print('dag')"


def _set_tracking_timestamp(bundle: ManifestLocalDagBundle, version: str, timestamp: str) -> Path:
    # Written in place, so a lock already held on the tracking file stays on it.
    tracking_file = get_bundle_tracking_file(bundle_name=bundle.name, version=version)
    tracking_file.parent.mkdir(parents=True, exist_ok=True)
    tracking_file.write_text(timestamp)
    return tracking_file


class TestStaleVersionCleanup:
    STALE = "2000-01-01T00:00:00+00:00"

    @pytest.fixture(autouse=True)
    def _evict_every_stale_version(self):
        with conf_vars(
            {
                ("dag_processor", "stale_bundle_cleanup_min_versions"): "0",
                ("dag_processor", "stale_bundle_cleanup_age_threshold"): "60",
            }
        ):
            yield

    @staticmethod
    def _remove_stale_versions(bundle: ManifestLocalDagBundle) -> None:
        BundleUsageTrackingManager()._remove_stale_bundle_versions_for_bundle(bundle_name=bundle.name)

    @pytest.mark.parametrize("snapshot_failure", ["missing", "corrupt"])
    def test_failed_first_refresh_leaves_no_tracking_file(self, tmp_path, snapshot_failure):
        bundle, _, published = _published_bundle(tmp_path)
        if snapshot_failure == "missing":
            remove_bundle_tree_forcefully(published.version_path)
        else:
            published_dag = published.version_path / "dags/example.py"
            _add_write_bits(published_dag)
            published_dag.write_text("corrupt")
        tracking_file = get_bundle_tracking_file(bundle_name=bundle.name, version=published.version)

        with pytest.raises(BundleManifestError):
            bundle.refresh()

        assert not (bundle.versions_dir / published.version).exists()
        assert not tracking_file.exists()
        del bundle
        gc.collect()
        BundleUsageTrackingManager()._remove_stale_bundle_versions_for_bundle(bundle_name=BUNDLE_NAME)
        assert not tracking_file.exists()

    @pytest.mark.parametrize("snapshot_failure", ["missing", "corrupt"])
    def test_failed_refresh_preserves_previous_version_lock(self, tmp_path, snapshot_failure):
        bundle, source, first = _published_bundle(tmp_path)
        bundle.refresh()
        first_path = bundle.path
        _write_file(source, "dags/example.py", "print('dag v2')")
        second = _publish(bundle, source)
        if snapshot_failure == "missing":
            remove_bundle_tree_forcefully(second.version_path)
        else:
            published_dag = second.version_path / "dags/example.py"
            _add_write_bits(published_dag)
            published_dag.write_text("corrupt")

        with pytest.raises(BundleManifestError):
            bundle.refresh()

        assert bundle.get_current_version().version == first.version
        assert bundle.path == first_path
        assert not (bundle.versions_dir / second.version).exists()
        assert not get_bundle_tracking_file(bundle_name=bundle.name, version=second.version).exists()
        first_tracking_file = _set_tracking_timestamp(bundle, first.version, self.STALE)
        self._remove_stale_versions(bundle)
        assert (first_path / "dags/example.py").read_text() == "print('dag')"

        del bundle
        gc.collect()
        BundleUsageTrackingManager()._remove_stale_bundle_versions_for_bundle(bundle_name=BUNDLE_NAME)
        assert not first_path.exists()
        assert not first_tracking_file.exists()

    @mock.patch.object(ManifestLocalDagBundle, "_materialize_cached_version", autospec=True)
    def test_failed_refresh_preserves_another_refreshers_tracking_record(self, mock_materialize, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        tracking_file = get_bundle_tracking_file(bundle_name=bundle.name, version=published.version)

        def replace_tracking_then_fail(*args, **kwargs):
            replacement = tracking_file.with_suffix(".replacement")
            replacement.write_text(self.STALE)
            replacement.replace(tracking_file)
            raise BundleManifestError("materialization failed")

        mock_materialize.side_effect = replace_tracking_then_fail

        with pytest.raises(BundleManifestError, match="materialization failed"):
            bundle.refresh()

        assert not (bundle.versions_dir / published.version).exists()
        assert tracking_file.read_text() == self.STALE

    def test_cleanup_keeps_the_version_being_served(self, tmp_path):
        # Cleanup ranks versions by when tasks last used them, not by release order, so the
        # current release can look stale.
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        _set_tracking_timestamp(bundle, published.version, self.STALE)

        self._remove_stale_versions(bundle)

        assert (bundle.versions_dir / published.version / "dags/example.py").read_text() == "print('dag')"

    def test_refresh_marks_the_version_in_use_before_checking_the_cache(self, tmp_path, monkeypatch):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        del bundle
        gc.collect()
        has_validated_cache = ManifestLocalDagBundle._has_validated_cache

        def run_cleanup_after_cache_check(bundle, version, *, manifest_ref):
            has_cache = has_validated_cache(bundle, version, manifest_ref=manifest_ref)
            _set_tracking_timestamp(bundle, version, self.STALE)
            self._remove_stale_versions(bundle)
            return has_cache

        monkeypatch.setattr(ManifestLocalDagBundle, "_has_validated_cache", run_cleanup_after_cache_check)
        bundle = _make_bundle(tmp_path)

        bundle.refresh()

        assert (bundle.versions_dir / published.version / "dags/example.py").read_text() == "print('dag')"

    def test_cleanup_can_remove_a_version_no_longer_served(self, tmp_path):
        bundle, source, first = _published_bundle(tmp_path)
        bundle.refresh()
        _write_file(source, "dags/example.py", "print('dag v2')")
        second = _publish(bundle, source)
        bundle.refresh()
        _set_tracking_timestamp(bundle, first.version, self.STALE)
        _set_tracking_timestamp(bundle, second.version, self.STALE)

        self._remove_stale_versions(bundle)

        assert not (bundle.versions_dir / first.version).exists()
        assert (bundle.versions_dir / second.version).is_dir()

    @pytest.mark.parametrize("pinned", [True, False])
    @pytest.mark.parametrize("reuse_bundle", [True, False])
    def test_partial_cleanup_failure_rebuilds_previously_validated_version(
        self, tmp_path, pinned, reuse_bundle
    ):
        bundle, source, first = _published_bundle(tmp_path)
        bundle.refresh()
        if reuse_bundle:
            restored = _make_bundle(tmp_path, version=first.version) if pinned else bundle
            restored.initialize()
        _write_file(source, "dags/example.py", "print('dag v2')")
        second = _publish(bundle, source)
        bundle.refresh()
        first_path = bundle.versions_dir / first.version
        cached_dag = first_path / "dags/example.py"
        _set_tracking_timestamp(bundle, first.version, self.STALE)

        def remove_cache_partially(path):
            assert path == first_path
            cached_dag.unlink()
            raise PermissionError("partial stale cleanup")

        with (
            mock.patch(
                "airflow.dag_processing.bundles.base.shutil.rmtree",
                autospec=True,
                side_effect=remove_cache_partially,
            ),
            pytest.raises(PermissionError, match="partial stale cleanup"),
        ):
            self._remove_stale_versions(bundle)

        assert first_path.is_dir()
        assert not cached_dag.exists()
        published_dag = first.version_path / "dags/example.py"
        assert published_dag.read_text() == "print('dag')"
        assert bundle.get_current_version().version == second.version
        assert (bundle.path / "dags/example.py").read_text() == "print('dag v2')"
        if not reuse_bundle:
            restored = _make_bundle(tmp_path, version=first.version if pinned else None)
        if pinned:
            restored.initialize()
        else:
            _write_manifest_ref(bundle.manifest_ref_path, first.ref_payload)
            restored.refresh()

        assert restored.get_current_version().version == first.version
        assert (restored.path / "dags/example.py").read_text() == published_dag.read_text()

    def test_refresh_retakes_the_lock_after_the_tracking_file_is_replaced(self, tmp_path):
        # A corrupt-cache rebuild in another process deletes the tracking file, leaving the
        # lock this bundle held on a file the cleanup no longer looks at.
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        get_bundle_tracking_file(bundle_name=bundle.name, version=published.version).unlink()

        bundle.refresh()
        _set_tracking_timestamp(bundle, published.version, self.STALE)
        self._remove_stale_versions(bundle)

        assert (bundle.versions_dir / published.version).is_dir()

    @mock.patch.object(bundle_module.BundleVersionLock, "acquire", autospec=True, side_effect=PermissionError)
    def test_refresh_serves_the_bundle_when_the_version_cannot_be_marked_in_use(self, mock_acquire, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)

        bundle.refresh()

        mock_acquire.assert_called_once()
        assert bundle.path == bundle.versions_dir / published.version
        assert bundle._release_version_in_use is None

    def test_refresh_preserves_current_lock_when_reacquisition_fails(self, tmp_path, monkeypatch):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        mock_acquire = mock.create_autospec(
            bundle_module.BundleVersionLock.acquire, side_effect=PermissionError
        )
        monkeypatch.setattr(bundle_module.BundleVersionLock, "acquire", mock_acquire)

        bundle.refresh()

        mock_acquire.assert_called_once()
        assert bundle.get_current_version().version == published.version
        _set_tracking_timestamp(bundle, published.version, self.STALE)
        self._remove_stale_versions(bundle)
        assert (bundle.path / "dags/example.py").read_text() == "print('dag')"

    @pytest.mark.filterwarnings("error::ResourceWarning", "error::pytest.PytestUnraisableExceptionWarning")
    def test_discarded_bundle_closes_its_lock_file(self, tmp_path):
        bundle, _, published = _published_bundle(tmp_path)
        bundle.refresh()
        tracking_file = get_bundle_tracking_file(bundle_name=bundle.name, version=published.version)

        del bundle
        gc.collect()

        _set_tracking_timestamp(_make_bundle(tmp_path), published.version, self.STALE)
        BundleUsageTrackingManager()._remove_stale_bundle_versions_for_bundle(bundle_name=BUNDLE_NAME)
        assert not tracking_file.exists()
