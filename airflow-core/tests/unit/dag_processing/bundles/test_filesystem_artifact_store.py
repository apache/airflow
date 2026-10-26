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
"""Tests for the shared-filesystem artifact store and its filesystem helpers."""

from __future__ import annotations

import errno
import json
import multiprocessing
import os
import shutil
import stat
from fcntl import F_GETFL, LOCK_EX, LOCK_NB, fcntl, flock
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.dag_processing.bundles import filesystem_artifact_store as store_module
from airflow.dag_processing.bundles.artifact_store import (
    BundleManifestNotFoundError,
    BundleManifestRefDurabilityError,
)
from airflow.dag_processing.bundles.filesystem_artifact_store import (
    FilesystemArtifactStore,
    read_json_file,
    remove_bundle_tree_forcefully,
)
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestSourceChangedError,
    build_bundle_version_manifest,
)

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

BUNDLE_NAME = "contract"


@pytest.fixture
def store(tmp_path):
    return FilesystemArtifactStore(
        bundle_name=BUNDLE_NAME,
        published_root=tmp_path / "published",
        cache_root=tmp_path / "cache",
    )


def _write_source(root: Path) -> Path:
    (root / "dags").mkdir(parents=True, exist_ok=True)
    (root / "dags" / "example.py").write_text("print('dag')\n")
    return root


def _publish(store, source: Path):
    result = build_bundle_version_manifest(bundle_name=BUNDLE_NAME, root=source, backend_type="local")
    store.prepare_publish_areas()
    created = store.publish_snapshot(
        result.version,
        manifest=result.manifest,
        source_root=source,
        validate_existing=lambda tree: None,
    )
    return result, created


def _acquire_guard_after_contention(store: FilesystemArtifactStore, status: Connection) -> None:
    def acquire_lock(lock_file, operation):
        if operation == LOCK_EX:
            try:
                flock(lock_file, LOCK_EX | LOCK_NB)
            except BlockingIOError:
                status.send("blocked")
            else:
                status.send("unlocked")
        flock(lock_file, operation)

    with status, mock.patch.object(store_module, "flock", autospec=True, side_effect=acquire_lock):
        with store.publication_guard():
            status.send("acquired")


def test_not_found_error_is_also_a_file_not_found_error():
    error = BundleManifestNotFoundError("missing")
    assert isinstance(error, BundleManifestError)
    assert isinstance(error, FileNotFoundError)


class TestFilesystemDocuments:
    def test_read_ref_missing_raises_not_found_with_message(self, store):
        with pytest.raises(BundleManifestNotFoundError, match="ref is gone"):
            store.read_ref(missing_message="ref is gone", invalid_message="ref is invalid")

    def test_ref_roundtrip(self, store):
        store.prepare_publish_areas()
        store.write_ref({"schema_version": 1, "bundle_name": BUNDLE_NAME})
        payload = store.read_ref(missing_message="missing", invalid_message="invalid")
        assert payload == {"schema_version": 1, "bundle_name": BUNDLE_NAME}

    @pytest.mark.parametrize("content", [b"{not json", b"\xff", b"[1, 2]"])
    def test_read_ref_invalid_document_raises_with_message(self, store, content):
        store.prepare_publish_areas()
        store.ref_path.write_bytes(content)
        with pytest.raises(BundleManifestError, match="ref is invalid"):
            store.read_ref(missing_message="ref is gone", invalid_message="ref is invalid")

    def test_read_json_file_decodes_utf8_regardless_of_locale(self, tmp_path):
        path = tmp_path / "payload.json"
        path.write_bytes('{"value": "valid UTF-8: ☃"}'.encode())

        with mock.patch.object(Path, "read_text", autospec=True, side_effect=Path.read_text) as mock_read:
            payload = read_json_file(path, missing_message="missing", invalid_message="invalid")

        assert payload == {"value": "valid UTF-8: ☃"}
        mock_read.assert_called_once_with(path, encoding="utf-8")


class TestFilesystemSnapshots:
    def test_publish_then_exists_then_fetch(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        result, created = _publish(store, source)

        assert created is True
        assert store.snapshot_exists(result.version)

        seen_trees: list[Path] = []
        destination = tmp_path / "dest"
        destination.mkdir()
        store.fetch_snapshot(result.version, destination, structural_validator=seen_trees.append)
        assert seen_trees == [store.snapshot_path(result.version)]
        assert (destination / "dags" / "example.py").read_text() == "print('dag')\n"
        embedded = json.loads((destination / MANIFEST_FILE_NAME).read_text())
        assert embedded["version"] == result.version

    def test_publish_is_idempotent_and_validates_existing(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        result, created = _publish(store, source)
        assert created is True

        validated: list[Path] = []
        created_again = store.publish_snapshot(
            result.version,
            manifest=result.manifest,
            source_root=source,
            validate_existing=validated.append,
        )
        assert created_again is False
        assert validated == [store.snapshot_path(result.version)]

    def test_fetch_unpublished_version_raises_not_found(self, store, tmp_path):
        destination = tmp_path / "dest"
        destination.mkdir()
        with pytest.raises(BundleManifestNotFoundError, match="is not published"):
            store.fetch_snapshot(
                "sha256-" + "0" * 64,
                destination,
                structural_validator=lambda tree: None,
            )

    def test_snapshot_exists_is_false_before_publication(self, store):
        assert store.snapshot_exists("sha256-" + "0" * 64) is False

    def test_sweep_publish_temps_removes_orphans(self, store):
        store.prepare_publish_areas()
        orphan_snapshot = store.snapshots_root / (".sha256-" + "0" * 64 + ".tmp123")
        orphan_snapshot.mkdir(parents=True)
        orphan_ref = store.ref_path.parent / f".{store.ref_path.name}.tmp123"
        orphan_ref.write_text("partial")

        store.sweep_publish_temps()

        assert not orphan_snapshot.exists()
        assert not orphan_ref.exists()

    @pytest.mark.parametrize(
        "name",
        [
            ".sha256-" + "0" * 64,
            ".sha256-" + "0" * 64 + ".",
            ".sha256-" + "A" * 64 + ".tmp123",
            ".sha256-" + "0" * 63 + ".tmp123",
            "sha256-" + "0" * 64,
        ],
    )
    def test_sweep_publish_temps_keeps_non_temp_directories(self, store, name):
        store.prepare_publish_areas()
        directory = store.snapshots_root / name
        directory.mkdir()

        store.sweep_publish_temps()

        assert directory.exists()


class TestFilesystemCoordination:
    def test_publication_guard_serializes_processes(self, store):
        context = multiprocessing.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(target=_acquire_guard_after_contention, args=(store, sender))

        try:
            with store.publication_guard():
                process.start()
                sender.close()
                assert receiver.poll(30), "second publisher did not test the held lock"
                assert receiver.recv() == "blocked"

            assert receiver.poll(30), "second publisher did not acquire the released lock"
            assert receiver.recv() == "acquired"
            process.join(30)
            assert process.exitcode == 0
        finally:
            receiver.close()
            sender.close()
            if process.pid is not None:
                if process.is_alive():
                    process.terminate()
                process.join(30)

    def test_validate_source_paths_rejects_overlap(self, store, tmp_path):
        with pytest.raises(ValueError, match="source_path must not be inside published_root"):
            store.validate_source_paths(
                store.root / "source", cache_versions_dir=tmp_path / "cache" / "versions"
            )

    def test_validate_source_paths_accepts_disjoint_source(self, store, tmp_path):
        source = _write_source(tmp_path / "elsewhere")
        store.validate_source_paths(source, cache_versions_dir=tmp_path / "cache" / "versions")

    def test_locators_are_scoped_to_the_bundle(self, store):
        version = "sha256-" + "a" * 64
        assert str(store.snapshot_path(version)).endswith(f"/{BUNDLE_NAME}/{version}")
        assert str(store.snapshots_root).startswith(str(store.root))
        assert str(store.ref_path).startswith(str(store.root))


class TestFilesystemConstruction:
    @pytest.mark.parametrize("published_root_position", ["inside", "ancestor"])
    def test_rejects_published_root_overlapping_cache(self, tmp_path, published_root_position):
        cache_root = tmp_path / "cache"
        published_root = cache_root / "published" if published_root_position == "inside" else tmp_path
        with pytest.raises(ValueError, match="must not overlap"):
            FilesystemArtifactStore(
                bundle_name=BUNDLE_NAME,
                published_root=published_root,
                cache_root=cache_root,
            )


class TestFilesystemPublication:
    """Durability and permission behavior specific to the shared-filesystem layout."""

    def test_publication_creates_readable_artifacts_and_group_writable_lock(self, tmp_path):
        source = _write_source(tmp_path / "source")
        script = source / "bin" / "task.sh"
        script.parent.mkdir()
        script.write_text("#!/bin/sh\n")
        script.chmod(0o700)
        store = FilesystemArtifactStore(
            bundle_name=BUNDLE_NAME,
            published_root=tmp_path / "share" / "nested" / "published",
            cache_root=tmp_path / "cache",
        )

        old_umask = os.umask(0o077)
        try:
            with store.publication_guard():
                result, _ = _publish(store, source)
                store.write_ref(result.ref_payload)
        finally:
            os.umask(old_umask)

        def get_mode(path: Path) -> int:
            return stat.S_IMODE(path.stat().st_mode)

        snapshot = store.snapshot_path(result.version)
        assert get_mode(store.ref_path) == 0o644
        assert get_mode(store.lock_path) == 0o664
        # Ancestors the publisher created above published_root must also be traversable.
        assert get_mode(tmp_path / "share") == 0o755
        assert get_mode(tmp_path / "share" / "nested") == 0o755
        assert get_mode(store.root) == 0o755
        assert get_mode(store.ref_path.parent) == 0o755
        assert get_mode(store.snapshots_root) == 0o755
        assert get_mode(snapshot) == 0o555
        assert get_mode(snapshot / "dags") == 0o555
        assert get_mode(snapshot / "dags" / "example.py") == 0o444
        assert get_mode(snapshot / "bin" / "task.sh") == 0o555
        assert get_mode(snapshot / MANIFEST_FILE_NAME) == 0o444

    def test_prepare_publish_areas_keeps_pre_existing_root_mode(self, store):
        store.root.mkdir(mode=0o750)

        store.prepare_publish_areas()

        assert stat.S_IMODE(store.root.stat().st_mode) == 0o750

    @pytest.mark.parametrize(("original_mode", "expected_mode"), [(0o600, 0o664), (0o666, 0o666)])
    def test_publication_guard_grants_group_access_without_removing_permissions(
        self, store, original_mode, expected_mode
    ):
        store.lock_path.parent.mkdir(parents=True)
        store.lock_path.touch()
        store.lock_path.chmod(original_mode)

        with store.publication_guard():
            pass

        assert stat.S_IMODE(store.lock_path.stat().st_mode) == expected_mode

    @mock.patch.object(store_module, "flock", autospec=True)
    def test_publication_guard_locks_a_writable_descriptor(self, mock_flock, store):
        def check_descriptor(lock_file, operation):
            if operation == LOCK_EX:
                assert fcntl(lock_file, F_GETFL) & os.O_ACCMODE == os.O_RDWR
                assert lock_file.readable()
                assert lock_file.writable()
            flock(lock_file, operation)

        mock_flock.side_effect = check_descriptor

        with store.publication_guard():
            pass

        assert mock_flock.call_count == 2

    def test_publication_guard_explains_inaccessible_lock_file(self, store):
        real_open = os.open

        def deny_lock_open(path, *args, **kwargs):
            if Path(path) == store.lock_path:
                raise PermissionError("denied")
            return real_open(path, *args, **kwargs)

        with (
            mock.patch.object(store_module.os, "open", autospec=True, side_effect=deny_lock_open),
            pytest.raises(BundleManifestError, match="read/write"),
            store.publication_guard(),
        ):
            pass

    @pytest.mark.parametrize("artifact", ["snapshot", "reference"])
    def test_publication_fsyncs_created_hierarchy_before_commit(self, tmp_path, artifact):
        source = _write_source(tmp_path / "source")
        store = FilesystemArtifactStore(
            bundle_name=BUNDLE_NAME,
            published_root=tmp_path / "share" / "nested" / "published",
            cache_root=tmp_path / "cache",
        )
        calls: list[tuple[str, Path]] = []
        real_replace = os.replace

        def record_replace(source_path, destination_path):
            calls.append(("replace", Path(destination_path)))
            real_replace(source_path, destination_path)

        with (
            mock.patch.object(
                store_module,
                "fsync_directory",
                autospec=True,
                side_effect=lambda path: calls.append(("fsync", path)),
            ),
            mock.patch.object(store_module.os, "replace", autospec=True, side_effect=record_replace),
        ):
            if artifact == "snapshot":
                result, _ = _publish(store, source)
                destination = store.snapshot_path(result.version)
            else:
                store.write_ref({"schema_version": 1})
                destination = store.ref_path

        commit_index = calls.index(("replace", destination))
        directory = destination.parent
        while True:
            assert ("fsync", directory) in calls[:commit_index]
            if directory == tmp_path:
                break
            directory = directory.parent
        assert ("fsync", destination.parent) in calls[commit_index + 1 :]

    @pytest.mark.parametrize("artifact", ["snapshot", "reference"])
    def test_publication_retry_fsyncs_interrupted_ancestor_creation(self, tmp_path, artifact):
        source = _write_source(tmp_path / "source")
        interrupted_directory = tmp_path / "share"
        store = FilesystemArtifactStore(
            bundle_name=BUNDLE_NAME,
            published_root=interrupted_directory / "nested" / "published",
            cache_root=tmp_path / "cache",
        )
        calls: list[tuple[str, Path]] = []
        real_replace = os.replace
        fail_sync = True

        def record_sync(path):
            nonlocal fail_sync
            calls.append(("fsync", path))
            if fail_sync and path == interrupted_directory:
                fail_sync = False
                raise OSError("sync interrupted")

        def record_replace(source_path, destination_path):
            calls.append(("replace", Path(destination_path)))
            real_replace(source_path, destination_path)

        publish = (
            partial(_publish, store, source)
            if artifact == "snapshot"
            else partial(store.write_ref, {"schema_version": 1})
        )
        with (
            mock.patch.object(store_module, "fsync_directory", autospec=True, side_effect=record_sync),
            mock.patch.object(store_module.os, "replace", autospec=True, side_effect=record_replace),
        ):
            with pytest.raises(OSError, match="sync interrupted"):
                publish()
            assert interrupted_directory.is_dir()
            calls.clear()

            publish()

        commit_index = next(index for index, (operation, _) in enumerate(calls) if operation == "replace")
        assert ("fsync", interrupted_directory) in calls[:commit_index]
        assert ("fsync", interrupted_directory.parent) in calls[:commit_index]

    @pytest.mark.parametrize("artifact", ["snapshot", "reference"])
    @mock.patch.object(store_module.os, "replace", autospec=True)
    @mock.patch.object(store_module, "fsync_directory", autospec=True, side_effect=OSError("sync failed"))
    def test_publication_stops_when_hierarchy_sync_fails(
        self, mock_fsync, mock_replace, store, tmp_path, artifact
    ):
        source = _write_source(tmp_path / "source")
        publish = (
            partial(_publish, store, source)
            if artifact == "snapshot"
            else partial(store.write_ref, {"schema_version": 1})
        )

        with pytest.raises(OSError, match="sync failed"):
            publish()

        mock_replace.assert_not_called()

    @mock.patch.object(
        store_module.os, "open", autospec=True, side_effect=PermissionError("cannot open directory")
    )
    def test_fsync_directory_propagates_open_failure(self, mock_open, tmp_path):
        with pytest.raises(PermissionError, match="cannot open directory"):
            store_module.fsync_directory(tmp_path)

    @mock.patch.object(
        store_module, "_fsync_tree_directories", autospec=True, side_effect=RuntimeError("failed to sync")
    )
    def test_publish_snapshot_removes_read_only_temp_tree_on_failure(self, mock_fsync, store, tmp_path):
        source = _write_source(tmp_path / "source")

        with pytest.raises(RuntimeError, match="failed to sync"):
            _publish(store, source)

        assert list(store.snapshots_root.iterdir()) == []

    @pytest.mark.parametrize(
        "drift",
        [
            pytest.param(lambda dst: os.chmod(dst, 0o755), id="executable-bit"),
            pytest.param(lambda dst: Path(dst).write_text("print('bad')\n"), id="same-size-content"),
            pytest.param(lambda dst: Path(dst).write_text("print('changed')\n"), id="size"),
        ],
    )
    def test_publish_snapshot_rejects_source_drift_before_commit(self, store, tmp_path, drift):
        # The exec bit is part of the content address; failing only after os.replace would
        # permanently poison the published snapshot for that content version.
        source = _write_source(tmp_path / "source")
        real_copy2 = shutil.copy2

        def drift_after_copy(src, dst, **kwargs):
            copied = real_copy2(src, dst, **kwargs)
            drift(dst)
            return copied

        with (
            mock.patch.object(store_module.shutil, "copy2", autospec=True, side_effect=drift_after_copy),
            pytest.raises(BundleManifestSourceChangedError, match="source changed"),
        ):
            _publish(store, source)

        assert list(store.snapshots_root.iterdir()) == []

    def test_publish_snapshot_rejects_file_at_version_path(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        result = build_bundle_version_manifest(bundle_name=BUNDLE_NAME, root=source, backend_type="local")
        store.prepare_publish_areas()
        store.snapshot_path(result.version).write_text("not a snapshot")

        with pytest.raises(FileExistsError, match="is not a directory"):
            store.publish_snapshot(
                result.version,
                manifest=result.manifest,
                source_root=source,
                validate_existing=lambda tree: None,
            )

    @pytest.mark.parametrize(
        ("previous_payload", "payload", "expected_version"),
        [
            pytest.param(None, {"version": "v2"}, "v2", id="first-publication"),
            pytest.param({"version": "v1"}, {"version": "v2"}, "v2", id="replacement"),
            pytest.param({"version": "v1"}, {"schema_version": 1}, None, id="missing-version"),
            pytest.param({"version": "v1"}, {"version": 2}, None, id="non-string-version"),
        ],
    )
    @mock.patch.object(
        store_module, "fsync_directory", autospec=True, side_effect=store_module.fsync_directory
    )
    def test_write_ref_reports_durability_failure_after_replacement(
        self, mock_fsync, store, previous_payload, payload, expected_version
    ):
        if previous_payload is not None:
            store.write_ref(previous_payload)
        original_error = OSError(errno.EIO, "reference directory sync failed")
        real_fsync_directory = mock_fsync.side_effect

        def fail_after_replacement(path):
            if (
                path == store.ref_path.parent
                and store.ref_path.exists()
                and store.read_ref(missing_message="missing", invalid_message="invalid") == payload
            ):
                raise original_error
            real_fsync_directory(path)

        mock_fsync.side_effect = fail_after_replacement

        with pytest.raises(
            BundleManifestRefDurabilityError, match="was replaced, but durability could not be confirmed"
        ) as raised:
            store.write_ref(payload)

        assert isinstance(raised.value, BundleManifestError)
        assert raised.value.ref_path == store.ref_path
        assert raised.value.version == expected_version
        assert raised.value.__cause__ is original_error
        assert str(store.ref_path) in str(raised.value)
        assert store.read_ref(missing_message="missing", invalid_message="invalid") == payload
        assert list(store.ref_path.parent.iterdir()) == [store.ref_path]

    @pytest.mark.parametrize(
        ("existing_reference", "cleanup_fails"), [(False, False), (True, False), (True, True)]
    )
    @mock.patch.object(Path, "unlink", autospec=True, side_effect=Path.unlink)
    @mock.patch.object(store_module.os, "replace", autospec=True, side_effect=store_module.os.replace)
    def test_write_ref_preserves_original_error_when_replace_fails(
        self, mock_replace, mock_unlink, store, existing_reference, cleanup_fails
    ):
        store.prepare_publish_areas()
        if existing_reference:
            store.write_ref({"version": "v1"})
        original_error = OSError(errno.EIO, "replace failed")
        mock_replace.side_effect = original_error
        if cleanup_fails:
            mock_unlink.side_effect = PermissionError("cleanup failed")

        with pytest.raises(OSError, match="replace failed") as raised:
            store.write_ref({"version": "v2"})

        assert raised.value is original_error
        if existing_reference:
            assert store.read_ref(missing_message="missing", invalid_message="invalid") == {"version": "v1"}
        else:
            assert not store.ref_path.exists()
        temp_path = mock_replace.call_args.args[0]
        mock_unlink.assert_called_once_with(temp_path, missing_ok=True)
        assert temp_path.exists() is cleanup_fails
        assert set(store.ref_path.parent.iterdir()) == (
            ({store.ref_path} if existing_reference else set()) | ({temp_path} if cleanup_fails else set())
        )

    def test_sweep_publish_temps_removes_read_only_orphan_and_keeps_other_hidden_dirs(self, store):
        store.prepare_publish_areas()
        orphan = store.snapshots_root / f".sha256-{'1' * 64}.abandoned"
        (orphan / "dags").mkdir(parents=True)
        (orphan / "dags" / "partial.py").write_text("print('partial')")
        store_module._set_snapshot_permissions(orphan)
        unrelated_hidden_dir = store.snapshots_root / ".not-a-publisher-temp"
        unrelated_hidden_dir.mkdir()

        store.sweep_publish_temps()

        assert not orphan.exists()
        assert unrelated_hidden_dir.exists()

    @mock.patch.object(
        store_module, "remove_bundle_tree_forcefully", autospec=True, side_effect=PermissionError("denied")
    )
    def test_sweep_publish_temps_survives_undeletable_orphan(self, mock_remove, store):
        store.prepare_publish_areas()
        orphan = store.snapshots_root / f".sha256-{'1' * 64}.abandoned"
        orphan.mkdir()

        store.sweep_publish_temps()

        mock_remove.assert_called_once_with(orphan)
        assert orphan.exists()

    @pytest.mark.parametrize(
        ("source_position", "match"),
        [
            ("contains-published-root", "published_root must not be inside source_path"),
            ("inside-published-root", "source_path must not be inside published_root"),
            ("contains-cache", "source_path and versions_dir must not overlap"),
            ("inside-cache", "source_path and versions_dir must not overlap"),
        ],
    )
    def test_validate_source_paths_rejects_each_overlap(self, tmp_path, source_position, match):
        published_root = tmp_path / "published"
        cache_versions_dir = tmp_path / "cache" / "versions"
        source = {
            "contains-published-root": published_root.parent,
            "inside-published-root": published_root / "source",
            "contains-cache": cache_versions_dir.parent,
            "inside-cache": cache_versions_dir / "source",
        }[source_position]
        store = FilesystemArtifactStore(
            bundle_name=BUNDLE_NAME,
            published_root=published_root,
            cache_root=tmp_path / "cache",
        )

        with pytest.raises(ValueError, match=match):
            store.validate_source_paths(source, cache_versions_dir=cache_versions_dir)


class TestRemoveBundleTreeForcefully:
    def test_removes_read_only_tree(self, tmp_path):
        tree = tmp_path / "tree"
        (tree / "dags").mkdir(parents=True)
        (tree / "dags" / "example.py").write_text("print('dag')")
        store_module._set_snapshot_permissions(tree)

        remove_bundle_tree_forcefully(tree)

        assert not tree.exists()

    def test_unlinks_a_symlink_without_following_it(self, tmp_path):
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        link.symlink_to(target, target_is_directory=True)

        remove_bundle_tree_forcefully(link)

        assert not link.exists()
        assert target.is_dir()
