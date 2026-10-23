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
from fcntl import F_GETFL, LOCK_EX, LOCK_NB, LOCK_UN, fcntl, flock
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.dag_processing.bundles import (
    _filesystem as filesystem_module,
    filesystem_artifact_store as store_module,
)
from airflow.dag_processing.bundles._filesystem import read_json_file
from airflow.dag_processing.bundles.artifact_store import (
    BundleManifestNotFoundError,
    BundleManifestRefDurabilityError,
)
from airflow.dag_processing.bundles.filesystem_artifact_store import FilesystemArtifactStore
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestSourceChangedError,
    build_bundle_version_manifest,
)

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

BUNDLE_NAME = "contract"


def _create_published_root(path: Path) -> Path:
    path.mkdir(parents=True)
    # Created directories copy the root's group bits, so pin its mode against the umask.
    path.chmod(0o755)
    return path


@pytest.fixture
def store(tmp_path):
    return FilesystemArtifactStore(
        bundle_name=BUNDLE_NAME,
        published_root=_create_published_root(tmp_path / "published"),
    )


def _validate_tree(tree: Path) -> None:
    """Spec for snapshot validator mocks, so a wrong callback signature fails the test."""


_real_replace = os.replace


def _replace_then_fail(src, dst):
    """Complete the rename, then report failure the way a lost NFS reply does."""
    _real_replace(src, dst)
    raise OSError(errno.EIO, "rename reply lost")


def _sweep_then_fail(src, dst):
    """Lose the temporary file to another publisher's leftover sweep before the rename."""
    os.unlink(src)
    raise FileNotFoundError(errno.ENOENT, "No such file or directory", os.fspath(src))


_real_scandir = os.scandir


def _scandir_unless_unlistable(path="."):
    """List directories, failing for any named ``unlistable`` as an I/O fault would."""
    if os.path.basename(os.fspath(path)) == "unlistable":
        raise PermissionError(errno.EACCES, "cannot list directory", os.fspath(path))
    return _real_scandir(path)


def _fail_fast_copy(src, dst, **kwargs):
    """Fail the way shutil's fast-copy paths do: the error names both files."""
    raise OSError(errno.ENOSPC, "No space left on device", os.fspath(src), None, os.fspath(dst))


def _is_dir_as_in_python_314(path, *, follow_symlinks=True):
    """``Path.is_dir()`` as of Python 3.14, which returns False on any OSError."""
    return os.path.isdir(path)


def _exists_as_in_python_314(path, *, follow_symlinks=True):
    """``Path.exists()`` as of Python 3.14, which returns False on any OSError."""
    return os.path.exists(path)


def _fail_unlock(lock_file, operation):
    if operation == LOCK_UN:
        raise OSError(errno.EIO, "unlock failed")
    return flock(lock_file, operation)


def _get_lock_path(store: FilesystemArtifactStore) -> Path:
    return Path(store.root) / "_locks" / f"{BUNDLE_NAME}.lock"


def _write_source(root: Path) -> Path:
    (root / "dags").mkdir(parents=True, exist_ok=True)
    (root / "dags" / "example.py").write_text("print('dag')\n")
    return root


def _publish(store, source: Path):
    result = build_bundle_version_manifest(bundle_name=BUNDLE_NAME, root=source, backend_type="local")
    store.prepare_publish_areas()
    created = store.publish_snapshot(
        manifest=result.manifest,
        source_root=source,
        validate_existing=lambda tree: None,
    )
    return result, created


def _acquire_guard_after_contention(store: FilesystemArtifactStore, status: Connection) -> None:
    reported = False

    def acquire_lock(lock_file, operation):
        nonlocal reported
        try:
            flock(lock_file, operation)
        except BlockingIOError:
            if not reported:
                status.send("blocked")
                reported = True
            raise
        if operation & LOCK_EX and not reported:
            status.send("unlocked")
            reported = True

    with status, mock.patch.object(store_module, "flock", autospec=True, side_effect=acquire_lock):
        with store.publication_guard():
            status.send("acquired")


def test_not_found_error_is_also_a_file_not_found_error():
    error = BundleManifestNotFoundError("missing")
    assert isinstance(error, BundleManifestError)
    assert isinstance(error, FileNotFoundError)


class TestFilesystemDocuments:
    def test_read_ref_missing_raises_not_found(self, store):
        with pytest.raises(
            BundleManifestNotFoundError, match=f"Bundle '{BUNDLE_NAME}' has no release reference"
        ):
            store.read_ref()

    def test_ref_roundtrip(self, store):
        store.prepare_publish_areas()
        store.write_ref({"schema_version": 1, "bundle_name": BUNDLE_NAME})
        payload = store.read_ref()
        assert payload == {"schema_version": 1, "bundle_name": BUNDLE_NAME}

    @pytest.mark.parametrize("content", [b"{not json", b"\xff", b"[1, 2]"])
    def test_read_ref_invalid_document_raises(self, store, content):
        store.prepare_publish_areas()
        Path(store.ref_path).write_bytes(content)
        with pytest.raises(BundleManifestError, match="release reference .* is not a JSON object"):
            store.read_ref()


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
        assert seen_trees == [Path(store.snapshot_path(result.version))]
        assert (destination / "dags" / "example.py").read_text() == "print('dag')\n"
        embedded = json.loads((destination / MANIFEST_FILE_NAME).read_text())
        assert embedded["version"] == result.version

    def test_fetch_rejects_non_empty_destination(self, store, tmp_path):
        result, _ = _publish(store, _write_source(tmp_path / "source"))
        destination = tmp_path / "dest"
        store.fetch_snapshot(result.version, destination, structural_validator=lambda tree: None)
        structural_validator = mock.create_autospec(_validate_tree)

        with pytest.raises(ValueError, match="must be absent or empty"):
            store.fetch_snapshot(result.version, destination, structural_validator=structural_validator)

        structural_validator.assert_not_called()

    def test_fetch_validates_before_copying(self, store, tmp_path):
        result, _ = _publish(store, _write_source(tmp_path / "source"))
        destination = tmp_path / "dest"
        structural_validator = mock.create_autospec(
            _validate_tree, side_effect=BundleManifestError("snapshot rejected")
        )

        with pytest.raises(BundleManifestError, match="snapshot rejected"):
            store.fetch_snapshot(result.version, destination, structural_validator=structural_validator)

        structural_validator.assert_called_once_with(Path(store.snapshot_path(result.version)))
        assert not destination.exists()

    def test_publish_is_idempotent_and_validates_existing(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        result, created = _publish(store, source)
        assert created is True

        validated: list[Path] = []
        created_again = store.publish_snapshot(
            manifest=result.manifest,
            source_root=source,
            validate_existing=validated.append,
        )
        assert created_again is False
        assert validated == [Path(store.snapshot_path(result.version))]

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

    def test_publication_guard_removes_orphans(self, store):
        store.prepare_publish_areas()
        orphan_snapshot = Path(store.snapshots_root) / (".sha256-" + "0" * 64 + ".tmp123")
        orphan_snapshot.mkdir(parents=True)
        orphan_ref = Path(store.ref_path).parent / f".{Path(store.ref_path).name}.tmp123"
        orphan_ref.write_text("partial")

        with store.publication_guard():
            pass

        assert not orphan_snapshot.exists()
        assert not orphan_ref.exists()

    @mock.patch.object(store_module, "remove_orphaned_temp_snapshots", autospec=True)
    @mock.patch.object(store_module, "flock", autospec=True)
    def test_publication_guard_sweeps_only_while_holding_the_lock(self, mock_flock, mock_sweep, store):
        calls: list[str] = []
        mock_flock.side_effect = lambda lock_file, operation: calls.append(f"flock-{operation}")
        mock_sweep.side_effect = lambda versions_dir: calls.append("sweep")

        with store.publication_guard():
            calls.append("body")

        assert calls == [f"flock-{LOCK_EX | LOCK_NB}", "sweep", "body", f"flock-{LOCK_UN}"]

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
    def test_publication_guard_keeps_non_temp_directories(self, store, name):
        store.prepare_publish_areas()
        directory = Path(store.snapshots_root) / name
        directory.mkdir()

        with store.publication_guard():
            pass

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

    def test_locators_are_opaque_strings_scoped_to_the_bundle(self, store, tmp_path):
        version = "sha256-" + "a" * 64
        published_root = tmp_path / "published"
        assert store.root == str(published_root)
        assert store.ref_path == str(published_root / "refs" / BUNDLE_NAME / "latest.json")
        assert store.snapshots_root == str(published_root / "versions" / BUNDLE_NAME)
        assert store.snapshot_path(version) == str(published_root / "versions" / BUNDLE_NAME / version)


class TestFilesystemPublication:
    """Durability and permission behavior specific to the shared-filesystem layout."""

    def test_publication_creates_readable_artifacts_and_group_writable_lock(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        script = source / "bin" / "task.sh"
        script.parent.mkdir()
        script.write_text("#!/bin/sh\n")
        script.chmod(0o700)

        old_umask = os.umask(0o077)
        try:
            with store.publication_guard():
                result, _ = _publish(store, source)
                store.write_ref(result.ref_payload)
        finally:
            os.umask(old_umask)

        def get_mode(path: Path) -> int:
            return stat.S_IMODE(path.stat().st_mode)

        snapshot = Path(store.snapshot_path(result.version))
        assert get_mode(Path(store.ref_path)) == 0o644
        assert get_mode(_get_lock_path(store)) == 0o664
        assert get_mode(Path(store.ref_path).parent) == 0o755
        assert get_mode(Path(store.snapshots_root)) == 0o755
        assert get_mode(snapshot) == 0o555
        assert get_mode(snapshot / "dags") == 0o555
        assert get_mode(snapshot / "dags" / "example.py") == 0o444
        assert get_mode(snapshot / "bin" / "task.sh") == 0o555
        assert get_mode(snapshot / MANIFEST_FILE_NAME) == 0o444

    def test_prepare_publish_areas_keeps_pre_existing_root_mode(self, store):
        Path(store.root).chmod(0o750)

        store.prepare_publish_areas()

        assert stat.S_IMODE(Path(store.root).stat().st_mode) == 0o750

    @pytest.mark.parametrize("root_state", ["missing", "file"])
    @pytest.mark.parametrize("operation", ["prepare_publish_areas", "publication_guard", "write_ref"])
    def test_publication_requires_an_existing_published_root(self, tmp_path, operation, root_state):
        mount_point = tmp_path / "share"
        mount_point.mkdir()
        published_root = mount_point / "published"
        if root_state == "file":
            published_root.write_text("not a directory")
        store = FilesystemArtifactStore(bundle_name=BUNDLE_NAME, published_root=published_root)
        operations = {
            "prepare_publish_areas": store.prepare_publish_areas,
            "publication_guard": lambda: store.publication_guard().__enter__(),
            "write_ref": partial(store.write_ref, {"version": "v1"}),
        }

        with pytest.raises(BundleManifestError, match="does not exist or is not a directory") as raised:
            operations[operation]()

        assert type(raised.value) is BundleManifestError
        assert list(mount_point.iterdir()) == ([published_root] if root_state == "file" else [])

    def test_created_directories_inherit_the_group_permissions_of_published_root(self, store, tmp_path):
        Path(store.root).chmod(0o2775)
        source = _write_source(tmp_path / "source")

        old_umask = os.umask(0o077)
        try:
            with store.publication_guard():
                result, _ = _publish(store, source)
                store.write_ref(result.ref_payload)
        finally:
            os.umask(old_umask)

        created = [
            _get_lock_path(store).parent,
            Path(store.snapshots_root).parent,
            Path(store.snapshots_root),
            Path(store.ref_path).parent.parent,
            Path(store.ref_path).parent,
        ]
        assert {path: stat.S_IMODE(path.stat().st_mode) for path in created} == dict.fromkeys(created, 0o2775)

    @pytest.mark.parametrize("area", ["versions", "refs"])
    @mock.patch.object(Path, "mkdir", autospec=True, side_effect=Path.mkdir)
    def test_concurrent_bundles_keep_group_write_during_directory_creation(self, mock_mkdir, store, area):
        published_root = Path(store.root)
        published_root.chmod(0o2775)
        shared_directory = published_root / area
        rival = FilesystemArtifactStore(bundle_name="rival", published_root=published_root)
        real_mkdir = mock_mkdir.side_effect

        def prepare_rival_before_chmod(path, *args, **kwargs):
            result = real_mkdir(path, *args, **kwargs)
            if path == shared_directory:
                rival.prepare_publish_areas()
            return result

        mock_mkdir.side_effect = prepare_rival_before_chmod
        previous_umask = os.umask(0o077)
        try:
            store.prepare_publish_areas()
        finally:
            os.umask(previous_umask)

        directories = [
            shared_directory,
            shared_directory / store.bundle_name,
            shared_directory / rival.bundle_name,
        ]
        assert {path: stat.S_IMODE(path.stat().st_mode) for path in directories} == dict.fromkeys(
            directories, 0o2775
        )

    @pytest.mark.parametrize(("original_mode", "expected_mode"), [(0o600, 0o664), (0o666, 0o666)])
    def test_publication_guard_grants_group_access_without_removing_permissions(
        self, store, original_mode, expected_mode
    ):
        _get_lock_path(store).parent.mkdir(parents=True)
        _get_lock_path(store).touch()
        _get_lock_path(store).chmod(original_mode)

        with store.publication_guard():
            pass

        assert stat.S_IMODE(_get_lock_path(store).stat().st_mode) == expected_mode

    @mock.patch.object(store_module, "flock", autospec=True)
    def test_publication_guard_locks_a_writable_descriptor(self, mock_flock, store):
        def check_descriptor(lock_file, operation):
            if operation & LOCK_EX:
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
            if Path(path) == _get_lock_path(store):
                raise PermissionError("denied")
            return real_open(path, *args, **kwargs)

        with (
            mock.patch.object(store_module.os, "open", autospec=True, side_effect=deny_lock_open),
            pytest.raises(BundleManifestError, match="read/write"),
            store.publication_guard(),
        ):
            pass

    @mock.patch.object(store_module, "time", autospec=True)
    @mock.patch.object(store_module, "flock", autospec=True)
    def test_publication_guard_waits_for_a_held_lock(self, mock_flock, mock_time, store):
        mock_time.monotonic.return_value = 0.0
        mock_flock.side_effect = [BlockingIOError(), BlockingIOError(), None, None]
        entered = []

        with store.publication_guard():
            entered.append(True)

        assert entered == [True]
        assert mock_time.sleep.call_count == 2

    @mock.patch.object(store_module, "log", autospec=True)
    @mock.patch.object(store_module, "time", autospec=True)
    @mock.patch.object(store_module, "flock", autospec=True, side_effect=BlockingIOError())
    def test_publication_guard_times_out_and_logs_while_waiting(
        self, mock_flock, mock_time, mock_log, tmp_path
    ):
        clock = 0.0

        def advance(seconds):
            nonlocal clock
            clock += seconds

        mock_time.monotonic.side_effect = lambda: clock
        mock_time.sleep.side_effect = advance
        store = FilesystemArtifactStore(
            bundle_name=BUNDLE_NAME,
            published_root=_create_published_root(tmp_path / "published"),
            lock_timeout=65,
        )

        with (
            pytest.raises(BundleManifestError, match="Timed out after 65s waiting for publication lock"),
            store.publication_guard(),
        ):
            pass

        assert clock == 65
        assert (
            mock_log.info.call_args_list
            == [mock.call("Waiting for publication lock %s held by another publisher", _get_lock_path(store))]
            * 3
        )

    @mock.patch.object(store_module, "flock", autospec=True, side_effect=OSError(errno.ENOLCK, "No locks"))
    def test_publication_guard_reports_unsupported_locking(self, mock_flock, store):
        with (
            pytest.raises(BundleManifestError, match="must support flock across hosts") as raised,
            store.publication_guard(),
        ):
            pass

        assert raised.value.__cause__ is mock_flock.side_effect

    @pytest.mark.parametrize("artifact", ["snapshot", "reference"])
    def test_publication_fsyncs_created_hierarchy_before_commit(self, store, tmp_path, artifact):
        source = _write_source(tmp_path / "source")
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
                destination = Path(store.snapshot_path(result.version))
            else:
                store.write_ref({"schema_version": 1})
                destination = Path(store.ref_path)

        commit_index = calls.index(("replace", destination))
        directory = destination.parent
        while True:
            assert ("fsync", directory) in calls[:commit_index]
            if directory == Path(store.root):
                break
            directory = directory.parent
        assert ("fsync", destination.parent) in calls[commit_index + 1 :]

    @pytest.mark.parametrize("artifact", ["snapshot", "reference"])
    def test_publication_retry_fsyncs_interrupted_ancestor_creation(self, store, tmp_path, artifact):
        source = _write_source(tmp_path / "source")
        # The first directory each publication creates below published_root.
        interrupted_directory = (
            Path(store.snapshots_root).parent
            if artifact == "snapshot"
            else Path(store.ref_path).parent.parent
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
            with pytest.raises(BundleManifestError, match="sync interrupted"):
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

        with pytest.raises(BundleManifestError, match="sync failed"):
            publish()

        mock_replace.assert_not_called()

    @mock.patch.object(
        store_module, "fsync_tree_directories", autospec=True, side_effect=RuntimeError("failed to sync")
    )
    def test_publish_snapshot_removes_read_only_temp_tree_on_failure(self, mock_fsync, store, tmp_path):
        source = _write_source(tmp_path / "source")

        with pytest.raises(RuntimeError, match="failed to sync"):
            _publish(store, source)

        assert list(Path(store.snapshots_root).iterdir()) == []

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
        real_copy = shutil.copy

        def drift_after_copy(src, dst, **kwargs):
            copied = real_copy(src, dst, **kwargs)
            drift(dst)
            return copied

        with (
            mock.patch.object(store_module.shutil, "copy", autospec=True, side_effect=drift_after_copy),
            pytest.raises(BundleManifestSourceChangedError, match="source changed"),
        ):
            _publish(store, source)

        assert list(Path(store.snapshots_root).iterdir()) == []

    def test_publish_snapshot_validates_a_version_committed_concurrently(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        result = build_bundle_version_manifest(bundle_name=BUNDLE_NAME, root=source, backend_type="local")
        store.prepare_publish_areas()
        rival = FilesystemArtifactStore(bundle_name=BUNDLE_NAME, published_root=tmp_path / "published")
        real_fsync_tree_directories = store_module.fsync_tree_directories
        raced = False

        def commit_rival_first(path):
            nonlocal raced
            if not raced:
                raced = True
                rival.publish_snapshot(
                    manifest=result.manifest, source_root=source, validate_existing=lambda tree: None
                )
            real_fsync_tree_directories(path)

        validate_existing = mock.create_autospec(_validate_tree)
        with mock.patch.object(
            store_module, "fsync_tree_directories", autospec=True, side_effect=commit_rival_first
        ):
            created = store.publish_snapshot(
                manifest=result.manifest, source_root=source, validate_existing=validate_existing
            )

        assert created is False
        validate_existing.assert_called_once_with(Path(store.snapshot_path(result.version)))
        assert list(Path(store.snapshots_root).iterdir()) == [Path(store.snapshot_path(result.version))]

    @mock.patch.object(store_module.os, "replace", autospec=True, side_effect=_replace_then_fail)
    @mock.patch.object(
        store_module, "fsync_directory", autospec=True, side_effect=store_module.fsync_directory
    )
    def test_publish_snapshot_syncs_a_rename_that_reported_failure(
        self, mock_fsync, mock_replace, store, tmp_path
    ):
        source = _write_source(tmp_path / "source")
        result = build_bundle_version_manifest(bundle_name=BUNDLE_NAME, root=source, backend_type="local")
        store.prepare_publish_areas()
        mock_fsync.reset_mock()
        validate_existing = mock.create_autospec(_validate_tree)

        created = store.publish_snapshot(
            manifest=result.manifest, source_root=source, validate_existing=validate_existing
        )

        assert created is False
        mock_replace.assert_called_once()
        mock_fsync.assert_called_once_with(Path(store.snapshots_root))
        validate_existing.assert_called_once_with(Path(store.snapshot_path(result.version)))
        assert list(Path(store.snapshots_root).iterdir()) == [Path(store.snapshot_path(result.version))]

    @mock.patch.object(store_module.os, "replace", autospec=True, side_effect=OSError(errno.EIO, "I/O error"))
    def test_publish_snapshot_reraises_a_failed_commit(self, mock_replace, store, tmp_path):
        store.prepare_publish_areas()
        validate_existing = mock.create_autospec(_validate_tree)

        with pytest.raises(BundleManifestError, match="I/O error"):
            store.publish_snapshot(
                manifest=build_bundle_version_manifest(
                    bundle_name=BUNDLE_NAME, root=_write_source(tmp_path / "source"), backend_type="local"
                ).manifest,
                source_root=tmp_path / "source",
                validate_existing=validate_existing,
            )

        validate_existing.assert_not_called()
        assert list(Path(store.snapshots_root).iterdir()) == []

    def test_publish_snapshot_syncs_files_after_final_permissions(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        (source / "dags" / "example.py").chmod(0o600)
        synced_modes: dict[str, int] = {}

        def record_mode(path):
            temp_tree = next(parent for parent in path.parents if parent.parent == Path(store.snapshots_root))
            synced_modes[path.relative_to(temp_tree).as_posix()] = stat.S_IMODE(path.stat().st_mode)
            filesystem_module.fsync_file(path)

        with mock.patch.object(store_module, "fsync_file", autospec=True, side_effect=record_mode):
            _publish(store, source)

        assert synced_modes == {"dags/example.py": 0o444, MANIFEST_FILE_NAME: 0o444}

    @mock.patch.object(store_module.os, "scandir", autospec=True, side_effect=_scandir_unless_unlistable)
    def test_snapshot_permissions_fail_on_an_unlistable_directory(self, mock_scandir, tmp_path):
        (tmp_path / "snapshot" / "unlistable").mkdir(parents=True)

        with pytest.raises(PermissionError, match="cannot list directory"):
            store_module._set_snapshot_permissions(tmp_path / "snapshot")

    @mock.patch.object(
        store_module, "fsync_directory", autospec=True, side_effect=store_module.fsync_directory
    )
    def test_publish_snapshot_retry_syncs_a_snapshot_committed_before_a_sync_failure(
        self, mock_fsync, store, tmp_path
    ):
        source = _write_source(tmp_path / "source")
        result = build_bundle_version_manifest(bundle_name=BUNDLE_NAME, root=source, backend_type="local")
        store.prepare_publish_areas()
        real_fsync_directory = mock_fsync.side_effect
        mock_fsync.side_effect = OSError(errno.EIO, "directory sync failed")
        with pytest.raises(BundleManifestError, match="directory sync failed"):
            store.publish_snapshot(
                manifest=result.manifest, source_root=source, validate_existing=_validate_tree
            )
        assert store.snapshot_exists(result.version)
        mock_fsync.side_effect = real_fsync_directory
        mock_fsync.reset_mock()
        validate_existing = mock.create_autospec(_validate_tree)

        created = store.publish_snapshot(
            manifest=result.manifest, source_root=source, validate_existing=validate_existing
        )

        assert created is False
        validate_existing.assert_called_once_with(Path(store.snapshot_path(result.version)))
        mock_fsync.assert_called_once_with(Path(store.snapshots_root))

    @pytest.mark.parametrize("area", ["versions", "snapshots", "reference", "lock"])
    @mock.patch.object(Path, "chmod", autospec=True, side_effect=Path.chmod)
    def test_publication_retries_directory_creation_after_chmod_failure(
        self, mock_chmod, store, tmp_path, area
    ):
        source = _write_source(tmp_path / "source")
        failed_directory = {
            "versions": Path(store.snapshots_root).parent,
            "snapshots": Path(store.snapshots_root),
            "reference": Path(store.ref_path).parent,
            "lock": _get_lock_path(store).parent,
        }[area]
        original_error = OSError(errno.EIO, "chmod failed")
        real_chmod = mock_chmod.side_effect

        def fail_chmod(path, mode, **kwargs):
            if path == failed_directory:
                raise original_error
            return real_chmod(path, mode, **kwargs)

        previous_umask = os.umask(0o077)
        try:
            mock_chmod.side_effect = fail_chmod
            with pytest.raises(BundleManifestError, match="chmod failed") as raised:
                with store.publication_guard():
                    _publish(store, source)
            assert raised.value.__cause__ is original_error
            assert not failed_directory.exists()

            mock_chmod.side_effect = real_chmod
            with store.publication_guard():
                result, created = _publish(store, source)
                store.write_ref(result.ref_payload)
        finally:
            os.umask(previous_umask)

        assert created is True
        assert store.read_ref() == result.ref_payload
        assert stat.S_IMODE(failed_directory.stat().st_mode) == 0o755

    @pytest.mark.parametrize("other_owner", [False, True])
    @mock.patch.object(Path, "rmdir", autospec=True, side_effect=Path.rmdir)
    @mock.patch.object(Path, "chmod", autospec=True, side_effect=Path.chmod)
    @mock.patch.object(Path, "mkdir", autospec=True, side_effect=Path.mkdir)
    def test_failed_chmod_preserves_a_directory_created_concurrently(
        self, mock_mkdir, mock_chmod, mock_rmdir, store, other_owner
    ):
        raced_directory = Path(store.snapshots_root).parent
        real_mkdir = mock_mkdir.side_effect
        real_chmod = mock_chmod.side_effect

        def create_rival_first(path, *args, **kwargs):
            if path == raced_directory:
                real_mkdir(path, mode=0o750)
                real_chmod(path, 0o750)
            return real_mkdir(path, *args, **kwargs)

        def fail_chmod(path, mode, **kwargs):
            if path == raced_directory:
                raise PermissionError(errno.EPERM, "chmod denied")
            return real_chmod(path, mode, **kwargs)

        mock_mkdir.side_effect = create_rival_first
        mock_chmod.side_effect = fail_chmod
        with mock.patch.object(
            store_module.os, "geteuid", autospec=True, return_value=os.geteuid() + other_owner
        ):
            if other_owner:
                store.prepare_publish_areas()
            else:
                with pytest.raises(BundleManifestError, match="chmod denied"):
                    store.prepare_publish_areas()

        assert stat.S_IMODE(raced_directory.stat().st_mode) == 0o750
        mock_rmdir.assert_not_called()

    @mock.patch.object(Path, "chmod", autospec=True, side_effect=Path.chmod)
    @mock.patch.object(Path, "mkdir", autospec=True, side_effect=Path.mkdir)
    def test_publication_directory_creation_preserves_a_racing_file(self, mock_mkdir, mock_chmod, store):
        raced_file = Path(store.snapshots_root).parent
        real_mkdir = mock_mkdir.side_effect

        def create_file_first(path, *args, **kwargs):
            if path == raced_file:
                path.write_text("unrelated data")
            return real_mkdir(path, *args, **kwargs)

        mock_mkdir.side_effect = create_file_first
        with pytest.raises(BundleManifestError) as raised:
            store.prepare_publish_areas()

        assert isinstance(raised.value.__cause__, FileExistsError)
        assert raced_file.read_text() == "unrelated data"
        mock_chmod.assert_not_called()

    @mock.patch.object(store_module, "log", autospec=True)
    @mock.patch.object(Path, "rmdir", autospec=True, side_effect=OSError(errno.EIO, "cleanup failed"))
    @mock.patch.object(Path, "chmod", autospec=True, side_effect=OSError(errno.EIO, "chmod failed"))
    def test_failed_directory_cleanup_preserves_the_permission_error(
        self, mock_chmod, mock_rmdir, mock_log, store
    ):
        with pytest.raises(BundleManifestError, match="chmod failed") as raised:
            store.prepare_publish_areas()

        versions_directory = Path(store.snapshots_root).parent
        assert raised.value.__cause__ is mock_chmod.side_effect
        mock_rmdir.assert_called_once_with(versions_directory)
        assert versions_directory.is_dir()
        mock_log.warning.assert_called_once_with(
            "Could not clean up publication directory %s after failed permission setup; "
            "inspect its permissions before retrying",
            versions_directory,
            exc_info=True,
        )

    @pytest.mark.parametrize(
        ("owner_offset", "fails"),
        [pytest.param(0, True, id="owned"), pytest.param(1, False, id="owned-by-another-publisher")],
    )
    @mock.patch.object(
        store_module.os, "fchmod", autospec=True, side_effect=OSError(errno.EIO, "fchmod failed")
    )
    def test_publication_guard_reports_chmod_failures_on_an_owned_lock(
        self, mock_fchmod, store, owner_offset, fails
    ):
        owner = os.geteuid() + owner_offset
        entered = False
        with mock.patch.object(store_module.os, "geteuid", autospec=True, return_value=owner):
            if fails:
                with pytest.raises(BundleManifestError, match="fchmod failed"), store.publication_guard():
                    entered = True
            else:
                with store.publication_guard():
                    entered = True

        assert entered is not fails

    @mock.patch.object(store_module, "log", autospec=True)
    @mock.patch.object(store_module, "flock", autospec=True, side_effect=_fail_unlock)
    def test_publication_guard_keeps_the_publication_error_when_unlock_fails(
        self, mock_flock, mock_log, store
    ):
        durability_error = BundleManifestRefDurabilityError(ref_path=store.ref_path, version="v1")

        with pytest.raises(BundleManifestRefDurabilityError) as raised, store.publication_guard():
            raise durability_error

        assert raised.value is durability_error
        mock_log.warning.assert_called_once_with(
            "Cannot release publication lock %s", _get_lock_path(store), exc_info=True
        )

    @mock.patch.object(store_module, "flock", autospec=True, side_effect=_fail_unlock)
    def test_publication_guard_reports_a_failed_unlock(self, mock_flock, store):
        with (
            pytest.raises(BundleManifestError, match="Cannot release publication lock .*unlock failed"),
            store.publication_guard(),
        ):
            pass

    @mock.patch.object(store_module.shutil, "copystat", autospec=True)
    def test_publish_snapshot_does_not_copy_source_metadata(self, mock_copystat, store, tmp_path):
        _publish(store, _write_source(tmp_path / "source"))

        mock_copystat.assert_not_called()

    def test_publish_snapshot_rejects_file_at_version_path(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        result = build_bundle_version_manifest(bundle_name=BUNDLE_NAME, root=source, backend_type="local")
        store.prepare_publish_areas()
        Path(store.snapshot_path(result.version)).write_text("not a snapshot")

        with pytest.raises(BundleManifestError, match="is not a directory"):
            store.publish_snapshot(
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
                path == Path(store.ref_path).parent
                and Path(store.ref_path).exists()
                and store.read_ref() == payload
            ):
                raise original_error
            real_fsync_directory(path)

        mock_fsync.side_effect = fail_after_replacement

        with pytest.raises(
            BundleManifestRefDurabilityError,
            match="may have been replaced, but durability could not be confirmed",
        ) as raised:
            store.write_ref(payload)

        assert isinstance(raised.value, BundleManifestError)
        assert raised.value.ref_path == store.ref_path
        assert raised.value.version == expected_version
        assert raised.value.__cause__ is original_error
        assert str(store.ref_path) in str(raised.value)
        assert store.read_ref() == payload
        assert list(Path(store.ref_path).parent.iterdir()) == [Path(store.ref_path)]

    @pytest.mark.parametrize(
        ("existing_reference", "cleanup_fails"), [(False, False), (True, False), (True, True)]
    )
    @mock.patch.object(Path, "unlink", autospec=True, side_effect=Path.unlink)
    @mock.patch.object(store_module.os, "replace", autospec=True, side_effect=store_module.os.replace)
    def test_write_ref_chains_original_error_when_replace_fails(
        self, mock_replace, mock_unlink, store, existing_reference, cleanup_fails
    ):
        store.prepare_publish_areas()
        if existing_reference:
            store.write_ref({"version": "v1"})
        original_error = OSError(errno.EIO, "replace failed")
        mock_replace.side_effect = original_error
        if cleanup_fails:
            mock_unlink.side_effect = PermissionError("cleanup failed")

        with pytest.raises(BundleManifestError, match="replace failed") as raised:
            store.write_ref({"version": "v2"})

        assert raised.value.__cause__ is original_error
        if existing_reference:
            assert store.read_ref() == {"version": "v1"}
        else:
            assert not Path(store.ref_path).exists()
        temp_path = mock_replace.call_args.args[0]
        mock_unlink.assert_called_once_with(temp_path, missing_ok=True)
        assert temp_path.exists() is cleanup_fails
        assert set(Path(store.ref_path).parent.iterdir()) == (
            ({Path(store.ref_path)} if existing_reference else set())
            | ({temp_path} if cleanup_fails else set())
        )

    @mock.patch.object(store_module.os, "replace", autospec=True, side_effect=_replace_then_fail)
    @mock.patch.object(
        store_module, "fsync_directory", autospec=True, side_effect=store_module.fsync_directory
    )
    def test_write_ref_accepts_a_rename_that_reported_failure(self, mock_fsync, mock_replace, store):
        store.prepare_publish_areas()

        store.write_ref({"version": "v1"})

        mock_replace.assert_called_once()
        assert store.read_ref() == {"version": "v1"}
        assert mock_fsync.call_args == mock.call(Path(store.ref_path).parent)
        assert list(Path(store.ref_path).parent.iterdir()) == [Path(store.ref_path)]

    @pytest.mark.parametrize(
        "previous_payload",
        [
            pytest.param(None, id="first-publication"),
            pytest.param({"version": "v1"}, id="replacement"),
        ],
    )
    @mock.patch.object(store_module.os, "replace", autospec=True, side_effect=_real_replace)
    def test_write_ref_reports_a_swept_temp_file_as_durability_error(
        self, mock_replace, store, previous_payload
    ):
        store.prepare_publish_areas()
        if previous_payload is not None:
            store.write_ref(previous_payload)
        mock_replace.side_effect = _sweep_then_fail

        with pytest.raises(BundleManifestRefDurabilityError) as raised:
            store.write_ref({"version": "v2"})

        assert raised.value.version == "v2"
        assert isinstance(raised.value.__cause__, FileNotFoundError)
        assert raised.value.__cause__.filename == os.fspath(mock_replace.call_args.args[0])
        if previous_payload is None:
            assert list(Path(store.ref_path).parent.iterdir()) == []
        else:
            assert store.read_ref() == previous_payload
            assert list(Path(store.ref_path).parent.iterdir()) == [Path(store.ref_path)]

    @mock.patch.object(
        store_module.os, "replace", autospec=True, side_effect=OSError(errno.EIO, "rename reply lost")
    )
    def test_write_ref_reports_an_unverifiable_rename_as_durability_error(self, mock_replace, store):
        store.prepare_publish_areas()
        lstat_error = OSError(errno.EIO, "cannot stat temporary reference")
        real_lstat = Path.lstat

        def fail_temp_lstat(path):
            if path == mock_replace.call_args.args[0]:
                raise lstat_error
            return real_lstat(path)

        with (
            mock.patch.object(Path, "lstat", autospec=True, side_effect=fail_temp_lstat),
            pytest.raises(BundleManifestRefDurabilityError) as raised,
        ):
            store.write_ref({"version": "v1"})

        assert raised.value.version == "v1"
        assert raised.value.__cause__ is lstat_error

    def test_publication_guard_removes_read_only_orphan_and_keeps_other_hidden_dirs(self, store):
        store.prepare_publish_areas()
        orphan = Path(store.snapshots_root) / f".sha256-{'1' * 64}.abandoned"
        (orphan / "dags").mkdir(parents=True)
        (orphan / "dags" / "partial.py").write_text("print('partial')")
        store_module._set_snapshot_permissions(orphan)
        unrelated_hidden_dir = Path(store.snapshots_root) / ".not-a-publisher-temp"
        unrelated_hidden_dir.mkdir()

        with store.publication_guard():
            pass

        assert not orphan.exists()
        assert unrelated_hidden_dir.exists()

    @mock.patch.object(
        filesystem_module,
        "remove_bundle_tree_forcefully",
        autospec=True,
        side_effect=PermissionError("denied"),
    )
    def test_publication_guard_survives_undeletable_orphan(self, mock_remove, store):
        store.prepare_publish_areas()
        orphan = Path(store.snapshots_root) / f".sha256-{'1' * 64}.abandoned"
        orphan.mkdir()

        with store.publication_guard():
            pass

        mock_remove.assert_called_once_with(orphan)
        assert orphan.exists()


_STORAGE_FAULTS = [
    ("prepare_publish_areas", store_module, "_ensure_public_dir"),
    ("snapshot_exists", Path, "stat"),
    ("fetch_snapshot", store_module.shutil, "copytree"),
    ("publish_snapshot", store_module, "fsync_tree_directories"),
    ("publication_guard", store_module, "remove_orphaned_temp_snapshots"),
    ("read_ref", Path, "read_text"),
    ("write_ref", store_module.os, "replace"),
]
_STORAGE_ERRORS = [
    ("missing", FileNotFoundError(errno.ENOENT, "gone"), BundleManifestNotFoundError),
    ("denied", PermissionError(errno.EACCES, "denied"), BundleManifestError),
]


class TestFilesystemStorageErrors:
    @pytest.mark.parametrize(
        ("operation", "patch_owner", "patch_name", "error", "expected"),
        [
            pytest.param(operation, owner, name, error, expected, id=f"{operation}-{error_id}")
            for operation, owner, name in _STORAGE_FAULTS
            for error_id, error, expected in _STORAGE_ERRORS
            # A missing reference is covered by test_read_ref_missing_raises_not_found, and a
            # missing snapshot is not an error for snapshot_exists.
            if (operation, error_id) not in {("read_ref", "missing"), ("snapshot_exists", "missing")}
        ],
    )
    def test_storage_oserror_is_reported_as_bundle_error(
        self, store, tmp_path, operation, patch_owner, patch_name, error, expected
    ):
        source = _write_source(tmp_path / "source")
        store.prepare_publish_areas()
        if operation == "fetch_snapshot":
            version = _publish(store, source)[0].version
        operations = {
            "prepare_publish_areas": store.prepare_publish_areas,
            "snapshot_exists": partial(store.snapshot_exists, "sha256-" + "0" * 64),
            "fetch_snapshot": lambda: store.fetch_snapshot(
                version, tmp_path / "dest", structural_validator=lambda tree: None
            ),
            "publish_snapshot": partial(_publish, store, source),
            "publication_guard": lambda: store.publication_guard().__enter__(),
            "read_ref": store.read_ref,
            "write_ref": partial(store.write_ref, {"version": "v1"}),
        }

        with (
            mock.patch.object(patch_owner, patch_name, autospec=True, side_effect=error),
            pytest.raises(expected) as raised,
        ):
            operations[operation]()

        assert not isinstance(raised.value, BundleManifestSourceChangedError)
        assert raised.value.__cause__ is error

    @pytest.mark.parametrize(
        ("failing_side", "error", "expected"),
        [
            pytest.param(
                "source",
                FileNotFoundError(errno.ENOENT, "gone"),
                BundleManifestSourceChangedError,
                id="source-gone",
            ),
            pytest.param(
                "source", PermissionError(errno.EACCES, "denied"), BundleManifestError, id="source-denied"
            ),
            pytest.param(
                "published",
                FileNotFoundError(errno.ENOENT, "gone"),
                BundleManifestNotFoundError,
                id="published-gone",
            ),
        ],
    )
    def test_publish_snapshot_attributes_copy_failures(self, store, tmp_path, failing_side, error, expected):
        source = _write_source(tmp_path / "source")

        def fail_copy(src, dst, **kwargs):
            error.filename = os.fspath(src if failing_side == "source" else dst)
            raise error

        with (
            mock.patch.object(store_module.shutil, "copy", autospec=True, side_effect=fail_copy),
            pytest.raises(BundleManifestError) as raised,
        ):
            _publish(store, source)

        assert type(raised.value) is expected
        assert list(Path(store.snapshots_root).iterdir()) == []

    @mock.patch.object(store_module.shutil, "copy", autospec=True, side_effect=_fail_fast_copy)
    def test_publish_snapshot_reports_fast_copy_failures_as_storage_errors(self, mock_copy, store, tmp_path):
        with pytest.raises(BundleManifestError, match="Cannot publish snapshot .*No space left") as raised:
            _publish(store, _write_source(tmp_path / "source"))

        assert type(raised.value) is BundleManifestError
        assert list(Path(store.snapshots_root).iterdir()) == []

    @pytest.mark.parametrize("operation", ["snapshot_exists", "fetch_snapshot", "publish_snapshot"])
    @mock.patch.object(Path, "exists", autospec=True, side_effect=_exists_as_in_python_314)
    @mock.patch.object(Path, "is_dir", autospec=True, side_effect=_is_dir_as_in_python_314)
    def test_snapshot_storage_errors_are_not_reported_as_missing(
        self, mock_is_dir, mock_exists, store, tmp_path, operation
    ):
        source = _write_source(tmp_path / "source")
        result, _ = _publish(store, source)
        version_path = os.fspath(store.snapshot_path(result.version))
        stat_error = PermissionError(errno.EACCES, "denied")
        real_stat = os.stat

        def deny_snapshot(path, *args, **kwargs):
            if os.fspath(path) == version_path:
                raise stat_error
            return real_stat(path, *args, **kwargs)

        operations = {
            "snapshot_exists": partial(store.snapshot_exists, result.version),
            "fetch_snapshot": partial(
                store.fetch_snapshot, result.version, tmp_path / "dest", structural_validator=_validate_tree
            ),
            "publish_snapshot": partial(
                store.publish_snapshot,
                manifest=result.manifest,
                source_root=source,
                validate_existing=_validate_tree,
            ),
        }
        with (
            mock.patch.object(os, "stat", autospec=True, side_effect=deny_snapshot),
            pytest.raises(BundleManifestError) as raised,
        ):
            operations[operation]()

        assert type(raised.value) is BundleManifestError
        assert raised.value.__cause__ is stat_error

    @mock.patch.object(Path, "read_text", autospec=True, side_effect=PermissionError(errno.EACCES, "denied"))
    def test_read_json_file_reports_unreadable_document(self, mock_read_text, tmp_path):
        with pytest.raises(BundleManifestError, match="Cannot read") as raised:
            read_json_file(tmp_path / "doc.json", missing_message="missing", invalid_message="invalid")

        assert raised.value.__cause__ is mock_read_text.side_effect


class TestFsyncTree:
    @mock.patch.object(store_module, "fsync_file", autospec=True, side_effect=store_module.fsync_file)
    def test_ignored_file_can_disappear_while_authoritative_files_are_synced(self, mock_fsync, tmp_path):
        tree = _write_source(tmp_path / "tree")
        manifest = tree / MANIFEST_FILE_NAME
        manifest.write_text("{}")
        temporary_bytecode = tree / "dags" / "__pycache__" / "example.cpython-312.pyc.123"
        temporary_bytecode.parent.mkdir()
        temporary_bytecode.write_bytes(b"partial bytecode")

        def ignore_file(path: Path) -> bool:
            if path != temporary_bytecode:
                return False
            temporary_bytecode.unlink()
            return True

        store_module.fsync_tree(tree, ignore_file=ignore_file)

        assert not temporary_bytecode.exists()
        assert mock_fsync.call_count == 2
        mock_fsync.assert_has_calls(
            [mock.call(manifest), mock.call(tree / "dags" / "example.py")], any_order=True
        )
