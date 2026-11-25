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

import gzip
import json
import os
import shutil
import stat
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from unittest import mock

import pytest
import time_machine

from airflow.providers.google.version_compat import get_base_airflow_version_tuple

if get_base_airflow_version_tuple() >= (3, 5, 0):
    from airflow.dag_processing.bundles import object_source as object_source_module
    from airflow.dag_processing.bundles.artifact_store import BundleManifestNotFoundError
    from airflow.dag_processing.bundles.base import BundleNotReadyError
    from airflow.dag_processing.bundles.manager import _sign_bundle_url
    from airflow.dag_processing.bundles.manifest import (
        MANIFEST_FILE_NAME,
        BundleManifestError,
        BundleManifestSourceChangedError,
    )
    from airflow.dag_processing.bundles.manifest_bundle import BundleManifestReferenceChangedError
    from airflow.dag_processing.bundles.manifest_local import (
        ManifestLocalDagBundle,
        publish_manifest_local_dag_bundle,
    )
    from airflow.models.dagbundle import DagBundleModel
    from airflow.providers.google.cloud.bundles import (
        gcs_artifact_store as gcs_store_module,
        manifest_gcs as gcs_module,
    )
    from airflow.providers.google.cloud.bundles.manifest_gcs import (
        ManifestGCSDagBundle,
        publish_manifest_gcs_dag_bundle,
    )

else:
    pytest.skip("Manifest bundles require Airflow 3.5+", allow_module_level=True)

from tests_common.test_utils.config import conf_vars
from unit.google.cloud.bundles.gcs_fakes import FakeStoreGCSClient


@pytest.fixture(autouse=True)
def isolate_manifest_storage(tmp_path):
    (tmp_path / "published").mkdir(exist_ok=True)
    with mock.patch(
        "airflow.dag_processing.bundles.base.STALE_BUNDLE_TRACKING_FOLDER", tmp_path / "tracking"
    ):
        yield


class FakeGCSError(Exception):
    def __init__(self, code: int, operation: str) -> None:
        super().__init__(f"{operation}: {code}")
        self.code = code


class FakeGCSBlob:
    def __init__(
        self,
        client: FakeGCSClient,
        name: str,
        *,
        generation: int | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        self._client = client
        self.name = name
        self._requested_generation = generation
        data = metadata if metadata is not None else client.objects.get(name)
        self.content_encoding = data.get("content_encoding") if data is not None else None
        if data is None:
            self.size = None
            self.generation = generation
            self.metageneration = None
            self.updated = None
            self.etag = None
        else:
            self.size = len(data["body"])
            self.generation = data["generation"]
            self.metageneration = data["metageneration"]
            self.updated = data["updated"]
            self.etag = data["etag"]

    def download_to_filename(
        self, filename: str, *, if_generation_match: int, raw_download: bool = False
    ) -> None:
        assert raw_download is True
        if self._client.fail_download:
            raise RuntimeError("download failed")
        mutation = self._client.mutate_before_download
        if mutation is not None:
            name, body = mutation
            self._client.put(name, body)
            self._client.mutate_before_download = None
        data = self._client.objects.get(self.name)
        if data is None:
            raise FakeGCSError(404, "Download")
        if data["generation"] != if_generation_match or self._requested_generation != if_generation_match:
            raise FakeGCSError(412, "Download")
        Path(filename).write_bytes(data["body"])
        self._client.downloads.append((self.name, if_generation_match))
        mutation = self._client.mutate_after_download
        if mutation is not None:
            name, body = mutation
            self._client.put(name, body)
            self._client.mutate_after_download = None


class FakeGCSBucket:
    def __init__(self, client: FakeGCSClient, name: str) -> None:
        self._client = client
        self.name = name

    def get_blob(self, name: str) -> FakeGCSBlob | None:
        data = self._client.objects.get(name)
        return None if data is None else FakeGCSBlob(self._client, name, metadata=dict(data))

    def blob(self, name: str, *, generation: int | None = None) -> FakeGCSBlob:
        return FakeGCSBlob(self._client, name, generation=generation)


class FakeGCSClient:
    def __init__(self, *, endpoint: str = "https://storage.example.test") -> None:
        self._connection = SimpleNamespace(API_BASE_URL=endpoint)
        self.objects: dict[str, dict[str, object]] = {}
        self.downloads: list[tuple[str, int]] = []
        self.list_calls = 0
        self.bucket_exists = True
        self.bucket_error: Exception | None = None
        self.fail_download = False
        self.mutate_on_list: tuple[int, str, bytes] | None = None
        self.mutate_before_download: tuple[str, bytes] | None = None
        self.mutate_after_download: tuple[str, bytes] | None = None
        self._next_generation = 1

    def put(self, name: str, body: bytes, *, content_encoding: str | None = None) -> None:
        generation = self._next_generation
        self._next_generation += 1
        self.objects[name] = {
            "body": body,
            "generation": generation,
            "metageneration": 1,
            "updated": f"2026-08-03T12:00:{generation:02d}+00:00",
            "etag": f"etag-{generation}",
            "content_encoding": content_encoding,
        }

    def delete(self, name: str) -> None:
        del self.objects[name]

    def get_bucket(self, name: str) -> FakeGCSBucket:
        assert name == "dag-bucket"
        if self.bucket_error is not None:
            raise self.bucket_error
        if not self.bucket_exists:
            raise FakeGCSError(404, "GetBucket")
        return FakeGCSBucket(self, name)

    def bucket(self, name: str) -> FakeGCSBucket:
        assert name == "dag-bucket"
        return FakeGCSBucket(self, name)

    def list_blobs(self, bucket_name: str, *, prefix: str, max_results: int | None = None):
        assert bucket_name == "dag-bucket"
        self.list_calls += 1
        mutation = self.mutate_on_list
        if mutation is not None and mutation[0] == self.list_calls:
            _, name, body = mutation
            self.put(name, body)
            self.mutate_on_list = None
        blobs = [
            FakeGCSBlob(self, name, metadata=dict(data))
            for name, data in sorted(self.objects.items())
            if name.startswith(prefix)
        ]
        return blobs if max_results is None else blobs[:max_results]


def _install_fake_hook(monkeypatch, client: FakeGCSClient):
    constructed: list[str] = []

    class FakeGCSHook:
        def __init__(self, *, gcp_conn_id: str) -> None:
            constructed.append(gcp_conn_id)

        def get_conn(self):
            return client

    monkeypatch.setattr(gcs_module, "GCSHook", FakeGCSHook)
    return constructed


def _bundle(tmp_path: Path, **kwargs) -> ManifestGCSDagBundle:
    prefix = kwargs.pop("prefix", "dags/")
    published_root = kwargs.pop("published_root", str(tmp_path / "published"))
    if not published_root.startswith("gs://"):
        Path(published_root).mkdir(parents=True, exist_ok=True)
    return ManifestGCSDagBundle(
        name="manifest-gcs",
        bucket_name="dag-bucket",
        prefix=prefix,
        published_root=published_root,
        source_stability_seconds=kwargs.pop("source_stability_seconds", 0),
        **kwargs,
    )


def _version_string(version) -> str:
    return getattr(version, "version", version)


def test_constructor_matches_stock_defaults_and_constructs_hook_lazily(tmp_path, monkeypatch):
    client = FakeGCSClient()
    constructed = _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        custom = _bundle(tmp_path, gcp_conn_id="custom")

        assert bundle.gcp_conn_id == "google_cloud_default"
        assert bundle.bucket_name == "dag-bucket"
        assert bundle.prefix == "dags/"
        assert bundle.max_file_count == gcs_module.DEFAULT_MAX_FILE_COUNT
        assert bundle.max_file_size_bytes == gcs_module.DEFAULT_MAX_FILE_SIZE_BYTES
        assert bundle.max_total_size_bytes == gcs_module.DEFAULT_MAX_TOTAL_SIZE_BYTES
        assert bundle.auto_publish is True
        assert bundle.gcs_dags_dir == bundle.base_dir / "_gcs_source"
        assert bundle.gcs_dags_dir != bundle.versions_dir
        assert bundle.view_url_template() is None
        assert constructed == []
        assert custom.gcs_hook is not None
        assert constructed == ["custom"]


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("auto_publish", 1),
        ("max_file_count", 0),
        ("max_file_size_bytes", -1),
        ("max_total_size_bytes", 1.5),
        ("gcp_conn_id", ""),
        ("prefix", 12),
        ("deployment_marker_key", ""),
        ("deployment_marker_key", "/ready"),
        ("deployment_marker_key", "../ready"),
        ("deployment_marker_key", "ready/"),
        ("deployment_marker_key", "a//ready"),
        ("deployment_marker_key", "a\\ready"),
        ("deployment_marker_key", "ready\x00"),
        ("published_root_conn_id", "source"),
    ],
)
def test_constructor_rejects_invalid_options(tmp_path, option, value):
    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(TypeError, match=option),
    ):
        _bundle(tmp_path, **{option: value})


def test_constructor_rejects_cross_cloud_published_root(tmp_path, monkeypatch):
    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(TypeError, match="Unsupported published_root scheme"),
    ):
        ManifestGCSDagBundle(
            name="manifest-gcs",
            bucket_name="dag-bucket",
            prefix="dags/",
            published_root="s3://dag-releases/team",
        )


def test_first_refresh_downloads_and_publishes_version(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"print('dag')")
    generation = client.objects["dags/example.py"]["generation"]
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        version = _version_string(bundle.get_current_version())

        assert client.downloads == [("dags/example.py", generation)]
        assert bundle.path == bundle.versions_dir / version
        assert (bundle.path / "example.py").read_bytes() == b"print('dag')"
        ref = json.loads(Path(bundle._store.ref_locator).read_text())
        assert ref["source"]["type"] == "gcs"
        assert ref["source"]["identity"].startswith("sha256:")
        assert ref["source"]["observation"].startswith("sha256:")
        assert (Path(bundle._store.snapshot_locator(version)) / MANIFEST_FILE_NAME).is_file()


def test_explicit_mode_refresh_uses_release_without_accessing_gcs(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "example.py").write_text("print('dag')")

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        local_bundle = ManifestLocalDagBundle(
            name="manifest-gcs",
            published_root=str(tmp_path / "published"),
        )
        published = publish_manifest_local_dag_bundle(bundle=local_bundle, source_path=source)

        class ForbiddenHook:
            def __init__(self, **kwargs):
                raise AssertionError(f"GCS hook must not be constructed: {kwargs}")

        monkeypatch.setattr(gcs_module, "GCSHook", ForbiddenHook)
        explicit = _bundle(tmp_path, auto_publish=False)
        explicit.refresh()

        assert _version_string(explicit.get_current_version()) == published.version
        assert (explicit.path / "example.py").read_text() == "print('dag')"
        assert explicit._gcs_hook is None
        assert not explicit.gcs_dags_dir.exists()


def test_explicit_publisher_guards_modes_before_gcs_access(tmp_path, monkeypatch):
    monkeypatch.setattr(gcs_module, "GCSHook", None)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(BundleManifestError, match="auto_publish enabled"):
            publish_manifest_gcs_dag_bundle(bundle=_bundle(tmp_path))
        with pytest.raises(BundleManifestError, match="Cannot explicitly publish pinned bundle"):
            publish_manifest_gcs_dag_bundle(
                bundle=_bundle(
                    tmp_path,
                    auto_publish=False,
                    version=f"sha256-{'a' * 64}",
                )
            )


def test_explicit_publisher_records_source_and_expected_version(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"first")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, auto_publish=False)
        with mock.patch.object(bundle, "lock", wraps=bundle.lock, spec=bundle.lock) as bundle_lock:
            first = publish_manifest_gcs_dag_bundle(bundle=bundle)

        assert bundle_lock.call_count == 1
        assert first.version.startswith("sha256-")
        assert first.ref_payload["source"]["type"] == "gcs"
        assert not Path(bundle._store.state_locator).exists()
        repeated = publish_manifest_gcs_dag_bundle(bundle=bundle)
        assert repeated.version == first.version
        assert repeated.created_snapshot is False

        client.put("dags/example.py", b"second")
        second = publish_manifest_gcs_dag_bundle(
            bundle=bundle,
            expected_current_version=first.version,
        )
        client.put("dags/example.py", b"third")
        with pytest.raises(BundleManifestReferenceChangedError, match="manifest reference changed"):
            publish_manifest_gcs_dag_bundle(
                bundle=bundle,
                expected_current_version=first.version,
            )
        assert json.loads(Path(bundle._store.ref_locator).read_text())["version"] == second.version


def test_same_size_generation_change_and_delete_repair_mirror(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    client.put("dags/delete.py", b"gone")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        client.put("dags/example.py", b"new")
        new_generation = client.objects["dags/example.py"]["generation"]
        client.delete("dags/delete.py")
        bundle.refresh()

        assert _version_string(bundle.get_current_version()) != first_version
        assert (bundle.gcs_dags_dir / "example.py").read_bytes() == b"new"
        assert not (bundle.gcs_dags_dir / "delete.py").exists()
        assert ("dags/example.py", new_generation) in client.downloads


def test_generation_precondition_stops_changed_download(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"candidate")
    client.mutate_before_download = ("dags/example.py", b"changed")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(
            BundleManifestSourceChangedError,
            match="changed before it could be downloaded",
        ),
    ):
        publish_manifest_gcs_dag_bundle(bundle=_bundle(tmp_path, auto_publish=False))

    assert not (tmp_path / "published/refs/manifest-gcs/latest.json").exists()


def test_new_process_repairs_corrupt_reused_mirror(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"good")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        first = _bundle(tmp_path)
        first.refresh()
        first.gcs_dags_dir.joinpath("example.py").write_bytes(b"evil")
        downloads_before = len(client.downloads)

        fresh = _bundle(tmp_path)
        fresh.refresh()

        assert len(client.downloads) == downloads_before + 1
        assert (fresh.gcs_dags_dir / "example.py").read_bytes() == b"good"


def test_marker_change_repairs_same_size_corrupt_reused_mirror(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"remote")
    client.put("dags/.ready", b"release-1")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready")
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        mirror_file = bundle.gcs_dags_dir / "example.py"
        original_stat = mirror_file.stat()

        mirror_file.write_bytes(b"broken")
        os.utime(
            mirror_file,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        client.put("dags/.ready", b"release-2")
        client.downloads.clear()
        bundle.refresh()

        assert [name for name, _ in client.downloads] == ["dags/example.py"]
        assert mirror_file.read_bytes() == b"remote"
        assert (bundle.path / "example.py").read_bytes() == b"remote"
        assert _version_string(bundle.get_current_version()) == first_version


@pytest.mark.parametrize(
    "unsafe_name",
    ["dags/../escape.py", "dags//empty.py", "dags/back\\slash.py", "dags/bad\x00.py"],
)
def test_unsafe_gcs_name_is_rejected_before_download(tmp_path, monkeypatch, unsafe_name):
    client = FakeGCSClient()
    client.put(unsafe_name, b"dag")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestError, match="unsafe"),
    ):
        _bundle(tmp_path).refresh()

    assert client.downloads == []


def test_file_directory_collision_is_rejected(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/collision", b"file")
    client.put("dags/collision/child.py", b"child")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestError, match="collides"),
    ):
        _bundle(tmp_path).refresh()

    assert client.downloads == []


def test_prefix_boundary_directory_markers_and_ignored_files(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    client.put("dags/subdir/", b"")
    client.put("dags/.git/config", b"ignored")
    client.put("dags/cache.pyc", b"ignored")
    client.put("dags-archive/old.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, prefix="dags")
        bundle.refresh()

        assert [name for name, _ in client.downloads] == ["dags/example.py"]
        assert (bundle.path / "example.py").is_file()
        assert not (bundle.path / "old.py").exists()


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("max_file_count", 1, "max_file_count"),
        ("max_file_size_bytes", 2, "max_file_size_bytes"),
        ("max_total_size_bytes", 4, "max_total_size_bytes"),
    ],
)
def test_source_limits_are_enforced_before_download(tmp_path, monkeypatch, option, value, message):
    client = FakeGCSClient()
    client.put("dags/first.py", b"abc")
    client.put("dags/second.py", b"def")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestError, match=message),
    ):
        _bundle(tmp_path, **{option: value}).refresh()

    assert client.downloads == []


def test_changed_inventory_does_not_advance_mirror_state_or_release(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        original_ref = json.loads(Path(bundle._store.ref_locator).read_text())
        original_state = bundle.gcs_mirror_state_path.read_text()

        client.put("dags/example.py", b"candidate")
        client.mutate_after_download = ("dags/example.py", b"changed-again")
        bundle.refresh()

        assert json.loads(Path(bundle._store.ref_locator).read_text()) == original_ref
        assert bundle.gcs_mirror_state_path.read_text() == original_state


def test_sync_failure_keeps_current_release(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        version = _version_string(bundle.get_current_version())
        client.put("dags/example.py", b"new")
        client.fail_download = True

        bundle.refresh()

        assert _version_string(bundle.get_current_version()) == version
        assert (bundle.path / "example.py").read_bytes() == b"old"


@pytest.mark.parametrize("has_source", [False, True])
def test_deployment_marker_is_required_excluded_and_defines_release(tmp_path, monkeypatch, has_source):
    client = FakeGCSClient()
    if has_source:
        client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready")
        with pytest.raises(BundleNotReadyError, match="no release is available") as error:
            bundle.refresh()
        assert isinstance(error.value.__cause__, BundleManifestSourceChangedError)
        assert "deployment marker" in str(error.value.__cause__)

        client.put("dags/example.py", b"old")
        client.put("dags/.ready", b"release-1")
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        assert not (bundle.gcs_dags_dir / ".ready").exists()

        client.put("dags/example.py", b"new")
        bundle.refresh()
        assert _version_string(bundle.get_current_version()) == first_version

        client.put("dags/.ready", b"release-2")
        bundle.refresh()
        assert _version_string(bundle.get_current_version()) != first_version
        assert (bundle.path / "example.py").read_bytes() == b"new"


def test_remote_change_before_reference_write_keeps_previous_release(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        client.put("dags/example.py", b"candidate")
        client.mutate_on_list = (
            client.list_calls + 3,
            "dags/example.py",
            b"changed-again",
        )
        bundle.refresh()

        assert _version_string(bundle.get_current_version()) == first_version
        assert json.loads(Path(bundle._store.ref_locator).read_text())["version"] == first_version


def test_empty_source_requires_explicit_opt_in(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/.git/config", b"ignored")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(BundleManifestError, match="explicitly publish empty source tree"):
            publish_manifest_gcs_dag_bundle(bundle=_bundle(tmp_path, auto_publish=False))

        client.delete("dags/.git/config")
        published = publish_manifest_gcs_dag_bundle(
            bundle=_bundle(tmp_path, auto_publish=False, allow_empty_source=True)
        )
        assert published.file_count == 0


def test_bucket_errors_use_the_bundle_error_contract(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.bucket_exists = False
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestNotFoundError, match="does not exist"),
    ):
        _bundle(tmp_path).refresh()

    client.bucket_exists = True
    client.bucket_error = FakeGCSError(403, "GetBucket")
    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles2")}),
        pytest.raises(BundleManifestError, match="Could not access"),
    ):
        _bundle(tmp_path).refresh()


def test_missing_prefix_is_a_recoverable_not_found_error(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags-archive/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestNotFoundError, match="prefix .* does not exist"),
    ):
        _bundle(tmp_path).refresh()

    assert client.downloads == []


def test_pinned_initialization_does_not_construct_gcs_hook_or_read_latest(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "example.py").write_text("print('dag')")
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        local_bundle = ManifestLocalDagBundle(
            name="manifest-gcs",
            published_root=str(tmp_path / "published"),
        )
        published = publish_manifest_local_dag_bundle(bundle=local_bundle, source_path=source)
        Path(local_bundle._store.ref_locator).unlink()

        class ForbiddenHook:
            def __init__(self, **kwargs):
                raise AssertionError(f"GCS hook must not be constructed: {kwargs}")

        monkeypatch.setattr(gcs_module, "GCSHook", ForbiddenHook)
        pinned = _bundle(tmp_path, version=published.version)
        assert pinned.view_url_template() is None
        pinned.initialize()

        assert (pinned.path / "example.py").read_text() == "print('dag')"
        assert not pinned.gcs_dags_dir.exists()


def test_source_identity_excludes_connection_and_rejects_endpoint_change(tmp_path, monkeypatch):
    first_client = FakeGCSClient(endpoint="https://first.example.test")
    first_client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, first_client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        first = _bundle(tmp_path, gcp_conn_id="first")
        same = _bundle(tmp_path, gcp_conn_id="second")
        assert first._get_source_identity(first._get_source_client()) == same._get_source_identity(
            same._get_source_client()
        )
        first.refresh()
        original_ref = json.loads(Path(first._store.ref_locator).read_text())

        second_client = FakeGCSClient(endpoint="https://second.example.test")
        second_client.put("dags/example.py", b"dag")
        _install_fake_hook(monkeypatch, second_client)
        second = _bundle(tmp_path)
        second.refresh()

        assert json.loads(Path(second._store.ref_locator).read_text()) == original_ref


def test_source_identity_normalizes_the_default_public_endpoint(tmp_path, monkeypatch):
    default_client = FakeGCSClient(endpoint="https://storage.googleapis.com")
    _install_fake_hook(monkeypatch, default_client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        default_identity = bundle._get_source_identity(bundle._get_source_client())

        # A library upgrade that hides the private endpoint attributes must not
        # change the identity of a default deployment.
        stripped_client = FakeGCSClient()
        stripped_client._connection = SimpleNamespace()
        _install_fake_hook(monkeypatch, stripped_client)
        stripped = _bundle(tmp_path)
        assert stripped._get_source_identity(stripped._get_source_client()) == default_identity

        custom_client = FakeGCSClient(endpoint="https://custom.example.test")
        _install_fake_hook(monkeypatch, custom_client)
        custom = _bundle(tmp_path)
        assert custom._get_source_identity(custom._get_source_client()) != default_identity


def test_prepare_publish_source_populates_copy_hints(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"print('dag')")
    generation = client.objects["dags/example.py"]["generation"]
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, published_root="gs://releases-bucket/root")
        bundle.prepare_publish_source()

    assert bundle._store.copy_hints == {
        "example.py": {
            "type": "gcs",
            "endpoint": "https://storage.example.test",
            "bucket": "dag-bucket",
            "name": "dags/example.py",
            "generation": generation,
        }
    }


def _install_fake_store_hook(monkeypatch, client):
    class FakeStoreHook:
        default_conn_name = "google_cloud_default"

        def __init__(self, *, gcp_conn_id: str) -> None:
            self.gcp_conn_id = gcp_conn_id

        def get_conn(self):
            return client

    monkeypatch.setattr(gcs_store_module, "GCSHook", FakeStoreHook)


def test_gcs_source_with_gcs_published_root_publishes_and_pins(tmp_path, monkeypatch):
    source_client = FakeGCSClient()
    source_client.put("dags/example.py", b"print('dag')")
    _install_fake_hook(monkeypatch, source_client)
    store_client = FakeStoreGCSClient()
    _install_fake_store_hook(monkeypatch, store_client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
    ):
        bundle = _bundle(tmp_path, published_root="gs://release-bucket/releases")
        bundle.refresh()
        version = _version_string(bundle.get_current_version())

        assert (bundle.path / "example.py").read_bytes() == b"print('dag')"
        ref = json.loads(store_client.get("releases/refs/manifest-gcs/latest.json", bucket="release-bucket"))
        assert ref["version"] == version
        assert ref["source"]["type"] == "gcs"
        # The gs:// root removes the shared filesystem, so no fallback hint is logged.

        # Pinned execution reads only the published root, never the source.
        class ForbiddenHook:
            def __init__(self, **kwargs):
                raise AssertionError(f"GCS source hook must not be constructed: {kwargs}")

        monkeypatch.setattr(gcs_module, "GCSHook", ForbiddenHook)
        shutil.rmtree(bundle.path)
        store_client.pop("releases/refs/manifest-gcs/latest.json", bucket="release-bucket")
        pinned = _bundle(tmp_path, published_root="gs://release-bucket/releases", version=version)
        pinned.initialize()
        assert (pinned.path / "example.py").read_bytes() == b"print('dag')"


def test_replicas_share_stability_despite_different_local_mirrors(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"first")
    _install_fake_hook(monkeypatch, client)
    with time_machine.travel("2026-11-19T10:00:00Z", tick=False) as clock:
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "host-one")}):
            first = _bundle(tmp_path, source_stability_seconds=30)
            with pytest.raises(BundleNotReadyError):
                first.refresh()
            state = first._store.read_state()

        clock.shift(30)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "host-two")}):
            second = _bundle(tmp_path, source_stability_seconds=30)
            second.refresh()
            assert (second.path / "example.py").read_bytes() == b"first"
            assert second.gcs_dags_dir != first.gcs_dags_dir
            assert second._store.read_state() == state


def test_marker_only_release_updates_reference_without_changing_version(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"first")
    client.put("dags/.ready", b"release-one")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready")
        bundle.refresh()
        original = bundle._store.read_ref()
        client.put("dags/.ready", b"release-two")
        bundle.refresh()
        replacement = bundle._store.read_ref()
        assert replacement["version"] == original["version"]
        assert replacement["source"]["deployment_marker"] != original["source"]["deployment_marker"]

        client.put("dags/example.py", b"incomplete release-three")
        bundle.refresh()
        assert bundle._store.read_ref() == replacement
        assert (bundle.path / "example.py").read_bytes() == b"first"


@pytest.mark.parametrize("location", ["mirror", "file", "directory"])
def test_mirror_symlinks_are_replaced_without_changing_targets(tmp_path, monkeypatch, location):
    client = FakeGCSClient()
    client.put("dags/sub/example.py", b"published")
    _install_fake_hook(monkeypatch, client)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "example.py").write_bytes(b"keep")
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        target = {
            "mirror": bundle.gcs_dags_dir,
            "file": bundle.gcs_dags_dir / "sub/example.py",
            "directory": bundle.gcs_dags_dir / "sub",
        }[location]
        if target.is_dir():
            shutil.rmtree(target)
            target.symlink_to(outside, target_is_directory=True)
        else:
            target.unlink()
            target.symlink_to(outside / "example.py")
        _bundle(tmp_path).refresh()
        assert not target.is_symlink()
        assert (bundle.gcs_dags_dir / "sub/example.py").read_bytes() == b"published"
        assert (outside / "example.py").read_bytes() == b"keep"


@pytest.mark.parametrize("published_prefix", ["", "dags", "dags/releases"])
def test_source_cannot_include_published_artifacts(tmp_path, monkeypatch, published_prefix):
    client = FakeGCSClient()
    client.put("dags/example.py", b"source")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(TypeError, match="must not overlap"):
            _bundle(tmp_path, published_root=f"gs://dag-bucket/{published_prefix}")
    assert client.downloads == []


@pytest.mark.parametrize("source_metadata", [None, [], {"identity": 1}])
def test_invalid_release_source_metadata_is_rejected(tmp_path, monkeypatch, source_metadata):
    client = FakeGCSClient()
    client.put("dags/example.py", b"source")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        ref = bundle._store.read_ref()
        ref["source"] = source_metadata
        with bundle._store.publication_guard():
            bundle._store.write_ref(ref)
        with pytest.raises(BundleManifestError, match="invalid source metadata"):
            _bundle(tmp_path).refresh()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", None),
        ("size", True),
        ("size", -1),
        ("generation", 0),
        ("generation", True),
        ("metageneration", None),
        ("updated", ""),
        ("etag", None),
    ],
)
@mock.patch.object(FakeGCSClient, "list_blobs", autospec=True)
def test_invalid_object_metadata_is_rejected_before_download(list_blobs, tmp_path, monkeypatch, field, value):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    blob = FakeGCSBlob(client, "dags/example.py")
    setattr(blob, field, value)
    list_blobs.return_value = [blob]
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(BundleManifestError, match="invalid object metadata"):
            _bundle(tmp_path).prepare_publish_source()
    assert client.downloads == []


def test_marker_changing_during_listing_prevents_publication(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    client.put("dags/.ready", b"first")
    client.mutate_on_list = (1, "dags/.ready", b"second")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready")
        with pytest.raises(BundleManifestSourceChangedError, match="marker changed"):
            bundle.prepare_publish_source()
        assert not Path(bundle._store.ref_locator).exists()
    assert client.downloads == []


@pytest.mark.parametrize("code", [404, 403])
@mock.patch.object(FakeGCSBucket, "get_blob", autospec=True)
def test_marker_read_errors_preserve_error_contract(get_blob, tmp_path, monkeypatch, code):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    get_blob.side_effect = FakeGCSError(code, "Read marker")
    expected = BundleManifestSourceChangedError if code == 404 else BundleManifestError
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(expected, match="deployment marker"):
            _bundle(tmp_path, deployment_marker_key=".ready").prepare_publish_source()


@pytest.mark.parametrize("failure", ["constructor", "client"])
@mock.patch.object(gcs_module, "GCSHook", autospec=True)
def test_hook_errors_are_bundle_errors(hook, tmp_path, failure):
    if failure == "constructor":
        hook.side_effect = RuntimeError("credentials unavailable")
    else:
        hook.return_value.get_conn.side_effect = RuntimeError("credentials unavailable")
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(BundleManifestError, match="client"):
            _bundle(tmp_path).prepare_publish_source()


@pytest.mark.parametrize("corruption", ["json", "list", "schema", "record", "duplicate", "hash"])
def test_invalid_mirror_state_is_rebuilt(tmp_path, monkeypatch, corruption):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        state = json.loads(bundle.gcs_mirror_state_path.read_text())
        if corruption == "json":
            contents = "invalid json"
        else:
            if corruption == "list":
                state = []
            elif corruption == "schema":
                state["schema_version"] = -1
            elif corruption == "record":
                state["files"] = [None]
            elif corruption == "duplicate":
                state["files"] *= 2
            else:
                state["files"][0]["local_sha256"] = "invalid"
            contents = json.dumps(state)
        bundle.gcs_mirror_state_path.write_text(contents)
        bundle.refresh()
        assert len(client.downloads) == 2
        assert (bundle.gcs_dags_dir / "example.py").read_bytes() == b"dag"
        assert json.loads(bundle.gcs_mirror_state_path.read_text())["files"][0]["local_sha256"].startswith(
            "sha256:"
        )


@pytest.mark.parametrize("failure", ["size", "missing", "symlink"])
@mock.patch.object(FakeGCSBlob, "download_to_filename", autospec=True)
def test_incomplete_download_never_becomes_a_release(download, tmp_path, monkeypatch, failure):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    outside = tmp_path / "outside.py"
    outside.write_bytes(b"original")

    def incomplete_download(blob, filename, **kwargs):
        destination = Path(filename)
        if failure == "size":
            destination.write_bytes(b"truncated")
        else:
            destination.unlink()
            if failure == "symlink":
                destination.symlink_to(outside)

    download.side_effect = incomplete_download
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, auto_publish=False)
        with pytest.raises(BundleManifestError, match="size|regular file"):
            publish_manifest_gcs_dag_bundle(bundle=bundle)
        assert not Path(bundle._store.ref_locator).exists()
        assert not bundle.gcs_mirror_state_path.exists()
        assert list(bundle.gcs_dags_dir.glob(".example.py.*")) == []
    assert outside.read_bytes() == b"original"


def test_local_mirror_change_after_preparation_prevents_publication(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        prepared = bundle.prepare_publish_source()
        (prepared.root / "example.py").write_bytes(b"modified")
        with pytest.raises(BundleManifestSourceChangedError, match="mirror changed"):
            bundle.confirm_publish_source(prepared)


def test_unchanged_refresh_reuses_mirror_and_object_hashes(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, prefix="")
        bundle.refresh()
        version = bundle.get_current_version()
        bundle.refresh()
        assert bundle.get_current_version() == version
        assert client.downloads == [("example.py", 1)]


@pytest.mark.parametrize(
    ("prefix", "names", "message"),
    [
        ("dags", ["other/example.py"], "outside normalized prefix"),
        ("dags", ["dags/../"], "unsafe directory marker"),
        ("dags", ["dags/example.py", "dags/example.py"], "duplicate path"),
    ],
)
@mock.patch.object(FakeGCSClient, "list_blobs", autospec=True)
def test_invalid_inventory_is_rejected_before_mirroring(
    list_blobs, tmp_path, monkeypatch, prefix, names, message
):
    client = FakeGCSClient()
    for name in names:
        client.put(name, b"dag")
    list_blobs.return_value = [FakeGCSBlob(client, name) for name in names]
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(BundleManifestError, match=message):
            _bundle(tmp_path, prefix=prefix).prepare_publish_source()
    assert not client.downloads


@pytest.mark.parametrize("prefix", ["", "dags"])
@mock.patch.object(FakeGCSClient, "list_blobs", autospec=True)
def test_prefix_marker_itself_is_excluded(list_blobs, tmp_path, monkeypatch, prefix):
    client = FakeGCSClient()
    names = [prefix, prefix + "/"] if prefix else ["/"]
    names.append((prefix + "/" if prefix else "") + "example.py")
    for name in names:
        client.put(name, b"dag")
    list_blobs.return_value = [FakeGCSBlob(client, name) for name in names]
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, prefix=prefix)
        bundle.refresh()
        assert (bundle.path / "example.py").read_bytes() == b"dag"
    assert [name for name, _ in client.downloads] == [names[-1]]


@pytest.mark.parametrize("damage", ["missing_file", "missing_mirror", "fifo"])
def test_incomplete_or_unsafe_mirror_is_repaired(tmp_path, monkeypatch, damage):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        if damage == "missing_mirror":
            shutil.rmtree(bundle.gcs_dags_dir)
        else:
            (bundle.gcs_dags_dir / "example.py").unlink()
            if damage == "fifo":
                os.mkfifo(bundle.gcs_dags_dir / "example.py")
        bundle.refresh()
        assert (bundle.gcs_dags_dir / "example.py").read_bytes() == b"dag"
    assert len(client.downloads) == 2


@pytest.mark.parametrize("damage", ["extra_file", "wrong_size"])
def test_mirror_inventory_validation_rejects_drift(tmp_path, monkeypatch, damage):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        prepared = bundle.prepare_publish_source()
        name = "extra.py" if damage == "extra_file" else "example.py"
        (prepared.root / name).write_bytes(b"unexpected")
        with pytest.raises(BundleManifestSourceChangedError, match="file set|wrong size"):
            bundle._validate_mirror_against_inventory(prepared.confirmation_data)


@pytest.mark.parametrize("operation", ["lstat", "open", "fchmod"])
def test_mirror_permissions_errors_are_bundle_errors(tmp_path, operation):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        directory = tmp_path / "mirror"
        directory.mkdir()
        target = Path if operation == "lstat" else os
        with mock.patch.object(target, operation, autospec=True, side_effect=PermissionError("denied")):
            with pytest.raises(BundleManifestError, match="mirror directory"):
                bundle._normalize_mirror_directory(directory)


def test_mirror_directory_cannot_be_replaced_by_a_file(tmp_path):
    directory = tmp_path / "mirror"
    directory.write_bytes(b"file")
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        with pytest.raises(BundleManifestError, match="unsafe directory"):
            bundle._normalize_mirror_directory(directory)


def test_custom_view_url_is_preserved(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, view_url_template="https://example.test/source")
        assert bundle.view_url_template() == "https://example.test/source"


@pytest.mark.parametrize("pinned", [False, True])
@pytest.mark.parametrize(
    ("published_root", "expected_path"),
    [
        ("gs://release-bucket", "release-bucket/versions/manifest-gcs"),
        ("gs://release-bucket/root", "release-bucket/root/versions/manifest-gcs"),
        ("gs://release-bucket/team {name}", "release-bucket/team%20%7Bname%7D/versions/manifest-gcs"),
    ],
)
def test_persisted_view_url_links_to_requested_snapshot(tmp_path, published_root, expected_path, pinned):
    version = "sha256-" + "a" * 64
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, published_root=published_root, version=version if pinned else None)
        model = DagBundleModel(name=bundle.name)
        model.signed_url_template = _sign_bundle_url(bundle.view_url_template(), bundle.name)
        model.template_params = {}

        for requested_version in [version, "sha256-" + "b" * 64]:
            assert model.render_url(requested_version) == (
                f"https://console.cloud.google.com/storage/browser/{expected_path}/{requested_version}"
            )


@pytest.mark.parametrize("has_release", [False, True])
def test_busy_mirror_returns_without_waiting_for_another_publisher(tmp_path, monkeypatch, has_release):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        holder = _bundle(tmp_path)
        if has_release:
            holder.refresh()
        refresher = _bundle(tmp_path)
        original_calls = client.list_calls
        started, done = Event(), Event()
        errors = []

        def refresh():
            started.set()
            try:
                refresher.refresh()
            except Exception as error:
                errors.append(error)
            finally:
                done.set()

        worker = Thread(target=refresh)
        try:
            with holder.lock():
                worker.start()
                assert started.wait(5)
                returned_while_busy = done.wait(2)
        finally:
            worker.join(5)
        assert not worker.is_alive()
        assert returned_while_busy, "Refresh blocked on the source mirror lock"
        assert client.list_calls == original_calls
        if has_release:
            assert errors == []
            assert refresher.get_current_version() == holder.get_current_version()
            assert (refresher.path / "example.py").read_bytes() == b"dag"
        else:
            assert len(errors) == 1
            assert isinstance(errors[0], BundleNotReadyError)
            assert not Path(refresher._store.ref_locator).exists()

        refresher.refresh()
        assert (refresher.path / "example.py").read_bytes() == b"dag"


def test_marker_removal_prevents_mixing_deployments_when_a_release_is_skipped(tmp_path, monkeypatch):
    client = FakeGCSClient()
    _install_fake_hook(monkeypatch, client)
    names = ["first.py", "second.py"]
    for name in names:
        client.put(f"dags/{name}", b"release1")
    client.put("dags/.ready", b"release1")
    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        time_machine.travel("2026-11-19T10:00:00Z", tick=False) as clock,
    ):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready", source_stability_seconds=5)
        with pytest.raises(BundleNotReadyError):
            bundle.refresh()
        clock.shift(6)
        bundle.refresh()
        first_version = bundle.get_current_version()

        # The processor misses deployment 2 entirely before deployment 3 starts.
        client.delete("dags/.ready")
        for name in names:
            client.put(f"dags/{name}", b"release2")
        client.put("dags/.ready", b"release2")
        client.delete("dags/.ready")
        client.put("dags/first.py", b"release3")
        bundle.refresh()
        clock.shift(6)
        bundle.refresh()
        assert bundle.get_current_version() == first_version
        assert [(bundle.path / name).read_bytes() for name in names] == [b"release1", b"release1"]
        with pytest.raises(BundleManifestSourceChangedError, match="deployment is not complete"):
            publish_manifest_gcs_dag_bundle(
                bundle=_bundle(tmp_path, deployment_marker_key=".ready", auto_publish=False)
            )

        client.put("dags/second.py", b"release3")
        client.put("dags/.ready", b"release3")
        bundle.refresh()
        assert bundle.get_current_version() == first_version
        clock.shift(6)
        bundle.refresh()
        assert bundle.get_current_version() != first_version
        assert [(bundle.path / name).read_bytes() for name in names] == [b"release3", b"release3"]


@pytest.mark.parametrize("content_encoding", ["gzip", "br"])
@pytest.mark.parametrize("has_release", [False, True])
def test_content_encoded_source_is_rejected_before_publication(
    tmp_path, monkeypatch, content_encoding, has_release
):
    client = FakeGCSClient()
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        if has_release:
            client.put("dags/example.py", b"print('old')")
            bundle.refresh()
        ref_path = Path(bundle._store.ref_locator)
        original_ref = ref_path.read_bytes() if has_release else None
        original_downloads = list(client.downloads)
        client.put(
            "dags/example.py",
            gzip.compress(b"print('new')", mtime=0) if content_encoding == "gzip" else b"encoded",
            content_encoding=content_encoding,
        )
        if has_release:
            bundle.refresh()
            assert (bundle.path / "example.py").read_bytes() == b"print('old')"
        else:
            with pytest.raises(BundleManifestError, match="unsupported Content-Encoding"):
                bundle.refresh()
        with pytest.raises(BundleManifestError, match="unsupported Content-Encoding"):
            publish_manifest_gcs_dag_bundle(bundle=_bundle(tmp_path, auto_publish=False))
        assert (ref_path.read_bytes() if ref_path.exists() else None) == original_ref
        assert client.downloads == original_downloads


@pytest.mark.parametrize("content_encoding", [None, "identity", "IDENTITY"])
def test_unencoded_source_preserves_python_and_binary_file_bytes(tmp_path, monkeypatch, content_encoding):
    client = FakeGCSClient()
    archive = gzip.compress(b"archive contents", mtime=0)
    client.put("dags/example.py", b"print('dag')", content_encoding=content_encoding)
    client.put("dags/archive.gz", archive)
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        assert (bundle.path / "example.py").read_bytes() == b"print('dag')"
        assert (bundle.path / "archive.gz").read_bytes() == archive


def test_explicit_gcs_publication_does_not_bypass_source_guard_with_invalid_reference(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, auto_publish=False)
        publish_manifest_gcs_dag_bundle(bundle=bundle)
        ref_path = Path(bundle._store.ref_locator)
        ref_path.write_text("{")
        client.put("dags/example.py", b"new")
        with pytest.raises(BundleManifestError):
            publish_manifest_gcs_dag_bundle(bundle=bundle)
        assert ref_path.read_text() == "{"


@pytest.mark.parametrize("bucket_name", ["", None])
def test_source_bucket_must_be_nonempty(tmp_path, bucket_name):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(TypeError, match="bucket_name"):
            ManifestGCSDagBundle(
                name="manifest-gcs", bucket_name=bucket_name, published_root=str(tmp_path / "published")
            )


@mock.patch.object(ManifestGCSDagBundle, "create_artifact_store", autospec=True)
def test_source_rejects_incompatible_custom_store(create_store, tmp_path):
    create_store.return_value = SimpleNamespace(backend_type="s3")
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        with pytest.raises(TypeError, match="Cross-cloud publication is not supported"):
            _bundle(tmp_path)


def test_confirmation_requires_remote_observation(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        prepared = replace(bundle.prepare_publish_source(), confirmation_data=None)
        with pytest.raises(BundleManifestError, match="remote observation"):
            bundle.confirm_publish_source(prepared)


def test_explicit_publication_can_replace_a_reference_without_source_metadata(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "example.py").write_bytes(b"old")
    client = FakeGCSClient()
    client.put("dags/example.py", b"new")
    _install_fake_hook(monkeypatch, client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        local = ManifestLocalDagBundle(name="manifest-gcs", published_root=str(tmp_path / "published"))
        old = publish_manifest_local_dag_bundle(bundle=local, source_path=source)
        bundle = _bundle(tmp_path, auto_publish=False)
        new = publish_manifest_gcs_dag_bundle(bundle=bundle, expected_current_version=old.version)
        assert new.version != old.version
        assert bundle._store.read_ref()["source"]["type"] == "gcs"


def test_disappearing_mirror_file_is_source_drift(tmp_path, monkeypatch):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        assert bundle._get_observed_mirror_files() == set()
        bundle.gcs_dags_dir.mkdir(parents=True)
        path = bundle.gcs_dags_dir / "example.py"
        path.write_bytes(b"dag")
        original_stat = Path.stat

        def disappear_on_stat(self, **kwargs):
            if self == path and kwargs.get("follow_symlinks", True):
                raise FileNotFoundError(path)
            return original_stat(self, **kwargs)

        monkeypatch.setattr(Path, "stat", disappear_on_stat)
        with pytest.raises(BundleManifestSourceChangedError, match="disappeared during validation"):
            bundle._get_observed_mirror_files()


def test_mirror_directory_replacement_is_detected_after_open(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        directory = tmp_path / "mirror"
        directory.mkdir()
        with mock.patch.object(
            os, "fstat", autospec=True, return_value=SimpleNamespace(st_mode=stat.S_IFREG)
        ):
            with pytest.raises(BundleManifestError, match="unsafe directory"):
                bundle._normalize_mirror_directory(directory)


@pytest.mark.parametrize("kind", ["symlink", "directory"])
def test_cached_mirror_record_cannot_make_an_unsafe_file_reusable(tmp_path, kind):
    path = tmp_path / "entry"
    if kind == "symlink":
        path.symlink_to(tmp_path / "elsewhere")
    else:
        path.mkdir()
    assert not object_source_module._can_reuse_mirror_file(path, entry=None, state={})


@mock.patch.object(Path, "rmdir", autospec=True, side_effect=PermissionError("not empty"))
def test_obsolete_nonempty_directory_can_remain_without_affecting_inventory(rmdir, tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        child = bundle.gcs_dags_dir / "subdirectory"
        child.mkdir(parents=True)
        bundle._remove_empty_mirror_directories()
        assert bundle._get_observed_mirror_files() == set()
    rmdir.assert_called_once_with(child)


def test_listing_accepts_datetime_metadata(tmp_path):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    blob = FakeGCSBlob(client, "dags/example.py")
    blob.updated = datetime(2026, 1, 1, tzinfo=UTC)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        observed = _bundle(tmp_path)._build_object_observation(blob)
        assert observed.updated == "2026-01-01T00:00:00+00:00"


def test_disjoint_prefixes_can_share_a_bucket_with_separate_connections(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(
            tmp_path,
            published_root="gs://dag-bucket/releases",
            published_root_conn_id="releases",
            gcp_conn_id="source",
        )
        assert bundle.gcp_conn_id == "source"
        assert bundle._store.gcp_conn_id == "releases"
        assert bundle._store.root == "gs://dag-bucket/releases"


def test_publisher_follows_identical_release_committed_after_stability_check(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)
    store_client = FakeStoreGCSClient()
    _install_fake_store_hook(monkeypatch, store_client)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "first")}):
        first = _bundle(tmp_path, published_root="gs://releases-bucket/root")
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "second")}):
        second = _bundle(tmp_path, published_root="gs://releases-bucket/root")
    original_write = first._store.write_state

    def publish_peer_after_state(payload):
        original_write(payload)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "second")}):
            second.refresh()

    monkeypatch.setattr(first._store, "write_state", publish_peer_after_state)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "first")}):
        first.refresh()
    assert first.get_current_version() == second.get_current_version()
    reference_writes = [key for key, _ in store_client.put_conditions if key.endswith("latest.json")]
    assert len(reference_writes) == 1
