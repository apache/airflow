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

import io
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed

import airflow.version
from airflow.models import Connection
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from tests_common.test_utils.config import conf_vars

if airflow.version.version.strip().startswith("3"):
    from airflow.providers.google.cloud.bundles.gcs import GCSDagBundle

GCP_CONN_ID = "gcs_dags_connection"
GCS_BUCKET_NAME = "my-airflow-dags-bucket"
GCS_BUCKET_PREFIX = "project1/dags"
GCS_ARCHIVE_OBJECT = "bundle-archives/dags.tar.gz"


def _make_archive(files: dict[str, bytes]) -> bytes:
    """Build an in-memory .tar.gz with the given member name -> content mapping."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, data in files.items():
            tar_info = tarfile.TarInfo(name=name)
            tar_info.size = len(data)
            tar.addfile(tar_info, io.BytesIO(data))
    return buffer.getvalue()


class _FakeBlob:
    def __init__(self, store: dict[str, tuple[bytes, str]], name: str):
        self._store = store
        self.name = name
        self.etag: str | None = None
        self.size: int | None = None
        self.generation: int | None = None

    def reload(self) -> None:
        if self.name not in self._store:
            raise NotFound(self.name)
        data, etag = self._store[self.name]
        self.etag = etag
        self.size = len(data)
        self.generation = 1

    def download_to_filename(self, filename: str, if_generation_match: int | None = None) -> None:
        if self.name not in self._store:
            raise NotFound(self.name)
        data, etag = self._store[self.name]
        if if_generation_match is not None and if_generation_match != 1:
            raise PreconditionFailed("generation mismatch")
        self.etag = etag
        self.size = len(data)
        self.generation = 1
        Path(filename).write_bytes(data)


class _FakeBucket:
    def __init__(self, store: dict[str, tuple[bytes, str]]):
        self._store = store

    def blob(self, blob_name: str) -> _FakeBlob:
        return _FakeBlob(self._store, blob_name)


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


@pytest.mark.skipif(not airflow.version.version.strip().startswith("3"), reason="Airflow >=3.0.0 test")
class TestGCSDagBundle:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=GCP_CONN_ID,
                conn_type="google_cloud_platform",
            )
        )

    def test_view_url_generates_console_url(self):
        bundle = GCSDagBundle(
            name="test", gcp_conn_id=GCP_CONN_ID, prefix=GCS_BUCKET_PREFIX, bucket_name=GCS_BUCKET_NAME
        )

        url: str = bundle.view_url()
        assert (
            url == f"https://console.cloud.google.com/storage/browser/{GCS_BUCKET_NAME}/{GCS_BUCKET_PREFIX}"
        )

    def test_view_url_template_generates_console_url(self):
        bundle = GCSDagBundle(
            name="test", gcp_conn_id=GCP_CONN_ID, prefix=GCS_BUCKET_PREFIX, bucket_name=GCS_BUCKET_NAME
        )
        url: str = bundle.view_url_template()
        assert (
            url == f"https://console.cloud.google.com/storage/browser/{GCS_BUCKET_NAME}/{GCS_BUCKET_PREFIX}"
        )

    def test_view_url_template_uses_high_value_cookie_domain(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_CLOUD_HIGH_VALUE_COOKIE_DOMAIN", "googleapis.cn")
        bundle = GCSDagBundle(
            name="test", gcp_conn_id=GCP_CONN_ID, prefix=GCS_BUCKET_PREFIX, bucket_name=GCS_BUCKET_NAME
        )

        assert (
            bundle.view_url_template()
            == f"https://console.cloud.googleapis.cn/storage/browser/{GCS_BUCKET_NAME}/{GCS_BUCKET_PREFIX}"
        )

    def test_supports_versioning(self):
        bundle = GCSDagBundle(
            name="test", gcp_conn_id=GCP_CONN_ID, prefix=GCS_BUCKET_PREFIX, bucket_name=GCS_BUCKET_NAME
        )
        assert GCSDagBundle.supports_versioning is False

        # set version, it's not supported
        bundle.version = "test_version"

        with pytest.raises(ValueError, match="Refreshing a specific version is not supported"):
            bundle.refresh()
        with pytest.raises(ValueError, match="GCS url with version is not supported"):
            bundle.view_url("test_version")

    def test_local_dags_path_is_not_a_directory(self, bundle_temp_dir):
        bundle_name = "test"
        # Create a file where the directory should be
        file_path = bundle_temp_dir / bundle_name
        file_path.touch()

        bundle = GCSDagBundle(
            name=bundle_name,
            gcp_conn_id=GCP_CONN_ID,
            prefix="project1_dags",
            bucket_name="airflow_dags",
        )
        with pytest.raises(NotADirectoryError, match=f"Local Dags path: {file_path} is not a directory."):
            bundle.initialize()

    def test_correct_bundle_path_used(self):
        bundle = GCSDagBundle(
            name="test", gcp_conn_id=GCP_CONN_ID, prefix="project1_dags", bucket_name="airflow_dags"
        )
        assert str(bundle.base_dir) == str(bundle.gcs_dags_dir)

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_gcs_bucket_and_prefix_validated(self, mock_gcs_hook_property):
        mock_hook = MagicMock()
        mock_gcs_hook_property.return_value = mock_hook

        mock_hook.get_bucket.side_effect = NotFound("Bucket not found")
        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            prefix="project1_dags",
            bucket_name="non-existing-bucket",
        )
        with pytest.raises(ValueError, match="GCS bucket 'non-existing-bucket' does not exist."):
            bundle.initialize()
        mock_hook.get_bucket.assert_called_once_with(bucket_name="non-existing-bucket")

        mock_hook.get_bucket.side_effect = None
        mock_hook.get_bucket.return_value = True
        mock_hook.list.return_value = []
        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            prefix="non-existing-prefix",
            bucket_name=GCS_BUCKET_NAME,
        )
        with pytest.raises(
            ValueError,
            match=f"GCS prefix 'gs://{GCS_BUCKET_NAME}/non-existing-prefix' does not exist.",
        ):
            bundle.initialize()
        mock_hook.list.assert_called_once_with(bucket_name=GCS_BUCKET_NAME, prefix="non-existing-prefix")

        mock_hook.list.return_value = ["some/object"]
        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            prefix=GCS_BUCKET_PREFIX,
            bucket_name=GCS_BUCKET_NAME,
        )
        # initialize succeeds, with correct prefix and bucket
        bundle.initialize()

        mock_hook.list.reset_mock()
        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            prefix="",
            bucket_name=GCS_BUCKET_NAME,
        )
        # initialize succeeds, with empty prefix
        bundle.initialize()
        mock_hook.list.assert_not_called()

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_refresh(self, mock_gcs_hook_property):
        mock_hook = MagicMock()
        mock_gcs_hook_property.return_value = mock_hook

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            prefix=GCS_BUCKET_PREFIX,
            bucket_name=GCS_BUCKET_NAME,
        )
        bundle._log.debug = MagicMock()
        download_log_call = call(
            "Downloading Dags from gs://%s/%s to %s", GCS_BUCKET_NAME, GCS_BUCKET_PREFIX, bundle.gcs_dags_dir
        )
        sync_call = call(
            bucket_name=GCS_BUCKET_NAME,
            prefix=GCS_BUCKET_PREFIX,
            local_dir=bundle.gcs_dags_dir,
            delete_stale=True,
        )

        bundle.initialize()
        assert bundle._log.debug.call_count == 1
        assert bundle._log.debug.call_args_list == [download_log_call]
        assert mock_hook.sync_to_local_dir.call_count == 1
        assert mock_hook.sync_to_local_dir.call_args_list == [sync_call]

        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]
        assert mock_hook.sync_to_local_dir.call_count == 2
        assert mock_hook.sync_to_local_dir.call_args_list == [sync_call, sync_call]

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_refresh_without_prefix(self, mock_gcs_hook_property):
        mock_hook = MagicMock()
        mock_gcs_hook_property.return_value = mock_hook

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
        )
        bundle._log.debug = MagicMock()
        download_log_call = call(
            "Downloading Dags from gs://%s/%s to %s", GCS_BUCKET_NAME, "", bundle.gcs_dags_dir
        )
        sync_call = call(
            bucket_name=GCS_BUCKET_NAME, prefix="", local_dir=bundle.gcs_dags_dir, delete_stale=True
        )

        assert bundle.prefix == ""
        bundle.initialize()
        assert bundle._log.debug.call_count == 1
        assert bundle._log.debug.call_args_list == [download_log_call]
        assert mock_hook.sync_to_local_dir.call_count == 1
        assert mock_hook.sync_to_local_dir.call_args_list == [sync_call]

        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]
        assert mock_hook.sync_to_local_dir.call_count == 2
        assert mock_hook.sync_to_local_dir.call_args_list == [sync_call, sync_call]


def _archive_hook(store: dict[str, tuple[bytes, str]]) -> MagicMock:
    hook = MagicMock(spec=GCSHook)
    hook.get_bucket.return_value = object()
    client = MagicMock()
    client.bucket.return_value = _FakeBucket(store)
    hook.get_conn.return_value = client
    return hook


class TestGCSDagBundleArchive:
    @pytest.fixture(autouse=True)
    def bundle_storage_path(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            yield tmp_path

    def test_archive_object_and_prefix_are_mutually_exclusive(self):
        with pytest.raises(ValueError, match="either 'prefix' or 'archive_object', not both"):
            GCSDagBundle(
                name="test",
                gcp_conn_id=GCP_CONN_ID,
                bucket_name=GCS_BUCKET_NAME,
                prefix=GCS_BUCKET_PREFIX,
                archive_object=GCS_ARCHIVE_OBJECT,
            )

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_refresh_from_archive(self, mock_gcs_hook_property):
        archive = _make_archive({"dag_01.py": b"print(1)\n", "subdir/dag_02.py": b"print(2)\n"})
        store = {GCS_ARCHIVE_OBJECT: (archive, "etag-1")}
        mock_gcs_hook_property.return_value = _archive_hook(store)

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        bundle.initialize()

        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"
        assert (bundle.path / "subdir" / "dag_02.py").read_bytes() == b"print(2)\n"
        assert (bundle.path / bundle.archive_etag_marker).read_text() == "etag-1"
        mock_gcs_hook_property.return_value.sync_to_local_dir.assert_not_called()
        mock_gcs_hook_property.return_value.list.assert_not_called()

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_refresh_from_archive_skips_unchanged_etag(self, mock_gcs_hook_property):
        archive = _make_archive({"dag_01.py": b"print(1)\n"})
        store = {GCS_ARCHIVE_OBJECT: (archive, "etag-1")}
        hook = _archive_hook(store)
        mock_gcs_hook_property.return_value = hook

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        bundle.initialize()
        sentinel = bundle.path / "local-only.txt"
        sentinel.write_text("keep me")

        with patch.object(
            _FakeBlob,
            "download_to_filename",
            autospec=True,
            side_effect=_FakeBlob.download_to_filename,
        ) as spy:
            bundle.refresh()

        assert spy.call_count == 0
        assert sentinel.read_text() == "keep me"
        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_empty_etag_always_restages(self, mock_gcs_hook_property):
        archive = _make_archive({"dag_01.py": b"print(1)\n"})
        store = {GCS_ARCHIVE_OBJECT: (archive, "")}
        mock_gcs_hook_property.return_value = _archive_hook(store)

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        with patch.object(
            _FakeBlob,
            "download_to_filename",
            autospec=True,
            side_effect=_FakeBlob.download_to_filename,
        ) as spy:
            bundle.initialize()
            assert not (bundle.path / bundle.archive_etag_marker).exists()
            bundle.refresh()

        assert spy.call_count == 2
        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"
        assert not (bundle.path / bundle.archive_etag_marker).exists()

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_refresh_from_archive_replaces_tree_when_etag_changes(self, mock_gcs_hook_property):
        store = {GCS_ARCHIVE_OBJECT: (_make_archive({"dag_01.py": b"print(1)\n"}), "etag-1")}
        mock_gcs_hook_property.return_value = _archive_hook(store)

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        bundle.initialize()
        (bundle.path / "stale.py").write_text("gone")

        store[GCS_ARCHIVE_OBJECT] = (_make_archive({"dag_02.py": b"print(2)\n"}), "etag-2")
        bundle.refresh()

        assert not (bundle.path / "dag_01.py").exists()
        assert not (bundle.path / "stale.py").exists()
        assert (bundle.path / "dag_02.py").read_bytes() == b"print(2)\n"
        assert (bundle.path / bundle.archive_etag_marker).read_text() == "etag-2"

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_initialize_missing_archive_raises(self, mock_gcs_hook_property):
        mock_gcs_hook_property.return_value = _archive_hook({})

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        with pytest.raises(ValueError, match="does not exist"):
            bundle.initialize()
        mock_gcs_hook_property.return_value.sync_to_local_dir.assert_not_called()

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_generation_mismatch_keeps_previous_tree(self, mock_gcs_hook_property):
        store = {GCS_ARCHIVE_OBJECT: (_make_archive({"dag_01.py": b"print(1)\n"}), "etag-1")}
        mock_gcs_hook_property.return_value = _archive_hook(store)

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        bundle.initialize()
        store[GCS_ARCHIVE_OBJECT] = (_make_archive({"dag_02.py": b"print(2)\n"}), "etag-2")

        with (
            patch.object(
                _FakeBlob,
                "download_to_filename",
                side_effect=PreconditionFailed("generation mismatch"),
            ),
            pytest.raises(PreconditionFailed, match="generation mismatch"),
        ):
            bundle.refresh()

        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"
        assert not (bundle.path / "dag_02.py").exists()
        assert list(bundle.path.parent.glob(".gcs-archive-*")) == []

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_corrupt_archive_keeps_previous_tree(self, mock_gcs_hook_property):
        store = {GCS_ARCHIVE_OBJECT: (_make_archive({"dag_01.py": b"print(1)\n"}), "etag-1")}
        mock_gcs_hook_property.return_value = _archive_hook(store)

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        bundle.initialize()
        store[GCS_ARCHIVE_OBJECT] = (b"this is not a tar archive", "etag-2")

        with pytest.raises(tarfile.ReadError):
            bundle.refresh()

        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"
        assert list(bundle.path.parent.glob(".gcs-archive-*")) == []

    @patch("airflow.providers.google.cloud.bundles.gcs.GCSDagBundle.gcs_hook", new_callable=PropertyMock)
    def test_failed_swap_restores_previous_tree(self, mock_gcs_hook_property):
        store = {GCS_ARCHIVE_OBJECT: (_make_archive({"dag_01.py": b"print(1)\n"}), "etag-1")}
        mock_gcs_hook_property.return_value = _archive_hook(store)

        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        bundle.initialize()
        store[GCS_ARCHIVE_OBJECT] = (_make_archive({"dag_02.py": b"print(2)\n"}), "etag-2")

        real_rename = Path.rename

        def failing_rename(self, target):
            if self.name == "unpacked" and Path(target) == bundle.path:
                raise OSError("simulated swap failure")
            return real_rename(self, target)

        with (
            patch.object(Path, "rename", failing_rename),
            pytest.raises(OSError, match="simulated swap failure"),
        ):
            bundle.refresh()

        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"
        assert not (bundle.path / "dag_02.py").exists()
        assert list(bundle.path.parent.glob(".gcs-archive-*")) == []

    def test_view_url_template_points_at_archive(self):
        bundle = GCSDagBundle(
            name="test",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            archive_object=GCS_ARCHIVE_OBJECT,
        )
        url = bundle.view_url_template()
        assert url is not None
        assert url.endswith(f"/storage/browser/_details/{GCS_BUCKET_NAME}/{GCS_ARCHIVE_OBJECT}")
