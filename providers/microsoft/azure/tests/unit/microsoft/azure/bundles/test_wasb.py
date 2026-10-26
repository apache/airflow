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
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

import airflow.version
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

from tests_common.test_utils.config import conf_vars

if airflow.version.version.strip().startswith("3"):
    from airflow.providers.microsoft.azure.bundles.wasb import WasbDagBundle

WASB_CONN_ID = "wasb_dags_connection"
CONTAINER_NAME = "my-airflow-dags-container"
CONTAINER_PREFIX = "project1/dags"
ACCOUNT_NAME = "myaccount"
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
ARCHIVE_BLOB = "bundle-archives/dags.tar.gz"


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


@pytest.mark.skipif(not airflow.version.version.strip().startswith("3"), reason="Airflow >=3.0.0 test")
class TestWasbDagBundle:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=WASB_CONN_ID,
                conn_type="wasb",
                login=ACCOUNT_NAME,
                password="password",
            )
        )

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_view_url_generates_blob_url(self, mock_wasb_hook_property):
        mock_hook = MagicMock()
        mock_hook.blob_service_client.url = ACCOUNT_URL
        mock_wasb_hook_property.return_value = mock_hook

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        url: str = bundle.view_url()
        assert url == f"{ACCOUNT_URL}/{CONTAINER_NAME}/{CONTAINER_PREFIX}"

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_view_url_template_generates_blob_url(self, mock_wasb_hook_property):
        mock_hook = MagicMock()
        mock_hook.blob_service_client.url = ACCOUNT_URL
        mock_wasb_hook_property.return_value = mock_hook

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        url: str = bundle.view_url_template()
        assert url == f"{ACCOUNT_URL}/{CONTAINER_NAME}/{CONTAINER_PREFIX}"

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_view_url_template_strips_sas_token(self, mock_wasb_hook_property):
        mock_hook = MagicMock()
        mock_hook.blob_service_client.url = f"{ACCOUNT_URL}/?sv=2021-08-06&sp=r&sig=samplesignature"
        mock_wasb_hook_property.return_value = mock_hook

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        assert bundle.view_url_template() == f"{ACCOUNT_URL}/{CONTAINER_NAME}/{CONTAINER_PREFIX}"

    def test_supports_versioning(self):
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        assert WasbDagBundle.supports_versioning is False

        bundle.version = "test_version"

        with pytest.raises(ValueError, match="Refreshing a specific version is not supported"):
            bundle.refresh()
        with pytest.raises(ValueError, match="WASB url with version is not supported"):
            bundle.view_url("test_version")

    def test_local_dags_path_is_not_a_directory(self, bundle_temp_dir):
        bundle_name = "test"
        file_path = bundle_temp_dir / bundle_name
        file_path.touch()

        bundle = WasbDagBundle(
            name=bundle_name,
            wasb_conn_id=WASB_CONN_ID,
            prefix="project1_dags",
            container_name="airflow_dags",
        )
        with pytest.raises(NotADirectoryError, match=f"Local Dags path: {file_path} is not a directory."):
            bundle.initialize()

    def test_correct_bundle_path_used(self):
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix="project1_dags",
            container_name="airflow_dags",
        )
        assert str(bundle.base_dir) == str(bundle.wasb_dags_dir)

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_wasb_container_and_prefix_validated(self, mock_wasb_hook_property):
        mock_hook = MagicMock()
        mock_wasb_hook_property.return_value = mock_hook

        mock_hook.check_for_container.return_value = False
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix="project1_dags",
            container_name="non-existing-container",
        )
        with pytest.raises(ValueError, match="WASB container 'non-existing-container' does not exist."):
            bundle.initialize()
        mock_hook.check_for_container.assert_called_once_with(container_name="non-existing-container")

        mock_hook.check_for_container.return_value = True
        mock_hook.check_for_prefix.return_value = False
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix="non-existing-prefix",
            container_name=CONTAINER_NAME,
        )
        with pytest.raises(
            ValueError,
            match=f"WASB prefix 'wasb://{CONTAINER_NAME}/non-existing-prefix' does not exist.",
        ):
            bundle.initialize()
        mock_hook.check_for_prefix.assert_called_once_with(
            container_name=CONTAINER_NAME, prefix="non-existing-prefix/", delimiter="/"
        )

        mock_hook.check_for_prefix.return_value = True
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        bundle.initialize()

        mock_hook.check_for_prefix.reset_mock()
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix="",
            container_name=CONTAINER_NAME,
        )
        bundle.initialize()
        mock_hook.check_for_prefix.assert_not_called()

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_refresh(self, mock_wasb_hook_property):
        mock_hook = MagicMock()
        mock_hook.check_for_container.return_value = True
        mock_hook.check_for_prefix.return_value = True
        mock_wasb_hook_property.return_value = mock_hook

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        bundle._log.debug = MagicMock()
        download_log_call = call(
            "Downloading Dags from wasb://%s/%s to %s",
            CONTAINER_NAME,
            CONTAINER_PREFIX,
            bundle.wasb_dags_dir,
        )
        sync_call = call(
            container_name=CONTAINER_NAME,
            prefix=CONTAINER_PREFIX,
            local_dir=bundle.wasb_dags_dir,
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

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_refresh_without_prefix(self, mock_wasb_hook_property):
        mock_hook = MagicMock()
        mock_hook.check_for_container.return_value = True
        mock_wasb_hook_property.return_value = mock_hook

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
        )
        bundle._log.debug = MagicMock()
        download_log_call = call(
            "Downloading Dags from wasb://%s/%s to %s",
            CONTAINER_NAME,
            "",
            bundle.wasb_dags_dir,
        )
        sync_call = call(
            container_name=CONTAINER_NAME,
            prefix="",
            local_dir=bundle.wasb_dags_dir,
            delete_stale=True,
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


def _make_archive(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class _FakeProps:
    def __init__(self, etag: str, size: int):
        self.etag = etag
        self.size = size


class _FakeStream:
    def __init__(self, payload: bytes):
        self._payload = payload

    def readall(self) -> bytes:
        return self._payload


class _FakeBlob:
    def __init__(self, store: dict[str, tuple[bytes, str]], name: str):
        self._store = store
        self.name = name

    def get_blob_properties(self) -> _FakeProps:
        if self.name not in self._store:
            raise ResourceNotFoundError(self.name)
        data, etag = self._store[self.name]
        return _FakeProps(etag, len(data))

    def download_blob(self, etag: str | None = None, match_condition=None) -> _FakeStream:
        if self.name not in self._store:
            raise ResourceNotFoundError(self.name)
        data, current = self._store[self.name]
        if etag is not None and etag != current:
            raise HttpResponseError(message="condition not met")
        return _FakeStream(data)


def _archive_hook(store: dict[str, tuple[bytes, str]]) -> MagicMock:
    hook = MagicMock(spec=WasbHook)
    hook.check_for_container.return_value = True
    service = MagicMock()
    service.get_blob_client.side_effect = lambda _container, name: _FakeBlob(store, name)
    hook.get_conn.return_value = service
    hook.blob_service_client.url = ACCOUNT_URL
    return hook


@pytest.mark.skipif(not airflow.version.version.strip().startswith("3"), reason="Airflow >=3.0.0 test")
class TestWasbDagBundleArchive:
    def test_archive_blob_and_prefix_are_mutually_exclusive(self):
        with pytest.raises(ValueError, match="either 'prefix' or 'archive_blob', not both"):
            WasbDagBundle(
                name="test",
                wasb_conn_id=WASB_CONN_ID,
                container_name=CONTAINER_NAME,
                prefix=CONTAINER_PREFIX,
                archive_blob=ARCHIVE_BLOB,
            )

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_refresh_from_archive(self, mock_wasb_hook_property):
        archive = _make_archive({"dag_01.py": b"print(1)\n", "subdir/dag_02.py": b"print(2)\n"})
        mock_wasb_hook_property.return_value = _archive_hook({ARCHIVE_BLOB: (archive, "etag-1")})

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        bundle.initialize()

        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"
        assert (bundle.path / "subdir" / "dag_02.py").read_bytes() == b"print(2)\n"
        assert (bundle.path / bundle.archive_etag_marker).read_text() == "etag-1"
        mock_wasb_hook_property.return_value.sync_to_local_dir.assert_not_called()
        mock_wasb_hook_property.return_value.check_for_prefix.assert_not_called()

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_refresh_from_archive_skips_unchanged_etag(self, mock_wasb_hook_property):
        archive = _make_archive({"dag_01.py": b"print(1)\n"})
        mock_wasb_hook_property.return_value = _archive_hook({ARCHIVE_BLOB: (archive, "etag-1")})

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        bundle.initialize()
        sentinel = bundle.path / "local-only.txt"
        sentinel.write_text("keep me")

        with patch.object(
            _FakeBlob, "download_blob", autospec=True, side_effect=_FakeBlob.download_blob
        ) as spy:
            bundle.refresh()

        assert spy.call_count == 0
        assert sentinel.read_text() == "keep me"

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_empty_etag_always_restages(self, mock_wasb_hook_property):
        archive = _make_archive({"dag_01.py": b"print(1)\n"})
        mock_wasb_hook_property.return_value = _archive_hook({ARCHIVE_BLOB: (archive, "")})

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        with patch.object(
            _FakeBlob, "download_blob", autospec=True, side_effect=_FakeBlob.download_blob
        ) as spy:
            bundle.initialize()
            assert not (bundle.path / bundle.archive_etag_marker).exists()
            bundle.refresh()

        assert spy.call_count == 2
        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_refresh_replaces_tree_when_etag_changes(self, mock_wasb_hook_property):
        store = {ARCHIVE_BLOB: (_make_archive({"dag_01.py": b"print(1)\n"}), "etag-1")}
        mock_wasb_hook_property.return_value = _archive_hook(store)

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        bundle.initialize()
        (bundle.path / "stale.py").write_text("gone")
        store[ARCHIVE_BLOB] = (_make_archive({"dag_02.py": b"print(2)\n"}), "etag-2")
        bundle.refresh()

        assert not (bundle.path / "dag_01.py").exists()
        assert not (bundle.path / "stale.py").exists()
        assert (bundle.path / "dag_02.py").read_bytes() == b"print(2)\n"
        assert (bundle.path / bundle.archive_etag_marker).read_text() == "etag-2"

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_initialize_missing_archive_raises(self, mock_wasb_hook_property):
        mock_wasb_hook_property.return_value = _archive_hook({})
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        with pytest.raises(ValueError, match="does not exist"):
            bundle.initialize()
        mock_wasb_hook_property.return_value.sync_to_local_dir.assert_not_called()

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_etag_mismatch_keeps_previous_tree(self, mock_wasb_hook_property):
        store = {ARCHIVE_BLOB: (_make_archive({"dag_01.py": b"print(1)\n"}), "etag-1")}
        mock_wasb_hook_property.return_value = _archive_hook(store)
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        bundle.initialize()
        store[ARCHIVE_BLOB] = (_make_archive({"dag_02.py": b"print(2)\n"}), "etag-2")

        with (
            patch.object(
                _FakeBlob, "download_blob", side_effect=HttpResponseError(message="condition not met")
            ),
            pytest.raises(HttpResponseError, match="condition not met"),
        ):
            bundle.refresh()

        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"
        assert not (bundle.path / "dag_02.py").exists()
        assert list(bundle.path.parent.glob(".wasb-archive-*")) == []

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_corrupt_archive_keeps_previous_tree(self, mock_wasb_hook_property):
        store = {ARCHIVE_BLOB: (_make_archive({"dag_01.py": b"print(1)\n"}), "etag-1")}
        mock_wasb_hook_property.return_value = _archive_hook(store)
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        bundle.initialize()
        store[ARCHIVE_BLOB] = (b"this is not a tar archive", "etag-2")

        with pytest.raises(tarfile.ReadError):
            bundle.refresh()

        assert (bundle.path / "dag_01.py").read_bytes() == b"print(1)\n"
        assert list(bundle.path.parent.glob(".wasb-archive-*")) == []

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_failed_swap_restores_previous_tree(self, mock_wasb_hook_property):
        store = {ARCHIVE_BLOB: (_make_archive({"dag_01.py": b"print(1)\n"}), "etag-1")}
        mock_wasb_hook_property.return_value = _archive_hook(store)
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        bundle.initialize()
        store[ARCHIVE_BLOB] = (_make_archive({"dag_02.py": b"print(2)\n"}), "etag-2")

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
        assert list(bundle.path.parent.glob(".wasb-archive-*")) == []

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_view_url_template_points_at_archive(self, mock_wasb_hook_property):
        mock_wasb_hook_property.return_value = _archive_hook({})
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            container_name=CONTAINER_NAME,
            archive_blob=ARCHIVE_BLOB,
        )
        assert bundle.view_url_template() == f"{ACCOUNT_URL}/{CONTAINER_NAME}/{ARCHIVE_BLOB}"
