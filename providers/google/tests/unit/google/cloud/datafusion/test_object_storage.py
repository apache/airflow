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

import json
import os
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException
from airflow.providers.google.cloud.datafusion.object_storage import GCSObjectStorageProvider

MODULE = "airflow.providers.google.cloud.datafusion.object_storage"
GCS_PATH = "gs://demo-data/path"


def _gcp_connection(extra: dict | None = None, conn_type: str = "google_cloud_platform") -> Connection:
    return Connection(conn_id="google_cloud_default", conn_type=conn_type, extra=json.dumps(extra or {}))


class TestGCSObjectStorageProvider:
    def test_storage_type_and_scheme(self):
        provider = GCSObjectStorageProvider()

        assert provider.get_storage_type == StorageType.GCS
        assert provider.get_scheme() == "gs://"
        assert provider.get_bucket(GCS_PATH) == "demo-data"

    @mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    @mock.patch(f"{MODULE}.GoogleCloud", autospec=True)
    def test_explicit_key_path_skips_connection_lookup(self, mock_gcs, mock_get_connection, tmp_path):
        key_path = tmp_path / "key.json"
        key_path.write_text("{}")
        config = ConnectionConfig(conn_id="google_cloud_default", credentials={"key_path": str(key_path)})

        store = GCSObjectStorageProvider().create_object_store(GCS_PATH, config)

        mock_get_connection.assert_not_called()
        mock_gcs.assert_called_once_with(bucket_name="demo-data", service_account_path=str(key_path))
        assert store == mock_gcs.return_value

    @mock.patch(f"{MODULE}.GoogleCloud", autospec=True)
    def test_keyfile_dict_written_to_temp_file_and_removed(self, mock_gcs):
        keyfile_dict = {"type": "service_account", "private_key": "fake"}
        written = {}

        def _capture(*, bucket_name, service_account_path):
            with open(service_account_path) as f:
                written["data"] = json.load(f)

        mock_gcs.side_effect = _capture
        config = ConnectionConfig(conn_id="google_cloud_default", credentials={"keyfile_dict": keyfile_dict})

        GCSObjectStorageProvider().create_object_store(GCS_PATH, config)

        assert written["data"] == keyfile_dict
        assert not os.path.exists(mock_gcs.call_args.kwargs["service_account_path"])

    @pytest.mark.parametrize(
        ("extra", "env", "expected_kwargs"),
        [
            pytest.param({"key_path": "KEY"}, None, {"service_account_path": "KEY"}, id="key_path"),
            pytest.param(
                {"extra__google_cloud_platform__key_path": "KEY"},
                None,
                {"service_account_path": "KEY"},
                id="legacy_prefixed_key_path",
            ),
            pytest.param({}, "KEY", {"service_account_path": "KEY"}, id="google_application_credentials"),
            pytest.param(
                {"key_path": "KEY"}, "/env/other.json", {"service_account_path": "KEY"}, id="conn_over_env"
            ),
            pytest.param({}, None, {"service_account_path": None}, id="ambient"),
        ],
    )
    @mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    @mock.patch(f"{MODULE}.GoogleCloud", autospec=True)
    def test_credentials_resolved_from_connection(
        self, mock_gcs, mock_get_connection, extra, env, expected_kwargs, tmp_path, monkeypatch
    ):
        key_path = tmp_path / "key.json"
        key_path.write_text("{}")
        extra = {k: str(key_path) if v == "KEY" else v for k, v in extra.items()}
        if env is None:
            monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        else:
            monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key_path) if env == "KEY" else env)
        mock_get_connection.return_value = _gcp_connection(extra)

        GCSObjectStorageProvider().create_object_store(
            GCS_PATH, ConnectionConfig(conn_id="google_cloud_default")
        )

        mock_get_connection.assert_called_once_with("google_cloud_default")
        expected = {k: str(key_path) if v == "KEY" else v for k, v in expected_kwargs.items()}
        mock_gcs.assert_called_once_with(bucket_name="demo-data", **expected)

    @mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    @mock.patch(f"{MODULE}.GoogleCloud", autospec=True)
    def test_keyfile_dict_resolved_from_connection(self, mock_gcs, mock_get_connection, monkeypatch):
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        keyfile = '{"type": "service_account"}'
        mock_get_connection.return_value = _gcp_connection({"keyfile_dict": keyfile})
        written = {}

        def _capture(*, bucket_name, service_account_path):
            with open(service_account_path) as f:
                written["data"] = f.read()

        mock_gcs.side_effect = _capture

        GCSObjectStorageProvider().create_object_store(
            GCS_PATH, ConnectionConfig(conn_id="google_cloud_default")
        )

        assert written["data"] == keyfile

    @pytest.mark.parametrize(
        ("extra", "match"),
        [
            pytest.param({"key_path": "/a.json", "keyfile_dict": "{}"}, "mutually exclusive", id="both_keys"),
            pytest.param({"key_secret_name": "x"}, "'key_secret_name' is not supported", id="secret"),
            pytest.param(
                {"credential_config_file": "x"}, "'credential_config_file' is not supported", id="cfg_file"
            ),
            pytest.param(
                {"impersonation_chain": "x"}, "'impersonation_chain' is not supported", id="impersonation"
            ),
            pytest.param(
                {"extra__google_cloud_platform__impersonation_chain": "x"},
                "'impersonation_chain' is not supported",
                id="legacy_prefixed_impersonation",
            ),
        ],
    )
    @mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    @mock.patch(f"{MODULE}.GoogleCloud", autospec=True)
    def test_invalid_connection_fields_raise_value_error(self, mock_gcs, mock_get_connection, extra, match):
        mock_get_connection.return_value = _gcp_connection(extra)

        with pytest.raises(ValueError, match=match):
            GCSObjectStorageProvider().create_object_store(
                GCS_PATH, ConnectionConfig(conn_id="google_cloud_default")
            )

        mock_gcs.assert_not_called()

    @mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    @mock.patch(f"{MODULE}.GoogleCloud", autospec=True)
    def test_non_gcp_connection_raises_unknown_connection_type(self, mock_gcs, mock_get_connection):
        mock_get_connection.return_value = _gcp_connection(conn_type="aws")

        with pytest.raises(ValueError, match="Unknown connection type aws"):
            GCSObjectStorageProvider().create_object_store(
                GCS_PATH, ConnectionConfig(conn_id="google_cloud_default")
            )

        mock_gcs.assert_not_called()

    def test_requires_connection_config(self):
        with pytest.raises(ValueError, match="connection_config must be provided for gcs"):
            GCSObjectStorageProvider().create_object_store(GCS_PATH)

    @mock.patch(f"{MODULE}.GoogleCloud", autospec=True, side_effect=Exception("Error"))
    def test_store_constructor_failure_raises_object_store_creation_exception(self, mock_gcs, tmp_path):
        key_path = tmp_path / "key.json"
        key_path.write_text("{}")
        config = ConnectionConfig(conn_id="google_cloud_default", credentials={"key_path": str(key_path)})

        with pytest.raises(ObjectStoreCreationException, match="Failed to create GCS object store"):
            GCSObjectStorageProvider().create_object_store(GCS_PATH, config)

    def test_missing_key_file_raises_object_store_creation_exception(self):
        config = ConnectionConfig(
            conn_id="google_cloud_default", credentials={"key_path": "/nonexistent/key.json"}
        )

        with pytest.raises(ObjectStoreCreationException, match="Failed to create GCS object store"):
            GCSObjectStorageProvider().create_object_store(GCS_PATH, config)
