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

from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest
from google.api_core.exceptions import NotFound

import airflow.version
from airflow.models import Connection

from tests_common.test_utils.config import conf_vars

if airflow.version.version.strip().startswith("3"):
    from airflow.providers.google.cloud.bundles.gcs import GCSDagBundle

GCP_CONN_ID = "gcs_dags_connection"
GCS_BUCKET_NAME = "my-airflow-dags-bucket"
GCS_BUCKET_PREFIX = "project1/dags"


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
