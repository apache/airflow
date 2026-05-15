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

import airflow.version
from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException

from tests_common.test_utils.config import conf_vars

if airflow.version.version.strip().startswith("3"):
    from airflow.providers.microsoft.azure.bundles.wasb import WasbDagBundle

WASB_CONN_ID = "wasb_dags_connection"
CONTAINER_NAME = "my-airflow-dags-container"
CONTAINER_PREFIX = "project1/dags"
ACCOUNT_NAME = "myaccount"


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
        mock_hook.blob_service_client.account_name = ACCOUNT_NAME
        mock_wasb_hook_property.return_value = mock_hook

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        url: str = bundle.view_url()
        assert url == f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{CONTAINER_PREFIX}"

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle.wasb_hook", new_callable=PropertyMock
    )
    def test_view_url_template_generates_blob_url(self, mock_wasb_hook_property):
        mock_hook = MagicMock()
        mock_hook.blob_service_client.account_name = ACCOUNT_NAME
        mock_wasb_hook_property.return_value = mock_hook

        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        url: str = bundle.view_url_template()
        assert url == f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{CONTAINER_PREFIX}"

    def test_supports_versioning(self):
        bundle = WasbDagBundle(
            name="test",
            wasb_conn_id=WASB_CONN_ID,
            prefix=CONTAINER_PREFIX,
            container_name=CONTAINER_NAME,
        )
        assert WasbDagBundle.supports_versioning is False

        bundle.version = "test_version"

        with pytest.raises(AirflowException, match="Refreshing a specific version is not supported"):
            bundle.refresh()
        with pytest.raises(AirflowException, match="WASB url with version is not supported"):
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
        with pytest.raises(AirflowException, match=f"Local Dags path: {file_path} is not a directory."):
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
        with pytest.raises(AirflowException, match="WASB container 'non-existing-container' does not exist."):
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
            AirflowException,
            match=f"WASB prefix 'wasb://{CONTAINER_NAME}/non-existing-prefix' does not exist.",
        ):
            bundle.initialize()
        mock_hook.check_for_prefix.assert_called_once_with(
            container_name=CONTAINER_NAME, prefix="non-existing-prefix", delimiter="/"
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
