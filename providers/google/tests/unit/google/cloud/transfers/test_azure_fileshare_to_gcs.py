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

import datetime
from unittest import mock

import pytest

from airflow import DAG
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs import AzureFileShareToGCSOperator

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

DEFAULT_DATE = datetime.datetime(2024, 1, 1)
TASK_ID = "test-azure-fileshare-to-gcs"
AZURE_FILESHARE_SHARE = "test-share"
AZURE_FILESHARE_DIRECTORY_PATH = "/path/to/dir"
GCS_PATH_PREFIX = "gs://gcs-bucket/data/"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
AZURE_FILESHARE_CONN_ID = "azure_fileshare_default"
GCS_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestAzureFileShareToGCSOperator:
    def test_init(self):
        """Test AzureFileShareToGCSOperator instance is properly initialized."""

        operator = AzureFileShareToGCSOperator(
            task_id=TASK_ID,
            share_name=AZURE_FILESHARE_SHARE,
            directory_path=AZURE_FILESHARE_DIRECTORY_PATH,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )

        assert operator.task_id == TASK_ID
        assert operator.share_name == AZURE_FILESHARE_SHARE
        assert operator.directory_path == AZURE_FILESHARE_DIRECTORY_PATH
        assert operator.azure_fileshare_conn_id == AZURE_FILESHARE_CONN_ID
        assert operator.gcp_conn_id == GCS_CONN_ID
        assert operator.dest_gcs == GCS_PATH_PREFIX
        assert operator.google_impersonation_chain == IMPERSONATION_CHAIN

    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareHook")
    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.GCSHook")
    def test_directory_name_alias_uses_rendered_value(self, gcs_mock_hook, azure_fileshare_mock_hook):
        """A templated directory_name is aliased to directory_path using its rendered value, not the Jinja expression."""
        dag = DAG("test_azure_fileshare_alias", schedule=None, start_date=DEFAULT_DATE)
        with pytest.warns(AirflowProviderDeprecationWarning, match="Use 'directory_path' instead"):
            operator = AzureFileShareToGCSOperator(
                task_id=TASK_ID,
                share_name=AZURE_FILESHARE_SHARE,
                directory_name="{{ params.legacy_dir }}",
                params={"legacy_dir": "rendered/dir"},
                azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
                gcp_conn_id=GCS_CONN_ID,
                dest_gcs=GCS_PATH_PREFIX,
                return_gcs_uris=True,
                dag=dag,
            )
        assert operator.directory_path is None

        operator.render_template_fields({"params": {"legacy_dir": "rendered/dir"}})
        assert operator.directory_path is None

        azure_fileshare_mock_hook.return_value.list_files.return_value = MOCK_FILES
        operator.execute(None)
        azure_fileshare_mock_hook.assert_any_call(
            share_name=AZURE_FILESHARE_SHARE,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            directory_path="rendered/dir",
        )

    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareHook")
    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.GCSHook")
    def test_native_directory_path_rendering_to_none_is_not_aliased(
        self, gcs_mock_hook, azure_fileshare_mock_hook
    ):
        """
        Behaviour-preservation guard: the alias decision must come from the un-rendered values.

        With render_template_as_native_obj an explicitly supplied directory_path can render to None,
        and re-deciding the alias after rendering would wrongly fall back to the deprecated
        directory_name. Deciding in __init__ (pre-render) keeps this case an explicit error.
        """
        dag = DAG(
            "test_azure_fileshare_native",
            schedule=None,
            start_date=DEFAULT_DATE,
            render_template_as_native_obj=True,
        )
        operator = AzureFileShareToGCSOperator(
            task_id=TASK_ID,
            share_name=AZURE_FILESHARE_SHARE,
            directory_path="{{ params.p }}",
            directory_name="legacy",
            params={"p": None},
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            return_gcs_uris=True,
            dag=dag,
        )

        operator.render_template_fields({"params": {"p": None}})
        assert operator.directory_path is None

        # The deprecated alias must not silently substitute directory_name; a genuinely unset
        # directory surfaces as the operator's own error instead of listing the wrong directory.
        azure_fileshare_mock_hook.return_value.list_files.return_value = MOCK_FILES
        with pytest.raises(RuntimeError, match="directory_name must be set"):
            operator.execute(None)

    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareHook")
    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.GCSHook")
    def test_mapped_task_aliases_directory_name(self, gcs_mock_hook, azure_fileshare_mock_hook):
        """
        Mapped tasks reach execute() through MappedOperator.unmap(), never through
        render_template_fields overrides, so the alias must not depend on one.
        """
        mapped = AzureFileShareToGCSOperator.partial(
            task_id=TASK_ID,
            share_name=AZURE_FILESHARE_SHARE,
            directory_name=AZURE_FILESHARE_DIRECTORY_PATH,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            return_gcs_uris=True,
        ).expand(prefix=["sub/a/", "sub/b/"])

        with pytest.warns(AirflowProviderDeprecationWarning, match="Use 'directory_path' instead"):
            operator = mapped.unmap({"prefix": "sub/a/"})

        azure_fileshare_mock_hook.return_value.list_files.return_value = MOCK_FILES
        operator.execute(None)
        azure_fileshare_mock_hook.assert_any_call(
            share_name=AZURE_FILESHARE_SHARE,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            directory_path=AZURE_FILESHARE_DIRECTORY_PATH,
        )

    @pytest.mark.parametrize("return_gcs_uris", [True, False])
    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareHook")
    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.GCSHook")
    def test_execute(self, gcs_mock_hook, azure_fileshare_mock_hook, return_gcs_uris):
        """Test the execute function when the run is successful."""

        operator = AzureFileShareToGCSOperator(
            task_id=TASK_ID,
            share_name=AZURE_FILESHARE_SHARE,
            directory_path=AZURE_FILESHARE_DIRECTORY_PATH,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
            return_gcs_uris=return_gcs_uris,
        )

        azure_fileshare_mock_hook.return_value.list_files.return_value = MOCK_FILES

        uploaded_files = operator.execute(None)

        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call("gcs-bucket", "data/TEST1.csv", mock.ANY, gzip=False),
                mock.call("gcs-bucket", "data/TEST3.csv", mock.ANY, gzip=False),
                mock.call("gcs-bucket", "data/TEST2.csv", mock.ANY, gzip=False),
            ],
            any_order=True,
        )

        assert azure_fileshare_mock_hook.return_value.get_file_to_stream.call_count == 3

        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        expected_files = (
            [f"gs://gcs-bucket/data/{file_name}" for file_name in MOCK_FILES]
            if return_gcs_uris
            else MOCK_FILES
        )
        assert sorted(expected_files) == sorted(uploaded_files)

    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareHook")
    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.GCSHook")
    def test_execute_with_gzip(self, gcs_mock_hook, azure_fileshare_mock_hook):
        """Test the execute function when the run is successful."""

        operator = AzureFileShareToGCSOperator(
            task_id=TASK_ID,
            share_name=AZURE_FILESHARE_SHARE,
            directory_path=AZURE_FILESHARE_DIRECTORY_PATH,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
            gzip=True,
        )

        azure_fileshare_mock_hook.return_value.list_files.return_value = MOCK_FILES

        operator.execute(None)

        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call("gcs-bucket", "data/TEST1.csv", mock.ANY, gzip=True),
                mock.call("gcs-bucket", "data/TEST3.csv", mock.ANY, gzip=True),
                mock.call("gcs-bucket", "data/TEST2.csv", mock.ANY, gzip=True),
            ],
            any_order=True,
        )
