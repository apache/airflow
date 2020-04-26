#
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

from unittest import TestCase, mock

from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateReportOperator, GoogleDisplayVideo360DeleteReportOperator,
    GoogleDisplayVideo360DownloadLineItemsOperator, GoogleDisplayVideo360DownloadReportOperator,
    GoogleDisplayVideo360RunReportOperator, GoogleDisplayVideo360UploadLineItemsOperator,
)

API_VERSION = "api_version"
GCP_CONN_ID = "google_cloud_default"
DELEGATE_TO = None


class TestGoogleDisplayVideo360CreateReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360CreateReportOperator.xcom_push"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.BaseOperator"
    )
    def test_execute(self, mock_base_op, hook_mock, xcom_mock):
        body = {"body": "test"}
        query_id = "TEST"
        hook_mock.return_value.create_query.return_value = {"queryId": query_id}
        op = GoogleDisplayVideo360CreateReportOperator(
            body=body, api_version=API_VERSION, task_id="test_task"
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=None, api_version=API_VERSION
        )
        hook_mock.return_value.create_query.assert_called_once_with(query=body)
        xcom_mock.assert_called_once_with(None, key="report_id", value=query_id)


class TestGoogleDisplayVideo360DeleteReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.BaseOperator"
    )
    def test_execute(self, mock_base_op, hook_mock):
        query_id = "QUERY_ID"
        op = GoogleDisplayVideo360DeleteReportOperator(
            report_id=query_id, api_version=API_VERSION, task_id="test_task"
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=None, api_version=API_VERSION
        )
        hook_mock.return_value.delete_query.assert_called_once_with(query_id=query_id)


class TestGoogleDisplayVideo360GetReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.shutil"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.urllib.request"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.tempfile"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360DownloadReportOperator.xcom_push"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GCSHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.BaseOperator"
    )
    def test_execute(
        self,
        mock_base_op,
        mock_hook,
        mock_gcs_hook,
        mock_xcom,
        mock_temp,
        mock_reuqest,
        mock_shutil,
    ):
        report_id = "REPORT_ID"
        bucket_name = "BUCKET"
        report_name = "TEST.csv"
        filename = "test"
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = filename
        mock_hook.return_value.get_query.return_value = {
            "metadata": {
                "running": False,
                "googleCloudStoragePathForLatestReport": "test",
            }
        }
        op = GoogleDisplayVideo360DownloadReportOperator(
            report_id=report_id,
            api_version=API_VERSION,
            bucket_name=bucket_name,
            report_name=report_name,
            task_id="test_task",
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=None, api_version=API_VERSION
        )
        mock_hook.return_value.get_query.assert_called_once_with(query_id=report_id)

        mock_gcs_hook.assert_called_once_with(
            google_cloud_storage_conn_id=GCP_CONN_ID, delegate_to=None
        )
        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=bucket_name,
            filename=filename,
            gzip=True,
            mime_type="text/csv",
            object_name=report_name + ".gz",
        )
        mock_xcom.assert_called_once_with(
            None, key="report_name", value=report_name + ".gz"
        )


class TestGoogleDisplayVideo360RunReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.BaseOperator"
    )
    def test_execute(self, mock_base_op, hook_mock):
        report_id = "QUERY_ID"
        params = {"param": "test"}
        op = GoogleDisplayVideo360RunReportOperator(
            report_id=report_id,
            params=params,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=None, api_version=API_VERSION
        )
        hook_mock.return_value.run_query.assert_called_once_with(
            query_id=report_id, params=params
        )


class TestGoogleDisplayVideo360DownloadLineItemsOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GCSHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.tempfile"
    )
    def test_execute(self, mock_temp, gcs_hook_mock, hook_mock):
        request_body = {
            "filterType": "filter_type",
            "filterIds": [],
            "format": "format",
            "fileSpec": "file_spec"
        }
        bucket_name = "bucket_name"
        object_name = "object_name"
        filename = "test"
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = filename
        gzip = False

        op = GoogleDisplayVideo360DownloadLineItemsOperator(
            request_body=request_body,
            bucket_name=bucket_name,
            object_name=object_name,
            gzip=gzip,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            task_id="test_task",
        )

        op.execute(context=None)

        gcs_hook_mock.return_value.upload.assert_called_with(
            bucket_name=bucket_name,
            object_name=object_name,
            filename=filename,
            gzip=gzip,
            mime_type='text/csv',
        )

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, api_version=API_VERSION, delegate_to=DELEGATE_TO
        )
        hook_mock.return_value.download_line_items.assert_called_once_with(
            request_body=request_body
        )


class TestGoogleDisplayVideo360UploadLineItemsOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.tempfile"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GCSHook"
    )
    def test_execute(self, gcs_hook_mock, hook_mock, mock_tempfile):
        filename = "filename"
        object_name = "object_name"
        bucket_name = "bucket_name"
        line_items = "holy_hand_grenade"
        gcs_hook_mock.return_value.download.return_value = line_items
        mock_tempfile.NamedTemporaryFile.return_value.__enter__.return_value.name = filename

        op = GoogleDisplayVideo360UploadLineItemsOperator(
            bucket_name=bucket_name,
            object_name=object_name,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            delegate_to=DELEGATE_TO
        )

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )

        gcs_hook_mock.return_value.download.assert_called_once_with(
            bucket_name=bucket_name,
            object_name=object_name,
            filename=filename,
        )
        hook_mock.return_value.upload_line_items.assert_called_once()
        hook_mock.return_value.upload_line_items.assert_called_once_with(line_items=line_items)
