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
from __future__ import annotations

import json
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest

from airflow.models import DAG, TaskInstance as TI
from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateQueryOperator,
    GoogleDisplayVideo360CreateSDFDownloadTaskOperator,
    GoogleDisplayVideo360DeleteReportOperator,
    GoogleDisplayVideo360DownloadLineItemsOperator,
    GoogleDisplayVideo360DownloadReportV2Operator,
    GoogleDisplayVideo360RunQueryOperator,
    GoogleDisplayVideo360SDFtoGCSOperator,
    GoogleDisplayVideo360UploadLineItemsOperator,
)
from airflow.utils import timezone
from airflow.utils.session import create_session

API_VERSION = "v2"
GCP_CONN_ID = "google_cloud_default"
DELEGATE_TO: str | None = None
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
REPORT_ID = "report_id"
BUCKET_NAME = "test_bucket"
REPORT_NAME = "test_report.csv"
QUERY_ID = FILENAME = "test"
OBJECT_NAME = "object_name"


class TestGoogleDisplayVideo360DeleteReportOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, hook_mock):
        op = GoogleDisplayVideo360DeleteReportOperator(
            report_id=QUERY_ID, api_version=API_VERSION, task_id="test_task"
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.delete_query.assert_called_once_with(query_id=QUERY_ID)


class TestGoogleDisplayVideo360DownloadReportV2Operator:
    def setup_method(self):
        with create_session() as session:
            session.query(TI).delete()

    def teardown_method(self):
        with create_session() as session:
            session.query(TI).delete()

    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.shutil")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.urllib.request")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360DownloadReportV2Operator.xcom_push"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(
        self,
        mock_hook,
        mock_gcs_hook,
        mock_xcom,
        mock_temp,
        mock_request,
        mock_shutil,
    ):
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        mock_hook.return_value.get_report.return_value = {
            "metadata": {
                "status": {"state": "DONE"},
                "googleCloudStoragePath": "TEST",
            }
        }
        op = GoogleDisplayVideo360DownloadReportV2Operator(
            query_id=QUERY_ID,
            report_id=REPORT_ID,
            bucket_name=BUCKET_NAME,
            report_name=REPORT_NAME,
            task_id="test_task",
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version="v2",
            impersonation_chain=None,
        )
        mock_hook.return_value.get_report.assert_called_once_with(report_id=REPORT_ID, query_id=QUERY_ID)

        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=None,
        )
        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            filename=FILENAME,
            gzip=True,
            mime_type="text/csv",
            object_name=REPORT_NAME + ".gz",
        )
        mock_xcom.assert_called_once_with(None, key="report_name", value=REPORT_NAME + ".gz")

    @pytest.mark.parametrize(
        "test_bucket_name",
        [BUCKET_NAME, f"gs://{BUCKET_NAME}", "XComArg", "{{ ti.xcom_pull(task_ids='f') }}"],
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.shutil")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.urllib.request")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_set_bucket_name(
        self,
        mock_hook,
        mock_gcs_hook,
        mock_temp,
        mock_request,
        mock_shutil,
        test_bucket_name,
    ):
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        mock_hook.return_value.get_report.return_value = {
            "metadata": {"status": {"state": "DONE"}, "googleCloudStoragePath": "TEST"}
        }

        dag = DAG(
            dag_id="test_set_bucket_name",
            start_date=DEFAULT_DATE,
            schedule=None,
            catchup=False,
        )

        if BUCKET_NAME not in test_bucket_name:

            @dag.task
            def f():
                return BUCKET_NAME

            taskflow_op = f()
            taskflow_op.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        op = GoogleDisplayVideo360DownloadReportV2Operator(
            query_id=QUERY_ID,
            report_id=REPORT_ID,
            bucket_name=test_bucket_name if test_bucket_name != "XComArg" else taskflow_op,
            report_name=REPORT_NAME,
            task_id="test_task",
            dag=dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            filename=FILENAME,
            gzip=True,
            mime_type="text/csv",
            object_name=REPORT_NAME + ".gz",
        )


class TestGoogleDisplayVideo360RunQueryOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360RunQueryOperator.xcom_push"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, hook_mock, mock_xcom):
        parameters = {"param": "test"}
        hook_mock.return_value.run_query.return_value = {
            "key": {
                "queryId": QUERY_ID,
                "reportId": REPORT_ID,
            }
        }
        op = GoogleDisplayVideo360RunQueryOperator(
            query_id=QUERY_ID,
            parameters=parameters,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version=API_VERSION,
            impersonation_chain=None,
        )

        mock_xcom.assert_any_call(None, key="query_id", value=QUERY_ID)
        mock_xcom.assert_any_call(None, key="report_id", value=REPORT_ID)
        hook_mock.return_value.run_query.assert_called_once_with(query_id=QUERY_ID, params=parameters)


class TestGoogleDisplayVideo360DownloadLineItemsOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    def test_execute(self, mock_temp, gcs_hook_mock, hook_mock):
        request_body = {
            "filterType": "filter_type",
            "filterIds": [],
            "format": "format",
            "fileSpec": "file_spec",
        }
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        gzip = False

        op = GoogleDisplayVideo360DownloadLineItemsOperator(
            request_body=request_body,
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            gzip=gzip,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=None)

        gcs_hook_mock.return_value.upload.assert_called_with(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            filename=FILENAME,
            gzip=gzip,
            mime_type="text/csv",
        )

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.download_line_items.assert_called_once_with(request_body=request_body)


class TestGoogleDisplayVideo360UploadLineItemsOperator:
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    def test_execute(self, gcs_hook_mock, hook_mock, mock_tempfile):
        line_items = "holy_hand_grenade"
        gcs_hook_mock.return_value.download.return_value = line_items
        mock_tempfile.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME

        op = GoogleDisplayVideo360UploadLineItemsOperator(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            delegate_to=DELEGATE_TO,
            impersonation_chain=None,
        )

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=None,
        )

        gcs_hook_mock.return_value.download.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            filename=FILENAME,
        )
        hook_mock.return_value.upload_line_items.assert_called_once()
        hook_mock.return_value.upload_line_items.assert_called_once_with(line_items=line_items)


class TestGoogleDisplayVideo360SDFtoGCSOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    def test_execute(self, mock_temp, gcs_mock_hook, mock_hook):
        operation_name = "operation_name"
        operation = {"response": {"resourceName": "test_name"}}
        gzip = False

        # mock_hook.return_value.create_sdf_download_operation.return_value = response_name
        mock_hook.return_value.get_sdf_download_operation.return_value = operation
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME

        op = GoogleDisplayVideo360SDFtoGCSOperator(
            operation_name=operation_name,
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            gzip=gzip,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_sdf_download_operation.assert_called_once()
        mock_hook.return_value.get_sdf_download_operation.assert_called_once_with(
            operation_name=operation_name
        )

        mock_hook.return_value.download_media.assert_called_once()
        mock_hook.return_value.download_media.assert_called_once_with(
            resource_name=mock_hook.return_value.get_sdf_download_operation.return_value["response"][
                "resourceName"
            ]
        )

        mock_hook.return_value.download_content_from_request.assert_called_once()
        mock_hook.return_value.download_content_from_request.assert_called_once_with(
            mock_temp.NamedTemporaryFile.return_value.__enter__.return_value,
            mock_hook.return_value.download_media(),
            chunk_size=1024 * 1024,
        )

        gcs_mock_hook.assert_called_once()
        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        gcs_mock_hook.return_value.upload.assert_called_once()
        gcs_mock_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            filename=FILENAME,
            gzip=gzip,
        )


class TestGoogleDisplayVideo360CreateSDFDownloadTaskOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360CreateSDFDownloadTaskOperator.xcom_push"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, mock_hook, xcom_mock):
        body_request = {
            "version": "1",
            "id": "id",
            "filter": {"id": []},
        }
        test_name = "test_task"
        mock_hook.return_value.create_sdf_download_operation.return_value = {"name": test_name}

        op = GoogleDisplayVideo360CreateSDFDownloadTaskOperator(
            body_request=body_request,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
        )

        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            delegate_to=DELEGATE_TO,
            impersonation_chain=None,
        )

        mock_hook.return_value.create_sdf_download_operation.assert_called_once()
        mock_hook.return_value.create_sdf_download_operation.assert_called_once_with(
            body_request=body_request
        )
        xcom_mock.assert_called_once_with(None, key="name", value=test_name)


class TestGoogleDisplayVideo360CreateQueryOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "display_video.GoogleDisplayVideo360CreateQueryOperator.xcom_push"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, hook_mock, xcom_mock):
        body = {"body": "test"}
        hook_mock.return_value.create_query.return_value = {"queryId": QUERY_ID}
        op = GoogleDisplayVideo360CreateQueryOperator(body=body, task_id="test_task")
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version="v2",
            impersonation_chain=None,
        )
        hook_mock.return_value.create_query.assert_called_once_with(query=body)
        xcom_mock.assert_called_once_with(None, key="query_id", value=QUERY_ID)

    def test_prepare_template(self):
        body = {"key": "value"}
        with NamedTemporaryFile("w+", suffix=".json") as f:
            f.write(json.dumps(body))
            f.flush()
            op = GoogleDisplayVideo360CreateQueryOperator(body=body, task_id="test_task")
            op.prepare_template()

        assert isinstance(op.body, dict)
        assert op.body == body
