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
import json
from tempfile import NamedTemporaryFile
from unittest import TestCase, mock

from airflow.providers.google.marketing_platform.operators.search_ads import (
    GoogleSearchAdsDownloadReportOperator, GoogleSearchAdsInsertReportOperator,
)

API_VERSION = "api_version"
GCP_CONN_ID = "google_cloud_default"


class TestGoogleSearchAdsInsertReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.GoogleSearchAdsHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.BaseOperator"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.GoogleSearchAdsInsertReportOperator.xcom_push"
    )
    def test_execute(self, xcom_mock, mock_base_op, hook_mock):
        report = {"report": "test"}
        report_id = "TEST"
        hook_mock.return_value.insert_report.return_value = {"id": report_id}
        op = GoogleSearchAdsInsertReportOperator(
            report=report, api_version=API_VERSION, task_id="test_task"
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.insert_report.assert_called_once_with(report=report)
        xcom_mock.assert_called_once_with(None, key="report_id", value=report_id)

    def test_prepare_template(self):
        report = {"key": "value"}
        with NamedTemporaryFile("w+", suffix=".json") as f:
            f.write(json.dumps(report))
            f.flush()
            op = GoogleSearchAdsInsertReportOperator(
                report=report, api_version=API_VERSION, task_id="test_task"
            )
            op.prepare_template()

        assert isinstance(op.report, dict)
        assert op.report == report


class TestGoogleSearchAdsDownloadReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.NamedTemporaryFile"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.GCSHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.GoogleSearchAdsHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.BaseOperator"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.GoogleSearchAdsDownloadReportOperator.xcom_push"
    )
    def test_execute(
        self, xcom_mock, mock_base_op, hook_mock, gcs_hook_mock, tempfile_mock
    ):
        report_id = "REPORT_ID"
        file_name = "TEST"
        temp_file_name = "TEMP"
        bucket_name = "test"
        data = b"data"

        hook_mock.return_value.get.return_value = {"files": [0], "isReportReady": True}
        hook_mock.return_value.get_file.return_value = data
        tempfile_mock.return_value.__enter__.return_value.name = temp_file_name

        op = GoogleSearchAdsDownloadReportOperator(
            report_id=report_id,
            report_name=file_name,
            bucket_name=bucket_name,
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
        hook_mock.return_value.get_file.assert_called_once_with(
            report_fragment=0, report_id=report_id
        )
        tempfile_mock.return_value.__enter__.return_value.write.assert_called_once_with(
            data
        )
        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=bucket_name,
            gzip=True,
            object_name=file_name + ".csv.gz",
            filename=temp_file_name,
        )
        xcom_mock.assert_called_once_with(
            None, key="file_name", value=file_name + ".csv.gz"
        )
