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

from airflow.providers.google.marketing_platform.operators.campaign_manager import (
    GoogleCampaignManagerBatchInsertConversionsOperator,
    GoogleCampaignManagerBatchUpdateConversionsOperator,
    GoogleCampaignManagerDeleteReportOperator,
    GoogleCampaignManagerDownloadReportOperator,
    GoogleCampaignManagerInsertReportOperator,
    GoogleCampaignManagerRunReportOperator,
)

API_VERSION = "api_version"
GCP_CONN_ID = "google_cloud_default"

CONVERSION = {
    "kind": "dfareporting#conversion",
    "floodlightActivityId": 1234,
    "floodlightConfigurationId": 1234,
    "gclid": "971nc2849184c1914019v1c34c14",
    "ordinal": "0",
    "customVariables": [{"kind": "dfareporting#customFloodlightVariable", "type": "U10", "value": "value",}],
}


class TestGoogleCampaignManagerDeleteReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators." "campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.BaseOperator")
    def test_execute(self, mock_base_op, hook_mock):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"
        op = GoogleCampaignManagerDeleteReportOperator(
            profile_id=profile_id, report_id=report_id, api_version=API_VERSION, task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=None, api_version=API_VERSION, impersonation_chain=None,
        )
        hook_mock.return_value.delete_report.assert_called_once_with(
            profile_id=profile_id, report_id=report_id
        )


class TestGoogleCampaignManagerGetReportOperator(TestCase):
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.http")
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.tempfile")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators." "campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.BaseOperator")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "campaign_manager.GoogleCampaignManagerDownloadReportOperator.xcom_push"
    )
    def test_execute(
        self, xcom_mock, mock_base_op, gcs_hook_mock, hook_mock, tempfile_mock, http_mock,
    ):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"
        file_id = "FILE_ID"
        bucket_name = "test_bucket"
        report_name = "test_report.csv"
        temp_file_name = "TEST"

        http_mock.MediaIoBaseDownload.return_value.next_chunk.return_value = (
            None,
            True,
        )
        tempfile_mock.NamedTemporaryFile.return_value.__enter__.return_value.name = temp_file_name
        op = GoogleCampaignManagerDownloadReportOperator(
            profile_id=profile_id,
            report_id=report_id,
            file_id=file_id,
            bucket_name=bucket_name,
            report_name=report_name,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=None, api_version=API_VERSION, impersonation_chain=None,
        )
        hook_mock.return_value.get_report_file.assert_called_once_with(
            profile_id=profile_id, report_id=report_id, file_id=file_id
        )
        gcs_hook_mock.assert_called_once_with(
            google_cloud_storage_conn_id=GCP_CONN_ID, delegate_to=None, impersonation_chain=None,
        )
        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=bucket_name,
            object_name=report_name + ".gz",
            gzip=True,
            filename=temp_file_name,
            mime_type="text/csv",
        )
        xcom_mock.assert_called_once_with(None, key="report_name", value=report_name + ".gz")


class TestGoogleCampaignManagerInsertReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators." "campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.BaseOperator")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "campaign_manager.GoogleCampaignManagerInsertReportOperator.xcom_push"
    )
    def test_execute(self, xcom_mock, mock_base_op, hook_mock):
        profile_id = "PROFILE_ID"
        report = {"report": "test"}
        report_id = "test"

        hook_mock.return_value.insert_report.return_value = {"id": report_id}

        op = GoogleCampaignManagerInsertReportOperator(
            profile_id=profile_id, report=report, api_version=API_VERSION, task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=None, api_version=API_VERSION, impersonation_chain=None,
        )
        hook_mock.return_value.insert_report.assert_called_once_with(profile_id=profile_id, report=report)
        xcom_mock.assert_called_once_with(None, key="report_id", value=report_id)

    def test_prepare_template(self):
        profile_id = "PROFILE_ID"
        report = {"key": "value"}
        with NamedTemporaryFile("w+", suffix=".json") as f:
            f.write(json.dumps(report))
            f.flush()
            op = GoogleCampaignManagerInsertReportOperator(
                profile_id=profile_id, report=f.name, api_version=API_VERSION, task_id="test_task",
            )
            op.prepare_template()

        assert isinstance(op.report, dict)
        assert op.report == report


class TestGoogleCampaignManagerRunReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators." "campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.BaseOperator")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "campaign_manager.GoogleCampaignManagerRunReportOperator.xcom_push"
    )
    def test_execute(self, xcom_mock, mock_base_op, hook_mock):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"
        file_id = "FILE_ID"
        synchronous = True

        hook_mock.return_value.run_report.return_value = {"id": file_id}

        op = GoogleCampaignManagerRunReportOperator(
            profile_id=profile_id,
            report_id=report_id,
            synchronous=synchronous,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=None, api_version=API_VERSION, impersonation_chain=None,
        )
        hook_mock.return_value.run_report.assert_called_once_with(
            profile_id=profile_id, report_id=report_id, synchronous=synchronous
        )
        xcom_mock.assert_called_once_with(None, key="file_id", value=file_id)


class TestGoogleCampaignManagerBatchInsertConversionsOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators." "campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.BaseOperator")
    def test_execute(self, mock_base_op, hook_mock):
        profile_id = "PROFILE_ID"
        op = GoogleCampaignManagerBatchInsertConversionsOperator(
            task_id="insert_conversion",
            profile_id=profile_id,
            conversions=[CONVERSION],
            encryption_source="AD_SERVING",
            encryption_entity_type="DCM_ADVERTISER",
            encryption_entity_id=123456789,
        )
        op.execute(None)
        hook_mock.return_value.conversions_batch_insert.assert_called_once_with(
            profile_id=profile_id,
            conversions=[CONVERSION],
            encryption_source="AD_SERVING",
            encryption_entity_type="DCM_ADVERTISER",
            encryption_entity_id=123456789,
            max_failed_inserts=0,
        )


class TestGoogleCampaignManagerBatchUpdateConversionOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators." "campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators." "campaign_manager.BaseOperator")
    def test_execute(self, mock_base_op, hook_mock):
        profile_id = "PROFILE_ID"
        op = GoogleCampaignManagerBatchUpdateConversionsOperator(
            task_id="update_conversion",
            profile_id=profile_id,
            conversions=[CONVERSION],
            encryption_source="AD_SERVING",
            encryption_entity_type="DCM_ADVERTISER",
            encryption_entity_id=123456789,
        )
        op.execute(None)
        hook_mock.return_value.conversions_batch_update.assert_called_once_with(
            profile_id=profile_id,
            conversions=[CONVERSION],
            encryption_source="AD_SERVING",
            encryption_entity_type="DCM_ADVERTISER",
            encryption_entity_id=123456789,
            max_failed_updates=0,
        )
