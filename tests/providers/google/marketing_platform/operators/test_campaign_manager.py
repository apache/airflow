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
from unittest import TestCase, mock

from parameterized import parameterized

from airflow.models import DAG, TaskInstance as TI
from airflow.providers.google.marketing_platform.operators.campaign_manager import (
    GoogleCampaignManagerBatchInsertConversionsOperator,
    GoogleCampaignManagerBatchUpdateConversionsOperator,
    GoogleCampaignManagerDeleteReportOperator,
    GoogleCampaignManagerDownloadReportOperator,
    GoogleCampaignManagerInsertReportOperator,
    GoogleCampaignManagerRunReportOperator,
)
from airflow.utils import timezone
from airflow.utils.session import create_session

API_VERSION = "api_version"
GCP_CONN_ID = "google_cloud_default"

CONVERSION = {
    "kind": "dfareporting#conversion",
    "floodlightActivityId": 1234,
    "floodlightConfigurationId": 1234,
    "gclid": "971nc2849184c1914019v1c34c14",
    "ordinal": "0",
    "customVariables": [
        {
            "kind": "dfareporting#customFloodlightVariable",
            "type": "U10",
            "value": "value",
        }
    ],
}

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
PROFILE_ID = "profile_id"
REPORT_ID = "report_id"
FILE_ID = "file_id"
BUCKET_NAME = "test_bucket"
REPORT_NAME = "test_report.csv"
TEMP_FILE_NAME = "test"


class TestGoogleCampaignManagerDeleteReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    def test_execute(self, mock_base_op, hook_mock):
        op = GoogleCampaignManagerDeleteReportOperator(
            profile_id=PROFILE_ID,
            report_id=REPORT_ID,
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
        hook_mock.return_value.delete_report.assert_called_once_with(
            profile_id=PROFILE_ID, report_id=REPORT_ID
        )


class TestGoogleCampaignManagerDownloadReportOperator(TestCase):
    def setUp(self):
        with create_session() as session:
            session.query(TI).delete()

    def tearDown(self):
        with create_session() as session:
            session.query(TI).delete()

    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.http")
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.tempfile")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "campaign_manager.GoogleCampaignManagerDownloadReportOperator.xcom_push"
    )
    def test_execute(
        self,
        xcom_mock,
        mock_base_op,
        gcs_hook_mock,
        hook_mock,
        tempfile_mock,
        http_mock,
    ):
        http_mock.MediaIoBaseDownload.return_value.next_chunk.return_value = (
            None,
            True,
        )
        tempfile_mock.NamedTemporaryFile.return_value.__enter__.return_value.name = TEMP_FILE_NAME
        op = GoogleCampaignManagerDownloadReportOperator(
            profile_id=PROFILE_ID,
            report_id=REPORT_ID,
            file_id=FILE_ID,
            bucket_name=BUCKET_NAME,
            report_name=REPORT_NAME,
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
        hook_mock.return_value.get_report_file.assert_called_once_with(
            profile_id=PROFILE_ID, report_id=REPORT_ID, file_id=FILE_ID
        )
        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=None,
        )
        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=REPORT_NAME + ".gz",
            gzip=True,
            filename=TEMP_FILE_NAME,
            mime_type="text/csv",
        )
        xcom_mock.assert_called_once_with(None, key="report_name", value=REPORT_NAME + ".gz")

    @parameterized.expand([BUCKET_NAME, f"gs://{BUCKET_NAME}", "XComArg", "{{ ti.xcom_pull(task_ids='f') }}"])
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.http")
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.tempfile")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.GCSHook")
    def test_set_bucket_name(
        self,
        test_bucket_name,
        gcs_hook_mock,
        hook_mock,
        tempfile_mock,
        http_mock,
    ):
        http_mock.MediaIoBaseDownload.return_value.next_chunk.return_value = (
            None,
            True,
        )
        tempfile_mock.NamedTemporaryFile.return_value.__enter__.return_value.name = TEMP_FILE_NAME

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

        op = GoogleCampaignManagerDownloadReportOperator(
            profile_id=PROFILE_ID,
            report_id=REPORT_ID,
            file_id=FILE_ID,
            bucket_name=test_bucket_name if test_bucket_name != "XComArg" else taskflow_op,
            report_name=REPORT_NAME,
            api_version=API_VERSION,
            task_id="test_task",
            dag=dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=REPORT_NAME + ".gz",
            gzip=True,
            filename=TEMP_FILE_NAME,
            mime_type="text/csv",
        )


class TestGoogleCampaignManagerInsertReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "campaign_manager.GoogleCampaignManagerInsertReportOperator.xcom_push"
    )
    def test_execute(self, xcom_mock, mock_base_op, hook_mock):
        report = {"report": "test"}

        hook_mock.return_value.insert_report.return_value = {"id": REPORT_ID}

        op = GoogleCampaignManagerInsertReportOperator(
            profile_id=PROFILE_ID,
            report=report,
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
        hook_mock.return_value.insert_report.assert_called_once_with(profile_id=PROFILE_ID, report=report)
        xcom_mock.assert_called_once_with(None, key="report_id", value=REPORT_ID)

    def test_prepare_template(self):
        report = {"key": "value"}
        with NamedTemporaryFile("w+", suffix=".json") as f:
            f.write(json.dumps(report))
            f.flush()
            op = GoogleCampaignManagerInsertReportOperator(
                profile_id=PROFILE_ID,
                report=f.name,
                api_version=API_VERSION,
                task_id="test_task",
            )
            op.prepare_template()

        assert isinstance(op.report, dict)
        assert op.report == report


class TestGoogleCampaignManagerRunReportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "campaign_manager.GoogleCampaignManagerRunReportOperator.xcom_push"
    )
    def test_execute(self, xcom_mock, mock_base_op, hook_mock):
        synchronous = True

        hook_mock.return_value.run_report.return_value = {"id": FILE_ID}

        op = GoogleCampaignManagerRunReportOperator(
            profile_id=PROFILE_ID,
            report_id=REPORT_ID,
            synchronous=synchronous,
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
        hook_mock.return_value.run_report.assert_called_once_with(
            profile_id=PROFILE_ID, report_id=REPORT_ID, synchronous=synchronous
        )
        xcom_mock.assert_called_once_with(None, key="file_id", value=FILE_ID)


class TestGoogleCampaignManagerBatchInsertConversionsOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    def test_execute(self, mock_base_op, hook_mock):
        op = GoogleCampaignManagerBatchInsertConversionsOperator(
            task_id="insert_conversion",
            profile_id=PROFILE_ID,
            conversions=[CONVERSION],
            encryption_source="AD_SERVING",
            encryption_entity_type="DCM_ADVERTISER",
            encryption_entity_id=123456789,
        )
        op.execute(None)
        hook_mock.return_value.conversions_batch_insert.assert_called_once_with(
            profile_id=PROFILE_ID,
            conversions=[CONVERSION],
            encryption_source="AD_SERVING",
            encryption_entity_type="DCM_ADVERTISER",
            encryption_entity_id=123456789,
            max_failed_inserts=0,
        )


class TestGoogleCampaignManagerBatchUpdateConversionOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    def test_execute(self, mock_base_op, hook_mock):
        op = GoogleCampaignManagerBatchUpdateConversionsOperator(
            task_id="update_conversion",
            profile_id=PROFILE_ID,
            conversions=[CONVERSION],
            encryption_source="AD_SERVING",
            encryption_entity_type="DCM_ADVERTISER",
            encryption_entity_id=123456789,
        )
        op.execute(None)
        hook_mock.return_value.conversions_batch_update.assert_called_once_with(
            profile_id=PROFILE_ID,
            conversions=[CONVERSION],
            encryption_source="AD_SERVING",
            encryption_entity_type="DCM_ADVERTISER",
            encryption_entity_id=123456789,
            max_failed_updates=0,
        )
