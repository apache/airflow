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

from airflow.models import TaskInstance as TI
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

from tests_common.test_utils.taskinstance import run_task_instance

API_VERSION = "v4"
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


class TestGoogleCampaignManagerDeleteReportOperator:
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
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.delete_report.assert_called_once_with(
            profile_id=PROFILE_ID, report_id=REPORT_ID
        )


@pytest.mark.db_test
class TestGoogleCampaignManagerDownloadReportOperator:
    def setup_method(self):
        with create_session() as session:
            session.query(TI).delete()

    def teardown_method(self):
        with create_session() as session:
            session.query(TI).delete()

    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.http")
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.tempfile")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    def test_execute(
        self,
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

        mock_context = {"task_instance": mock.Mock()}

        op = GoogleCampaignManagerDownloadReportOperator(
            profile_id=PROFILE_ID,
            report_id=REPORT_ID,
            file_id=FILE_ID,
            bucket_name=BUCKET_NAME,
            report_name=REPORT_NAME,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=mock_context)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.get_report_file.assert_called_once_with(
            profile_id=PROFILE_ID, report_id=REPORT_ID, file_id=FILE_ID
        )
        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=REPORT_NAME + ".gz",
            gzip=True,
            filename=TEMP_FILE_NAME,
            mime_type="text/csv",
        )
        mock_context["task_instance"].xcom_push.assert_called_once_with(
            key="report_name", value=REPORT_NAME + ".gz"
        )

    @pytest.mark.parametrize(
        "test_bucket_name",
        [BUCKET_NAME, f"gs://{BUCKET_NAME}", "XComArg", "{{ ti.xcom_pull(task_ids='taskflow_op') }}"],
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.http")
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.tempfile")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.GCSHook")
    def test_set_bucket_name(
        self,
        gcs_hook_mock,
        hook_mock,
        tempfile_mock,
        http_mock,
        test_bucket_name,
        dag_maker,
    ):
        http_mock.MediaIoBaseDownload.return_value.next_chunk.return_value = (
            None,
            True,
        )
        tempfile_mock.NamedTemporaryFile.return_value.__enter__.return_value.name = TEMP_FILE_NAME

        with dag_maker(dag_id="test_set_bucket_name", start_date=DEFAULT_DATE) as dag:
            if BUCKET_NAME not in test_bucket_name:

                @dag.task(task_id="taskflow_op")
                def f():
                    return BUCKET_NAME

                taskflow_op = f()

            GoogleCampaignManagerDownloadReportOperator(
                profile_id=PROFILE_ID,
                report_id=REPORT_ID,
                file_id=FILE_ID,
                bucket_name=test_bucket_name if test_bucket_name != "XComArg" else taskflow_op,
                report_name=REPORT_NAME,
                api_version=API_VERSION,
                task_id="test_task",
            )

        dr = dag_maker.create_dagrun()

        for ti in dr.get_task_instances():
            run_task_instance(ti, dag_maker.dag.get_task(ti.task_id))

        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=REPORT_NAME + ".gz",
            gzip=True,
            filename=TEMP_FILE_NAME,
            mime_type="text/csv",
        )


class TestGoogleCampaignManagerInsertReportOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    def test_execute(self, mock_base_op, hook_mock):
        report = {"report": "test"}

        mock_context = {"task_instance": mock.Mock()}

        hook_mock.return_value.insert_report.return_value = {"id": REPORT_ID}

        op = GoogleCampaignManagerInsertReportOperator(
            profile_id=PROFILE_ID,
            report=report,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=mock_context)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.insert_report.assert_called_once_with(profile_id=PROFILE_ID, report=report)
        mock_context["task_instance"].xcom_push.assert_called_once_with(key="report_id", value=REPORT_ID)

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


class TestGoogleCampaignManagerRunReportOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerHook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.campaign_manager.BaseOperator")
    def test_execute(self, mock_base_op, hook_mock):
        synchronous = True

        mock_context = {"task_instance": mock.Mock()}

        hook_mock.return_value.run_report.return_value = {"id": FILE_ID}

        op = GoogleCampaignManagerRunReportOperator(
            profile_id=PROFILE_ID,
            report_id=REPORT_ID,
            synchronous=synchronous,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=mock_context)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.run_report.assert_called_once_with(
            profile_id=PROFILE_ID, report_id=REPORT_ID, synchronous=synchronous
        )
        mock_context["task_instance"].xcom_push.assert_called_once_with(key="file_id", value=FILE_ID)


class TestGoogleCampaignManagerBatchInsertConversionsOperator:
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


class TestGoogleCampaignManagerBatchUpdateConversionOperator:
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
