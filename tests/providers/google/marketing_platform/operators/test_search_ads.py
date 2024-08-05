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
from airflow.providers.google.marketing_platform.operators.search_ads import (
    GoogleSearchAdsDownloadReportOperator,
    GoogleSearchAdsInsertReportOperator,
)
from airflow.utils import timezone
from airflow.utils.session import create_session

API_VERSION = "api_version"
GCP_CONN_ID = "google_cloud_default"

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
END_DATE = timezone.datetime(2021, 1, 2)
REPORT_ID = "report_id"
BUCKET_NAME = "test_bucket"
REPORT_NAME = "test_report.csv"
FILE_NAME = "test"


class TestGoogleSearchAdsInsertReportOperator:
    @mock.patch("airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.search_ads.BaseOperator")
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.GoogleSearchAdsInsertReportOperator.xcom_push"
    )
    def test_execute(self, xcom_mock, mock_base_op, hook_mock):
        report = {"report": "test"}
        hook_mock.return_value.insert_report.return_value = {"id": REPORT_ID}
        op = GoogleSearchAdsInsertReportOperator(report=report, api_version=API_VERSION, task_id="test_task")
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.insert_report.assert_called_once_with(report=report)
        xcom_mock.assert_called_once_with(None, key="report_id", value=REPORT_ID)

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


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
class TestGoogleSearchAdsDownloadReportOperator:
    def setup_method(self):
        with create_session() as session:
            session.query(TI).delete()

    def teardown_method(self):
        with create_session() as session:
            session.query(TI).delete()

    @mock.patch("airflow.providers.google.marketing_platform.operators.search_ads.NamedTemporaryFile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.search_ads.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsHook")
    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "operators.search_ads.GoogleSearchAdsDownloadReportOperator.xcom_push"
    )
    def test_execute(self, xcom_mock, hook_mock, gcs_hook_mock, tempfile_mock):
        temp_file_name = "TEMP"
        data = b"data"

        hook_mock.return_value.get.return_value = {"files": [0], "isReportReady": True}
        hook_mock.return_value.get_file.return_value = data
        tempfile_mock.return_value.__enter__.return_value.name = temp_file_name

        op = GoogleSearchAdsDownloadReportOperator(
            report_id=REPORT_ID,
            report_name=FILE_NAME,
            bucket_name=BUCKET_NAME,
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
        hook_mock.return_value.get_file.assert_called_once_with(report_fragment=0, report_id=REPORT_ID)
        tempfile_mock.return_value.__enter__.return_value.write.assert_called_once_with(data)
        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            gzip=True,
            object_name=FILE_NAME + ".csv.gz",
            filename=temp_file_name,
        )
        xcom_mock.assert_called_once_with(None, key="file_name", value=FILE_NAME + ".csv.gz")

    @pytest.mark.parametrize(
        "test_bucket_name",
        [BUCKET_NAME, f"gs://{BUCKET_NAME}", "XComArg", "{{ ti.xcom_pull(task_ids='f') }}"],
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.search_ads.NamedTemporaryFile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.search_ads.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsHook")
    def test_set_bucket_name(self, hook_mock, gcs_hook_mock, tempfile_mock, test_bucket_name):
        temp_file_name = "TEMP"
        data = b"data"

        hook_mock.return_value.get.return_value = {"files": [0], "isReportReady": True}
        hook_mock.return_value.get_file.return_value = data
        tempfile_mock.return_value.__enter__.return_value.name = temp_file_name

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

        op = GoogleSearchAdsDownloadReportOperator(
            report_id=REPORT_ID,
            report_name=FILE_NAME,
            bucket_name=test_bucket_name if test_bucket_name != "XComArg" else taskflow_op,
            api_version=API_VERSION,
            task_id="test_task",
            dag=dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            gzip=True,
            object_name=FILE_NAME + ".csv.gz",
            filename=temp_file_name,
        )
