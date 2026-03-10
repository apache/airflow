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
from sqlalchemy import delete

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance as TI
from airflow.providers.google.marketing_platform.operators.bid_manager import (
    GoogleBidManagerCreateQueryOperator,
    GoogleBidManagerDeleteQueryOperator,
    GoogleBidManagerDownloadReportOperator,
    GoogleBidManagerRunQueryOperator,
)
from airflow.utils import timezone
from airflow.utils.session import create_session

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

API_VERSION = "v2"
GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
REPORT_ID = "report_id"
BUCKET_NAME = "test_bucket"
REPORT_NAME = "test_report.csv"
QUERY_ID = FILENAME = "test.csv"


class TestGoogleBidManagerDeleteQueryOperator:
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerHook")
    def test_execute(self, hook_mock):
        op = GoogleBidManagerDeleteQueryOperator(
            query_id=QUERY_ID, api_version=API_VERSION, task_id="test_task"
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.delete_query.assert_called_once_with(query_id=QUERY_ID)


@pytest.mark.db_test
class TestGoogleBidManagerDownloadReportOperator:
    def setup_method(self):
        with create_session() as session:
            session.execute(delete(TI))

    def teardown_method(self):
        with create_session() as session:
            session.execute(delete(TI))

    @pytest.mark.parametrize(
        ("file_path", "should_except"), [("https://host/path", False), ("file:/path/to/file", True)]
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.shutil")
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.urllib.request")
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.tempfile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerHook")
    def test_execute(
        self,
        mock_hook,
        mock_gcs_hook,
        mock_temp,
        mock_request,
        mock_shutil,
        file_path,
        should_except,
    ):
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        mock_hook.return_value.get_report.return_value = {
            "metadata": {
                "status": {"state": "DONE"},
                "googleCloudStoragePath": file_path,
            }
        }
        # Create mock context with task_instance
        mock_context = {"task_instance": mock.Mock()}

        op = GoogleBidManagerDownloadReportOperator(
            query_id=QUERY_ID,
            report_id=REPORT_ID,
            bucket_name=BUCKET_NAME,
            report_name=REPORT_NAME,
            task_id="test_task",
        )
        if should_except:
            with pytest.raises(AirflowException):
                op.execute(context=mock_context)
            return
        op.execute(context=mock_context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v2",
            impersonation_chain=None,
        )
        mock_hook.return_value.get_report.assert_called_once_with(report_id=REPORT_ID, query_id=QUERY_ID)

        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            filename=FILENAME,
            gzip=True,
            mime_type="text/csv",
            object_name=REPORT_NAME + ".gz",
        )
        mock_context["task_instance"].xcom_push.assert_called_once_with(
            key="report_name", value=REPORT_NAME + ".gz"
        )

    @pytest.mark.parametrize(
        "test_bucket_name",
        [BUCKET_NAME, f"gs://{BUCKET_NAME}", "XComArg", "{{ ti.xcom_pull(task_ids='taskflow_op') }}"],
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.shutil")
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.urllib.request")
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.tempfile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerHook")
    def test_set_bucket_name(
        self,
        mock_hook,
        mock_gcs_hook,
        mock_temp,
        mock_request,
        mock_shutil,
        test_bucket_name,
        dag_maker,
    ):
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        mock_hook.return_value.get_report.return_value = {
            "metadata": {"status": {"state": "DONE"}, "googleCloudStoragePath": "TEST"}
        }
        with dag_maker(dag_id="test_set_bucket_name", start_date=DEFAULT_DATE) as dag:
            if BUCKET_NAME not in test_bucket_name:

                @dag.task(task_id="taskflow_op")
                def f():
                    return BUCKET_NAME

                taskflow_op = f()

            op = GoogleBidManagerDownloadReportOperator(
                query_id=QUERY_ID,
                report_id=REPORT_ID,
                bucket_name=test_bucket_name if test_bucket_name != "XComArg" else taskflow_op,
                report_name=REPORT_NAME,
                task_id="test_task",
            )

            if test_bucket_name == "{{ ti.xcom_pull(task_ids='taskflow_op') }}":
                taskflow_op >> op

        if AIRFLOW_V_3_0_PLUS:
            dag.test()
        else:
            dr = dag_maker.create_dagrun()
            for ti in dr.get_task_instances():
                ti.run()

        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            filename=FILENAME,
            gzip=True,
            mime_type="text/csv",
            object_name=REPORT_NAME + ".gz",
        )


class TestGoogleBidManagerRunQueryOperator:
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerHook")
    def test_execute(self, hook_mock):
        parameters = {"param": "test"}

        # Create mock context with task_instance
        mock_context = {"task_instance": mock.Mock()}

        hook_mock.return_value.run_query.return_value = {
            "key": {
                "queryId": QUERY_ID,
                "reportId": REPORT_ID,
            }
        }
        op = GoogleBidManagerRunQueryOperator(
            query_id=QUERY_ID,
            parameters=parameters,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=mock_context)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )

        mock_context["task_instance"].xcom_push.assert_any_call(key="query_id", value=QUERY_ID)
        mock_context["task_instance"].xcom_push.assert_any_call(key="report_id", value=REPORT_ID)
        hook_mock.return_value.run_query.assert_called_once_with(query_id=QUERY_ID, params=parameters)


class TestGoogleBidManagerCreateQueryOperator:
    @mock.patch("airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerHook")
    def test_execute(self, hook_mock):
        body = {"body": "test"}

        # Create mock context with task_instance
        mock_context = {"task_instance": mock.Mock()}

        hook_mock.return_value.create_query.return_value = {"queryId": QUERY_ID}
        op = GoogleBidManagerCreateQueryOperator(body=body, task_id="test_task")
        op.execute(context=mock_context)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v2",
            impersonation_chain=None,
        )
        hook_mock.return_value.create_query.assert_called_once_with(query=body)
        mock_context["task_instance"].xcom_push.assert_called_once_with(key="query_id", value=QUERY_ID)

    def test_prepare_template(self):
        body = {"key": "value"}
        with NamedTemporaryFile("w+", suffix=".json") as f:
            f.write(json.dumps(body))
            f.flush()
            op = GoogleBidManagerCreateQueryOperator(body=body, task_id="test_task")
            op.prepare_template()

            assert isinstance(op.body, dict)
            assert op.body == body
