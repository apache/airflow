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

from unittest import mock

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateAssetOperator,
    DataplexCreateDataQualityOperator,
    DataplexCreateLakeOperator,
    DataplexCreateTaskOperator,
    DataplexCreateZoneOperator,
    DataplexDeleteAssetOperator,
    DataplexDeleteDataQualityOperator,
    DataplexDeleteLakeOperator,
    DataplexDeleteTaskOperator,
    DataplexDeleteZoneOperator,
    DataplexGetDataQualityJobOperator,
    DataplexGetDataQualityOperator,
    DataplexGetTaskOperator,
    DataplexListTasksOperator,
    DataplexRunDataQualityOperator,
)

HOOK_STR = "airflow.providers.google.cloud.operators.dataplex.DataplexHook"
TASK_STR = "airflow.providers.google.cloud.operators.dataplex.Task"
LAKE_STR = "airflow.providers.google.cloud.operators.dataplex.Lake"

PROJECT_ID = "project-id"
REGION = "region"
LAKE_ID = "lake-id"
BODY = {"body": "test"}
BODY_LAKE = {
    "display_name": "test_display_name",
    "labels": [],
    "description": "test_description",
    "metastore": {"service": ""},
}
DATAPLEX_TASK_ID = "testTask001"

GCP_CONN_ID = "google_cloud_default"
API_VERSION = "v1"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
DATA_SCAN_ID = "test-data-scan-id"
TASK_ID = "test_task_id"
ASSET_ID = "test_asset_id"
ZONE_ID = "test_zone_id"


class TestDataplexCreateTaskOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(TASK_STR)
    def test_execute(self, task_mock, hook_mock):
        op = DataplexCreateTaskOperator(
            task_id="create_dataplex_task",
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY,
            dataplex_task_id=DATAPLEX_TASK_ID,
            validate_only=None,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.wait_for_operation.return_value = None
        task_mock.return_value.to_dict.return_value = None
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.create_task.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY,
            dataplex_task_id=DATAPLEX_TASK_ID,
            validate_only=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexDeleteTaskOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexDeleteTaskOperator(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            dataplex_task_id=DATAPLEX_TASK_ID,
            task_id="delete_dataplex_task",
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.delete_task.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            dataplex_task_id=DATAPLEX_TASK_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexListTasksOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexListTasksOperator(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            task_id="list_dataplex_task",
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.list_tasks.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            page_size=None,
            page_token=None,
            filter=None,
            order_by=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexGetTaskOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(TASK_STR)
    def test_execute(self, task_mock, hook_mock):
        op = DataplexGetTaskOperator(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            dataplex_task_id=DATAPLEX_TASK_ID,
            task_id="get_dataplex_task",
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.wait_for_operation.return_value = None
        task_mock.return_value.to_dict.return_value = None
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.get_task.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            dataplex_task_id=DATAPLEX_TASK_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexDeleteLakeOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexDeleteLakeOperator(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            task_id="delete_dataplex_lake",
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.delete_lake.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexCreateLakeOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(LAKE_STR)
    def test_execute(self, lake_mock, hook_mock):
        op = DataplexCreateLakeOperator(
            task_id="create_dataplex_lake",
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY_LAKE,
            validate_only=None,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.wait_for_operation.return_value = None
        lake_mock.return_value.to_dict.return_value = None
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.create_lake.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY_LAKE,
            validate_only=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexRunDataQualityOperator:
    @mock.patch(HOOK_STR)
    @mock.patch("airflow.providers.google.cloud.operators.dataplex.DataScanJob")
    def test_execute(self, mock_data_scan_job, hook_mock):
        op = DataplexRunDataQualityOperator(
            task_id="run_data_scan",
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            asynchronous=False,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.run_data_scan.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        hook_mock.return_value.wait_for_job.assert_called_once()

    @mock.patch(HOOK_STR)
    @mock.patch("airflow.providers.google.cloud.operators.dataplex.DataScanJob")
    def test_execute_async(self, mock_data_scan_job, hook_mock):
        op = DataplexRunDataQualityOperator(
            task_id="run_data_scan",
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            asynchronous=True,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.run_data_scan.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        hook_mock.return_value.wait_for_job.assert_not_called()


class TestDataplexGetDataQualityJobOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexGetDataQualityJobOperator(
            task_id="run_data_scan",
            project_id=PROJECT_ID,
            region=REGION,
            name="test/test/test",
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.get_data_scan_job.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            name="test/test/test",
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexCreateAssetOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexCreateAssetOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            asset_id=ASSET_ID,
            asset={},
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.create_asset.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            asset_id=ASSET_ID,
            asset={},
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexCreateZoneOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexCreateZoneOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            zone={},
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.create_zone.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            zone={},
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexDeleteZoneOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexDeleteZoneOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.delete_zone.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexDeleteAssetOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexDeleteAssetOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            asset_id=ASSET_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.delete_asset.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            asset_id=ASSET_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexDeleteDataQualityOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexDeleteDataQualityOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.delete_data_scan.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexGetDataQualityOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexGetDataQualityOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.get_data_scan.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexCreateDataQualityOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexCreateDataQualityOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            data_scan={},
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.create_data_scan.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            data_scan={},
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
