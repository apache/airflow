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

import pytest
from google.api_core.gapic_v1.method import DEFAULT

from airflow.exceptions import TaskDeferred
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateAssetOperator,
    DataplexCreateLakeOperator,
    DataplexCreateOrUpdateDataProfileScanOperator,
    DataplexCreateOrUpdateDataQualityScanOperator,
    DataplexCreateTaskOperator,
    DataplexCreateZoneOperator,
    DataplexDeleteAssetOperator,
    DataplexDeleteDataProfileScanOperator,
    DataplexDeleteDataQualityScanOperator,
    DataplexDeleteLakeOperator,
    DataplexDeleteTaskOperator,
    DataplexDeleteZoneOperator,
    DataplexGetDataProfileScanResultOperator,
    DataplexGetDataQualityScanResultOperator,
    DataplexGetTaskOperator,
    DataplexListTasksOperator,
    DataplexRunDataProfileScanOperator,
    DataplexRunDataQualityScanOperator,
)
from airflow.providers.google.cloud.triggers.dataplex import DataplexDataQualityJobTrigger
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

HOOK_STR = "airflow.providers.google.cloud.operators.dataplex.DataplexHook"
TASK_STR = "airflow.providers.google.cloud.operators.dataplex.Task"
LAKE_STR = "airflow.providers.google.cloud.operators.dataplex.Lake"
DATASCANJOB_STR = "airflow.providers.google.cloud.operators.dataplex.DataScanJob"
ZONE_STR = "airflow.providers.google.cloud.operators.dataplex.Zone"
ASSET_STR = "airflow.providers.google.cloud.operators.dataplex.Asset"

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
JOB_ID = "test_job_id"


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


class TestDataplexRunDataQualityScanOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(DATASCANJOB_STR)
    def test_execute(self, mock_data_scan_job, hook_mock):
        op = DataplexRunDataQualityScanOperator(
            task_id="execute_data_scan",
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
        hook_mock.return_value.run_data_scan.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(HOOK_STR)
    @mock.patch(DATASCANJOB_STR)
    def test_execute_deferrable(self, mock_data_scan_job, hook_mock):
        op = DataplexRunDataQualityScanOperator(
            task_id="execute_data_scan",
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            op.execute(mock.MagicMock())

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
        hook_mock.return_value.wait_for_data_scan_job.assert_not_called()

        assert isinstance(exc.value.trigger, DataplexDataQualityJobTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME


class TestDataplexRunDataProfileScanOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(DATASCANJOB_STR)
    def test_execute(self, mock_data_scan_job, hook_mock):
        op = DataplexRunDataProfileScanOperator(
            task_id="execute_data_scan",
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
        hook_mock.return_value.run_data_scan.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexGetDataQualityScanResultOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(DATASCANJOB_STR)
    def test_execute(self, mock_data_scan_job, hook_mock):
        op = DataplexGetDataQualityScanResultOperator(
            task_id="get_data_scan_result",
            project_id=PROJECT_ID,
            region=REGION,
            job_id=JOB_ID,
            data_scan_id=DATA_SCAN_ID,
            api_version=API_VERSION,
            wait_for_results=False,
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
            job_id=JOB_ID,
            data_scan_id=DATA_SCAN_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(HOOK_STR)
    @mock.patch(DATASCANJOB_STR)
    def test_execute_deferrable(self, mock_data_scan_job, hook_mock):
        op = DataplexGetDataQualityScanResultOperator(
            task_id="get_data_scan_result",
            project_id=PROJECT_ID,
            region=REGION,
            job_id=JOB_ID,
            data_scan_id=DATA_SCAN_ID,
            api_version=API_VERSION,
            wait_for_results=True,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            op.execute(mock.MagicMock())

        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        hook_mock.return_value.wait_for_data_scan_job.assert_not_called()

        assert isinstance(exc.value.trigger, DataplexDataQualityJobTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME


class TestDataplexGetDataProfileScanResultOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(DATASCANJOB_STR)
    def test_execute(self, mock_data_scan_job, hook_mock):
        op = DataplexGetDataProfileScanResultOperator(
            task_id="get_data_scan_result",
            project_id=PROJECT_ID,
            region=REGION,
            job_id=JOB_ID,
            data_scan_id=DATA_SCAN_ID,
            api_version=API_VERSION,
            wait_for_results=False,
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
            job_id=JOB_ID,
            data_scan_id=DATA_SCAN_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexCreateAssetOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(ASSET_STR)
    def test_execute(self, asset_mock, hook_mock):
        op = DataplexCreateAssetOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            asset_id=ASSET_ID,
            body={},
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.wait_for_operation.return_value = None
        asset_mock.return_value.to_dict.return_value = None
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
            body={},
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexCreateZoneOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(ZONE_STR)
    def test_execute(self, zone_mock, hook_mock):
        op = DataplexCreateZoneOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            body={},
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.wait_for_operation.return_value = None
        zone_mock.return_value.to_dict.return_value = None
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
            body={},
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


class TestDataplexDeleteDataQualityScanOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexDeleteDataQualityScanOperator(
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


class TestDataplexDeleteDataProfileScanOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexDeleteDataProfileScanOperator(
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


class TestDataplexCreateDataQualityScanOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexCreateOrUpdateDataQualityScanOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            body={},
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
            body={},
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataplexCreateDataProfileScanOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataplexCreateOrUpdateDataProfileScanOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_SCAN_ID,
            body={},
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
            body={},
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
