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

from airflow.providers.google.cloud.operators.vertex_ai.feature_store import (
    GetFeatureViewSyncOperator,
    SyncFeatureViewOperator,
)

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
FEATURE_ONLINE_STORE_ID = "test-store"
FEATURE_VIEW_ID = "test-view"
FEATURE_VIEW_SYNC_NAME = f"projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/featureOnlineStores/{FEATURE_ONLINE_STORE_ID}/featureViews/{FEATURE_VIEW_ID}/featureViewSyncs/sync123"


class TestSyncFeatureViewOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook_class):
        # Create the mock hook and set up its return value
        mock_hook = mock.MagicMock()
        mock_hook_class.return_value = mock_hook

        # Set up the return value for sync_feature_view to match the hook implementation
        mock_hook.sync_feature_view.return_value = FEATURE_VIEW_SYNC_NAME

        op = SyncFeatureViewOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            feature_online_store_id=FEATURE_ONLINE_STORE_ID,
            feature_view_id=FEATURE_VIEW_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        response = op.execute(context={"ti": mock.MagicMock()})

        # Verify hook initialization
        mock_hook_class.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        # Verify hook method call
        mock_hook.sync_feature_view.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            feature_online_store_id=FEATURE_ONLINE_STORE_ID,
            feature_view_id=FEATURE_VIEW_ID,
        )

        # Verify response matches expected value
        assert response == FEATURE_VIEW_SYNC_NAME


class TestGetFeatureViewSyncOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook_class):
        # Create the mock hook and set up expected response
        mock_hook = mock.MagicMock()
        mock_hook_class.return_value = mock_hook

        expected_response = {
            "name": FEATURE_VIEW_SYNC_NAME,
            "start_time": 1000,
            "end_time": 2000,
            "sync_summary": {"row_synced": 500, "total_slot": 4},
        }

        # Set up the return value for get_feature_view_sync to match the hook implementation
        mock_hook.get_feature_view_sync.return_value = expected_response

        op = GetFeatureViewSyncOperator(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        response = op.execute(context={"ti": mock.MagicMock()})

        # Verify hook initialization
        mock_hook_class.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        # Verify hook method call
        mock_hook.get_feature_view_sync.assert_called_once_with(
            location=GCP_LOCATION,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
        )

        # Verify response matches expected structure
        assert response == expected_response
