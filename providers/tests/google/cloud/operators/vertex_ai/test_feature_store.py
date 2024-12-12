from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.google.cloud.operators.vertex_ai.feature_store import (
    SyncFeatureViewOperator,
    GetFeatureViewSyncOperator,
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
    def test_execute(self, mock_hook):
        mock_hook.return_value.sync_feature_view.return_value.feature_view_sync = FEATURE_VIEW_SYNC_NAME

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

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.sync_feature_view.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            feature_online_store_id=FEATURE_ONLINE_STORE_ID,
            feature_view_id=FEATURE_VIEW_ID,
        )
        assert response == FEATURE_VIEW_SYNC_NAME


class TestGetFeatureViewSyncOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook):
        mock_response = mock.MagicMock()
        mock_response.run_time.start_time.seconds = 1000
        mock_response.run_time.end_time.seconds = 2000
        mock_response.sync_summary.row_synced = 500
        mock_response.sync_summary.total_slot = 4

        mock_hook.return_value.get_feature_view_sync.return_value = mock_response

        expected_response = {
            "name": FEATURE_VIEW_SYNC_NAME,
            "start_time": 1000,
            "end_time": 2000,
            "sync_summary": {
                "row_synced": 500,
                "total_slot": 4
            }
        }

        op = GetFeatureViewSyncOperator(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        response = op.execute(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_feature_view_sync.assert_called_once_with(
            location=GCP_LOCATION,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
        )
        assert response == expected_response
