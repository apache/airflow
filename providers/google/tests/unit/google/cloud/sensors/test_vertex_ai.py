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
from unittest.mock import Mock

import pytest

ray = pytest.importorskip("ray")

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.sensors.vertex_ai.feature_store import FeatureViewSyncSensor

TASK_ID = "test-task"
GCP_CONN_ID = "test-conn"
GCP_LOCATION = "us-central1"
FEATURE_VIEW_SYNC_NAME = "projects/123/locations/us-central1/featureViews/test-view/operations/sync-123"
TIMEOUT = 120


class TestFeatureViewSyncSensor:
    def create_sync_response(self, end_time=None, row_synced=None, total_slot=None):
        response = {}
        if end_time is not None:
            response["end_time"] = end_time
        if row_synced is not None and total_slot is not None:
            response["sync_summary"] = {"row_synced": str(row_synced), "total_slot": str(total_slot)}
        return response

    @mock.patch("airflow.providers.google.cloud.sensors.vertex_ai.feature_store.FeatureStoreHook")
    def test_sync_completed(self, mock_hook):
        mock_hook.return_value.get_feature_view_sync.return_value = self.create_sync_response(
            end_time=1234567890, row_synced=1000, total_slot=5
        )

        sensor = FeatureViewSyncSensor(
            task_id=TASK_ID,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
            location=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        ret = sensor.poke(context={})

        mock_hook.return_value.get_feature_view_sync.assert_called_once_with(
            location=GCP_LOCATION,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
        )
        assert ret

    @mock.patch("airflow.providers.google.cloud.sensors.vertex_ai.feature_store.FeatureStoreHook")
    def test_sync_running(self, mock_hook):
        mock_hook.return_value.get_feature_view_sync.return_value = self.create_sync_response(
            end_time=0, row_synced=0, total_slot=5
        )

        sensor = FeatureViewSyncSensor(
            task_id=TASK_ID,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
            location=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        ret = sensor.poke(context={})

        mock_hook.return_value.get_feature_view_sync.assert_called_once_with(
            location=GCP_LOCATION,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
        )
        assert not ret

    @mock.patch("airflow.providers.google.cloud.sensors.vertex_ai.feature_store.FeatureStoreHook")
    def test_sync_error_with_retry(self, mock_hook):
        mock_hook.return_value.get_feature_view_sync.side_effect = Exception("API Error")

        sensor = FeatureViewSyncSensor(
            task_id=TASK_ID,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
            location=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        ret = sensor.poke(context={})

        mock_hook.return_value.get_feature_view_sync.assert_called_once_with(
            location=GCP_LOCATION,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
        )
        assert not ret

    @mock.patch("airflow.providers.google.cloud.sensors.vertex_ai.feature_store.FeatureStoreHook")
    def test_timeout_during_running(self, mock_hook):
        mock_hook.return_value.get_feature_view_sync.return_value = self.create_sync_response(
            end_time=0, row_synced=0, total_slot=5
        )

        sensor = FeatureViewSyncSensor(
            task_id=TASK_ID,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
            location=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
            wait_timeout=300,
        )

        sensor._duration = Mock()
        sensor._duration.return_value = 301

        with pytest.raises(
            AirflowException,
            match=f"Timeout: Feature View sync {FEATURE_VIEW_SYNC_NAME} not completed after 300s",
        ):
            sensor.poke(context={})

    @mock.patch("airflow.providers.google.cloud.sensors.vertex_ai.feature_store.FeatureStoreHook")
    def test_timeout_during_error(self, mock_hook):
        mock_hook.return_value.get_feature_view_sync.side_effect = Exception("API Error")

        sensor = FeatureViewSyncSensor(
            task_id=TASK_ID,
            feature_view_sync_name=FEATURE_VIEW_SYNC_NAME,
            location=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
            wait_timeout=300,
        )

        sensor._duration = Mock()
        sensor._duration.return_value = 301

        with pytest.raises(
            AirflowException,
            match=f"Timeout: Feature View sync {FEATURE_VIEW_SYNC_NAME} not completed after 300s",
        ):
            sensor.poke(context={})
