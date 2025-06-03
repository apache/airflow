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

from google.cloud.aiplatform_v1beta1.types import (
    FeatureOnlineStore,
    FeatureView,
    FeatureViewDataFormat,
    FeatureViewDataKey,
    FetchFeatureValuesResponse,
)

from airflow.providers.google.cloud.operators.vertex_ai.feature_store import (
    CreateFeatureOnlineStoreOperator,
    CreateFeatureViewOperator,
    DeleteFeatureOnlineStoreOperator,
    DeleteFeatureViewOperator,
    FetchFeatureValuesOperator,
    GetFeatureOnlineStoreOperator,
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
FEATURE_ONLINE_STORE_NAME = (
    f"projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/featureOnlineStores/{FEATURE_ONLINE_STORE_ID}"
)
FEATURE_VIEW_SYNC_NAME = (
    f"projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/featureOnlineStores/"
    f"{FEATURE_ONLINE_STORE_ID}/featureViews/{FEATURE_VIEW_ID}/featureViewSyncs/sync123"
)
FEATURE_VIEW_NAME = (
    f"projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/featureOnlineStores/"
    f"{FEATURE_ONLINE_STORE_ID}/featureViews/{FEATURE_VIEW_ID}"
)


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


class TestCreateFeatureOnlineStoreOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook_class):
        FEATURE_ONLINE_STORE_CONF = FeatureOnlineStore({"name": FEATURE_ONLINE_STORE_NAME, "optimized": {}})
        sample_result = {
            "etag": "",
            "labels": {},
            "name": FEATURE_ONLINE_STORE_NAME,
            "optimized": {},
            "satisfies_pzi": False,
            "satisfies_pzs": False,
            "state": 0,
        }
        mock_hook = mock.MagicMock()
        mock_hook_class.return_value = mock_hook
        # Set up the return value for hook method to match the hook implementation
        sample_operation = mock.MagicMock()
        sample_operation.result.return_value = FEATURE_ONLINE_STORE_CONF
        mock_hook.create_feature_online_store.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()
        common_kwargs = {
            "project_id": GCP_PROJECT,
            "location": GCP_LOCATION,
            "feature_online_store_id": FEATURE_ONLINE_STORE_ID,
            "feature_online_store": FEATURE_ONLINE_STORE_CONF,
            "metadata": (),
            "timeout": 100,
            "retry": None,
        }
        op = CreateFeatureOnlineStoreOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            **common_kwargs,
        )
        result = op.execute(context={"ti": mock.MagicMock()})
        # Verify hook initialization
        mock_hook_class.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        # Verify hook method call
        mock_hook.create_feature_online_store.assert_called_once_with(**common_kwargs)
        # Verify result matches expected value
        assert result == sample_result


class TestCreateFeatureViewOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook_class):
        feature_view_conf_params = {
            "big_query_source": FeatureView.BigQuerySource(
                uri="bq://{BQ_TABLE}",
                entity_id_columns=["entity_id"],
            ),
            "sync_config": FeatureView.SyncConfig(cron="TZ=Europe/London 56 * * * *"),
        }
        FEATURE_VIEW_CONF = FeatureView(**feature_view_conf_params)

        sample_result = {
            "big_query_source": {"entity_id_columns": ["entity_id"], "uri": "bq://{BQ_TABLE}"},
            "etag": "",
            "labels": {},
            "name": "projects/test-project/locations/us-central1/featureOnlineStores/test-store"
            "/featureViews/test-view",
            "satisfies_pzi": False,
            "satisfies_pzs": False,
            "service_account_email": "",
            "service_agent_type": 0,
            "sync_config": {"cron": "TZ=Europe/London 56 * * * *"},
        }
        mock_hook = mock.MagicMock()
        mock_hook_class.return_value = mock_hook
        # Set up the return value for hook method to match the hook implementation
        sample_operation = mock.MagicMock()
        sample_operation.result.return_value = FeatureView(
            {**feature_view_conf_params, **{"name": FEATURE_VIEW_NAME}}
        )
        mock_hook.create_feature_view.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()
        common_kwargs = {
            "project_id": GCP_PROJECT,
            "location": GCP_LOCATION,
            "feature_online_store_id": FEATURE_ONLINE_STORE_ID,
            "feature_view_id": FEATURE_VIEW_ID,
            "feature_view": FEATURE_VIEW_CONF,
            "run_sync_immediately": False,
            "metadata": (),
            "timeout": 100,
            "retry": None,
        }
        op = CreateFeatureViewOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            **common_kwargs,
        )
        result = op.execute(context={"ti": mock.MagicMock()})
        # Verify hook initialization
        mock_hook_class.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        # Verify hook method call
        mock_hook.create_feature_view.assert_called_once_with(**common_kwargs)
        # Verify result matches expected value
        assert result == sample_result


class TestFetchFeatureValuesOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook_class):
        ENTITY_ID = "entity-id"
        FEATURE_VIEW_DATA_KEY = {"key": "28098"}
        sample_result = {
            "key_values": {
                "features": [
                    {"name": "brand", "value": {"string_value": "rip curl"}},
                    {"name": "cost", "value": {"double_value": 36.56684834767282}},
                    {"name": "feature_timestamp", "value": {"int64_value": "1750151356612667"}},
                ]
            }
        }
        mock_hook = mock.MagicMock()
        mock_hook_class.return_value = mock_hook
        # Set up the return value for hook method to match the hook implementation
        mock_hook.get_feature_online_store = mock.MagicMock()
        PUBLIC_DOMAIN_NAME = "public.domain.url"
        get_public_domain_mock = mock.MagicMock()
        get_public_domain_mock.return_value = PUBLIC_DOMAIN_NAME
        mock_hook._get_featurestore_public_endpoint = get_public_domain_mock
        mock_hook.fetch_feature_values.return_value = FetchFeatureValuesResponse(sample_result)
        common_kwargs = {
            "project_id": GCP_PROJECT,
            "location": GCP_LOCATION,
            "feature_online_store_id": FEATURE_ONLINE_STORE_ID,
            "feature_view_id": FEATURE_VIEW_ID,
            "entity_id": ENTITY_ID,
            "data_key": FeatureViewDataKey(FEATURE_VIEW_DATA_KEY),
            "metadata": (),
            "timeout": 100,
            "retry": None,
        }
        op = FetchFeatureValuesOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            **common_kwargs,
        )
        result = op.execute(context={"ti": mock.MagicMock()})
        # Verify hook initialization
        mock_hook_class.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        # Verify hook method call
        mock_hook.fetch_feature_values.assert_called_once_with(
            data_format=FeatureViewDataFormat.KEY_VALUE,
            endpoint_domain_name=PUBLIC_DOMAIN_NAME,
            **common_kwargs,
        )
        # Verify result matches expected value
        assert result == sample_result


class TestGetFeatureOnlineStoreOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook_class):
        # Create the mock hook and set up its return value
        mock_hook = mock.MagicMock()
        mock_hook_class.return_value = mock_hook
        # Set up the return value for get_feature_online_store to match the hook implementation
        SAMPLE_RESPONSE = {
            "etag": "",
            "labels": {},
            "name": FEATURE_ONLINE_STORE_ID,
            "satisfies_pzi": False,
            "satisfies_pzs": False,
            "state": 0,
        }
        mock_hook.get_feature_online_store.return_value = FeatureOnlineStore(SAMPLE_RESPONSE)
        common_kwargs = {
            "project_id": GCP_PROJECT,
            "location": GCP_LOCATION,
            "feature_online_store_id": FEATURE_ONLINE_STORE_ID,
            "metadata": (),
            "timeout": 100,
            "retry": None,
        }
        op = GetFeatureOnlineStoreOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            **common_kwargs,
        )
        response = op.execute(context={"ti": mock.MagicMock()})
        # Verify hook initialization
        mock_hook_class.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        # Verify hook method call
        mock_hook.get_feature_online_store.assert_called_once_with(**common_kwargs)
        # Verify response matches expected value
        assert response == SAMPLE_RESPONSE


class TestDeleteFeatureOnlineStoreOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook_class):
        # Create the mock hook and set up its return value
        mock_hook = mock.MagicMock()
        mock_hook_class.return_value = mock_hook
        sample_operation = mock.MagicMock()
        mock_hook.delete_feature_online_store.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()
        common_kwargs = {
            "project_id": GCP_PROJECT,
            "location": GCP_LOCATION,
            "feature_online_store_id": FEATURE_ONLINE_STORE_ID,
            "force": False,
            "metadata": (),
            "timeout": 100,
            "retry": None,
        }
        op = DeleteFeatureOnlineStoreOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            **common_kwargs,
        )
        response = op.execute(context={"ti": mock.MagicMock()})
        # Verify hook initialization
        mock_hook_class.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        # Verify hook method call
        mock_hook.delete_feature_online_store.assert_called_once_with(**common_kwargs)
        # Verify response matches expected value
        assert response == {"result": f"The {FEATURE_ONLINE_STORE_ID} has been deleted."}


class TestDeleteFeatureViewOperator:
    @mock.patch(VERTEX_AI_PATH.format("feature_store.FeatureStoreHook"))
    def test_execute(self, mock_hook_class):
        # Create the mock hook and set up its return value
        mock_hook = mock.MagicMock()
        mock_hook_class.return_value = mock_hook
        sample_operation = mock.MagicMock()
        mock_hook.delete_feature_view.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()
        common_kwargs = {
            "project_id": GCP_PROJECT,
            "location": GCP_LOCATION,
            "feature_online_store_id": FEATURE_ONLINE_STORE_ID,
            "feature_view_id": FEATURE_VIEW_ID,
            "metadata": (),
            "timeout": 100,
            "retry": None,
        }
        op = DeleteFeatureViewOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            **common_kwargs,
        )
        response = op.execute(context={"ti": mock.MagicMock()})
        # Verify hook initialization
        mock_hook_class.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        # Verify hook method call
        mock_hook.delete_feature_view.assert_called_once_with(**common_kwargs)
        # Verify response matches expected value
        assert response == {"result": f"The {FEATURE_VIEW_ID} has been deleted."}
