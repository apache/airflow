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
"""This module contains a Google Cloud Vertex AI Feature Store hook."""

from __future__ import annotations

from google.api_core.client_options import ClientOptions
from google.cloud.aiplatform_v1beta1 import (
    FeatureOnlineStoreAdminServiceClient,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook


class FeatureStoreHook(GoogleBaseHook):
    """
    Hook for interacting with Google Cloud Vertex AI Feature Store.

    This hook provides an interface to manage Feature Store resources in Vertex AI,
    including feature views and their synchronization operations. It handles authentication
    and provides methods for common Feature Store operations.

    :param gcp_conn_id: The connection ID to use for connecting to Google Cloud Platform.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials. Can be either a single account or a chain of accounts required to
        get the access_token of the last account in the list, which will be impersonated
        in the request. If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role. If set as a sequence, the identities
        from the list must grant Service Account Token Creator IAM role to the directly
        preceding identity, with first account from the list granting this role to the
        originating account.
    """

    def get_feature_online_store_admin_service_client(
        self,
        location: str | None = None,
    ) -> FeatureOnlineStoreAdminServiceClient:
        """
        Create and returns a FeatureOnlineStoreAdminServiceClient object.

        This method initializes a client for interacting with the Feature Store API,
        handling proper endpoint configuration based on the specified location.

        :param location: Optional. The Google Cloud region where the service is located.
            If provided and not 'global', the client will be configured to use the
            region-specific API endpoint.
        """
        if location and location != "global":
            client_options = ClientOptions(api_endpoint=f"{location}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()
        return FeatureOnlineStoreAdminServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_feature_view_sync(
        self,
        location: str,
        feature_view_sync_name: str,
    ) -> dict:
        """
        Retrieve the status and details of a Feature View synchronization operation.

        This method fetches information about a specific feature view sync operation,
        including its current status, timing information, and synchronization metrics.

        :param location: The Google Cloud region where the feature store is located
            (e.g., 'us-central1', 'us-east1').
        :param feature_view_sync_name: The full resource name of the feature view
            sync operation to retrieve.
        """
        client = self.get_feature_online_store_admin_service_client(location)

        try:
            response = client.get_feature_view_sync(name=feature_view_sync_name)

            report = {
                "name": feature_view_sync_name,
                "start_time": int(response.run_time.start_time.seconds),
            }

            if hasattr(response.run_time, "end_time") and response.run_time.end_time.seconds:
                report["end_time"] = int(response.run_time.end_time.seconds)
                report["sync_summary"] = {
                    "row_synced": int(response.sync_summary.row_synced),
                    "total_slot": int(response.sync_summary.total_slot),
                }

            return report

        except Exception as e:
            self.log.error("Failed to get feature view sync: %s", str(e))
            raise AirflowException(str(e))

    @GoogleBaseHook.fallback_to_default_project_id
    def sync_feature_view(
        self,
        location: str,
        feature_online_store_id: str,
        feature_view_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Initiate a synchronization operation for a Feature View.

        This method triggers a sync operation that updates the online serving data
        for a feature view based on the latest data in the underlying batch source.
        The sync operation ensures that the online feature values are up-to-date
        for real-time serving.

        :param location: The Google Cloud region where the feature store is located
            (e.g., 'us-central1', 'us-east1').
        :param feature_online_store_id: The ID of the online feature store that
            contains the feature view to be synchronized.
        :param feature_view_id: The ID of the feature view to synchronize.
        :param project_id: The ID of the Google Cloud project that contains the
            feature store. If not provided, will attempt to determine from the
            environment.
        """
        client = self.get_feature_online_store_admin_service_client(location)
        feature_view = f"projects/{project_id}/locations/{location}/featureOnlineStores/{feature_online_store_id}/featureViews/{feature_view_id}"

        try:
            response = client.sync_feature_view(feature_view=feature_view)

            return str(response.feature_view_sync)

        except Exception as e:
            self.log.error("Failed to sync feature view: %s", str(e))
            raise AirflowException(str(e))
