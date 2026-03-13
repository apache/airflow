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

from collections.abc import Sequence
from typing import (
    TYPE_CHECKING,
)

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform_v1beta1 import (
    FeatureOnlineStoreAdminServiceClient,
    FeatureOnlineStoreServiceClient,
)

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.cloud.aiplatform_v1beta1.types import (
        FeatureOnlineStore,
        FeatureView,
        FeatureViewDataKey,
        FetchFeatureValuesResponse,
    )


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

    @staticmethod
    def _get_client_options(
        location: str | None = None,
        custom_endpoint: str | None = None,
    ) -> ClientOptions:
        if custom_endpoint:
            client_options = ClientOptions(api_endpoint=custom_endpoint)
        elif location and location != "global":
            client_options = ClientOptions(api_endpoint=f"{location}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()
        return client_options

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
        return FeatureOnlineStoreAdminServiceClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=self._get_client_options(location),
        )

    def get_feature_online_store_service_client(
        self,
        location: str | None = None,
        custom_endpoint: str | None = None,
    ) -> FeatureOnlineStoreServiceClient:
        return FeatureOnlineStoreServiceClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=self._get_client_options(location, custom_endpoint),
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_feature_online_store(
        self,
        feature_online_store_id: str,
        feature_online_store: FeatureOnlineStore,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create and sends request for Feature Online store.

        This method initiates VertexAI Feature Online Store creation request.
        Feature Online Store aims to serve and manage features data as a part of VertexAI MLOps.

        :param feature_online_store_id: The ID of the online feature store.
        :param feature_online_store: The configuration of the online repository.
        :param project_id: The ID of the Google Cloud project that contains the
            feature store. If not provided, will attempt to determine from the environment.
        :param location: The Google Cloud region where the feature store is located
            (e.g., 'us-central1', 'us-east1').
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_feature_online_store_admin_service_client(location)
        return client.create_feature_online_store(
            request={
                "parent": f"projects/{project_id}/locations/{location}",
                "feature_online_store_id": feature_online_store_id,
                "feature_online_store": feature_online_store,
            },
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_feature_online_store(
        self,
        feature_online_store_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> FeatureOnlineStore:
        """
        Get Feature Online store data.

        Get the FeatureOnlineStore details.
        Vertex AI Feature Online Store provides a centralized repository for serving ML features
        and embedding indexes at low latency.

        :param feature_online_store_id: The ID of the online feature store.
        :param project_id: The ID of the Google Cloud project that contains the
            feature store. If not provided, will attempt to determine from the environment.
        :param location: The Google Cloud region where the feature store is located
            (e.g., 'us-central1', 'us-east1').
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_feature_online_store_admin_service_client(location)
        return client.get_feature_online_store(
            name=f"projects/{project_id}/locations/{location}/featureOnlineStores/{feature_online_store_id}",
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )

    @staticmethod
    def _get_featurestore_public_endpoint(feature_online_store: FeatureOnlineStore):
        public_endpoint = None
        featurestore_data = type(feature_online_store).to_dict(feature_online_store)
        if "dedicated_serving_endpoint" in featurestore_data:
            public_endpoint = featurestore_data["dedicated_serving_endpoint"].get(
                "public_endpoint_domain_name"
            )
        return public_endpoint

    @GoogleBaseHook.fallback_to_default_project_id
    def create_feature_view(
        self,
        feature_view_id: str,
        feature_view: FeatureView,
        feature_online_store_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        run_sync_immediately: bool = False,
        location: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create request for Feature View creation.

        This method initiates VertexAI Feature View request for the existing Feature Online Store.
        Feature View represents features and data according to the source provided.

        :param feature_view_id: The ID to use for the FeatureView, which will
            become the final component of the FeatureView's resource name.
            This value may be up to 60 characters, and valid characters are ``[a-z0-9_]``.
            The first character cannot be a number. The value must be unique within a FeatureOnlineStore.
        :param feature_view: The configuration of the FeatureView to create.
        :param feature_online_store_id: The ID of the online feature store.
        :param run_sync_immediately: If set to true, one on demand sync will be run
            immediately, regardless the FeatureView.sync_config.
        :param project_id: The ID of the Google Cloud project that contains the
            feature store. If not provided, will attempt to determine from the environment.
        :param location: The Google Cloud region where the feature store is located
            (e.g., 'us-central1', 'us-east1').
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_feature_online_store_admin_service_client(location)
        return client.create_feature_view(
            request={
                "parent": f"projects/{project_id}/locations/"
                f"{location}/featureOnlineStores/{feature_online_store_id}",
                "feature_view_id": feature_view_id,
                "feature_view": feature_view,
                "run_sync_immediately": run_sync_immediately,
            },
            timeout=timeout,
            retry=retry,
            metadata=metadata,
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
        feature_view = (
            f"projects/{project_id}/locations/{location}/featureOnlineStores/"
            f"{feature_online_store_id}/featureViews/{feature_view_id}"
        )

        try:
            response = client.sync_feature_view(feature_view=feature_view)
            return str(response.feature_view_sync)

        except Exception as e:
            self.log.error("Failed to sync feature view: %s", str(e))
            raise AirflowException(str(e))

    @GoogleBaseHook.fallback_to_default_project_id
    def fetch_feature_values(
        self,
        feature_view_id: str,
        feature_online_store_id: str,
        entity_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        endpoint_domain_name: str | None = None,
        data_key: FeatureViewDataKey | None = None,
        data_format: int | None = None,
        location: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> FetchFeatureValuesResponse:
        """
        Fetch data from the Feature View provided.

        This method fetches data from existing Feature view, filtered by provided (or default) data_key.
        Helps to retrieve actual features data hosted in the VertexAI Feature Store.

        :param entity_id: Simple ID to identify Entity to fetch feature values for.
        :param endpoint_domain_name: Optional. Public domain name, hosting the content of Optimized
            Feature Online store. Should be omitted, if bigtable configuration provided for the FeatureStore,
            and default feature store endpoint will be used, based on location provided.
        :param feature_view_id: The FeatureView ID to fetch data from.
        :param feature_online_store_id: The ID of the online feature store.
        :param data_key: Optional. The request key to fetch feature values for.
        :param data_format: Optional. Response data format. If not set, FeatureViewDataFormat.KEY_VALUE
            will be used.
        :param project_id: The ID of the Google Cloud project that contains the
            feature store. If not provided, will attempt to determine from the
            environment.
        :param location: The Google Cloud region where the feature store is located
            (e.g., 'us-central1', 'us-east1').
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        data_client = self.get_feature_online_store_service_client(location, endpoint_domain_name)
        return data_client.fetch_feature_values(
            request={
                "id": entity_id,
                "feature_view": f"projects/{project_id}/locations/{location}/featureOnlineStores/"
                f"{feature_online_store_id}/featureViews/{feature_view_id}",
                "data_key": data_key,
                "data_format": data_format,
            },
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_feature_view(
        self,
        feature_view_id: str,
        feature_online_store_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete the Feature View.

        This method deletes the Feature View from the Feature Online Store.

        :param feature_view_id: The ID to use for the FeatureView, to be deleted.
        :param feature_online_store_id: The ID of the online feature store.
        :param project_id: The ID of the Google Cloud project that contains the
            feature store. If not provided, will attempt to determine from the
            environment.
        :param location: The Google Cloud region where the feature store is located
            (e.g., 'us-central1', 'us-east1').
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_feature_online_store_admin_service_client(location)
        return client.delete_feature_view(
            name=f"projects/{project_id}/locations/{location}/featureOnlineStores/{feature_online_store_id}"
            f"/featureViews/{feature_view_id}",
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_feature_online_store(
        self,
        feature_online_store_id: str,
        force: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete the FeatureOnlineStore.

        This method deletes the Feature Online Store and all features data.
        The FeatureOnlineStore must not contain any FeatureViews.

        :param feature_online_store_id: The ID of the online feature store.
        :param force:  If set to true, any FeatureViews and Features for this FeatureOnlineStore
            will also be deleted.
        :param project_id: The ID of the Google Cloud project that contains the
            feature store. If not provided, will attempt to determine from the
            environment.
        :param location: The Google Cloud region where the feature store is located
            (e.g., 'us-central1', 'us-east1').
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_feature_online_store_admin_service_client(location)
        return client.delete_feature_online_store(
            name=f"projects/{project_id}/locations/{location}/featureOnlineStores/{feature_online_store_id}",
            force=force,
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )
