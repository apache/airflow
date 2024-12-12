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

from typing import Dict
from collections.abc import Sequence

import vertexai
from vertexai.resources.preview import FeatureView
from google.api_core import operation
from google.api_core.client_options import ClientOptions
from google.cloud.aiplatform_v1beta1 import (
    FeatureOnlineStoreAdminServiceClient,
    types,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook
from airflow.providers.google.common.consts import CLIENT_INFO

class FeatureStoreHook(GoogleBaseHook):
    """
    Hook for the Vertex AI Feature Store.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def get_feature_online_store_admin_service_client(
        self,
        location: str | None = None,
    ) -> FeatureOnlineStoreAdminServiceClient:
        """Return FeatureOnlineStoreAdminServiceClient object."""
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
    ) -> Dict:
        """
        Get a Feature View sync and returns sync operation details.
        """

        client = self.get_feature_online_store_admin_service_client(location)

        try:
            response = client.get_feature_view_sync(
                name=feature_view_sync_name
            )

            return response
            
        except Exception as e:
            self.log.error('Failed to get feature view sync: %s', str(e))
            raise AirflowException

    @GoogleBaseHook.fallback_to_default_project_id
    def sync_feature_view(
        self,
        location: str,
        feature_online_store_id: str,
        feature_view_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Dict:
        """
        Syncs a Feature View and returns sync operation details.
        """

        client = self.get_feature_online_store_admin_service_client(location)
        feature_view = f"projects/{project_id}/locations/{location}/featureOnlineStores/{feature_online_store_id}/featureViews/{feature_view_id}"

        try:
            response = client.sync_feature_view(
                    feature_view=feature_view
            )

            return response
            
        except Exception as e:
            self.log.error('Failed to sync feature view: %s', str(e))
            raise AirflowException

    @GoogleBaseHook.fallback_to_default_project_id
    def list_feature_view_syncs(
        self,
        location: str,
        feature_online_store_id: str,
        feature_view_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Dict:
        """
        Syncs a Feature View and returns sync operation details.
        """

        client = self.get_feature_online_store_admin_service_client(location)
        feature_view = f"projects/{project_id}/locations/{location}/featureOnlineStores/{feature_online_store_id}/featureViews/{feature_view_id}"

        try:
            response = client.list_feature_views(
                    parent=feature_view
            )

            return response
            
        except Exception as e:
            self.log.error('Failed to list feature view syncs: %s', str(e))
            raise AirflowException