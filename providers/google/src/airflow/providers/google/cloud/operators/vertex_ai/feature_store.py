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
"""This module contains Google Vertex AI Feature Store operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.google.cloud.hooks.vertex_ai.feature_store import FeatureStoreHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SyncFeatureViewOperator(GoogleCloudBaseOperator):
    """
    Initiate a synchronization operation for a Feature View in Vertex AI Feature Store.

    This operator triggers a sync operation that updates the online serving data for a feature view
    based on the latest data in the underlying batch source. The sync operation ensures that
    the online feature values are up-to-date for real-time serving.

    :param project_id: Required. The ID of the Google Cloud project that contains the feature store.
        This is used to identify which project's resources to interact with.
    :param location: Required. The location of the feature store (e.g., 'us-central1', 'us-east1').
        This specifies the Google Cloud region where the feature store resources are located.
    :param feature_online_store_id: Required. The ID of the online feature store that contains
        the feature view to be synchronized. This store serves as the online serving layer.
    :param feature_view_id: Required. The ID of the feature view to synchronize. This identifies
        the specific view that needs to have its online values updated from the batch source.
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

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "feature_online_store_id",
        "feature_view_id",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        feature_online_store_id: str,
        feature_view_id: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.feature_online_store_id = feature_online_store_id
        self.feature_view_id = feature_view_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        """Execute the feature view sync operation."""
        self.hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Submitting Feature View sync job now...")
        response = self.hook.sync_feature_view(
            project_id=self.project_id,
            location=self.location,
            feature_online_store_id=self.feature_online_store_id,
            feature_view_id=self.feature_view_id,
        )
        self.log.info("Retrieved Feature View sync: %s", response)

        return response


class GetFeatureViewSyncOperator(GoogleCloudBaseOperator):
    """
    Retrieve the status and details of a Feature View synchronization operation.

    This operator fetches information about a specific feature view sync operation,
    including its current status, timing information, and synchronization metrics.
    It's typically used to monitor the progress of a sync operation initiated by
    the SyncFeatureViewOperator.

    :param location: Required. The location of the feature store (e.g., 'us-central1', 'us-east1').
        This specifies the Google Cloud region where the feature store resources are located.
    :param feature_view_sync_name: Required. The full resource name of the feature view
        sync operation to retrieve. This is typically the return value from a
        SyncFeatureViewOperator execution.
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

    template_fields: Sequence[str] = (
        "location",
        "feature_view_sync_name",
    )

    def __init__(
        self,
        *,
        location: str,
        feature_view_sync_name: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.feature_view_sync_name = feature_view_sync_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the get feature view sync operation."""
        self.hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Retrieving Feature View sync job now...")
        response = self.hook.get_feature_view_sync(
            location=self.location, feature_view_sync_name=self.feature_view_sync_name
        )
        self.log.info("Retrieved Feature View sync: %s", self.feature_view_sync_name)
        self.log.info(response)

        return response
