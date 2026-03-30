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

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform_v1beta1.types import FeatureViewDataFormat

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.feature_store import FeatureStoreHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.operation_helpers import OperationHelper

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.aiplatform_v1beta1.types import (
        FeatureOnlineStore,
        FeatureView,
        FeatureViewDataKey,
    )

    from airflow.providers.common.compat.sdk import Context


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


class CreateFeatureOnlineStoreOperator(GoogleCloudBaseOperator, OperationHelper):
    """
    Create the Feature Online store.

    This method initiates VertexAI Feature Online Store creation request.
    Feature Online Store aims to serve and manage features data as a part of VertexAI MLOps.

    :param project_id: Required. The ID of the Google Cloud project that contains the feature store.
        This is used to identify which project's resources to interact with.
    :param location: Required. The location of the feature store (e.g., 'us-central1', 'us-east1').
        This specifies the Google Cloud region where the feature store resources are located.
    :param feature_online_store_id: Required. The ID of the online feature store that contains
        the feature view to be synchronized. This store serves as the online serving layer.
    :param feature_online_store: FeatureOnlineStore configuration object of the feature online store
        to be created.
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
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        feature_online_store_id: str,
        feature_online_store: FeatureOnlineStore,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.feature_online_store_id = feature_online_store_id
        self.feature_online_store = feature_online_store
        self.timeout = timeout
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the get feature view sync operation."""
        hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating the Feature Online Store id: %s", self.feature_online_store_id)
        result_operation = hook.create_feature_online_store(
            project_id=self.project_id,
            location=self.location,
            feature_online_store_id=self.feature_online_store_id,
            feature_online_store=self.feature_online_store,
            timeout=self.timeout,
            retry=self.retry,
            metadata=self.metadata,
        )
        op_result = self.wait_for_operation_result(operation=result_operation)
        self.log.info("The Feature Online Store has been created: %s", self.feature_online_store_id)
        result = type(op_result).to_dict(op_result)
        return result


class GetFeatureOnlineStoreOperator(GoogleCloudBaseOperator, OperationHelper):
    """
    Get Feature Online store instance.

    This method initiates VertexAI Feature Online Store creation request.
    Feature Online Store aims to serve and manage features data as a part of VertexAI MLOps.

    :param project_id: Required. The ID of the Google Cloud project that contains the feature store.
        This is used to identify which project's resources to interact with.
    :param location: Required. The location of the feature store (e.g., 'us-central1', 'us-east1').
        This specifies the Google Cloud region where the feature store resources are located.
    :param feature_online_store_id: Required. The ID of the online feature store that contains
        the feature view to be synchronized. This store serves as the online serving layer.
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
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        feature_online_store_id: str,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.feature_online_store_id = feature_online_store_id
        self.timeout = timeout
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the get feature view sync operation."""
        hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Get the Feature Online Store id: %s...", self.feature_online_store_id)
        try:
            result = hook.get_feature_online_store(
                project_id=self.project_id,
                location=self.location,
                feature_online_store_id=self.feature_online_store_id,
                timeout=self.timeout,
                retry=self.retry,
                metadata=self.metadata,
            )
        except GoogleAPICallError as ex:
            exc_msg = f"Google API error getting {self.feature_online_store_id} Feature Online Store instance"
            raise AirflowException(exc_msg) from ex

        result = type(result).to_dict(result)
        self.log.info("The Feature Online Store has been retrieved: %s", self.feature_online_store_id)
        return result


class CreateFeatureViewOperator(GoogleCloudBaseOperator, OperationHelper):
    """
    Create request for Feature View creation.

    This method initiates VertexAI Feature View request for the existing Feature Online Store.
    Feature View represents features and data according to the source provided.

    :param feature_view_id: The ID to use for the FeatureView, which will become the final component
        of the FeatureView's resource name. This value may be up to 60 characters, and valid characters
        are ``[a-z0-9_]``. The first character cannot be a number.
        The value must be unique within a FeatureOnlineStore.
    :param feature_view: The configuration of the FeatureView to create.
    :param feature_online_store_id: The ID of the online feature store.
    :param run_sync_immediately: If set to true, one on demand sync will be run
        immediately, regardless the FeatureView.sync_config.
    :param project_id: Required. The ID of the Google Cloud project that contains the feature store.
        This is used to identify which project's resources to interact with.
    :param location: Required. The location of the feature store (e.g., 'us-central1', 'us-east1').
        This specifies the Google Cloud region where the feature store resources are located.
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
    )

    def __init__(
        self,
        *,
        feature_view_id: str,
        feature_view: FeatureView,
        feature_online_store_id: str,
        run_sync_immediately: bool = False,
        project_id: str,
        location: str,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.feature_view_id = feature_view_id
        self.feature_view = feature_view
        self.run_sync_immediately = run_sync_immediately
        self.feature_online_store_id = feature_online_store_id
        self.timeout = timeout
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the get feature view sync operation."""
        hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating the Online Store Feature View...")
        result_operation = hook.create_feature_view(
            project_id=self.project_id,
            location=self.location,
            feature_view_id=self.feature_view_id,
            feature_view=self.feature_view,
            feature_online_store_id=self.feature_online_store_id,
            run_sync_immediately=self.run_sync_immediately,
            timeout=self.timeout,
            retry=self.retry,
            metadata=self.metadata,
        )
        op_result = self.wait_for_operation_result(operation=result_operation)
        self.log.info("The Online Store Feature View has been created: %s", self.feature_online_store_id)
        result = type(op_result).to_dict(op_result)
        return result


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


class FetchFeatureValuesOperator(GoogleCloudBaseOperator, OperationHelper):
    """
    Fetch features data from the Feature View provided.

    This method fetches data from existing Feature view, filtered by provided (or default) data_key.
    Helps to retrieve actual features data hosted in the VertexAI Feature Store.

    :param entity_id: Simple ID to identify Entity to fetch feature values for.
    :param feature_view_id: The FeatureView ID to fetch data from.
    :param feature_online_store_id: The ID of the online feature store.
    :param data_key: The request key to fetch feature values for.
    :param project_id: Required. The ID of the Google Cloud project that contains the feature store.
        This is used to identify which project's resources to interact with.
    :param location: Required. The location of the feature store (e.g., 'us-central1', 'us-east1').
        This specifies the Google Cloud region where the feature store resources are located.
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
        "entity_id",
    )

    def __init__(
        self,
        *,
        feature_view_id: str,
        feature_online_store_id: str,
        project_id: str,
        location: str,
        entity_id: str | None = None,
        data_key: FeatureViewDataKey | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.entity_id = entity_id
        self.feature_view_id = feature_view_id
        self.feature_online_store_id = feature_online_store_id
        self.data_key = data_key
        self.timeout = timeout
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the get feature view sync operation."""
        hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            feature_online_store = hook.get_feature_online_store(
                feature_online_store_id=self.feature_online_store_id,
                project_id=self.project_id,
                location=self.location,
            )
            public_domain_name = hook._get_featurestore_public_endpoint(feature_online_store)
        except GoogleAPICallError as ex:
            exc_msg = f"Google API error getting {self.feature_online_store_id} Feature Online Store instance"
            raise AirflowException(exc_msg) from ex

        self.log.info(
            "Fetching data from the Feature View %s, Online Feature Store %s.",
            self.feature_view_id,
            self.feature_online_store_id,
        )
        request_result = hook.fetch_feature_values(
            project_id=self.project_id,
            location=self.location,
            endpoint_domain_name=public_domain_name,
            entity_id=self.entity_id,
            feature_view_id=self.feature_view_id,
            feature_online_store_id=self.feature_online_store_id,
            data_key=self.data_key,
            data_format=FeatureViewDataFormat.KEY_VALUE,
            timeout=self.timeout,
            retry=self.retry,
            metadata=self.metadata,
        )
        self.log.info(
            "Fetching data from the Feature View %s, Online Feature Store %s. is finished.",
            self.feature_view_id,
            self.feature_online_store_id,
        )
        result = type(request_result).to_dict(request_result)
        return result


class DeleteFeatureOnlineStoreOperator(GoogleCloudBaseOperator, OperationHelper):
    """
    Delete the Feature Online store.

    This method initiates VertexAI Feature Online Store deletion request.
    There should be no FeatureViews to be deleted successfully.


    :param project_id: Required. The ID of the Google Cloud project that contains the feature store.
        This is used to identify which project's resources to interact with.
    :param location: Required. The location of the feature store (e.g., 'us-central1', 'us-east1').
        This specifies the Google Cloud region where the feature store resources are located.
    :param feature_online_store_id: Required. The ID of the online feature store that contains
        the feature view to be synchronized. This store serves as the online serving layer.
    :param force:  If set to true, any FeatureViews and Features for this FeatureOnlineStore
        will also be deleted.
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
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        feature_online_store_id: str,
        force: bool = False,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.feature_online_store_id = feature_online_store_id
        self.force = force
        self.timeout = timeout
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the get feature view sync operation."""
        hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting the Feature Online Store...")

        result_operation = hook.delete_feature_online_store(
            project_id=self.project_id,
            location=self.location,
            feature_online_store_id=self.feature_online_store_id,
            force=self.force,
            timeout=self.timeout,
            retry=self.retry,
            metadata=self.metadata,
        )
        self.wait_for_operation_result(operation=result_operation)
        self.log.info("The Feature Online Store deletion has been complete: %s", self.feature_online_store_id)

        return {"result": f"The {self.feature_online_store_id} has been deleted."}


class DeleteFeatureViewOperator(GoogleCloudBaseOperator, OperationHelper):
    """
    Delete the Feature View.

    This method deletes the Feature View from the Feature Online Store.

    :param project_id: Required. The ID of the Google Cloud project that contains the feature store.
        This is used to identify which project's resources to interact with.
    :param location: Required. The location of the feature store (e.g., 'us-central1', 'us-east1').
        This specifies the Google Cloud region where the feature store resources are located.
    :param feature_online_store_id: Required. The ID of the online feature store that contains
        the feature view to be synchronized. This store serves as the online serving layer.
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
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        feature_online_store_id: str,
        feature_view_id: str,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.feature_online_store_id = feature_online_store_id
        self.feature_view_id = feature_view_id
        self.timeout = timeout
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the get feature view sync operation."""
        hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting the Feature View %s ... ", self.feature_view_id)
        result_operation = hook.delete_feature_view(
            project_id=self.project_id,
            location=self.location,
            feature_online_store_id=self.feature_online_store_id,
            feature_view_id=self.feature_view_id,
            timeout=self.timeout,
            retry=self.retry,
            metadata=self.metadata,
        )
        self.wait_for_operation_result(operation=result_operation)
        self.log.info("The Feature View deletion has been complete: %s", self.feature_view_id)

        return {"result": f"The {self.feature_view_id} has been deleted."}
