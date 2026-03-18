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
"""Module contains a Google Alloy DB Hook."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from typing import TYPE_CHECKING

import tenacity
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud import alloydb_v1

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    import proto
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.protobuf.field_mask_pb2 import FieldMask


class AlloyDbHook(GoogleBaseHook):
    """Google Alloy DB Hook."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: alloydb_v1.AlloyDBAdminClient | None = None

    def get_alloy_db_admin_client(self) -> alloydb_v1.AlloyDBAdminClient:
        """Retrieve AlloyDB client."""
        if not self._client:
            self._client = alloydb_v1.AlloyDBAdminClient(
                credentials=self.get_credentials(),
                client_info=CLIENT_INFO,
            )
        return self._client

    def wait_for_operation(self, timeout: float | None, operation: Operation) -> proto.Message:
        """Wait for long-lasting operation to complete."""
        self.log.info("Waiting for operation to complete...")
        _timeout: int | None = int(timeout) if timeout else None
        try:
            return operation.result(timeout=_timeout)
        except Exception:
            error = operation.exception(timeout=_timeout)
            raise AirflowException(error)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_cluster(
        self,
        cluster_id: str,
        cluster: alloydb_v1.Cluster | dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create an Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.CreateClusterRequest

        :param cluster_id: Required. ID of the cluster to create.
        :param cluster: Required. Cluster to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Cluster
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.create_cluster(
            request={
                "parent": client.common_location_path(project_id, location),
                "cluster_id": cluster_id,
                "cluster": cluster,
                "request_id": request_id,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_secondary_cluster(
        self,
        cluster_id: str,
        cluster: alloydb_v1.Cluster | dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create a secondary Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.CreateClusterRequest

        :param cluster_id: Required. ID of the cluster to create.
        :param cluster: Required. Cluster to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Cluster
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.create_secondary_cluster(
            request={
                "parent": client.common_location_path(project_id, location),
                "cluster_id": cluster_id,
                "cluster": cluster,
                "request_id": request_id,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, max=10),
        retry=tenacity.retry_if_exception_type(ValueError),
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def get_cluster(
        self,
        cluster_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> alloydb_v1.Cluster:
        """
        Retrieve an Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.GetClusterRequest

        :param cluster_id: Required. ID of the cluster.
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.get_cluster(
            request={"name": client.cluster_path(project_id, location, cluster_id)},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def update_cluster(
        self,
        cluster_id: str,
        cluster: alloydb_v1.Cluster | dict,
        location: str,
        update_mask: FieldMask | dict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        allow_missing: bool = False,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Update an Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.UpdateClusterRequest

        :param cluster_id: Required. ID of the cluster to update.
        :param cluster: Required. Cluster to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Cluster
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param update_mask: Optional. Field mask is used to specify the fields to be overwritten in the
            Cluster resource by the update.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param allow_missing: Optional. If set to true, update succeeds even if cluster is not found.
            In that case, a new cluster is created and update_mask is ignored.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        _cluster = deepcopy(cluster) if isinstance(cluster, dict) else alloydb_v1.Cluster.to_dict(cluster)
        _cluster["name"] = client.cluster_path(project_id, location, cluster_id)
        return client.update_cluster(
            request={
                "update_mask": update_mask,
                "cluster": _cluster,
                "request_id": request_id,
                "validate_only": validate_only,
                "allow_missing": allow_missing,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_cluster(
        self,
        cluster_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        etag: str | None = None,
        validate_only: bool = False,
        force: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete an Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.DeleteClusterRequest

        :param cluster_id: Required. ID of the cluster to delete.
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param etag: Optional. The current etag of the Cluster. If an etag is provided and does not match the
            current etag of the Cluster, deletion will be blocked and an ABORTED error will be returned.
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param force: Optional. Whether to cascade delete child instances for given cluster.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.delete_cluster(
            request={
                "name": client.cluster_path(project_id, location, cluster_id),
                "request_id": request_id,
                "etag": etag,
                "validate_only": validate_only,
                "force": force,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_instance(
        self,
        cluster_id: str,
        instance_id: str,
        instance: alloydb_v1.Instance | dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create an instance in a given Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.CreateInstanceRequest

        :param cluster_id: Required. ID of the cluster for creating an instance in.
        :param instance_id: Required. ID of the instance to create.
        :param instance: Required. Instance to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Instance
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.create_instance(
            request={
                "parent": client.cluster_path(project_id, location, cluster_id),
                "instance_id": instance_id,
                "instance": instance,
                "request_id": request_id,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_secondary_instance(
        self,
        cluster_id: str,
        instance_id: str,
        instance: alloydb_v1.Instance | dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create a secondary instance in a given Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.CreateSecondaryInstanceRequest

        :param cluster_id: Required. ID of the cluster for creating an instance in.
        :param instance_id: Required. ID of the instance to create.
        :param instance: Required. Instance to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Instance
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.create_secondary_instance(
            request={
                "parent": client.cluster_path(project_id, location, cluster_id),
                "instance_id": instance_id,
                "instance": instance,
                "request_id": request_id,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, max=10),
        retry=tenacity.retry_if_exception_type(ValueError),
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance(
        self,
        cluster_id: str,
        instance_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> alloydb_v1.Instance:
        """
        Retrieve an Alloy DB instance.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.GetInstanceRequest

        :param cluster_id: Required. ID of the cluster.
        :param instance_id: Required. ID of the instance.
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.get_instance(
            request={"name": client.instance_path(project_id, location, cluster_id, instance_id)},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def update_instance(
        self,
        cluster_id: str,
        instance_id: str,
        instance: alloydb_v1.Instance | dict,
        location: str,
        update_mask: FieldMask | dict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        allow_missing: bool = False,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Update an Alloy DB instance.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.UpdateInstanceRequest

        :param cluster_id: Required. ID of the cluster.
        :param instance_id: Required. ID of the cluster to update.
        :param instance: Required. Cluster to update. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Instance
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param update_mask: Optional. Field mask is used to specify the fields to be overwritten in the
            Instance resource by the update.
        :param request_id: Optional. The ID of an existing request object.:param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param allow_missing: Optional. If set to true, update succeeds even if cluster is not found.
            In that case, a new cluster is created and update_mask is ignored.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        _instance = (
            deepcopy(instance) if isinstance(instance, dict) else alloydb_v1.Instance.to_dict(instance)
        )
        _instance["name"] = client.instance_path(project_id, location, cluster_id, instance_id)
        return client.update_instance(
            request={
                "update_mask": update_mask,
                "instance": _instance,
                "request_id": request_id,
                "validate_only": validate_only,
                "allow_missing": allow_missing,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance(
        self,
        instance_id: str,
        cluster_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        etag: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Delete an Alloy DB instance.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.DeleteInstanceRequest

        :param instance_id: Required. ID of the instance to delete.
        :param cluster_id: Required. ID of the cluster.
        :param location: Required. The ID of the Google Cloud region where the instance is located.
        :param project_id: Optional. The ID of the Google Cloud project where the instance is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param etag: Optional. The current etag of the Instance. If an etag is provided and does not match the
            current etag of the Instance, deletion will be blocked and an ABORTED error will be returned.
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the delete request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.delete_instance(
            request={
                "name": client.instance_path(project_id, location, cluster_id, instance_id),
                "request_id": request_id,
                "etag": etag,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_user(
        self,
        user_id: str,
        user: alloydb_v1.User | dict,
        cluster_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> alloydb_v1.User:
        """
        Create a user in a given Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.CreateUserRequest

        :param user_id: Required. ID of the user to create.
        :param user: Required. The user to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.User
        :param cluster_id: Required. ID of the cluster for creating a user in.
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.create_user(
            request={
                "parent": client.cluster_path(project_id, location, cluster_id),
                "user_id": user_id,
                "user": user,
                "request_id": request_id,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, max=10),
        retry=tenacity.retry_if_exception_type(ValueError),
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def get_user(
        self,
        user_id: str,
        cluster_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> alloydb_v1.User:
        """
        Get a user in a given Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.GetUserRequest

        :param user_id: Required. ID of the user to create.
        :param cluster_id: Required. ID of the cluster for creating a user in.
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.get_user(
            request={
                "name": client.user_path(project_id, location, cluster_id, user_id),
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def update_user(
        self,
        cluster_id: str,
        user_id: str,
        user: alloydb_v1.User | dict,
        location: str,
        update_mask: FieldMask | dict | None = None,
        allow_missing: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> alloydb_v1.User:
        """
        Update an Alloy DB user.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.UpdateUserRequest

        :param cluster_id: Required. ID of the cluster.
        :param user_id: Required. ID of the user to update.
        :param user: Required. User to update. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.User
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param update_mask: Optional. Field mask is used to specify the fields to be overwritten in the
            User resource by the update.
        :param request_id: Optional. The ID of an existing request object.:param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param allow_missing: Optional. If set to true, update succeeds even if cluster is not found.
            In that case, a new cluster is created and update_mask is ignored.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        _user = deepcopy(user) if isinstance(user, dict) else alloydb_v1.User.to_dict(user)
        _user["name"] = client.user_path(project_id, location, cluster_id, user_id)
        return client.update_user(
            request={
                "update_mask": update_mask,
                "user": _user,
                "request_id": request_id,
                "validate_only": validate_only,
                "allow_missing": allow_missing,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_user(
        self,
        user_id: str,
        cluster_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Delete an Alloy DB user.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.DeleteUserRequest

        :param user_id: Required. ID of the user to delete.
        :param cluster_id: Required. ID of the cluster.
        :param location: Required. The ID of the Google Cloud region where the instance is located.
        :param project_id: Optional. The ID of the Google Cloud project where the instance is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the delete request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.delete_user(
            request={
                "name": client.user_path(project_id, location, cluster_id, user_id),
                "request_id": request_id,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_backup(
        self,
        backup_id: str,
        backup: alloydb_v1.Backup | dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create a backup in a given Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.CreateBackupRequest

        :param backup_id: Required. ID of the backup to create.
        :param backup: Required. The backup to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Backup
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.create_backup(
            request={
                "parent": client.common_location_path(project_id, location),
                "backup_id": backup_id,
                "backup": backup,
                "request_id": request_id,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, max=10),
        retry=tenacity.retry_if_exception_type(ValueError),
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def get_backup(
        self,
        backup_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> alloydb_v1.Backup:
        """
        Get a backup in a given Alloy DB cluster.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.GetBackupRequest

        :param backup_id: Required. ID of the backup to create.
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.get_backup(
            request={
                "name": client.backup_path(project_id, location, backup_id),
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def update_backup(
        self,
        backup_id: str,
        backup: alloydb_v1.Backup | dict,
        location: str,
        update_mask: FieldMask | dict | None = None,
        allow_missing: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Update an Alloy DB backup.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.UpdateBackupRequest

        :param backup_id: Required. ID of the backup to update.
        :param backup: Required. Backup to update. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Backup
        :param location: Required. The ID of the Google Cloud region where the cluster is located.
        :param update_mask: Optional. Field mask is used to specify the fields to be overwritten in the
            Backup resource by the update.
        :param request_id: Optional. The ID of an existing request object.:param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the create request.
        :param allow_missing: Optional. If set to true, update succeeds even if cluster is not found.
            In that case, a new cluster is created and update_mask is ignored.
        :param project_id: Optional. The ID of the Google Cloud project where the cluster is located.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        _backup = deepcopy(backup) if isinstance(backup, dict) else alloydb_v1.Backup.to_dict(backup)
        _backup["name"] = client.backup_path(project_id, location, backup_id)
        return client.update_backup(
            request={
                "update_mask": update_mask,
                "backup": _backup,
                "request_id": request_id,
                "validate_only": validate_only,
                "allow_missing": allow_missing,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_backup(
        self,
        backup_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        validate_only: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Delete an Alloy DB backup.

        .. seealso::
            For more details see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.DeleteBackupRequest

        :param backup_id: Required. ID of the backup to delete.
        :param location: Required. The ID of the Google Cloud region where the instance is located.
        :param project_id: Optional. The ID of the Google Cloud project where the instance is located.
        :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
            so that if you must retry your request, the server ignores the request if it has already been
            completed. The server guarantees that for at least 60 minutes since the first request.
            For example, consider a situation where you make an initial request and the request times out.
            If you make the request again with the same request ID, the server can check if the original operation
            with the same request ID was received, and if so, ignores the second request.
            This prevents clients from accidentally creating duplicate commitments.
            The request ID must be a valid UUID with the exception that zero UUID is not supported
            (00000000-0000-0000-0000-000000000000).
        :param validate_only: Optional. If set, performs request validation, but does not actually execute
            the delete request.
        :param retry: Optional. Designation of what errors, if any, should be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_alloy_db_admin_client()
        return client.delete_backup(
            request={
                "name": client.backup_path(project_id, location, backup_id),
                "request_id": request_id,
                "validate_only": validate_only,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
