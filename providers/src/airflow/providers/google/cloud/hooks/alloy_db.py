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

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud import alloydb_v1

from airflow.exceptions import AirflowException
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
                credentials=self.get_credentials(), client_info=CLIENT_INFO
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
        :param request_id: Optional. The ID of an existing request object.
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
        :param request_id: Optional. The ID of an existing request object.
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

        :param cluster_id: Required. ID of the cluster to create.
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
        :param request_id: Optional. The ID of an existing request object.
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
        :param request_id: Optional. The ID of an existing request object.
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
