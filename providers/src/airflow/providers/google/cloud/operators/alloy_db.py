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
"""This module contains Google Cloud Alloy DB operators."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import AlreadyExists, InvalidArgument
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud import alloydb_v1

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.alloy_db import AlloyDbHook
from airflow.providers.google.cloud.links.alloy_db import AlloyDBClusterLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    import proto
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.utils.context import Context


class AlloyDBBaseOperator(GoogleCloudBaseOperator):
    """
    Base class for all AlloyDB operators.

    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "gcp_conn_id",
    )

    def __init__(
        self,
        project_id: str,
        location: str,
        gcp_conn_id: str = "google_cloud_default",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

    @cached_property
    def hook(self) -> AlloyDbHook:
        return AlloyDbHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class AlloyDBWriteBaseOperator(AlloyDBBaseOperator):
    """
    Base class for writing AlloyDB operators.

    These operators perform create, update or delete operations. with the objects (not inside of database).

    :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
        so that if you must retry your request, the server ignores the request if it has already been
        completed. The server guarantees that for at least 60 minutes since the first request.
        For example, consider a situation where you make an initial request and the request times out.
        If you make the request again with the same request ID, the server can check if the original operation
        with the same request ID was received, and if so, ignores the second request.
        This prevents clients from accidentally creating duplicate commitments.
        The request ID must be a valid UUID with the exception that zero UUID is not supported
        (00000000-0000-0000-0000-000000000000).
    :param validate_request: Optional. If set, performs request validation, but does not actually
        execute the request.
    """

    template_fields: Sequence[str] = tuple(
        {"request_id", "validate_request"} | set(AlloyDBBaseOperator.template_fields)
    )

    def __init__(
        self,
        request_id: str | None = None,
        validate_request: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.request_id = request_id
        self.validate_request = validate_request

    def get_operation_result(self, operation: Operation) -> proto.Message | None:
        """
        Retrieve operation result as a proto.Message.

        If the `validate_request` parameter is set, then no operation is performed and thus nothing to wait.
        """
        if self.validate_request:
            self.log.info("The request validation has been passed successfully!")
        else:
            return self.hook.wait_for_operation(timeout=self.timeout, operation=operation)
        return None


class AlloyDBCreateClusterOperator(AlloyDBWriteBaseOperator):
    """
    Create an Alloy DB cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBCreateClusterOperator`

    :param cluster_id: Required. ID of the cluster to create.
    :param cluster_configuration: Required. Cluster to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Cluster
    :param is_secondary: Required. Specifies if the Cluster to be created is Primary or Secondary.
        Please note, if set True, then specify the `secondary_config` field in the cluster so the created
        secondary cluster was pointing to the primary cluster.
    :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
        so that if you must retry your request, the server ignores the request if it has already been
        completed. The server guarantees that for at least 60 minutes since the first request.
        For example, consider a situation where you make an initial request and the request times out.
        If you make the request again with the same request ID, the server can check if the original operation
        with the same request ID was received, and if so, ignores the second request.
        This prevents clients from accidentally creating duplicate commitments.
        The request ID must be a valid UUID with the exception that zero UUID is not supported
        (00000000-0000-0000-0000-000000000000).
    :param validate_request: Optional. If set, performs request validation, but does not actually
        execute the request.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "is_secondary"} | set(AlloyDBWriteBaseOperator.template_fields)
    )
    operator_extra_links = (AlloyDBClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        cluster_configuration: alloydb_v1.Cluster | dict,
        is_secondary: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.cluster_configuration = cluster_configuration
        self.is_secondary = is_secondary

    def execute(self, context: Context) -> Any:
        message = (
            "Validating a Create AlloyDB cluster request."
            if self.validate_request
            else "Creating an AlloyDB cluster."
        )
        self.log.info(message)

        try:
            create_method = (
                self.hook.create_secondary_cluster if self.is_secondary else self.hook.create_cluster
            )
            operation = create_method(
                cluster_id=self.cluster_id,
                cluster=self.cluster_configuration,
                location=self.location,
                project_id=self.project_id,
                request_id=self.request_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info("AlloyDB cluster %s already exists.", self.cluster_id)
            result = self.hook.get_cluster(
                cluster_id=self.cluster_id,
                location=self.location,
                project_id=self.project_id,
            )
            result = alloydb_v1.Cluster.to_dict(result)
        except InvalidArgument as ex:
            if "cannot create more than one secondary cluster per primary cluster" in ex.message:
                result = self.hook.get_cluster(
                    cluster_id=self.cluster_id,
                    location=self.location,
                    project_id=self.project_id,
                )
                result = alloydb_v1.Cluster.to_dict(result)
                self.log.info("AlloyDB cluster %s already exists.", result.get("name").split("/")[-1])
            else:
                raise AirflowException(ex.message)
        except Exception as ex:
            raise AirflowException(ex)
        else:
            operation_result = self.get_operation_result(operation)
            result = alloydb_v1.Cluster.to_dict(operation_result) if operation_result else None

        if result:
            AlloyDBClusterLink.persist(
                context=context,
                task_instance=self,
                location_id=self.location,
                cluster_id=self.cluster_id,
                project_id=self.project_id,
            )

        return result


class AlloyDBUpdateClusterOperator(AlloyDBWriteBaseOperator):
    """
    Update an Alloy DB cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBUpdateClusterOperator`

    :param cluster_id: Required. ID of the cluster to create.
    :param cluster_configuration: Required. Cluster to update. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Cluster
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
    :param validate_request: Optional. If set, performs request validation, but does not actually
        execute the request.
    :param allow_missing: Optional. If set to true, update succeeds even if cluster is not found.
        In that case, a new cluster is created and update_mask is ignored.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "allow_missing"} | set(AlloyDBWriteBaseOperator.template_fields)
    )
    operator_extra_links = (AlloyDBClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        cluster_configuration: alloydb_v1.Cluster | dict,
        update_mask: FieldMask | dict | None = None,
        allow_missing: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.cluster_configuration = cluster_configuration
        self.update_mask = update_mask
        self.allow_missing = allow_missing

    def execute(self, context: Context) -> Any:
        message = (
            "Validating an Update AlloyDB cluster request."
            if self.validate_request
            else "Updating an AlloyDB cluster."
        )
        self.log.info(message)

        try:
            operation = self.hook.update_cluster(
                cluster_id=self.cluster_id,
                project_id=self.project_id,
                location=self.location,
                cluster=self.cluster_configuration,
                update_mask=self.update_mask,
                allow_missing=self.allow_missing,
                request_id=self.request_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except Exception as ex:
            raise AirflowException(ex) from ex
        else:
            operation_result = self.get_operation_result(operation)
            result = alloydb_v1.Cluster.to_dict(operation_result) if operation_result else None

        AlloyDBClusterLink.persist(
            context=context,
            task_instance=self,
            location_id=self.location,
            cluster_id=self.cluster_id,
            project_id=self.project_id,
        )

        if not self.validate_request:
            self.log.info("AlloyDB cluster %s was successfully updated.", self.cluster_id)
        return result


class AlloyDBDeleteClusterOperator(AlloyDBWriteBaseOperator):
    """
    Delete an Alloy DB cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBDeleteClusterOperator`

    :param cluster_id: Required. ID of the cluster to create.
    :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
        so that if you must retry your request, the server ignores the request if it has already been
        completed. The server guarantees that for at least 60 minutes since the first request.
        For example, consider a situation where you make an initial request and the request times out.
        If you make the request again with the same request ID, the server can check if the original operation
        with the same request ID was received, and if so, ignores the second request.
        This prevents clients from accidentally creating duplicate commitments.
        The request ID must be a valid UUID with the exception that zero UUID is not supported
        (00000000-0000-0000-0000-000000000000).
    :param validate_request: Optional. If set, performs request validation, but does not actually
        execute the request.
    :param etag: Optional. The current etag of the Cluster. If an etag is provided and does not match the
            current etag of the Cluster, deletion will be blocked and an ABORTED error will be returned.
    :param force: Optional. Whether to cascade delete child instances for given cluster.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "etag", "force"} | set(AlloyDBWriteBaseOperator.template_fields)
    )

    def __init__(
        self,
        cluster_id: str,
        etag: str | None = None,
        force: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.etag = etag
        self.force = force

    def execute(self, context: Context) -> Any:
        message = (
            "Validating a Delete AlloyDB cluster request."
            if self.validate_request
            else "Deleting an AlloyDB cluster."
        )
        self.log.info(message)

        try:
            operation = self.hook.delete_cluster(
                cluster_id=self.cluster_id,
                project_id=self.project_id,
                location=self.location,
                etag=self.etag,
                force=self.force,
                request_id=self.request_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except Exception as ex:
            raise AirflowException(ex) from ex
        else:
            self.get_operation_result(operation)

        if not self.validate_request:
            self.log.info("AlloyDB cluster %s was successfully removed.", self.cluster_id)
