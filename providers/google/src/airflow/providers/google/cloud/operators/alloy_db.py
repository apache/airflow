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

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud import alloydb_v1

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.alloy_db import AlloyDbHook
from airflow.providers.google.cloud.links.alloy_db import (
    AlloyDBBackupsLink,
    AlloyDBClusterLink,
    AlloyDBUsersLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    import proto
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.providers.common.compat.sdk import Context


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
            # Validation requests are only validated and aren't executed, thus no operation result is expected
            return None
        return self.hook.wait_for_operation(timeout=self.timeout, operation=operation)


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
        {"cluster_id", "cluster_configuration", "is_secondary"}
        | set(AlloyDBWriteBaseOperator.template_fields)
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

    def _get_cluster(self) -> proto.Message | None:
        self.log.info("Checking if the cluster %s exists already...", self.cluster_id)
        try:
            cluster = self.hook.get_cluster(
                cluster_id=self.cluster_id,
                location=self.location,
                project_id=self.project_id,
            )
        except NotFound:
            self.log.info("The cluster %s does not exist yet.", self.cluster_id)
        except Exception as ex:
            raise AirflowException(ex) from ex
        else:
            self.log.info("AlloyDB cluster %s already exists.", self.cluster_id)
            result = alloydb_v1.Cluster.to_dict(cluster)
            return result
        return None

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location_id": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict | None:
        AlloyDBClusterLink.persist(context=context)
        if cluster := self._get_cluster():
            return cluster

        if self.validate_request:
            self.log.info("Validating a Create AlloyDB cluster request.")
        else:
            self.log.info("Creating an AlloyDB cluster.")

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
        except Exception as ex:
            raise AirflowException(ex)
        else:
            operation_result = self.get_operation_result(operation)
            result = alloydb_v1.Cluster.to_dict(operation_result) if operation_result else None

        return result


class AlloyDBUpdateClusterOperator(AlloyDBWriteBaseOperator):
    """
    Update an Alloy DB cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBUpdateClusterOperator`

    :param cluster_id: Required. ID of the cluster to update.
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
        {"cluster_id", "cluster_configuration", "allow_missing"}
        | set(AlloyDBWriteBaseOperator.template_fields)
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

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location_id": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict | None:
        AlloyDBClusterLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating an Update AlloyDB cluster request.")
        else:
            self.log.info("Updating an AlloyDB cluster.")

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

        if not self.validate_request:
            self.log.info("AlloyDB cluster %s was successfully updated.", self.cluster_id)
        return result


class AlloyDBDeleteClusterOperator(AlloyDBWriteBaseOperator):
    """
    Delete an Alloy DB cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBDeleteClusterOperator`

    :param cluster_id: Required. ID of the cluster to delete.
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

    def execute(self, context: Context) -> None:
        if self.validate_request:
            self.log.info("Validating a Delete AlloyDB cluster request.")
        else:
            self.log.info("Deleting an AlloyDB cluster.")

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


class AlloyDBCreateInstanceOperator(AlloyDBWriteBaseOperator):
    """
    Create an Instance in an Alloy DB cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBCreateInstanceOperator`

    :param cluster_id: Required. ID of the cluster for creating an instance in.
    :param instance_id: Required. ID of the instance to create.
    :param instance_configuration: Required. Instance to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Instance
    :param is_secondary: Required. Specifies if the Instance to be created is Primary or Secondary.
        Please note, if set True, then specify the `instance_type` field in the instance.
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
        {"cluster_id", "instance_id", "is_secondary", "instance_configuration"}
        | set(AlloyDBWriteBaseOperator.template_fields)
    )
    operator_extra_links = (AlloyDBClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        instance_id: str,
        instance_configuration: alloydb_v1.Instance | dict,
        is_secondary: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.instance_id = instance_id
        self.instance_configuration = instance_configuration
        self.is_secondary = is_secondary

    def _get_instance(self) -> proto.Message | None:
        self.log.info("Checking if the instance %s exists already...", self.instance_id)
        try:
            instance = self.hook.get_instance(
                cluster_id=self.cluster_id,
                instance_id=self.instance_id,
                location=self.location,
                project_id=self.project_id,
            )
        except NotFound:
            self.log.info("The instance %s does not exist yet.", self.instance_id)
        except Exception as ex:
            raise AirflowException(ex) from ex
        else:
            self.log.info(
                "AlloyDB instance %s already exists in the cluster %s.",
                self.cluster_id,
                self.instance_id,
            )
            result = alloydb_v1.Instance.to_dict(instance)
            return result
        return None

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location_id": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict | None:
        AlloyDBClusterLink.persist(context=context)
        if instance := self._get_instance():
            return instance

        if self.validate_request:
            self.log.info("Validating a Create AlloyDB instance request.")
        else:
            self.log.info("Creating an AlloyDB instance.")

        try:
            create_method = (
                self.hook.create_secondary_instance if self.is_secondary else self.hook.create_instance
            )
            operation = create_method(
                cluster_id=self.cluster_id,
                instance_id=self.instance_id,
                instance=self.instance_configuration,
                location=self.location,
                project_id=self.project_id,
                request_id=self.request_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except Exception as ex:
            raise AirflowException(ex)
        else:
            operation_result = self.get_operation_result(operation)
            result = alloydb_v1.Instance.to_dict(operation_result) if operation_result else None

        return result


class AlloyDBUpdateInstanceOperator(AlloyDBWriteBaseOperator):
    """
    Update an Alloy DB instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBUpdateInstanceOperator`

    :param cluster_id: Required. ID of the cluster.
    :param instance_id: Required. ID of the instance to update.
    :param instance_configuration: Required. Instance to update. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Instance
    :param update_mask: Optional. Field mask is used to specify the fields to be overwritten in the
            Instance resource by the update.
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
    :param allow_missing: Optional. If set to true, update succeeds even if instance is not found.
        In that case, a new instance is created and update_mask is ignored.
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
        {"cluster_id", "instance_id", "instance_configuration", "update_mask", "allow_missing"}
        | set(AlloyDBWriteBaseOperator.template_fields)
    )
    operator_extra_links = (AlloyDBClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        instance_id: str,
        instance_configuration: alloydb_v1.Instance | dict,
        update_mask: FieldMask | dict | None = None,
        allow_missing: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.instance_id = instance_id
        self.instance_configuration = instance_configuration
        self.update_mask = update_mask
        self.allow_missing = allow_missing

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location_id": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict | None:
        AlloyDBClusterLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating an Update AlloyDB instance request.")
        else:
            self.log.info("Updating an AlloyDB instance.")

        try:
            operation = self.hook.update_instance(
                cluster_id=self.cluster_id,
                instance_id=self.instance_id,
                project_id=self.project_id,
                location=self.location,
                instance=self.instance_configuration,
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
            result = alloydb_v1.Instance.to_dict(operation_result) if operation_result else None

        if not self.validate_request:
            self.log.info("AlloyDB instance %s was successfully updated.", self.cluster_id)
        return result


class AlloyDBDeleteInstanceOperator(AlloyDBWriteBaseOperator):
    """
    Delete an Alloy DB instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBDeleteInstanceOperator`

    :param instance_id: Required. ID of the instance to delete.
    :param cluster_id: Required. ID of the cluster.
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
    :param etag: Optional. The current etag of the Instance. If an etag is provided and does not match the
            current etag of the Instance, deletion will be blocked and an ABORTED error will be returned.
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
        {"instance_id", "cluster_id", "etag"} | set(AlloyDBWriteBaseOperator.template_fields)
    )

    def __init__(
        self,
        instance_id: str,
        cluster_id: str,
        etag: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.instance_id = instance_id
        self.cluster_id = cluster_id
        self.etag = etag

    def execute(self, context: Context) -> None:
        if self.validate_request:
            self.log.info("Validating a Delete AlloyDB instance request.")
        else:
            self.log.info("Deleting an AlloyDB instance.")

        try:
            operation = self.hook.delete_instance(
                instance_id=self.instance_id,
                cluster_id=self.cluster_id,
                project_id=self.project_id,
                location=self.location,
                etag=self.etag,
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
            self.log.info("AlloyDB instance %s was successfully removed.", self.instance_id)


class AlloyDBCreateUserOperator(AlloyDBWriteBaseOperator):
    """
    Create a User in an Alloy DB cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBCreateUserOperator`

    :param user_id: Required. ID of the user to create.
    :param user_configuration: Required. The user to create. For more details please see API documentation:
        https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.User
    :param cluster_id: Required. ID of the cluster for creating a user in.
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
        {"user_id", "user_configuration", "cluster_id"} | set(AlloyDBWriteBaseOperator.template_fields)
    )
    operator_extra_links = (AlloyDBUsersLink(),)

    def __init__(
        self,
        user_id: str,
        user_configuration: alloydb_v1.User | dict,
        cluster_id: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.user_configuration = user_configuration
        self.cluster_id = cluster_id

    def _get_user(self) -> proto.Message | None:
        self.log.info("Checking if the user %s exists already...", self.user_id)
        try:
            user = self.hook.get_user(
                user_id=self.user_id,
                cluster_id=self.cluster_id,
                location=self.location,
                project_id=self.project_id,
            )
        except NotFound:
            self.log.info("The user %s does not exist yet.", self.user_id)
        except Exception as ex:
            raise AirflowException(ex) from ex
        else:
            self.log.info(
                "AlloyDB user %s already exists in the cluster %s.",
                self.user_id,
                self.cluster_id,
            )
            result = alloydb_v1.User.to_dict(user)
            return result
        return None

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location_id": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict | None:
        AlloyDBUsersLink.persist(context=context)
        if (_user := self._get_user()) is not None:
            return _user

        if self.validate_request:
            self.log.info("Validating a Create AlloyDB user request.")
        else:
            self.log.info("Creating an AlloyDB user.")

        try:
            user = self.hook.create_user(
                user_id=self.user_id,
                cluster_id=self.cluster_id,
                user=self.user_configuration,
                location=self.location,
                project_id=self.project_id,
                request_id=self.request_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except Exception as ex:
            raise AirflowException(ex)
        else:
            result = alloydb_v1.User.to_dict(user) if not self.validate_request else None

        if not self.validate_request:
            self.log.info("AlloyDB user %s was successfully created.", self.user_id)
        return result


class AlloyDBUpdateUserOperator(AlloyDBWriteBaseOperator):
    """
    Update an Alloy DB user.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBUpdateUserOperator`

    :param user_id: Required. The ID of the user to update.
    :param cluster_id: Required. ID of the cluster.
    :param user_configuration: Required. User to update. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.User
    :param update_mask: Optional. Field mask is used to specify the fields to be overwritten in the
            User resource by the update.
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
    :param allow_missing: Optional. If set to true, update succeeds even if instance is not found.
        In that case, a new user is created and update_mask is ignored.
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
        {"cluster_id", "user_id", "user_configuration", "update_mask", "allow_missing"}
        | set(AlloyDBWriteBaseOperator.template_fields)
    )
    operator_extra_links = (AlloyDBUsersLink(),)

    def __init__(
        self,
        cluster_id: str,
        user_id: str,
        user_configuration: alloydb_v1.User | dict,
        update_mask: FieldMask | dict | None = None,
        allow_missing: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.user_id = user_id
        self.user_configuration = user_configuration
        self.update_mask = update_mask
        self.allow_missing = allow_missing

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location_id": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict | None:
        AlloyDBUsersLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating an Update AlloyDB user request.")
        else:
            self.log.info("Updating an AlloyDB user.")

        try:
            user = self.hook.update_user(
                cluster_id=self.cluster_id,
                user_id=self.user_id,
                project_id=self.project_id,
                location=self.location,
                user=self.user_configuration,
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
            result = alloydb_v1.User.to_dict(user) if not self.validate_request else None

        if not self.validate_request:
            self.log.info("AlloyDB user %s was successfully updated.", self.user_id)
        return result


class AlloyDBDeleteUserOperator(AlloyDBWriteBaseOperator):
    """
    Delete an Alloy DB user.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBDeleteUserOperator`

    :param user_id: Required. ID of the user to delete.
    :param cluster_id: Required. ID of the cluster.
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
        {"user_id", "cluster_id"} | set(AlloyDBWriteBaseOperator.template_fields)
    )

    def __init__(
        self,
        user_id: str,
        cluster_id: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.cluster_id = cluster_id

    def execute(self, context: Context) -> None:
        if self.validate_request:
            self.log.info("Validating a Delete AlloyDB user request.")
        else:
            self.log.info("Deleting an AlloyDB user.")

        try:
            self.hook.delete_user(
                user_id=self.user_id,
                cluster_id=self.cluster_id,
                project_id=self.project_id,
                location=self.location,
                request_id=self.request_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except Exception as ex:
            raise AirflowException(ex) from ex

        if not self.validate_request:
            self.log.info("AlloyDB user %s was successfully removed.", self.user_id)


class AlloyDBCreateBackupOperator(AlloyDBWriteBaseOperator):
    """
    Create a Backup in an Alloy DB cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBCreateBackupOperator`

    :param backup_id: Required. ID of the backup to create.
    :param backup_configuration: Required. Backup to create. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Backup
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
    :param location: Required. The ID of the Google Cloud region where the backups should be saved.
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
        {"backup_id", "backup_configuration"} | set(AlloyDBWriteBaseOperator.template_fields)
    )
    operator_extra_links = (AlloyDBBackupsLink(),)

    def __init__(
        self,
        backup_id: str,
        backup_configuration: alloydb_v1.Backup | dict,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.backup_id = backup_id
        self.backup_configuration = backup_configuration

    def _get_backup(self) -> proto.Message | None:
        self.log.info("Checking if the backup %s exists already...", self.backup_id)
        try:
            backup = self.hook.get_backup(
                backup_id=self.backup_id,
                location=self.location,
                project_id=self.project_id,
            )
        except NotFound:
            self.log.info("The backup %s does not exist yet.", self.backup_id)
        except Exception as ex:
            raise AirflowException(ex) from ex
        else:
            self.log.info("AlloyDB backup %s already exists.", self.backup_id)
            result = alloydb_v1.Backup.to_dict(backup)
            return result
        return None

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict | None:
        AlloyDBBackupsLink.persist(context=context)
        if backup := self._get_backup():
            return backup

        if self.validate_request:
            self.log.info("Validating a Create AlloyDB backup request.")
        else:
            self.log.info("Creating an AlloyDB backup.")

        try:
            operation = self.hook.create_backup(
                backup_id=self.backup_id,
                backup=self.backup_configuration,
                location=self.location,
                project_id=self.project_id,
                request_id=self.request_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except Exception as ex:
            raise AirflowException(ex)
        else:
            operation_result = self.get_operation_result(operation)
            result = alloydb_v1.Backup.to_dict(operation_result) if operation_result else None

        return result


class AlloyDBUpdateBackupOperator(AlloyDBWriteBaseOperator):
    """
    Update an Alloy DB backup.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBUpdateBackupOperator`

    :param backup_id: Required. ID of the backup to update.
    :param backup_configuration: Required. Backup to update. For more details please see API documentation:
            https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.types.Backup
    :param update_mask: Optional. Field mask is used to specify the fields to be overwritten in the
            Backup resource by the update.
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
    :param allow_missing: Optional. If set to true, update succeeds even if backup is not found.
        In that case, a new backup is created and update_mask is ignored.
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
        {"backup_id", "backup_configuration", "update_mask", "allow_missing"}
        | set(AlloyDBWriteBaseOperator.template_fields)
    )
    operator_extra_links = (AlloyDBBackupsLink(),)

    def __init__(
        self,
        backup_id: str,
        backup_configuration: alloydb_v1.Backup | dict,
        update_mask: FieldMask | dict | None = None,
        allow_missing: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.backup_id = backup_id
        self.backup_configuration = backup_configuration
        self.update_mask = update_mask
        self.allow_missing = allow_missing

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict | None:
        AlloyDBBackupsLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating an Update AlloyDB backup request.")
        else:
            self.log.info("Updating an AlloyDB backup.")

        try:
            operation = self.hook.update_backup(
                backup_id=self.backup_id,
                project_id=self.project_id,
                location=self.location,
                backup=self.backup_configuration,
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
            result = alloydb_v1.Backup.to_dict(operation_result) if operation_result else None

        if not self.validate_request:
            self.log.info("AlloyDB backup %s was successfully updated.", self.backup_id)
        return result


class AlloyDBDeleteBackupOperator(AlloyDBWriteBaseOperator):
    """
    Delete an Alloy DB backup.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AlloyDBDeleteBackupOperator`

    :param backup_id: Required. ID of the backup to delete.
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

    template_fields: Sequence[str] = tuple({"backup_id"} | set(AlloyDBWriteBaseOperator.template_fields))

    def __init__(
        self,
        backup_id: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.backup_id = backup_id

    def execute(self, context: Context) -> None:
        if self.validate_request:
            self.log.info("Validating a Delete AlloyDB backup request.")
        else:
            self.log.info("Deleting an AlloyDB backup.")

        try:
            operation = self.hook.delete_backup(
                backup_id=self.backup_id,
                project_id=self.project_id,
                location=self.location,
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
            self.log.info("AlloyDB backup %s was successfully removed.", self.backup_id)
