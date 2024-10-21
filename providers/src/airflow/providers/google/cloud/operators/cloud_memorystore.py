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
"""
Operators for Google Cloud Memorystore service.

.. spelling:word-list::

    FieldMask
    memcache
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.memcache_v1beta2.types import cloud_memcache
from google.cloud.redis_v1 import FailoverInstanceRequest, InputConfig, Instance, OutputConfig

from airflow.providers.google.cloud.hooks.cloud_memorystore import (
    CloudMemorystoreHook,
    CloudMemorystoreMemcachedHook,
)
from airflow.providers.google.cloud.links.cloud_memorystore import (
    MemcachedInstanceDetailsLink,
    MemcachedInstanceListLink,
    RedisInstanceDetailsLink,
    RedisInstanceListLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.utils.context import Context


class CloudMemorystoreCreateInstanceOperator(GoogleCloudBaseOperator):
    """
    Creates a Redis instance based on the specified tier and memory size.

    By default, the instance is accessible from the project's `default network
    <https://cloud.google.com/compute/docs/networks-and-firewalls#networks>`__.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreCreateInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance_id: Required. The logical name of the Redis instance in the customer project with the
        following restrictions:

        -  Must contain only lowercase letters, numbers, and hyphens.
        -  Must start with a letter.
        -  Must be between 1-40 characters.
        -  Must end with a number or a letter.
        -  Must be unique within the customer project / location
    :param instance: Required. A Redis [Instance] resource

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.redis_v1.types.Instance`
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "instance_id",
        "instance",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        location: str,
        instance_id: str,
        instance: dict | Instance,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance_id = instance_id
        self.instance = instance
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.create_instance(
            location=self.location,
            instance_id=self.instance_id,
            instance=self.instance,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        RedisInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance_id,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return Instance.to_dict(result)


class CloudMemorystoreDeleteInstanceOperator(GoogleCloudBaseOperator):
    """
    Deletes a specific Redis instance. Instance stops serving and data is deleted.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreDeleteInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance: The logical name of the Redis instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "instance",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        instance: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance = instance
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        hook.delete_instance(
            location=self.location,
            instance=self.instance,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudMemorystoreExportInstanceOperator(GoogleCloudBaseOperator):
    """
    Export Redis instance data into a Redis RDB format file in Cloud Storage.

    Redis will continue serving during this operation.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreExportInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance: The logical name of the Redis instance in the customer project.
    :param output_config: Required. Specify data to be exported.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.redis_v1.types.OutputConfig`
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "instance",
        "output_config",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        location: str,
        instance: str,
        output_config: dict | OutputConfig,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance = instance
        self.output_config = output_config
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        hook.export_instance(
            location=self.location,
            instance=self.instance,
            output_config=self.output_config,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        RedisInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )


class CloudMemorystoreFailoverInstanceOperator(GoogleCloudBaseOperator):
    """
    Initiate a failover of the primary node for a specific STANDARD tier Cloud Memorystore for Redis instance.

    Uses the current replica node.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreFailoverInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance: The logical name of the Redis instance in the customer project.
    :param data_protection_mode: Optional. Available data protection modes that the user can choose. If it's
        unspecified, data protection mode will be LIMITED_DATA_LOSS by default.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "instance",
        "data_protection_mode",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        location: str,
        instance: str,
        data_protection_mode: FailoverInstanceRequest.DataProtectionMode,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance = instance
        self.data_protection_mode = data_protection_mode
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        hook.failover_instance(
            location=self.location,
            instance=self.instance,
            data_protection_mode=self.data_protection_mode,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        RedisInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )


class CloudMemorystoreGetInstanceOperator(GoogleCloudBaseOperator):
    """
    Gets the details of a specific Redis instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreGetInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance: The logical name of the Redis instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "instance",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        location: str,
        instance: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance = instance
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.get_instance(
            location=self.location,
            instance=self.instance,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        RedisInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return Instance.to_dict(result)


class CloudMemorystoreImportOperator(GoogleCloudBaseOperator):
    """
    Import a Redis RDB snapshot file from Cloud Storage into a Redis instance.

    Redis may stop serving during this operation. Instance state will be IMPORTING for entire operation. When
    complete, the instance will contain only data from the imported file.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreImportOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance: The logical name of the Redis instance in the customer project.
    :param input_config: Required. Specify data to be imported.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.redis_v1.types.InputConfig`
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "instance",
        "input_config",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        location: str,
        instance: str,
        input_config: dict | InputConfig,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance = instance
        self.input_config = input_config
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        hook.import_instance(
            location=self.location,
            instance=self.instance,
            input_config=self.input_config,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        RedisInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )


class CloudMemorystoreListInstancesOperator(GoogleCloudBaseOperator):
    """
    Lists all Redis instances owned by a project in either the specified location (region) or all locations.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreListInstancesOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        If it is specified as ``-`` (wildcard), then all regions available to the project are
        queried, and the results are aggregated.
    :param page_size: The maximum number of resources contained in the underlying API response. If page
        streaming is performed per- resource, this parameter does not affect the return value. If page
        streaming is performed per-page, this determines the maximum number of resources in a page.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "page_size",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceListLink(),)

    def __init__(
        self,
        *,
        location: str,
        page_size: int,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.page_size = page_size
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.list_instances(
            location=self.location,
            page_size=self.page_size,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        RedisInstanceListLink.persist(
            context=context,
            task_instance=self,
            project_id=self.project_id or hook.project_id,
        )
        instances = [Instance.to_dict(a) for a in result]
        return instances


class CloudMemorystoreUpdateInstanceOperator(GoogleCloudBaseOperator):
    """
    Updates the metadata and configuration of a specific Redis instance.

    :param update_mask: Required. Mask of fields to update. At least one path must be supplied in this field.
        The elements of the repeated paths field may only include these fields from ``Instance``:

        -  ``displayName``
        -  ``labels``
        -  ``memorySizeGb``
        -  ``redisConfig``

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.redis_v1.types.FieldMask`

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreUpdateInstanceOperator`

    :param instance: Required. Update description. Only fields specified in update_mask are updated.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.redis_v1.types.Instance`
    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance_id: The logical name of the Redis instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "update_mask",
        "instance",
        "location",
        "instance_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        update_mask: dict | FieldMask,
        instance: dict | Instance,
        location: str | None = None,
        instance_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.update_mask = update_mask
        self.instance = instance
        self.location = location
        self.instance_id = instance_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        res = hook.update_instance(
            update_mask=self.update_mask,
            instance=self.instance,
            location=self.location,
            instance_id=self.instance_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        # projects/PROJECT_NAME/locations/LOCATION/instances/INSTANCE
        location_id, instance_id = res.name.split("/")[-3::2]
        RedisInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance_id or instance_id,
            location_id=self.location or location_id,
            project_id=self.project_id or hook.project_id,
        )


class CloudMemorystoreScaleInstanceOperator(GoogleCloudBaseOperator):
    """
    Updates the metadata and configuration of a specific Redis instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreScaleInstanceOperator`

    :param memory_size_gb: Redis memory size in GiB.
    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance_id: The logical name of the Redis instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "memory_size_gb",
        "location",
        "instance_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        memory_size_gb: int,
        location: str | None = None,
        instance_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.memory_size_gb = memory_size_gb
        self.location = location
        self.instance_id = instance_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        res = hook.update_instance(
            update_mask={"paths": ["memory_size_gb"]},
            instance={"memory_size_gb": self.memory_size_gb},
            location=self.location,
            instance_id=self.instance_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        # projects/PROJECT_NAME/locations/LOCATION/instances/INSTANCE
        location_id, instance_id = res.name.split("/")[-3::2]
        RedisInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance_id or instance_id,
            location_id=self.location or location_id,
            project_id=self.project_id or hook.project_id,
        )


class CloudMemorystoreCreateInstanceAndImportOperator(GoogleCloudBaseOperator):
    """
    Create a Redis instance and import a Redis RDB snapshot file from Cloud Storage into this instance.

    By default, the instance is accessible from the project's `default network
    <https://cloud.google.com/compute/docs/networks-and-firewalls#networks>`__.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreCreateInstanceAndImportOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance_id: Required. The logical name of the Redis instance in the customer project with the
        following restrictions:

        -  Must contain only lowercase letters, numbers, and hyphens.
        -  Must start with a letter.
        -  Must be between 1-40 characters.
        -  Must end with a number or a letter.
        -  Must be unique within the customer project / location
    :param instance: Required. A Redis [Instance] resource

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.redis_v1.types.Instance`
    :param input_config: Required. Specify data to be imported.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.redis_v1.types.InputConfig`
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "instance_id",
        "instance",
        "input_config",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (RedisInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        location: str,
        instance_id: str,
        instance: dict | Instance,
        input_config: dict | InputConfig,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance_id = instance_id
        self.instance = instance
        self.input_config = input_config
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        hook.create_instance(
            location=self.location,
            instance_id=self.instance_id,
            instance=self.instance,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        hook.import_instance(
            location=self.location,
            instance=self.instance_id,
            input_config=self.input_config,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        RedisInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance_id,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )


class CloudMemorystoreExportAndDeleteInstanceOperator(GoogleCloudBaseOperator):
    """
    Export Redis instance data into a Redis RDB format file in Cloud Storage.

    In next step, deletes this instance.

    Redis will continue serving during this operation.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreExportAndDeleteInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance: The logical name of the Redis instance in the customer project.
    :param output_config: Required. Specify data to be exported.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.redis_v1.types.OutputConfig`
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "instance",
        "output_config",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        instance: str,
        output_config: dict | OutputConfig,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance = instance
        self.output_config = output_config
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudMemorystoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        hook.export_instance(
            location=self.location,
            instance=self.instance,
            output_config=self.output_config,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        hook.delete_instance(
            location=self.location,
            instance=self.instance,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudMemorystoreMemcachedApplyParametersOperator(GoogleCloudBaseOperator):
    """
    Will update current set of Parameters to the set of specified nodes of the Memcached Instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreMemcachedApplyParametersOperator`

    :param node_ids: Nodes to which we should apply the instance-level parameter group.
    :param apply_all: Whether to apply instance-level parameter group to all nodes. If set to true,
        will explicitly restrict users from specifying any nodes, and apply parameter group updates
        to all nodes within the instance.
    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance_id: The logical name of the Memcached instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = (
        "node_ids",
        "apply_all",
        "location",
        "instance_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (MemcachedInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        node_ids: Sequence[str],
        apply_all: bool,
        location: str,
        instance_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.node_ids = node_ids
        self.apply_all = apply_all
        self.location = location
        self.instance_id = instance_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudMemorystoreMemcachedHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        hook.apply_parameters(
            node_ids=self.node_ids,
            apply_all=self.apply_all,
            location=self.location,
            instance_id=self.instance_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        MemcachedInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance_id,
            location_id=self.location,
            project_id=self.project_id,
        )


class CloudMemorystoreMemcachedCreateInstanceOperator(GoogleCloudBaseOperator):
    """
    Creates a Memcached instance based on the specified tier and memory size.

    By default, the instance is accessible from the project's `default network
    <https://cloud.google.com/compute/docs/networks-and-firewalls#networks>`__.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreMemcachedCreateInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance_id: Required. The logical name of the Memcached instance in the customer project with the
        following restrictions:

        -  Must contain only lowercase letters, numbers, and hyphens.
        -  Must start with a letter.
        -  Must be between 1-40 characters.
        -  Must end with a number or a letter.
        -  Must be unique within the customer project / location
    :param instance: Required. A Memcached [Instance] resource

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.Instance`
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the GCP connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    """

    template_fields: Sequence[str] = (
        "location",
        "instance_id",
        "instance",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
    )
    operator_extra_links = (MemcachedInstanceDetailsLink(),)

    def __init__(
        self,
        location: str,
        instance_id: str,
        instance: dict | cloud_memcache.Instance,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.location = location
        self.instance_id = instance_id
        self.instance = instance
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context):
        hook = CloudMemorystoreMemcachedHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.create_instance(
            location=self.location,
            instance_id=self.instance_id,
            instance=self.instance,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        MemcachedInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance_id,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return cloud_memcache.Instance.to_dict(result)


class CloudMemorystoreMemcachedDeleteInstanceOperator(GoogleCloudBaseOperator):
    """
    Deletes a specific Memcached instance. Instance stops serving and data is deleted.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreMemcachedDeleteInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance: The logical name of the Memcached instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the GCP connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    """

    template_fields: Sequence[str] = (
        "location",
        "instance",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
    )

    def __init__(
        self,
        location: str,
        instance: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.location = location
        self.instance = instance
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context):
        hook = CloudMemorystoreMemcachedHook(gcp_conn_id=self.gcp_conn_id)
        hook.delete_instance(
            location=self.location,
            instance=self.instance,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudMemorystoreMemcachedGetInstanceOperator(GoogleCloudBaseOperator):
    """
    Gets the details of a specific Memcached instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreMemcachedGetInstanceOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance: The logical name of the Memcached instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
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
        "location",
        "instance",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (MemcachedInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        location: str,
        instance: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.instance = instance
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudMemorystoreMemcachedHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.get_instance(
            location=self.location,
            instance=self.instance,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        MemcachedInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return cloud_memcache.Instance.to_dict(result)


class CloudMemorystoreMemcachedListInstancesOperator(GoogleCloudBaseOperator):
    """
    List all Memcached instances owned by a project in either the specified location/region or all locations.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreMemcachedListInstancesOperator`

    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        If it is specified as ``-`` (wildcard), then all regions available to the project are
        queried, and the results are aggregated.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (MemcachedInstanceListLink(),)

    def __init__(
        self,
        *,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudMemorystoreMemcachedHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.list_instances(
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        MemcachedInstanceListLink.persist(
            context=context,
            task_instance=self,
            project_id=self.project_id or hook.project_id,
        )
        instances = [cloud_memcache.Instance.to_dict(a) for a in result]
        return instances


class CloudMemorystoreMemcachedUpdateInstanceOperator(GoogleCloudBaseOperator):
    """
    Updates the metadata and configuration of a specific Memcached instance.

    :param update_mask: Required. Mask of fields to update. At least one path must be supplied in this field.
        The elements of the repeated paths field may only include these fields from ``Instance``:

        -  ``displayName``

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreMemcachedUpdateInstanceOperator`

    :param instance: Required. Update description. Only fields specified in update_mask are updated.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.Instance`
    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance_id: The logical name of the Memcached instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "update_mask",
        "instance",
        "location",
        "instance_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (MemcachedInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        update_mask: dict | FieldMask,
        instance: dict | cloud_memcache.Instance,
        location: str | None = None,
        instance_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.update_mask = update_mask
        self.instance = instance
        self.location = location
        self.instance_id = instance_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudMemorystoreMemcachedHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        res = hook.update_instance(
            update_mask=self.update_mask,
            instance=self.instance,
            location=self.location,
            instance_id=self.instance_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        # projects/PROJECT_NAME/locations/LOCATION/instances/INSTANCE
        location_id, instance_id = res.name.split("/")[-3::2]
        MemcachedInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance_id or instance_id,
            location_id=self.location or location_id,
            project_id=self.project_id or hook.project_id,
        )


class CloudMemorystoreMemcachedUpdateParametersOperator(GoogleCloudBaseOperator):
    """
    Updates the defined Memcached Parameters for an existing Instance.

    This method only stages the parameters, it must be followed by apply_parameters
    to apply the parameters to nodes of the Memcached Instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudMemorystoreMemcachedApplyParametersOperator`

    :param update_mask: Required. Mask of fields to update.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param parameters: The parameters to apply to the instance.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.MemcacheParameters`
    :param location: The location of the Cloud Memorystore instance (for example europe-west1)
    :param instance_id: The logical name of the Memcached instance in the customer project.
    :param project_id: Project ID of the project that contains the instance. If set
        to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = (
        "update_mask",
        "parameters",
        "location",
        "instance_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (MemcachedInstanceDetailsLink(),)

    def __init__(
        self,
        *,
        update_mask: dict | FieldMask,
        parameters: dict | cloud_memcache.MemcacheParameters,
        location: str,
        instance_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.update_mask = update_mask
        self.parameters = parameters
        self.location = location
        self.instance_id = instance_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudMemorystoreMemcachedHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        hook.update_parameters(
            update_mask=self.update_mask,
            parameters=self.parameters,
            location=self.location,
            instance_id=self.instance_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        MemcachedInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            instance_id=self.instance_id,
            location_id=self.location,
            project_id=self.project_id,
        )
