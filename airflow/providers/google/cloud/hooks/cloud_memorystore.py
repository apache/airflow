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
"""Hooks for Cloud Memorystore service"""
from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core import path_template
from google.api_core.exceptions import NotFound
from google.api_core.retry import Retry
from google.cloud.memcache_v1beta2 import CloudMemcacheClient
from google.cloud.memcache_v1beta2.types import cloud_memcache
from google.cloud.redis_v1 import CloudRedisClient
from google.cloud.redis_v1.gapic.enums import FailoverInstanceRequest
from google.cloud.redis_v1.types import FieldMask, InputConfig, Instance, OutputConfig
from google.protobuf.json_format import ParseDict

from airflow import version
from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CloudMemorystoreHook(GoogleBaseHook):
    """
    Hook for Google Cloud Memorystore APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._client: Optional[CloudRedisClient] = None

    def get_conn(self):
        """Retrieves client library object that allow access to Cloud Memorystore service."""
        if not self._client:
            self._client = CloudRedisClient(credentials=self._get_credentials())
        return self._client

    @staticmethod
    def _append_label(instance: Instance, key: str, val: str) -> Instance:
        """
        Append labels to provided Instance type

        Labels must fit the regex ``[a-z]([-a-z0-9]*[a-z0-9])?`` (current
         airflow version string follows semantic versioning spec: x.y.z).

        :param instance: The proto to append resource_label airflow
            version to
        :type instance: google.cloud.container_v1.types.Cluster
        :param key: The key label
        :type key: str
        :param val:
        :type val: str
        :return: The cluster proto updated with new label
        """
        val = val.replace(".", "-").replace("+", "-")
        instance.labels.update({key: val})
        return instance

    @GoogleBaseHook.fallback_to_default_project_id
    def create_instance(
        self,
        location: str,
        instance_id: str,
        instance: Union[Dict, Instance],
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Creates a Redis instance based on the specified tier and memory size.

        By default, the instance is accessible from the project's `default network
        <https://cloud.google.com/compute/docs/networks-and-firewalls#networks>`__.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance_id: Required. The logical name of the Redis instance in the customer project with the
            following restrictions:

            -  Must contain only lowercase letters, numbers, and hyphens.
            -  Must start with a letter.
            -  Must be between 1-40 characters.
            -  Must end with a number or a letter.
            -  Must be unique within the customer project / location
        :type instance_id: str
        :param instance: Required. A Redis [Instance] resource

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.redis_v1.types.Instance`
        :type instance: Union[Dict, google.cloud.redis_v1.types.Instance]
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        parent = CloudRedisClient.location_path(project_id, location)
        instance_name = CloudRedisClient.instance_path(project_id, location, instance_id)
        try:
            instance = client.get_instance(
                name=instance_name, retry=retry, timeout=timeout, metadata=metadata
            )
            self.log.info("Instance exists. Skipping creation.")
            return instance
        except NotFound:
            self.log.info("Instance not exists.")

        if isinstance(instance, dict):
            instance = ParseDict(instance, Instance())
        elif not isinstance(instance, Instance):
            raise AirflowException("instance is not instance of Instance type or python dict")

        self._append_label(instance, "airflow-version", "v" + version.version)

        result = client.create_instance(
            parent=parent,
            instance_id=instance_id,
            instance=instance,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        result.result()
        self.log.info("Instance created.")
        return client.get_instance(name=instance_name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance(
        self,
        location: str,
        instance: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Deletes a specific Redis instance.  Instance stops serving and data is deleted.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Redis instance in the customer project.
        :type instance: str
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        name = CloudRedisClient.instance_path(project_id, location, instance)
        self.log.info("Fetching Instance: %s", name)
        instance = client.get_instance(name=name, retry=retry, timeout=timeout, metadata=metadata)

        if not instance:
            return

        self.log.info("Deleting Instance: %s", name)
        result = client.delete_instance(name=name, retry=retry, timeout=timeout, metadata=metadata)
        result.result()
        self.log.info("Instance deleted: %s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def export_instance(
        self,
        location: str,
        instance: str,
        output_config: Union[Dict, OutputConfig],
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Export Redis instance data into a Redis RDB format file in Cloud Storage.

        Redis will continue serving during this operation.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Redis instance in the customer project.
        :type instance: str
        :param output_config: Required. Specify data to be exported.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.redis_v1.types.OutputConfig`
        :type output_config: Union[Dict, google.cloud.redis_v1.types.OutputConfig]
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        name = CloudRedisClient.instance_path(project_id, location, instance)
        self.log.info("Exporting Instance: %s", name)
        result = client.export_instance(
            name=name, output_config=output_config, retry=retry, timeout=timeout, metadata=metadata
        )
        result.result()
        self.log.info("Instance exported: %s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def failover_instance(
        self,
        location: str,
        instance: str,
        data_protection_mode: FailoverInstanceRequest.DataProtectionMode,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Initiates a failover of the master node to current replica node for a specific STANDARD tier Cloud
        Memorystore for Redis instance.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Redis instance in the customer project.
        :type instance: str
        :param data_protection_mode: Optional. Available data protection modes that the user can choose. If
            it's unspecified, data protection mode will be LIMITED_DATA_LOSS by default.
        :type data_protection_mode: google.cloud.redis_v1.gapic.enums.FailoverInstanceRequest
            .DataProtectionMode
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        name = CloudRedisClient.instance_path(project_id, location, instance)
        self.log.info("Failovering Instance: %s", name)

        result = client.failover_instance(
            name=name,
            data_protection_mode=data_protection_mode,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        result.result()
        self.log.info("Instance failovered: %s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance(
        self,
        location: str,
        instance: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Gets the details of a specific Redis instance.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Redis instance in the customer project.
        :type instance: str
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        name = CloudRedisClient.instance_path(project_id, location, instance)
        result = client.get_instance(name=name, retry=retry, timeout=timeout, metadata=metadata)
        self.log.info("Fetched Instance: %s", name)
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def import_instance(
        self,
        location: str,
        instance: str,
        input_config: Union[Dict, InputConfig],
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Import a Redis RDB snapshot file from Cloud Storage into a Redis instance.

        Redis may stop serving during this operation. Instance state will be IMPORTING for entire operation.
        When complete, the instance will contain only data from the imported file.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Redis instance in the customer project.
        :type instance: str
        :param input_config: Required. Specify data to be imported.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.redis_v1.types.InputConfig`
        :type input_config: Union[Dict, google.cloud.redis_v1.types.InputConfig]
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        name = CloudRedisClient.instance_path(project_id, location, instance)
        self.log.info("Importing Instance: %s", name)
        result = client.import_instance(
            name=name, input_config=input_config, retry=retry, timeout=timeout, metadata=metadata
        )
        result.result()
        self.log.info("Instance imported: %s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_instances(
        self,
        location: str,
        page_size: int,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Lists all Redis instances owned by a project in either the specified location (region) or all
        locations.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)

                If it is specified as ``-`` (wildcard), then all regions available to the project are
                queried, and the results are aggregated.
        :type location: str
        :param page_size: The maximum number of resources contained in the underlying API response. If page
            streaming is performed per- resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number of resources in a page.
        :type page_size: int
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        parent = CloudRedisClient.location_path(project_id, location)
        result = client.list_instances(
            parent=parent, page_size=page_size, retry=retry, timeout=timeout, metadata=metadata
        )
        self.log.info("Fetched instances")
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_instance(
        self,
        update_mask: Union[Dict, FieldMask],
        instance: Union[Dict, Instance],
        project_id: str,
        location: Optional[str] = None,
        instance_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Updates the metadata and configuration of a specific Redis instance.

        :param update_mask: Required. Mask of fields to update. At least one path must be supplied in this
            field. The elements of the repeated paths field may only include these fields from ``Instance``:

            -  ``displayName``
            -  ``labels``
            -  ``memorySizeGb``
            -  ``redisConfig``

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.redis_v1.types.FieldMask`
        :type update_mask: Union[Dict, google.cloud.redis_v1.types.FieldMask]
        :param instance: Required. Update description. Only fields specified in ``update_mask`` are updated.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.redis_v1.types.Instance`
        :type instance: Union[Dict, google.cloud.redis_v1.types.Instance]
        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance_id: The logical name of the Redis instance in the customer project.
        :type instance_id: str
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()

        if isinstance(instance, dict):
            instance = ParseDict(instance, Instance())
        elif not isinstance(instance, Instance):
            raise AirflowException("instance is not instance of Instance type or python dict")

        if location and instance_id:
            name = CloudRedisClient.instance_path(project_id, location, instance_id)
            instance.name = name

        self.log.info("Updating instances: %s", instance.name)
        result = client.update_instance(
            update_mask=update_mask, instance=instance, retry=retry, timeout=timeout, metadata=metadata
        )
        result.result()
        self.log.info("Instance updated: %s", instance.name)


class CloudMemorystoreMemcachedHook(GoogleBaseHook):
    """
    Hook for Google Cloud Memorystore for Memcached service APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._client: Optional[CloudMemcacheClient] = None

    def get_conn(
        self,
    ):
        """Retrieves client library object that allow access to Cloud Memorystore Memcached service."""
        if not self._client:
            self._client = CloudMemcacheClient(credentials=self._get_credentials())
        return self._client

    @staticmethod
    def _append_label(instance: cloud_memcache.Instance, key: str, val: str) -> cloud_memcache.Instance:
        """
        Append labels to provided Instance type

        Labels must fit the regex ``[a-z]([-a-z0-9]*[a-z0-9])?`` (current
         airflow version string follows semantic versioning spec: x.y.z).

        :param instance: The proto to append resource_label airflow
            version to
        :type instance: google.cloud.memcache_v1beta2.types.cloud_memcache.Instance
        :param key: The key label
        :type key: str
        :param val:
        :type val: str
        :return: The cluster proto updated with new label
        """
        val = val.replace(".", "-").replace("+", "-")
        instance.labels.update({key: val})
        return instance

    @GoogleBaseHook.fallback_to_default_project_id
    def apply_parameters(
        self,
        node_ids: Sequence[str],
        apply_all: bool,
        project_id: str,
        location: str,
        instance_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Will update current set of Parameters to the set of specified nodes of the Memcached Instance.

        :param node_ids: Nodes to which we should apply the instance-level parameter group.
        :type node_ids: Sequence[str]
        :param apply_all: Whether to apply instance-level parameter group to all nodes. If set to true,
            will explicitly restrict users from specifying any nodes, and apply parameter group updates
            to all nodes within the instance.
        :type apply_all: bool
        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance_id: The logical name of the Memcached instance in the customer project.
        :type instance_id: str
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        metadata = metadata or ()
        name = CloudMemcacheClient.instance_path(project_id, location, instance_id)

        self.log.info("Applying update to instance: %s", instance_id)
        result = client.apply_parameters(
            name=name, node_ids=node_ids, apply_all=apply_all, retry=retry, timeout=timeout, metadata=metadata
        )
        result.result()
        self.log.info("Instance updated: %s", instance_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_instance(
        self,
        location: str,
        instance_id: str,
        instance: Union[Dict, cloud_memcache.Instance],
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Creates a Memcached instance based on the specified tier and memory size.

        By default, the instance is accessible from the project's `default network
        <https://cloud.google.com/compute/docs/networks-and-firewalls#networks>`__.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance_id: Required. The logical name of the Memcached instance in the customer project
            with the following restrictions:

            -  Must contain only lowercase letters, numbers, and hyphens.
            -  Must start with a letter.
            -  Must be between 1-40 characters.
            -  Must end with a number or a letter.
            -  Must be unique within the customer project / location
        :type instance_id: str
        :param instance: Required. A Memcached [Instance] resource

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.Instance`
        :type instance: Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.Instance]
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        metadata = metadata or ()
        parent = path_template.expand(
            "projects/{project}/locations/{location}", project=project_id, location=location
        )
        instance_name = CloudMemcacheClient.instance_path(project_id, location, instance_id)
        try:
            instance = client.get_instance(
                name=instance_name, retry=retry, timeout=timeout, metadata=metadata
            )
            self.log.info("Instance exists. Skipping creation.")
            return instance
        except NotFound:
            self.log.info("Instance not exists.")

        if isinstance(instance, dict):
            instance = cloud_memcache.Instance(instance)
        elif not isinstance(instance, cloud_memcache.Instance):
            raise AirflowException("instance is not instance of Instance type or python dict")

        self._append_label(instance, "airflow-version", "v" + version.version)

        result = client.create_instance(
            parent=parent,
            instance_id=instance_id,
            resource=instance,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        result.result()
        self.log.info("Instance created.")
        return client.get_instance(name=instance_name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance(
        self,
        location: str,
        instance: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Deletes a specific Memcached instance.  Instance stops serving and data is deleted.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Memcached instance in the customer project.
        :type instance: str
        :param project_id:  Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        metadata = metadata or ()
        name = CloudMemcacheClient.instance_path(project_id, location, instance)
        self.log.info("Fetching Instance: %s", name)
        instance = client.get_instance(name=name, retry=retry, timeout=timeout, metadata=metadata)

        if not instance:
            return

        self.log.info("Deleting Instance: %s", name)
        result = client.delete_instance(name=name, retry=retry, timeout=timeout, metadata=metadata)
        result.result()
        self.log.info("Instance deleted: %s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance(
        self,
        location: str,
        instance: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Gets the details of a specific Memcached instance.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Memcached instance in the customer project.
        :type instance: str
        :param project_id:  Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        metadata = metadata or ()
        name = CloudMemcacheClient.instance_path(project_id, location, instance)
        result = client.get_instance(name=name, retry=retry, timeout=timeout, metadata=metadata or ())
        self.log.info("Fetched Instance: %s", name)
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_instances(
        self,
        location: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Lists all Memcached instances owned by a project in either the specified location (region) or all
        locations.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)

                If it is specified as ``-`` (wildcard), then all regions available to the project are
                queried, and the results are aggregated.
        :type location: str
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        metadata = metadata or ()
        parent = path_template.expand(
            "projects/{project}/locations/{location}", project=project_id, location=location
        )
        result = client.list_instances(parent=parent, retry=retry, timeout=timeout, metadata=metadata)
        self.log.info("Fetched instances")
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_instance(
        self,
        update_mask: Union[Dict, cloud_memcache.field_mask.FieldMask],
        instance: Union[Dict, cloud_memcache.Instance],
        project_id: str,
        location: Optional[str] = None,
        instance_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Updates the metadata and configuration of a specific Memcached instance.

        :param update_mask: Required. Mask of fields to update. At least one path must be supplied in this
            field. The elements of the repeated paths field may only include these fields from ``Instance``:

            -  ``displayName``

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.field_mask.FieldMask`
        :type update_mask:
            Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.field_mask.FieldMask]
        :param instance: Required. Update description. Only fields specified in ``update_mask`` are updated.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.Instance`
        :type instance: Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.Instance]
        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance_id: The logical name of the Memcached instance in the customer project.
        :type instance_id: str
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        metadata = metadata or ()

        if isinstance(instance, dict):
            instance = cloud_memcache.Instance(instance)
        elif not isinstance(instance, cloud_memcache.Instance):
            raise AirflowException("instance is not instance of Instance type or python dict")

        if location and instance_id:
            name = CloudMemcacheClient.instance_path(project_id, location, instance_id)
            instance.name = name

        self.log.info("Updating instances: %s", instance.name)
        result = client.update_instance(
            update_mask=update_mask, resource=instance, retry=retry, timeout=timeout, metadata=metadata
        )
        result.result()
        self.log.info("Instance updated: %s", instance.name)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_parameters(
        self,
        update_mask: Union[Dict, cloud_memcache.field_mask.FieldMask],
        parameters: Union[Dict, cloud_memcache.MemcacheParameters],
        project_id: str,
        location: str,
        instance_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Updates the defined Memcached Parameters for an existing Instance. This method only stages the
            parameters, it must be followed by apply_parameters to apply the parameters to nodes of
            the Memcached Instance.

        :param update_mask: Required. Mask of fields to update.
            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.field_mask.FieldMask`
        :type update_mask:
            Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.field_mask.FieldMask]
        :param parameters: The parameters to apply to the instance.
            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.MemcacheParameters`
        :type parameters: Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.MemcacheParameters]
        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance_id: The logical name of the Memcached instance in the customer project.
        :type instance_id: str
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        metadata = metadata or ()

        if isinstance(parameters, dict):
            parameters = cloud_memcache.MemcacheParameters(parameters)
        elif not isinstance(parameters, cloud_memcache.MemcacheParameters):
            raise AirflowException("instance is not instance of MemcacheParameters type or python dict")

        name = CloudMemcacheClient.instance_path(project_id, location, instance_id)
        self.log.info("Staging update to instance: %s", instance_id)
        result = client.update_parameters(
            name=name,
            update_mask=update_mask,
            parameters=parameters,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        result.result()
        self.log.info("Update staged for instance: %s", instance_id)
