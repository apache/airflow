from typing import Dict, Sequence, Tuple, Union, Optional

from google.api_core.retry import Retry
from google.cloud.redis_v1 import CloudRedisClient
from google.cloud.redis_v1.gapic.enums import FailoverInstanceRequest
from google.cloud.redis_v1.types import FieldMask, InputConfig, Instance, OutputConfig
from google.cloud.redis_v1beta1 import CloudRedisClient

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class CloudMemorystoreHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Memorystore APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(self, gcp_conn_id: str = "google_cloud_default", delegate_to: str = None):
        super().__init__(gcp_conn_id, delegate_to)
        self._client: Optional[CloudRedisClient] = None

    def get_conn(self,):
        """
        Retrieves client library object that allow access to Cloud Memorystore service.

        """
        if not self._client:
            self._client = CloudRedisClient(credentials=self._get_credentials())
        return self._client

    def create_instance(
        self,
        location: str,
        instance_id: str,
        instance: Union[Dict, Instance],
        project_id: str = None,
        retry: Retry = None,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = None,
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
        parent = CloudRedisClient.location_path(project_id, location)
        result = client.create_instance(
            parent=parent,
            instance_id=instance_id,
            instance=instance,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def delete_instance(
        self,
        location: str,
        instance: str,
        project_id: str = None,
        retry: Retry = None,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = None,
    ):
        """
        Deletes a specific Redis instance.  Instance stops serving and data is deleted.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Redis instance in the customer project.
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
        name = CloudRedisClient.instance_path(project_id, location, instance)
        result = client.delete_instance(name=name, retry=retry, timeout=timeout, metadata=metadata)
        return result

    def export_instance(
        self,
        location: str,
        instance: str,
        output_config: Union[Dict, OutputConfig],
        project_id: str = None,
        retry: Retry = None,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = None,
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
        name = CloudRedisClient.instance_path(project_id, location, instance)
        result = client.export_instance(
            name=name, output_config=output_config, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def failover_instance(
        self,
        location: str,
        instance: str,
        data_protection_mode: FailoverInstanceRequest.DataProtectionMode,
        project_id: str = None,
        retry: Retry = None,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = None,
    ):
        """
        Initiates a failover of the master node to current replica node for a specific STANDARD tier Cloud
        Memorystore for Redis instance.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Redis instance in the customer project.
        :type instance: str
        :param data_protection_mode: Optional. Available data protection modes that the user can choose. If
            it's unspecified, data protection mode will be LIMITED\_DATA\_LOSS by default.
        :type data_protection_mode: google.cloud.redis_v1.gapic.enums.FailoverInstanceRequest.DataProtectionMode
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
        name = CloudRedisClient.instance_path(project_id, location, instance)
        result = client.failover_instance(
            name=name,
            data_protection_mode=data_protection_mode,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def get_instance(
        self,
        location: str,
        instance: str,
        project_id: str = None,
        retry: Retry = None,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = None,
    ):
        """
        Gets the details of a specific Redis instance.

        :param location: The location of the Cloud Memorystore instance (for example europe-west1)
        :type location: str
        :param instance: The logical name of the Redis instance in the customer project.
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
        name = CloudRedisClient.instance_path(project_id, location, instance)
        result = client.get_instance(name=name, retry=retry, timeout=timeout, metadata=metadata)
        return result

    def import_instance(
        self,
        location: str,
        instance: str,
        input_config: Union[Dict, InputConfig],
        project_id: str = None,
        retry: Retry = None,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = None,
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
        name = CloudRedisClient.instance_path(project_id, location, instance)
        result = client.import_instance(
            name=name, input_config=input_config, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def list_instances(
        self,
        location: str,
        page_size: int,
        project_id: str = None,
        retry: Retry = None,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = None,
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
        parent = CloudRedisClient.location_path(project_id, location)
        result = client.list_instances(
            parent=parent, page_size=page_size, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def update_instance(
        self,
        update_mask: Union[Dict, FieldMask],
        instance: Union[Dict, Instance],
        retry: Retry = None,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = None,
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
        result = client.update_instance(
            update_mask=update_mask, instance=instance, retry=retry, timeout=timeout, metadata=metadata
        )
        return result
