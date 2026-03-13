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
"""This module contains a Google Compute Engine Hook."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from google.cloud.compute_v1.services.instance_group_managers import InstanceGroupManagersClient
from google.cloud.compute_v1.services.instance_templates import InstanceTemplatesClient
from google.cloud.compute_v1.services.instances import InstancesClient
from googleapiclient.discovery import build

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.compute_v1.types import Instance, InstanceGroupManager, InstanceTemplate

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 1


class GceOperationStatus:
    """Class with GCE operations statuses."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"


class ComputeEngineHook(GoogleBaseHook):
    """
    Hook for Google Compute Engine APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        self.api_version = api_version

    _conn: Any | None = None

    def get_conn(self):
        """
        Retrieve connection to Google Compute Engine.

        :return: Google Compute Engine services object
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("compute", self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    def get_compute_instance_template_client(self):
        """Return Compute Engine Instance Template Client."""
        return InstanceTemplatesClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)

    def get_compute_instance_client(self):
        """Return Compute Engine Instance Client."""
        return InstancesClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)

    def get_compute_instance_group_managers_client(self):
        """Return Compute Engine Instance Group Managers Client."""
        return InstanceGroupManagersClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)

    @GoogleBaseHook.fallback_to_default_project_id
    def insert_instance_template(
        self,
        body: dict,
        request_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Create Instance Template using body specified.

        Must be called with keyword arguments rather than positional.

        :param body: Instance Template representation as an object.
        :param request_id: Unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again)
            It should be in UUID format as defined in RFC 4122
        :param project_id: Google Cloud project ID where the Compute Engine Instance Template exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_compute_instance_template_client()
        operation = client.insert(
            # Calling method insert() on client to create Instance Template.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.InsertInstanceTemplateRequest, dict] to construct a request
            # message.
            # The request object should be represented using arguments:
            #   instance_template_resource (google.cloud.compute_v1.types.InstanceTemplate):
            #       The body resource for this request.
            #   request_id (str):
            #       An optional request ID to identify requests.
            #   project (str):
            #       Project ID for this request.
            request={
                "instance_template_resource": body,
                "request_id": request_id,
                "project": project_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self._wait_for_operation_to_complete(operation_name=operation.name, project_id=project_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance_template(
        self,
        resource_id: str,
        request_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete Instance Template.

        Deleting an Instance Template is permanent and cannot be undone. It is not
        possible to delete templates that are already in use by a managed instance
        group. Must be called with keyword arguments rather than positional.

        :param resource_id: Name of the Compute Engine Instance Template resource.
        :param request_id: Unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again)
            It should be in UUID format as defined in RFC 4122
        :param project_id: Google Cloud project ID where the Compute Engine Instance Template exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_compute_instance_template_client()
        operation = client.delete(
            # Calling method delete() on client to delete Instance Template.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.DeleteInstanceTemplateRequest, dict] to
            # construct a request message.
            # The request object should be represented using arguments:
            #   instance_template (str):
            #       The name of the Instance Template to delete.
            #   project (str):
            #       Project ID for this request.
            #   request_id (str):
            #       An optional request ID to identify requests.
            request={
                "instance_template": resource_id,
                "project": project_id,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self._wait_for_operation_to_complete(operation_name=operation.name, project_id=project_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance_template(
        self,
        resource_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> InstanceTemplate:
        """
        Retrieve Instance Template by project_id and resource_id.

        Must be called with keyword arguments rather than positional.

        :param resource_id: Name of the Instance Template.
        :param project_id: Google Cloud project ID where the Compute Engine Instance Template exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: Instance Template representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
        :rtype: object
        """
        client = self.get_compute_instance_template_client()
        instance_template = client.get(
            # Calling method get() on client to get the specified Instance Template.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.GetInstanceTemplateRequest, dict] to construct a request
            # message.
            # The request object should be represented using arguments:
            #   instance_template (str):
            #       The name of the Instance Template.
            #   project (str):
            #       Project ID for this request.
            request={
                "instance_template": resource_id,
                "project": project_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return instance_template

    @GoogleBaseHook.fallback_to_default_project_id
    def insert_instance(
        self,
        body: dict,
        zone: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        source_instance_template: str | None = None,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Create Instance using body specified.

        Must be called with keyword arguments rather than positional.

        :param body: Instance representation as an object. Should at least include 'name', 'machine_type',
            'disks' and 'network_interfaces' fields but doesn't include 'zone' field, as it will be specified
            in 'zone' parameter.
            Full or partial URL and can be represented as examples below:
            1. "machine_type": "projects/your-project-name/zones/your-zone/machineTypes/your-machine-type"
            2. "source_image": "projects/your-project-name/zones/your-zone/diskTypes/your-disk-type"
            3. "subnetwork": "projects/your-project-name/regions/your-region/subnetworks/your-subnetwork"
        :param zone: Google Cloud zone where the Instance exists
        :param project_id: Google Cloud project ID where the Compute Engine Instance Template exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param source_instance_template: Existing Instance Template that will be used as a base while
            creating new Instance.
            When specified, only name of new Instance should be provided as input arguments in 'body'
            parameter when creating new Instance. All other parameters, will be passed to Instance as they
            are specified in the Instance Template.
            Full or partial URL and can be represented as examples below:
            1. "https://www.googleapis.com/compute/v1/projects/your-project/global/instanceTemplates/temp"
            2. "projects/your-project/global/instanceTemplates/temp"
            3. "global/instanceTemplates/temp"
        :param request_id: Unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again)
            It should be in UUID format as defined in RFC 4122
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_compute_instance_client()
        operation = client.insert(
            # Calling method insert() on client to create Instance.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.InsertInstanceRequest, dict] to construct a request
            # message.
            # The request object should be represented using arguments:
            #   instance_resource (google.cloud.compute_v1.types.Instance):
            #       The body resource for this request.
            #   request_id (str):
            #       Optional, request ID to identify requests.
            #   project (str):
            #       Project ID for this request.
            #   zone (str):
            #       The name of the zone for this request.
            #   source_instance_template (str):
            #       Optional, link to Instance Template, that can be used to create new Instance.
            request={
                "instance_resource": body,
                "request_id": request_id,
                "project": project_id,
                "zone": zone,
                "source_instance_template": source_instance_template,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation.name, zone=zone)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance(
        self,
        resource_id: str,
        zone: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Instance:
        """
        Retrieve Instance by project_id and resource_id.

        Must be called with keyword arguments rather than positional.

        :param resource_id: Name of the Instance
        :param zone: Google Cloud zone where the Instance exists
        :param project_id: Google Cloud project ID where the Compute Engine Instance exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: Instance representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/v1/instances
        :rtype: object
        """
        client = self.get_compute_instance_client()
        instance = client.get(
            # Calling method get() on client to get the specified Instance.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.GetInstanceRequest, dict] to construct a request
            # message.
            # The request object should be represented using arguments:
            #   instance (str):
            #       The name of the Instance.
            #   project (str):
            #       Project ID for this request.
            #   zone (str):
            #       The name of the zone for this request.
            request={
                "instance": resource_id,
                "project": project_id,
                "zone": zone,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return instance

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance(
        self,
        resource_id: str,
        zone: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Permanently and irrevocably deletes an Instance.

        It is not possible to delete Instances that are already in use by a managed instance group.
        Must be called with keyword arguments rather than positional.

        :param resource_id: Name of the Compute Engine Instance Template resource.
        :param request_id: Unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again)
            It should be in UUID format as defined in RFC 4122
        :param project_id: Google Cloud project ID where the Compute Engine Instance Template exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param zone: Google Cloud zone where the Instance exists
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_compute_instance_client()
        operation = client.delete(
            # Calling method delete() on client to delete Instance.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.DeleteInstanceRequest, dict] to construct a request
            # message.
            # The request object should be represented using arguments:
            #   instance (str):
            #       Name of the Instance resource to delete.
            #   project (str):
            #       Project ID for this request.
            #   request_id (str):
            #       An optional request ID to identify requests.
            #   zone (str):
            #       The name of the zone for this request.
            request={
                "instance": resource_id,
                "project": project_id,
                "request_id": request_id,
                "zone": zone,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation.name, zone=zone)

    @GoogleBaseHook.fallback_to_default_project_id
    def start_instance(self, zone: str, resource_id: str, project_id: str) -> None:
        """
        Start an existing instance defined by project_id, zone and resource_id.

        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud zone where the instance exists
        :param resource_id: Name of the Compute Engine instance resource
        :param project_id: Optional, Google Cloud project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .instances()
            .start(project=project_id, zone=zone, instance=resource_id)
            .execute(num_retries=self.num_retries)
        )
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(f"Wrong response '{response}' returned - it should contain 'name' field")
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name, zone=zone)

    @GoogleBaseHook.fallback_to_default_project_id
    def stop_instance(self, zone: str, resource_id: str, project_id: str) -> None:
        """
        Stop an instance defined by project_id, zone and resource_id.

        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud zone where the instance exists
        :param resource_id: Name of the Compute Engine instance resource
        :param project_id: Optional, Google Cloud project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .instances()
            .stop(project=project_id, zone=zone, instance=resource_id)
            .execute(num_retries=self.num_retries)
        )
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(f"Wrong response '{response}' returned - it should contain 'name' field")
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name, zone=zone)

    @GoogleBaseHook.fallback_to_default_project_id
    def set_machine_type(self, zone: str, resource_id: str, body: dict, project_id: str) -> None:
        """
        Set machine type of an instance defined by project_id, zone and resource_id.

        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud zone where the instance exists.
        :param resource_id: Name of the Compute Engine instance resource
        :param body: Body required by the Compute Engine setMachineType API,
            as described in
            https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType
        :param project_id: Optional, Google Cloud project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = self._execute_set_machine_type(zone, resource_id, body, project_id)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(f"Wrong response '{response}' returned - it should contain 'name' field")
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name, zone=zone)

    def _execute_set_machine_type(self, zone: str, resource_id: str, body: dict, project_id: str) -> dict:
        return (
            self.get_conn()
            .instances()
            .setMachineType(project=project_id, zone=zone, instance=resource_id, body=body)
            .execute(num_retries=self.num_retries)
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def insert_instance_group_manager(
        self,
        body: dict,
        zone: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Create an Instance Group Managers using the body specified.

        After the group is created, instances in the group are created using the specified Instance Template.
        Must be called with keyword arguments rather than positional.

        :param body: Instance Group Manager representation as an object.
        :param request_id: Unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new Instance Group Managers again)
            It should be in UUID format as defined in RFC 4122
        :param project_id: Google Cloud project ID where the Compute Engine Instance Group Managers exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param zone: Google Cloud zone where the Instance exists
        :param retry: A retry object used to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_compute_instance_group_managers_client()
        operation = client.insert(
            # Calling method insert() on client to create the specified Instance Group Managers.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.InsertInstanceGroupManagerRequest, dict] to construct
            # a request message.
            # The request object should be represented using arguments:
            #   instance_group_manager_resource (google.cloud.compute_v1.types.InstanceGroupManager):
            #       The body resource for this request.
            #   project (str):
            #       Project ID for this request.
            #   zone (str):
            #       The name of the zone where you want to create the managed instance group.
            #   request_id (str):
            #       An optional request ID to identify requests.
            request={
                "instance_group_manager_resource": body,
                "project": project_id,
                "zone": zone,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation.name, zone=zone)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance_group_manager(
        self,
        resource_id: str,
        zone: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> InstanceGroupManager:
        """
        Retrieve Instance Group Manager by project_id, zone and resource_id.

        Must be called with keyword arguments rather than positional.

        :param resource_id: The name of the Managed Instance Group
        :param zone: Google Cloud zone where the Instance Group Managers exists
        :param project_id: Google Cloud project ID where the Compute Engine Instance Group Managers exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: Instance Group Managers representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers
        :rtype: object
        """
        client = self.get_compute_instance_group_managers_client()
        instance_group_manager = client.get(
            # Calling method get() on client to get the specified Instance Group Manager.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.GetInstanceGroupManagerRequest, dict] to construct a
            # request message.
            # The request object should be represented using arguments:
            #   instance_group_manager (str):
            #       The name of the Managed Instance Group.
            #   project (str):
            #       Project ID for this request.
            #   zone (str):
            #       The name of the zone for this request.
            request={
                "instance_group_manager": resource_id,
                "project": project_id,
                "zone": zone,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return instance_group_manager

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance_group_manager(
        self,
        resource_id: str,
        zone: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Permanently and irrevocably deletes Instance Group Managers.

        Must be called with keyword arguments rather than positional.

        :param resource_id: Name of the Compute Engine Instance Group Managers resource.
        :param request_id: Unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again)
            It should be in UUID format as defined in RFC 4122
        :param project_id: Google Cloud project ID where the Compute Engine Instance Group Managers exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param zone: Google Cloud zone where the Instance Group Managers exists
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_compute_instance_group_managers_client()
        operation = client.delete(
            # Calling method delete() on client to delete Instance Group Managers.
            # This method accepts request object as an argument and should be of type
            # Union[google.cloud.compute_v1.types.DeleteInstanceGroupManagerRequest, dict] to construct a
            # request message.
            # The request object should be represented using arguments:
            #   instance_group_manager (str):
            #       Name of the Instance resource to delete.
            #   project (str):
            #       Project ID for this request.
            #   request_id (str):
            #       An optional request ID to identify requests.
            #   zone (str):
            #       The name of the zone for this request.
            request={
                "instance_group_manager": resource_id,
                "project": project_id,
                "request_id": request_id,
                "zone": zone,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation.name, zone=zone)

    @GoogleBaseHook.fallback_to_default_project_id
    def patch_instance_group_manager(
        self,
        zone: str,
        resource_id: str,
        body: dict,
        project_id: str,
        request_id: str | None = None,
    ) -> None:
        """
        Patches Instance Group Manager with the specified body.

        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud zone where the Instance Group Manager exists
        :param resource_id: Name of the Instance Group Manager
        :param body: Instance Group Manager representation as json-merge-patch object
            according to
            https://cloud.google.com/compute/docs/reference/rest/beta/instanceTemplates/patch
        :param request_id: Optional, unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again).
            It should be in UUID format as defined in RFC 4122
        :param project_id: Optional, Google Cloud project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .instanceGroupManagers()
            .patch(
                project=project_id,
                zone=zone,
                instanceGroupManager=resource_id,
                body=body,
                requestId=request_id,
            )
            .execute(num_retries=self.num_retries)
        )
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(f"Wrong response '{response}' returned - it should contain 'name' field")
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name, zone=zone)

    def _wait_for_operation_to_complete(
        self, project_id: str, operation_name: str, zone: str | None = None
    ) -> None:
        """
        Wait for the named operation to complete - checks status of the async call.

        :param operation_name: name of the operation
        :param zone: optional region of the request (might be None for global operations)
        :param project_id: Google Cloud project ID where the Compute Engine Instance exists.
        :return: None
        """
        service = self.get_conn()
        while True:
            self.log.info("Waiting for Operation to complete...")
            if zone is None:
                operation_response = self._check_global_operation_status(
                    service=service,
                    operation_name=operation_name,
                    project_id=project_id,
                    num_retries=self.num_retries,
                )
            else:
                operation_response = self._check_zone_operation_status(
                    service, operation_name, project_id, zone, self.num_retries
                )
            if operation_response.get("status") == GceOperationStatus.DONE:
                error = operation_response.get("error")
                if error:
                    code = operation_response.get("httpErrorStatusCode")
                    msg = operation_response.get("httpErrorMessage")
                    # Extracting the errors list as string and trimming square braces
                    error_msg = str(error.get("errors"))[1:-1]

                    raise AirflowException(f"{code} {msg}: " + error_msg)
                break
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)

    @staticmethod
    def _check_zone_operation_status(
        service: Any, operation_name: str, project_id: str, zone: str, num_retries: int
    ) -> dict:
        return (
            service.zoneOperations()
            .get(project=project_id, zone=zone, operation=operation_name)
            .execute(num_retries=num_retries)
        )

    @staticmethod
    def _check_global_operation_status(
        service: Any, operation_name: str, project_id: str, num_retries: int
    ) -> dict:
        return (
            service.globalOperations()
            .get(project=project_id, operation=operation_name)
            .execute(num_retries=num_retries)
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance_info(self, zone: str, resource_id: str, project_id: str) -> dict[str, Any]:
        """
        Get instance information.

        :param zone: Google Cloud zone where the Instance Group Manager exists
        :param resource_id: Name of the Instance Group Manager
        :param project_id: Optional, Google Cloud project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        """
        instance_info = (
            self.get_conn()
            .instances()
            .get(project=project_id, instance=resource_id, zone=zone)
            .execute(num_retries=self.num_retries)
        )
        return instance_info

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance_address(
        self, zone: str, resource_id: str, project_id: str = PROVIDE_PROJECT_ID, use_internal_ip: bool = False
    ) -> str:
        """
        Return network address associated to instance.

        :param zone: Google Cloud zone where the Instance Group Manager exists
        :param resource_id: Name of the Instance Group Manager
        :param project_id: Optional, Google Cloud project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :param use_internal_ip: If true, return private IP address.
        """
        instance_info = self.get_instance_info(project_id=project_id, resource_id=resource_id, zone=zone)
        if use_internal_ip:
            return instance_info["networkInterfaces"][0].get("networkIP")

        access_config = instance_info["networkInterfaces"][0].get("accessConfigs")
        if access_config:
            return access_config[0].get("natIP")
        raise AirflowException("The target instance does not have external IP")

    @GoogleBaseHook.fallback_to_default_project_id
    def set_instance_metadata(
        self, zone: str, resource_id: str, metadata: dict[str, str], project_id: str
    ) -> None:
        """
        Set instance metadata.

        :param zone: Google Cloud zone where the Instance Group Manager exists
        :param resource_id: Name of the Instance Group Manager
        :param metadata: The new instance metadata.
        :param project_id: Optional, Google Cloud project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        """
        response = (
            self.get_conn()
            .instances()
            .setMetadata(project=project_id, zone=zone, instance=resource_id, body=metadata)
            .execute(num_retries=self.num_retries)
        )
        operation_name = response["name"]
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name, zone=zone)
