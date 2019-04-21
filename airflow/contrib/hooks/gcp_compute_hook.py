# -*- coding: utf-8 -*-
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

import time
from googleapiclient.discovery import build

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 1


class GceOperationStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"


# noinspection PyAbstractClass
class GceHook(GoogleCloudBaseHook):
    """
    Hook for Google Compute Engine APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    """
    _conn = None

    def __init__(self,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version
        self.num_retries = self._get_field('num_retries', 5)

    def get_conn(self):
        """
        Retrieves connection to Google Compute Engine.

        :return: Google Compute Engine services object
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('compute', self.api_version,
                               http=http_authorized, cache_discovery=False)
        return self._conn

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def start_instance(self, zone, resource_id, project_id=None):
        """
        Starts an existing instance defined by project_id, zone and resource_id.
        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud Platform zone where the instance exists
        :type zone: str
        :param resource_id: Name of the Compute Engine instance resource
        :type resource_id: str
        :param project_id: Optional, Google Cloud Platform project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        response = self.get_conn().instances().start(
            project=project_id,
            zone=zone,
            instance=resource_id
        ).execute(num_retries=self.num_retries)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        self._wait_for_operation_to_complete(project_id=project_id,
                                             operation_name=operation_name,
                                             zone=zone)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def stop_instance(self, zone, resource_id, project_id=None):
        """
        Stops an instance defined by project_id, zone and resource_id
        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud Platform zone where the instance exists
        :type zone: str
        :param resource_id: Name of the Compute Engine instance resource
        :type resource_id: str
        :param project_id: Optional, Google Cloud Platform project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        response = self.get_conn().instances().stop(
            project=project_id,
            zone=zone,
            instance=resource_id
        ).execute(num_retries=self.num_retries)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        self._wait_for_operation_to_complete(project_id=project_id,
                                             operation_name=operation_name,
                                             zone=zone)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def set_machine_type(self, zone, resource_id, body, project_id=None):
        """
        Sets machine type of an instance defined by project_id, zone and resource_id.
        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud Platform zone where the instance exists.
        :type zone: str
        :param resource_id: Name of the Compute Engine instance resource
        :type resource_id: str
        :param body: Body required by the Compute Engine setMachineType API,
            as described in
            https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType
        :type body: dict
        :param project_id: Optional, Google Cloud Platform project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        response = self._execute_set_machine_type(zone, resource_id, body, project_id)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        self._wait_for_operation_to_complete(project_id=project_id,
                                             operation_name=operation_name,
                                             zone=zone)

    def _execute_set_machine_type(self, zone, resource_id, body, project_id):
        return self.get_conn().instances().setMachineType(
            project=project_id, zone=zone, instance=resource_id, body=body)\
            .execute(num_retries=self.num_retries)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_instance_template(self, resource_id, project_id=None):
        """
        Retrieves instance template by project_id and resource_id.
        Must be called with keyword arguments rather than positional.

        :param resource_id: Name of the instance template
        :type resource_id: str
        :param project_id: Optional, Google Cloud Platform project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        :return: Instance template representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
        :rtype: dict
        """
        response = self.get_conn().instanceTemplates().get(
            project=project_id,
            instanceTemplate=resource_id
        ).execute(num_retries=self.num_retries)
        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def insert_instance_template(self, body, request_id=None, project_id=None):
        """
        Inserts instance template using body specified
        Must be called with keyword arguments rather than positional.

        :param body: Instance template representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
        :type body: dict
        :param request_id: Optional, unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again)
            It should be in UUID format as defined in RFC 4122
        :type request_id: str
        :param project_id: Optional, Google Cloud Platform project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        response = self.get_conn().instanceTemplates().insert(
            project=project_id,
            body=body,
            requestId=request_id
        ).execute(num_retries=self.num_retries)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        self._wait_for_operation_to_complete(project_id=project_id,
                                             operation_name=operation_name)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_instance_group_manager(self, zone, resource_id, project_id=None):
        """
        Retrieves Instance Group Manager by project_id, zone and resource_id.
        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud Platform zone where the Instance Group Manager exists
        :type zone: str
        :param resource_id: Name of the Instance Group Manager
        :type resource_id: str
        :param project_id: Optional, Google Cloud Platform project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        :return: Instance group manager representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/beta/instanceGroupManagers
        :rtype: dict
        """
        response = self.get_conn().instanceGroupManagers().get(
            project=project_id,
            zone=zone,
            instanceGroupManager=resource_id
        ).execute(num_retries=self.num_retries)
        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def patch_instance_group_manager(self, zone, resource_id,
                                     body, request_id=None, project_id=None):
        """
        Patches Instance Group Manager with the specified body.
        Must be called with keyword arguments rather than positional.

        :param zone: Google Cloud Platform zone where the Instance Group Manager exists
        :type zone: str
        :param resource_id: Name of the Instance Group Manager
        :type resource_id: str
        :param body: Instance Group Manager representation as json-merge-patch object
            according to
            https://cloud.google.com/compute/docs/reference/rest/beta/instanceTemplates/patch
        :type body: dict
        :param request_id: Optional, unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again).
            It should be in UUID format as defined in RFC 4122
        :type request_id: str
        :param project_id: Optional, Google Cloud Platform project ID where the
            Compute Engine Instance exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        response = self.get_conn().instanceGroupManagers().patch(
            project=project_id,
            zone=zone,
            instanceGroupManager=resource_id,
            body=body,
            requestId=request_id
        ).execute(num_retries=self.num_retries)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        self._wait_for_operation_to_complete(project_id=project_id,
                                             operation_name=operation_name,
                                             zone=zone)

    def _wait_for_operation_to_complete(self, project_id, operation_name, zone=None):
        """
        Waits for the named operation to complete - checks status of the async call.

        :param operation_name: name of the operation
        :type operation_name: str
        :param zone: optional region of the request (might be None for global operations)
        :type zone: str
        :return: None
        """
        service = self.get_conn()
        while True:
            if zone is None:
                # noinspection PyTypeChecker
                operation_response = self._check_global_operation_status(
                    service, operation_name, project_id)
            else:
                # noinspection PyTypeChecker
                operation_response = self._check_zone_operation_status(
                    service, operation_name, project_id, zone, self.num_retries)
            if operation_response.get("status") == GceOperationStatus.DONE:
                error = operation_response.get("error")
                if error:
                    code = operation_response.get("httpErrorStatusCode")
                    msg = operation_response.get("httpErrorMessage")
                    # Extracting the errors list as string and trimming square braces
                    error_msg = str(error.get("errors"))[1:-1]
                    raise AirflowException("{} {}: ".format(code, msg) + error_msg)
                # No meaningful info to return from the response in case of success
                return
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)

    @staticmethod
    def _check_zone_operation_status(service, operation_name, project_id, zone, num_retries):
        return service.zoneOperations().get(
            project=project_id, zone=zone, operation=operation_name).execute(
            num_retries=num_retries)

    @staticmethod
    def _check_global_operation_status(service, operation_name, project_id, num_retries):
        return service.globalOperations().get(
            project=project_id, operation=operation_name).execute(
            num_retries=num_retries)
