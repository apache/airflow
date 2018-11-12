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

# Number of retries - used by googleapiclient method calls to perform retries
# For requests that are "retriable"
NUM_RETRIES = 5

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
    """
    _conn = None

    def __init__(self,
                 api_version,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GceHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

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

    def start_instance(self, project_id, zone, resource_id):
        """
        Starts an existing instance defined by project_id, zone and resource_id.

        :param project_id: Google Cloud Platform project ID where the Compute Engine
            Instance exists
        :type project_id: str
        :param zone: Google Cloud Platform zone where the instance exists
        :type zone: str
        :param resource_id: Name of the Compute Engine instance resource
        :type resource_id: str
        :return: True if the operation succeeded, raises an error otherwise.
        :rtype: bool
        """
        response = self.get_conn().instances().start(
            project=project_id,
            zone=zone,
            instance=resource_id
        ).execute(num_retries=NUM_RETRIES)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        return self._wait_for_operation_to_complete(project_id, operation_name, zone)

    def stop_instance(self, project_id, zone, resource_id):
        """
        Stops an instance defined by project_id, zone and resource_id

        :param project_id: Google Cloud Platform project ID where the Compute Engine
            Instance exists
        :type project_id: str
        :param zone: Google Cloud Platform zone where the instance exists
        :type zone: str
        :param resource_id: Name of the Compute Engine instance resource
        :type resource_id: str
        :return: True if the operation succeeded, raises an error otherwise.
        :rtype: bool
        """
        response = self.get_conn().instances().stop(
            project=project_id,
            zone=zone,
            instance=resource_id
        ).execute(num_retries=NUM_RETRIES)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        return self._wait_for_operation_to_complete(project_id, operation_name, zone)

    def set_machine_type(self, project_id, zone, resource_id, body):
        """
        Sets machine type of an instance defined by project_id, zone and resource_id.

        :param project_id: Google Cloud Platform project ID where the Compute Engine
            Instance exists
        :type project_id: str
        :param zone: Google Cloud Platform zone where the instance exists.
        :type zone: str
        :param resource_id: Name of the Compute Engine instance resource
        :type resource_id: str
        :param body: Body required by the Compute Engine setMachineType API,
            as described in
            https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType
        :type body: dict
        :return: True if the operation succeeded, raises an error otherwise.
        :rtype: bool
        """
        response = self._execute_set_machine_type(project_id, zone, resource_id, body)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        return self._wait_for_operation_to_complete(project_id, operation_name, zone)

    def _execute_set_machine_type(self, project_id, zone, resource_id, body):
        return self.get_conn().instances().setMachineType(
            project=project_id, zone=zone, instance=resource_id, body=body)\
            .execute(num_retries=NUM_RETRIES)

    def get_instance_template(self, project_id, resource_id):
        """
        Retrieves instance template by project_id and resource_id.

        :param project_id: Google Cloud Platform project ID where the Compute Engine
            Instance template exists
        :type project_id: str
        :param resource_id: Name of the instance template
        :type resource_id: str
        :return: Instance template representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
        :rtype: dict
        """
        response = self.get_conn().instanceTemplates().get(
            project=project_id,
            instanceTemplate=resource_id
        ).execute(num_retries=NUM_RETRIES)
        return response

    def insert_instance_template(self, project_id, body, request_id=None):
        """
        Inserts instance template using body specified

        :param project_id: Google Cloud Platform project ID where the Compute Engine
            Instance exists
        :type project_id: str
        :param body: Instance template representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
        :type body: dict
        :param request_id: Optional, unique request_id that you might add to achieve
            full idempotence (for example when client call times out repeating the request
            with the same request id will not create a new instance template again)
            It should be in UUID format as defined in RFC 4122
        :type request_id: str
        :return: True if the operation succeeded
        :rtype: bool
        """
        response = self.get_conn().instanceTemplates().insert(
            project=project_id,
            body=body,
            requestId=request_id
        ).execute(num_retries=NUM_RETRIES)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        return self._wait_for_operation_to_complete(project_id, operation_name)

    def get_instance_group_manager(self, project_id, zone, resource_id):
        """
        Retrieves Instance Group Manager by project_id, zone and resource_id.

        :param project_id: Google Cloud Platform project ID where the Compute Engine
            Instance Group Manager exists
        :type project_id: str
        :param zone: Google Cloud Platform zone where the Instance Group Manager exists
        :type zone: str
        :param resource_id: Name of the Instance Group Manager
        :type resource_id: str
        :return: Instance group manager representation as object according to
            https://cloud.google.com/compute/docs/reference/rest/beta/instanceGroupManagers
        :rtype: dict
        """
        response = self.get_conn().instanceGroupManagers().get(
            project=project_id,
            zone=zone,
            instanceGroupManager=resource_id
        ).execute(num_retries=NUM_RETRIES)
        return response

    def patch_instance_group_manager(self, project_id, zone, resource_id,
                                     body, request_id=None):
        """
        Patches Instance Group Manager with the specified body.

        :param project_id: Google Cloud Platform project ID where the Compute Engine
            Instance Group Manager exists
        :type project_id: str
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
        :return: True if the operation succeeded
        :rtype: bool
        """
        response = self.get_conn().instanceGroupManagers().patch(
            project=project_id,
            zone=zone,
            instanceGroupManager=resource_id,
            body=body,
            requestId=request_id
        ).execute(num_retries=NUM_RETRIES)
        try:
            operation_name = response["name"]
        except KeyError:
            raise AirflowException(
                "Wrong response '{}' returned - it should contain "
                "'name' field".format(response))
        return self._wait_for_operation_to_complete(project_id, operation_name, zone)

    def _wait_for_operation_to_complete(self, project_id, operation_name, zone=None):
        """
        Waits for the named operation to complete - checks status of the async call.

        :param operation_name: name of the operation
        :type operation_name: str
        :param zone: optional region of the request (might be None for global operations)
        :type zone: str
        :return: True if the operation succeeded, raises an error otherwise
        :rtype: bool
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
                    service, operation_name, project_id, zone)
            if operation_response.get("status") == GceOperationStatus.DONE:
                error = operation_response.get("error")
                if error:
                    code = operation_response.get("httpErrorStatusCode")
                    msg = operation_response.get("httpErrorMessage")
                    # Extracting the errors list as string and trimming square braces
                    error_msg = str(error.get("errors"))[1:-1]
                    raise AirflowException("{} {}: ".format(code, msg) + error_msg)
                # No meaningful info to return from the response in case of success
                return True
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)

    @staticmethod
    def _check_zone_operation_status(service, operation_name, project_id, zone):
        return service.zoneOperations().get(
            project=project_id, zone=zone, operation=operation_name).execute(
            num_retries=NUM_RETRIES)

    @staticmethod
    def _check_global_operation_status(service, operation_name, project_id):
        return service.globalOperations().get(
            project=project_id, operation=operation_name).execute(
            num_retries=NUM_RETRIES)
