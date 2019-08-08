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
"""Hook for Google Cloud Build service"""

import time
from typing import Dict

from googleapiclient.discovery import build

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 5


# noinspection PyAbstractClass
class CloudBuildHook(GoogleCloudBaseHook):
    """
    Hook for the Google Cloud Build APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param api_version: API version used (for example v1 or v1beta1).
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    _conn = None

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str = None
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version
        self.num_retries = self._get_field("num_retries", 5)

    def get_conn(self):
        """
        Retrieves the connection to Cloud Functions.

        :return: Google Cloud Build services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("cloudbuild", self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_build(self, body: Dict, project_id: str = None) -> Dict:
        """
        Starts a build with the specified configuration.

        :param body: The request body.
            See: https://cloud.google.com/cloud-build/docs/api/reference/rest/Shared.Types/Build
        :type body: dict
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :return: Dict
        """
        assert project_id is not None
        service = self.get_conn()

        # Create build
        response = (
            service.projects()  # pylint: disable=no-member
            .builds()
            .create(projectId=project_id, body=body)
            .execute(num_retries=self.num_retries)
        )

        # Wait
        operation_name = response["name"]
        self._wait_for_operation_to_complete(operation_name=operation_name)

        # Get result
        build_id = response["metadata"]["build"]["id"]

        result = (
            service.projects()  # pylint: disable=no-member
            .builds()
            .get(projectId=project_id, id=build_id)
            .execute(num_retries=self.num_retries)
        )

        return result

    def _wait_for_operation_to_complete(self, operation_name: str) -> None:
        """
        Waits for the named operation to complete - checks status of the
        asynchronous call.

        :param operation_name: The name of the operation.
        :type operation_name: str
        :return: The response returned by the operation.
        :rtype: dict
        :exception: AirflowException in case error is returned.
        """
        service = self.get_conn()
        while True:
            operation_response = (
                # pylint: disable=no-member
                service.operations().get(name=operation_name).execute(num_retries=self.num_retries)
            )
            if operation_response.get("done"):
                response = operation_response.get("response")
                error = operation_response.get("error")
                # Note, according to documentation always either response or error is
                # set when "done" == True
                if error:
                    raise AirflowException(str(error))
                return response
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)
