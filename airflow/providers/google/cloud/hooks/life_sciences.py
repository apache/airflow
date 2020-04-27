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
"""Hook for Google Cloud Life Sciences service"""

import time
from typing import Any, Dict, Optional

import google.api_core.path_template
from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 5


# noinspection PyAbstractClass
class LifeSciencesHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Life Sciences APIs.

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

    _conn = None  # type: Optional[Any]

    def __init__(
        self,
        api_version: str = "v2beta",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """
        Retrieves the connection to Cloud Life Sciences.

        :return: Google Cloud Life Sciences service object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("lifesciences", self.api_version,
                               http=http_authorized, cache_discovery=False)
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def run_pipeline(self, body: Dict, location: str, project_id: str):
        """
        Runs a pipeline

        :param body: The request body.
        :type body: dict
        :param location: The location of the project. For example: "us-east1".
        :type location: str
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :rtype: dict
        """
        parent = self._location_path(project_id=project_id, location=location)
        service = self.get_conn()

        request = (service.projects()  # pylint: disable=no-member
                   .locations()
                   .pipelines()
                   .run(parent=parent, body=body)
                   )

        response = request.execute(num_retries=self.num_retries)

        # wait
        operation_name = response['name']
        self._wait_for_operation_to_complete(operation_name)

        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def _location_path(self, project_id: str, location: str):
        """
        Return a location string.

        :param project_id: Optional, Google Cloud Project project_id where the
            function belongs. If set to None or missing, the default project_id
            from the GCP connection is used.
        :type project_id: str
        :param location: The location of the project. For example: "us-east1".
        :type location: str
        """
        return google.api_core.path_template.expand(
            'projects/{project}/locations/{location}',
            project=project_id,
            location=location,
        )

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
            operation_response = (service.projects()  # pylint: disable=no-member
                                  .locations()
                                  .operations()
                                  .get(name=operation_name)
                                  .execute(num_retries=self.num_retries))
            self.log.info('Waiting for pipeline operation to complete')
            if operation_response.get("done"):
                response = operation_response.get("response")
                error = operation_response.get("error")
                # Note, according to documentation always either response or error is
                # set when "done" == True
                if error:
                    raise AirflowException(str(error))
                return response
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)
