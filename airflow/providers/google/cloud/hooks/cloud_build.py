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
from typing import Any, Optional, Sequence, Union

from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 5


class CloudBuildHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Build APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param api_version: API version used (for example v1 or v1beta1).
    :type api_version: str
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

    _conn = None  # type: Optional[Any]

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

        self.api_version = api_version

    def get_conn(self) -> build:
        """
        Retrieves the connection to Cloud Build.

        :return: Google Cloud Build services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("cloudbuild", self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def create_build(self, body: dict, project_id: str) -> dict:
        """
        Starts a build with the specified configuration.

        :param body: The request body.
            See: https://cloud.google.com/cloud-build/docs/api/reference/rest/v1/projects.builds
        :type body: dict
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :return: Dict
        """
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
                service.operations()
                .get(name=operation_name)
                .execute(num_retries=self.num_retries)
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
