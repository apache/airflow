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
"""Hook for Google Cloud Firestore service"""

import time
from typing import Any, Dict, Optional, Sequence, Union

from googleapiclient.discovery import build, build_from_document

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 5


# noinspection PyAbstractClass
class CloudFirestoreHook(GoogleBaseHook):
    """
    Hook for the Google Firestore APIs.

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

    def get_conn(self):
        """
        Retrieves the connection to Cloud Firestore.

        :return: Google Cloud Firestore services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            # We cannot use an Authorized Client to retrieve discovery document due to an error in the API.
            # When the authorized customer will send a request to the address below
            # https://www.googleapis.com/discovery/v1/apis/firestore/v1/rest
            # then it will get the message below:
            # > Request contains an invalid argument.
            # At the same time, the Non-Authorized Client has no problems.
            non_authorized_conn = build("firestore", self.api_version, cache_discovery=False)
            self._conn = build_from_document(
                non_authorized_conn._rootDesc,  # pylint: disable=protected-access
                http=http_authorized
            )
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def export_documents(
        self, body: Dict, database_id: str = "(default)", project_id: Optional[str] = None
    ) -> None:
        """
        Starts a export with the specified configuration.

        :param database_id: The Database ID.
        :type database_id: str
        :param body: The request body.
            See:
            https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases/exportDocuments
        :type body: dict
        :param project_id: Optional, Google Cloud Project project_id where the database belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        """
        service = self.get_conn()

        name = f"projects/{project_id}/databases/{database_id}"

        operation = (
            service.projects()  # pylint: disable=no-member
            .databases()
            .exportDocuments(name=name, body=body)
            .execute(num_retries=self.num_retries)
        )

        self._wait_for_operation_to_complete(operation["name"])

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
                service.projects()  # pylint: disable=no-member
                .databases()
                .operations()
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
