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
#

from typing import Any, Dict, List, Optional, Sequence, Union

from googleapiclient.discovery import Resource, build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleDeploymentManagerHook(GoogleBaseHook):
    """
    Interact with Google Cloud Deployment Manager using the Google Cloud connection.
    This allows for scheduled and programmatic inspection and deletion fo resources managed by GDM.
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

    def get_conn(self) -> Resource:
        """
        Returns a Google Deployment Manager service object.

        :rtype: googleapiclient.discovery.Resource
        """
        http_authorized = self._authorize()
        return build('deploymentmanager', 'v2', http=http_authorized, cache_discovery=False)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_deployments(
        self,
        project_id: Optional[str] = None,
        deployment_filter: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Lists deployments in a google cloud project.

        :param project_id: The project ID for this request.
        :param deployment_filter: A filter expression which limits resources returned in the response.
        :param order_by: A field name to order by, ex: "creationTimestamp desc"
        :rtype: list
        """
        deployments = []  # type: List[Dict]
        conn = self.get_conn()

        request = conn.deployments().list(project=project_id, filter=deployment_filter, orderBy=order_by)

        while request is not None:
            response = request.execute(num_retries=self.num_retries)
            deployments.extend(response.get("deployments", []))
            request = conn.deployments().list_next(previous_request=request, previous_response=response)

        return deployments

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_deployment(
        self, project_id: Optional[str], deployment: Optional[str] = None, delete_policy: Optional[str] = None
    ) -> None:
        """
        Deletes a deployment and all associated resources in a google cloud project.

        :param project_id: The project ID for this request.
        :param deployment: The name of the deployment for this request.
        :param delete_policy: Sets the policy to use for deleting resources. (ABANDON | DELETE)

        :rtype: None
        """
        conn = self.get_conn()

        request = conn.deployments().delete(
            project=project_id, deployment=deployment, deletePolicy=delete_policy
        )
        resp = request.execute()
        if 'error' in resp.keys():
            raise AirflowException(
                'Errors deleting deployment: ', ', '.join(err['message'] for err in resp['error']['errors'])
            )
