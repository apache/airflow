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

from typing import Any, Dict, List

from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleDeploymentManagerHook(GoogleBaseHook):  # pylint: disable=abstract-method
    """
    Interact with Google Cloud Deployment Manager using the Google Cloud Platform connection.
    This allows for scheduled and programatic inspection and deletion fo resources managed by GDM.
    """

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        super(GoogleDeploymentManagerHook, self).__init__(gcp_conn_id, delegate_to=delegate_to)

    def get_conn(self):
        """
        Returns a Google Deployment Manager service object.

        :rtype: googleapiclient.discovery.Resource
        """
        http_authorized = self._authorize()
        return build('deploymentmanager', 'v2', http=http_authorized, cache_discovery=False)

    def list_deployments(self, project, deployment_filter=None,
                         max_results=None, order_by=None,
                         page_token=None) -> List[Dict[str, Any]]:  # pylint: disable=too-many-arguments
        """
        Lists deployments in a google cloud project.

        :param project: The project ID for this request.
        :type key_name: str
        :param filter: A filter expression which limits resources returned in the response.
        :type filter: string
        :param max_results: The maximum number of results to return
        :type max_results: int
        :param order_by: A filed name to order by, ex: "creationTimestamp desc"
        :type order_by: string
        :param page_token: specifies a page_token to use
        :type page_token: string

        :rtype: list
        """
        client = self.get_conn()
        request = client.deployments().list(project=project,    # pylint: disable=no-member
                                            filter=deployment_filter,
                                            maxResults=max_results,
                                            orderBy=order_by,
                                            pageToken=page_token)
        try:
            deployments = request.execute()['deployments']
        except KeyError:
            return []
        return deployments

    def delete_deployment(self, project: str, deployment: str, delete_policy: str = "DELETE"):
        """
        Deletes a deployment and all associated resources in a google cloud project.

        :param project: The project ID for this request.
        :type project: str
        :param deployment: The name of the deployment for this request.
        :type deployment: string
        :param delete_policy: Sets the policy to use for deleting resources. (ABANDON | DELETE)
        :type delete_policy: string

        :rtype: None
        """
        client = self.get_conn()
        request = client.deployments().delete(project=project,  # pylint: disable=no-member
                                              deployment=deployment,
                                              deletePolicy=delete_policy)
        resp = request.execute()
        if 'error' in resp.keys():
            raise AirflowException('Errors deleting deployment: ', ', '.join(resp['error']['errors']))
