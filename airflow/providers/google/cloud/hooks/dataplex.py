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

"""This module contains Google Dataplex hook."""
import os
from time import sleep
from typing import Any, Dict, Optional, Sequence, Union

from google.api_core.retry import exponential_sleep_generator
from googleapiclient.discovery import Resource, build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

API_KEY = os.environ.get("GCP_API_KEY", "INVALID API KEY")


class DataplexHook(GoogleBaseHook):
    """Hook for Google Dataplex."""

    _conn = None  # type: Optional[Resource]

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
        self.api_key = API_KEY
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """Retrieves connection to Dataplex."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "dataplex",
                self.api_version,
                developerKey=self.api_key,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def wait_for_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Waits for long-lasting operation to complete."""
        for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
            sleep(time_to_wait)
            operation = (
                self.get_conn().projects().locations().operations().get(name=operation.get("name")).execute()
            )
            if operation.get("done"):
                break
        if "error" in operation:
            raise AirflowException(operation["error"])
        return operation["response"]

    @GoogleBaseHook.fallback_to_default_project_id
    def create_task(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        body: Dict[str, Any],
        dataplex_task_id: str,
        validate_only: Optional[bool] = None,
    ) -> Any:
        """
        Creates a task resource within a lake.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :type region: str
        :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
        :type lake_id: str
        :param body: Required. The Request body contains an instance of Task.
        :type body: Dict[str, Any]
        :param dataplex_task_id: Required. Task identifier.
        :type dataplex_task_id: str
        :param validate_only: Optional. Only validate the request, but do not perform mutations.
            The default is false.
        :type validate_only: bool
        """
        parent = f'projects/{project_id}/locations/{region}/lakes/{lake_id}'
        response = (
            self.get_conn()
            .projects()
            .locations()
            .lakes()
            .tasks()
            .create(  # pylint: disable=no-member
                parent=parent, body=body, taskId=dataplex_task_id, validateOnly=validate_only
            )
            .execute(num_retries=self.num_retries)
        )
        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_task(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
    ) -> Any:
        """
        Delete the task resource.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :type region: str
        :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
        :type lake_id: str
        :param dataplex_task_id: Required. The ID of the Google Cloud task to be deleted.
        :type dataplex_task_id: str
        """
        name = f'projects/{project_id}/locations/{region}/lakes/{lake_id}/tasks/{dataplex_task_id}'
        response = (
            self.get_conn()
            .projects()
            .locations()
            .lakes()
            .tasks()
            .delete(name=name)  # pylint: disable=no-member
            .execute(num_retries=self.num_retries)
        )
        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def list_tasks(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        filter: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> Any:
        """
        Lists tasks under the given lake.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :type region: str
        :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
        :type lake_id: str
        :param page_size: Optional. Maximum number of tasks to return. The service may return fewer than this
            value. If unspecified, at most 10 tasks will be returned. The maximum value is 1000;
            values above 1000 will be coerced to 1000.
        :type page_size: Optional[int]
        :param page_token: Optional. Page token received from a previous ListZones call. Provide this to
            retrieve the subsequent page. When paginating, all other parameters provided to ListZones must
            match the call that provided the page token.
        :type page_token: Optional[str]
        :param filter: Optional. Filter request.
        :type filter: Optional[str]
        :param order_by: Optional. Order by fields for the result.
        :type order_by: Optional[str]
        """
        parent = f'projects/{project_id}/locations/{region}/lakes/{lake_id}'
        response = (
            self.get_conn()
            .projects()
            .locations()
            .lakes()
            .tasks()
            .list(  # pylint: disable=no-member
                parent=parent, pageSize=page_size, pageToken=page_token, filter=filter, orderBy=order_by
            )
            .execute(num_retries=self.num_retries)
        )
        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def get_task(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
    ) -> Any:
        """
        Get task resource.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :type region: str
        :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
        :type lake_id: str
        :param dataplex_task_id: Required. The ID of the Google Cloud task to be retrieved.
        :type dataplex_task_id: str
        """
        name = f'projects/{project_id}/locations/{region}/lakes/{lake_id}/tasks/{dataplex_task_id}'
        response = (
            self.get_conn()
            .projects()
            .locations()
            .lakes()
            .tasks()
            .get(name=name)  # pylint: disable=no-member
            .execute(num_retries=self.num_retries)
        )
        return response
