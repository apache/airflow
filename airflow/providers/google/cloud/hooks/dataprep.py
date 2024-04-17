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
"""This module contains Google Dataprep hook."""

from __future__ import annotations

import json
from enum import Enum
from typing import Any
from urllib.parse import urljoin

import requests
from requests import HTTPError
from tenacity import retry, stop_after_attempt, wait_exponential

from airflow.hooks.base import BaseHook


def _get_field(extras: dict, field_name: str):
    """Get field from extra, first checking short name, then for backcompat we check for prefixed name."""
    backcompat_prefix = "extra__dataprep__"
    if field_name.startswith("extra__"):
        raise ValueError(
            f"Got prefixed name {field_name}; please remove the '{backcompat_prefix}' prefix "
            "when using this method."
        )
    if field_name in extras:
        return extras[field_name] or None
    prefixed_name = f"{backcompat_prefix}{field_name}"
    return extras.get(prefixed_name) or None


class JobGroupStatuses(str, Enum):
    """Types of job group run statuses."""

    CREATED = "Created"
    UNDEFINED = "undefined"
    IN_PROGRESS = "InProgress"
    COMPLETE = "Complete"
    FAILED = "Failed"
    CANCELED = "Canceled"


class GoogleDataprepHook(BaseHook):
    """
    Hook for connection with Dataprep API.

    To get connection Dataprep with Airflow you need Dataprep token.

    https://clouddataprep.com/documentation/api#section/Authentication

    It should be added to the Connection in Airflow in JSON format.

    """

    conn_name_attr = "dataprep_conn_id"
    default_conn_name = "google_cloud_dataprep_default"
    conn_type = "dataprep"
    hook_name = "Google Dataprep"

    def __init__(self, dataprep_conn_id: str = default_conn_name, api_version: str = "v4") -> None:
        super().__init__()
        self.dataprep_conn_id = dataprep_conn_id
        self.api_version = api_version
        conn = self.get_connection(self.dataprep_conn_id)
        extras = conn.extra_dejson
        self._token = _get_field(extras, "token")
        self._base_url = _get_field(extras, "base_url") or "https://api.clouddataprep.com"

    @property
    def _headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._token}",
        }
        return headers

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def get_jobs_for_job_group(self, job_id: int) -> dict[str, Any]:
        """
        Get information about the batch jobs within a Cloud Dataprep job.

        :param job_id: The ID of the job that will be fetched
        """
        endpoint_path = f"{self.api_version}/jobGroups/{job_id}/jobs"
        url: str = urljoin(self._base_url, endpoint_path)
        response = requests.get(url, headers=self._headers)
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def get_job_group(self, job_group_id: int, embed: str, include_deleted: bool) -> dict[str, Any]:
        """
        Get the specified job group.

        A job group is a job that is executed from a specific node in a flow.

        :param job_group_id: The ID of the job that will be fetched
        :param embed: Comma-separated list of objects to pull in as part of the response
        :param include_deleted: if set to "true", will include deleted objects
        """
        params: dict[str, Any] = {"embed": embed, "includeDeleted": include_deleted}
        endpoint_path = f"{self.api_version}/jobGroups/{job_group_id}"
        url: str = urljoin(self._base_url, endpoint_path)
        response = requests.get(url, headers=self._headers, params=params)
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def run_job_group(self, body_request: dict) -> dict[str, Any]:
        """
        Create a ``jobGroup``, which launches the specified job as the authenticated user.

        This performs the same action as clicking on the Run Job button in the application.

        To get recipe_id please follow the Dataprep API documentation
        https://clouddataprep.com/documentation/api#operation/runJobGroup.

        :param body_request: The identifier for the recipe you would like to run.
        """
        endpoint_path = f"{self.api_version}/jobGroups"
        url: str = urljoin(self._base_url, endpoint_path)
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def create_flow(self, *, body_request: dict) -> dict:
        """
        Create flow.

        :param body_request: Body of the POST request to be sent.
            For more details check https://clouddataprep.com/documentation/api#operation/createFlow
        """
        endpoint = f"/{self.api_version}/flows"
        url: str = urljoin(self._base_url, endpoint)
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def copy_flow(
        self, *, flow_id: int, name: str = "", description: str = "", copy_datasources: bool = False
    ) -> dict:
        """
        Create a copy of the provided flow id, as well as all contained recipes.

        :param flow_id: ID of the flow to be copied
        :param name: Name for the copy of the flow
        :param description: Description of the copy of the flow
        :param copy_datasources: Bool value to define should copies of data inputs be made or not.
        """
        endpoint_path = f"{self.api_version}/flows/{flow_id}/copy"
        url: str = urljoin(self._base_url, endpoint_path)
        body_request = {
            "name": name,
            "description": description,
            "copyDatasources": copy_datasources,
        }
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def delete_flow(self, *, flow_id: int) -> None:
        """
        Delete the flow with the provided id.

        :param flow_id: ID of the flow to be copied
        """
        endpoint_path = f"{self.api_version}/flows/{flow_id}"
        url: str = urljoin(self._base_url, endpoint_path)
        response = requests.delete(url, headers=self._headers)
        self._raise_for_status(response)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def run_flow(self, *, flow_id: int, body_request: dict) -> dict:
        """
        Run the flow with the provided id copy of the provided flow id.

        :param flow_id: ID of the flow to be copied
        :param body_request: Body of the POST request to be sent.
        """
        endpoint = f"{self.api_version}/flows/{flow_id}/run"
        url: str = urljoin(self._base_url, endpoint)
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def get_job_group_status(self, *, job_group_id: int) -> JobGroupStatuses:
        """
        Check the status of the Dataprep task to be finished.

        :param job_group_id: ID of the job group to check
        """
        endpoint = f"/{self.api_version}/jobGroups/{job_group_id}/status"
        url: str = urljoin(self._base_url, endpoint)
        response = requests.get(url, headers=self._headers)
        self._raise_for_status(response)
        return response.json()

    def _raise_for_status(self, response: requests.models.Response) -> None:
        try:
            response.raise_for_status()
        except HTTPError:
            self.log.error(response.json().get("exception"))
            raise

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def create_imported_dataset(self, *, body_request: dict) -> dict:
        """
        Create imported dataset.

        :param body_request: Body of the POST request to be sent.
            For more details check https://clouddataprep.com/documentation/api#operation/createImportedDataset
        """
        endpoint = f"/{self.api_version}/importedDatasets"
        url: str = urljoin(self._base_url, endpoint)
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def create_wrangled_dataset(self, *, body_request: dict) -> dict:
        """
        Create wrangled dataset.

        :param body_request: Body of the POST request to be sent.
            For more details check
            https://clouddataprep.com/documentation/api#operation/createWrangledDataset
        """
        endpoint = f"/{self.api_version}/wrangledDatasets"
        url: str = urljoin(self._base_url, endpoint)
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def create_output_object(self, *, body_request: dict) -> dict:
        """
        Create output.

        :param body_request: Body of the POST request to be sent.
            For more details check
            https://clouddataprep.com/documentation/api#operation/createOutputObject
        """
        endpoint = f"/{self.api_version}/outputObjects"
        url: str = urljoin(self._base_url, endpoint)
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def create_write_settings(self, *, body_request: dict) -> dict:
        """
        Create write settings.

        :param body_request: Body of the POST request to be sent.
            For more details check
            https://clouddataprep.com/documentation/api#tag/createWriteSetting
        """
        endpoint = f"/{self.api_version}/writeSettings"
        url: str = urljoin(self._base_url, endpoint)
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def delete_imported_dataset(self, *, dataset_id: int) -> None:
        """
        Delete imported dataset.

        :param dataset_id: ID of the imported dataset for removal.
        """
        endpoint = f"/{self.api_version}/importedDatasets/{dataset_id}"
        url: str = urljoin(self._base_url, endpoint)
        response = requests.delete(url, headers=self._headers)
        self._raise_for_status(response)
