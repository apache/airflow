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
import os
from typing import Any

import requests
from requests import HTTPError
from tenacity import retry, stop_after_attempt, wait_exponential

from airflow.hooks.base import BaseHook


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

    def __init__(self, dataprep_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.dataprep_conn_id = dataprep_conn_id
        conn = self.get_connection(self.dataprep_conn_id)
        extra_dejson = conn.extra_dejson
        self._token = extra_dejson.get("extra__dataprep__token")
        self._base_url = extra_dejson.get("extra__dataprep__base_url", "https://api.clouddataprep.com")

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
        endpoint_path = f"v4/jobGroups/{job_id}/jobs"
        url: str = os.path.join(self._base_url, endpoint_path)
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
        endpoint_path = f"v4/jobGroups/{job_group_id}"
        url: str = os.path.join(self._base_url, endpoint_path)
        response = requests.get(url, headers=self._headers, params=params)
        self._raise_for_status(response)
        return response.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def run_job_group(self, body_request: dict) -> dict[str, Any]:
        """
        Creates a ``jobGroup``, which launches the specified job as the authenticated user.
        This performs the same action as clicking on the Run Job button in the application.
        To get recipe_id please follow the Dataprep API documentation
        https://clouddataprep.com/documentation/api#operation/runJobGroup

        :param body_request: The identifier for the recipe you would like to run.
        """
        endpoint_path = "v4/jobGroups"
        url: str = os.path.join(self._base_url, endpoint_path)
        response = requests.post(url, headers=self._headers, data=json.dumps(body_request))
        self._raise_for_status(response)
        return response.json()

    def _raise_for_status(self, response: requests.models.Response) -> None:
        try:
            response.raise_for_status()
        except HTTPError:
            self.log.error(response.json().get("exception"))
            raise
