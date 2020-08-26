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
"""
This module contains Google Dataprep hook.
"""
from typing import Any, Dict

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook


class GoogleDataprepHook(BaseHook):
    """
    Hook for connection with Dataprep API.
    To get connection Dataprep with Airflow you need Dataprep token.
    https://clouddataprep.com/documentation/api#section/Authentication

    It should be added to the Connection in Airflow in JSON format.

    """

    def __init__(self, dataprep_conn_id: str = "dataprep_conn_id") -> None:
        super().__init__()
        self.dataprep_conn_id = dataprep_conn_id
        self._url = "https://api.clouddataprep.com/v4/jobGroups"

    @property
    def _headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._token}",
        }
        return headers

    @property
    def _token(self) -> str:
        conn = self.get_connection(self.dataprep_conn_id)
        token = conn.extra_dejson.get("token")
        if token is None:
            raise AirflowException(
                "Dataprep token is missing or has invalid format. "
                "Please make sure that Dataprep token is added to the Airflow Connections."
            )
        return token

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
    def get_jobs_for_job_group(self, job_id: int) -> Dict[str, Any]:
        """
        Get information about the batch jobs within a Cloud Dataprep job.

        :param job_id The ID of the job that will be fetched.
        :type job_id: int
        """
        url: str = f"{self._url}/{job_id}/jobs"
        response = requests.get(url, headers=self._headers)
        response.raise_for_status()
        return response.json()
