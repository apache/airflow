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
from __future__ import annotations

import time
from typing import Any, TypeVar

from airbyte_api import AirbyteAPI
from airbyte_api.api import CancelJobRequest, GetJobRequest
from airbyte_api.models import JobCreateRequest, JobStatusEnum, JobTypeEnum, SchemeClientCredentials, Security
from requests import Session

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook

T = TypeVar("T", bound=Any)


class AirbyteHook(BaseHook):
    """
    Hook for Airbyte API.

    :param airbyte_conn_id: Optional. The name of the Airflow connection to get
        connection information for Airbyte. Defaults to "airbyte_default".
    :param api_version: Optional. Airbyte API version. Defaults to "v1".
    """

    conn_name_attr = "airbyte_conn_id"
    default_conn_name = "airbyte_default"
    conn_type = "airbyte"
    hook_name = "Airbyte"

    def __init__(
        self,
        airbyte_conn_id: str = "airbyte_default",
        api_version: str = "v1",
    ) -> None:
        super().__init__()
        self.api_version: str = api_version
        self.airbyte_conn_id = airbyte_conn_id
        self.conn = self.get_conn_params(self.airbyte_conn_id)
        self.airbyte_api = self.create_api_session()

    def get_conn_params(self, conn_id: str) -> Any:
        conn = self.get_connection(conn_id)

        # Intentionally left the password out, you can modify the log to print it out if you are doing testing.
        self.log.debug(
            "Connection attributes are: host - %s, url - %s, description - %s",
            conn.host,
            conn.schema,
            conn.description,
        )
        conn_params: dict = {}
        conn_params["host"] = conn.host
        conn_params["client_id"] = conn.login
        conn_params["client_secret"] = conn.password
        conn_params["token_url"] = conn.schema or "v1/applications/token"
        conn_params["proxies"] = conn.extra_dejson.get("proxies", None)

        return conn_params

    def create_api_session(self) -> AirbyteAPI:
        """Create Airbyte API session."""
        credentials = SchemeClientCredentials(
            client_id=self.conn["client_id"],
            client_secret=self.conn["client_secret"],
            token_url=self.conn["token_url"],
        )

        client = None
        if self.conn["proxies"]:
            self.log.debug("Creating client proxy...")
            client = Session()
            client.proxies = self.conn["proxies"]

        return AirbyteAPI(
            server_url=self.conn["host"],
            security=Security(client_credentials=credentials),
            client=client,
        )

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": [
                "extra",
                "port",
            ],
            "relabeling": {
                "host": "Server URL",
                "login": "Client ID",
                "password": "Client Secret",
                "schema": "Token URL",
            },
            "placeholders": {},
        }

    def get_job_details(self, job_id: int) -> Any:
        """
        Use Http async call to retrieve metadata for a specific job of an Airbyte Sync.

        :param job_id: The ID of an Airbyte Sync Job.
        """
        try:
            get_job_res = self.airbyte_api.jobs.get_job(
                request=GetJobRequest(
                    job_id=job_id,
                )
            )
            self.log.debug("Job details are: %s", get_job_res.job_response)
            return get_job_res.job_response
        except Exception as e:
            raise AirflowException(e)

    def get_job_status(self, job_id: int) -> str:
        """
        Retrieve the status for a specific job of an Airbyte Sync.

        :param job_id: The ID of an Airbyte Sync Job.
        """
        self.log.info("Getting the status of job run %s.", job_id)
        response = self.get_job_details(job_id=job_id)
        return response.status

    def wait_for_job(self, job_id: str | int, wait_seconds: float = 3, timeout: float | None = 3600) -> None:
        """
        Poll a job to check if it finishes.

        :param job_id: Required. Id of the Airbyte job
        :param wait_seconds: Optional. Number of seconds between checks.
        :param timeout: Optional. How many seconds wait for job to be ready.
            Used only if ``asynchronous`` is False.
        """
        state = None
        start = time.monotonic()
        while True:
            if timeout and start + timeout < time.monotonic():
                self.log.debug("Canceling job...")
                self.cancel_job(job_id=(int(job_id)))
                raise AirflowException(f"Timeout: Airbyte job {job_id} is not ready after {timeout}s")
            time.sleep(wait_seconds)
            try:
                job = self.get_job_details(job_id=(int(job_id)))
                state = job.status
                self.log.debug("Job State: %s. Job Details: %s", state, job)

            except AirflowException as err:
                self.log.info("Retrying. Airbyte API returned server error when waiting for job: %s", err)
                continue

            if state in (JobStatusEnum.RUNNING, JobStatusEnum.PENDING, JobStatusEnum.INCOMPLETE):
                continue
            if state == JobStatusEnum.SUCCEEDED:
                break
            if state == JobStatusEnum.FAILED:
                raise AirflowException(f"Job failed:\n{job}")
            if state == JobStatusEnum.CANCELLED:
                raise AirflowException(f"Job was cancelled:\n{job}")
            raise AirflowException(f"Encountered unexpected state `{state}` for job_id `{job_id}`")

    def submit_sync_connection(self, connection_id: str) -> Any:
        try:
            self.log.debug("Creating job request..")
            res = self.airbyte_api.jobs.create_job(
                request=JobCreateRequest(
                    connection_id=connection_id,
                    job_type=JobTypeEnum.SYNC,
                )
            )
            self.log.debug("Job request successful, response: %s", res.job_response)
            return res.job_response
        except Exception as e:
            raise AirflowException(e)

    def cancel_job(self, job_id: int) -> Any:
        """
        Cancel the job when task is cancelled.

        :param job_id: Required. Id of the Airbyte job
        """
        try:
            cancel_job_res = self.airbyte_api.jobs.cancel_job(
                request=CancelJobRequest(
                    job_id=job_id,
                )
            )
            return cancel_job_res.job_response
        except Exception as e:
            raise AirflowException(e)

    def test_connection(self):
        """Tests the Airbyte connection by hitting the health API."""
        try:
            health_check = self.airbyte_api.health.get_health_check()
            self.log.debug("Health check details: %s", health_check)
            if health_check.status_code == 200:
                return True, "Connection successfully tested"
            return False, str(health_check.raw_response)
        except Exception as e:
            return False, str(e)
