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

import base64
import json
import time
from typing import TYPE_CHECKING, Any, TypeVar

import aiohttp
from aiohttp import ClientResponseError
from asgiref.sync import sync_to_async

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

if TYPE_CHECKING:
    from airflow.models import Connection

T = TypeVar("T", bound=Any)


class AirbyteHook(HttpHook):
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

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    PENDING = "pending"
    FAILED = "failed"
    ERROR = "error"
    INCOMPLETE = "incomplete"

    def __init__(self, airbyte_conn_id: str = "airbyte_default", api_version: str = "v1") -> None:
        super().__init__(http_conn_id=airbyte_conn_id)
        self.api_version: str = api_version

    async def get_headers_tenants_from_connection(self) -> tuple[dict[str, Any], str]:
        """Get Headers, tenants from the connection details."""
        connection: Connection = await sync_to_async(self.get_connection)(self.http_conn_id)
        base_url = connection.host

        credentials = f"{connection.login}:{connection.password}"
        credentials_base64 = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        authorized_headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Basic {credentials_base64}",
        }

        return authorized_headers, base_url

    async def get_job_details(self, job_id: int) -> Any:
        """
        Uses Http async call to retrieve metadata for a specific job of an Airbyte Sync.

        :param job_id: The ID of an Airbyte Sync Job.
        """
        headers, base_url = await self.get_headers_tenants_from_connection()
        url = f"{base_url}/api/{self.api_version}/jobs/get"
        self.log.info("URL for api request: %s" % url)
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url=url, data=json.dumps({"id": job_id})) as response:
                try:
                    response.raise_for_status()
                    return await response.json()
                except ClientResponseError as e:
                    raise AirflowException(str(e.status) + ":" + e.message + "-" + str(e.request_info))

    async def get_job_status(self, job_id: int) -> int:
        """
        Retrieves the status for a specific job of an Airbyte Sync.

        :param job_id: The ID of an Airbyte Sync Job.
        """
        try:
            self.log.info("Getting the status of job run %s.", str(job_id))
            response = await self.get_job_details(job_id=job_id)
            job_run_status: int = response["job"]["status"]
            return job_run_status
        except Exception as e:
            raise e

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
                self.cancel_job(job_id=(int(job_id)))
                raise AirflowException(f"Timeout: Airbyte job {job_id} is not ready after {timeout}s")
            time.sleep(wait_seconds)
            try:
                job = self.get_job(job_id=(int(job_id)))
                state = job.json()["job"]["status"]
            except AirflowException as err:
                self.log.info("Retrying. Airbyte API returned server error when waiting for job: %s", err)
                continue

            if state in (self.RUNNING, self.PENDING, self.INCOMPLETE):
                continue
            if state == self.SUCCEEDED:
                break
            if state == self.ERROR:
                raise AirflowException(f"Job failed:\n{job}")
            elif state == self.CANCELLED:
                raise AirflowException(f"Job was cancelled:\n{job}")
            else:
                raise Exception(f"Encountered unexpected state `{state}` for job_id `{job_id}`")

    def submit_sync_connection(self, connection_id: str) -> Any:
        """
        Submit a job to a Airbyte server.

        :param connection_id: Required. The ConnectionId of the Airbyte Connection.
        """
        return self.run(
            endpoint=f"api/{self.api_version}/connections/sync",
            json={"connectionId": connection_id},
            headers={"accept": "application/json"},
        )

    def get_job(self, job_id: int) -> Any:
        """
        Get the resource representation for a job in Airbyte.

        :param job_id: Required. Id of the Airbyte job
        """
        return self.run(
            endpoint=f"api/{self.api_version}/jobs/get",
            json={"id": job_id},
            headers={"accept": "application/json"},
        )

    def cancel_job(self, job_id: int) -> Any:
        """
        Cancel the job when task is cancelled.

        :param job_id: Required. Id of the Airbyte job
        """
        return self.run(
            endpoint=f"api/{self.api_version}/jobs/cancel",
            json={"id": job_id},
            headers={"accept": "application/json"},
        )

    def test_connection(self):
        """Tests the Airbyte connection by hitting the health API."""
        self.method = "GET"
        try:
            res = self.run(
                endpoint=f"api/{self.api_version}/health",
                headers={"accept": "application/json"},
                extra_options={"check_response": False},
            )

            if res.status_code == 200:
                return True, "Connection successfully tested"
            else:
                return False, res.text
        except Exception as e:
            return False, str(e)
        finally:
            self.method = "POST"
