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
from typing import Optional

import time

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class AirbyteJobController:
    """Airbyte job status"""
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CANCELLED = "canceled"
    PENDING = "pending"
    FAILED = "failed"
    ERROR = "error"


class AirbyteHook(HttpHook, AirbyteJobController):
    """Hook for Airbyte API"""

    def __init__(self, airbyte_conn_id: str) -> None:
        super().__init__(http_conn_id=airbyte_conn_id)

    def wait_for_job(
        self, job_id: str, wait_time: int = 3, timeout: Optional[int] = None
    ) -> None:
        """
        Helper method which polls a job to check if it finishes.

        :param job_id: Id of the Airbyte job
        :type job_id: str
        :param wait_time: Number of seconds between checks
        :type wait_time: int
        :param timeout: How many seconds wait for job to be ready. Used only if ``asynchronous`` is False
        :type timeout: int
        """
        state = None
        start = time.monotonic()
        while state not in (self.ERROR, self.SUCCEEDED, self.CANCELLED):
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(f"Timeout: Airbyte job {job_id} is not ready after {timeout}s")
            time.sleep(wait_time)
            try:
                job = self.get_job(job_id=job_id)
                state = job.json().get("job").get("status")
            except AirflowException as err:
                self.log.info("Retrying. Airbyte API returned server error when waiting for job: %s", err)

        if state == self.ERROR:
            raise AirflowException(f"Job failed:\n{job}")
        if state == self.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job}")

    def submit_job(self, connection_id: str) -> dict:
        """
        Submits a job to a Airbyte server.

        :param connection_id: Required. The ConnectionId of the Airbyte Connection.
        :type connectiond_id: str
        """
        return self.run(
            endpoint="api/v1/connections/sync",
            json={"connectionId": connection_id},
            headers={"accept": "application/json"}
        )

    def get_job(self, job_id: str) -> dict:
        """
        Gets the resource representation for a job in Airbyte.

        :param job_id: Id of the Airbyte job
        :type job_id: str
        """
        return self.run(
            endpoint="api/v1/jobs/get",
            json={"id": job_id},
            headers={"accept": "application/json"}
        )
