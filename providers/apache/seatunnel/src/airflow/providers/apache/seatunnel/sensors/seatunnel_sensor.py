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

import json
from typing import TYPE_CHECKING

import requests

from airflow.exceptions import AirflowException
from airflow.providers.apache.seatunnel.hooks.seatunnel_hook import (
    SeaTunnelHook,
)
from airflow.sdk.bases.sensor import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class SeaTunnelJobSensor(BaseSensorOperator):
    """
    Sensor to wait for a SeaTunnel job to reach a specific state.

    This sensor polls the SeaTunnel API to check the status of a specific job.

    :param job_id: The SeaTunnel job ID to monitor.
    :param target_states: List of target states to wait for.
                         Default is ['FINISHED'].
    :param seatunnel_conn_id: Connection ID to use.
    :param poke_interval: Time in seconds to wait between polls.
    :param timeout: Time in seconds to wait before timing out.
    """

    template_fields = ("job_id", "target_states")
    ui_color = "#1CB8FF"  # SeaTunnel blue color

    def __init__(
        self,
        *,
        job_id: str,
        target_states: list | None = None,
        seatunnel_conn_id: str = "seatunnel_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.target_states = target_states or ["FINISHED"]
        self.seatunnel_conn_id = seatunnel_conn_id

    def poke(self, context: Context) -> bool:
        """
        Poke function to check if the job has reached the target state.

        :param context: Airflow context.
        :return: True if the job has reached the target state,
                 False otherwise.
        """
        hook = SeaTunnelHook(seatunnel_conn_id=self.seatunnel_conn_id)

        # Only supported on SeaTunnel Zeta engine with REST API enabled
        if hook.engine != "zeta":
            raise ValueError("SeaTunnelJobSensor only works with the 'zeta' engine.")

        # Try multiple API URL formats in sequence based on the
        # official documentation
        # According to SeaTunnel docs: /job-info/:jobId is the correct
        # endpoint for job info
        api_urls = [
            f"http://{hook.host}:{hook.port}/job-info/{self.job_id}",
            # Official V2 API
        ]

        # Try each URL format
        for api_url in api_urls:
            self.log.info("Trying job status URL: %s", api_url)

            try:
                response = requests.get(api_url, timeout=10)
                self.log.info("Response status code: %s", response.status_code)

                # If successful, use this URL and continue
                if response.status_code == 200:
                    self.log.info("Successfully found API endpoint: %s", api_url)

                    # Try to parse the JSON response
                    try:
                        job_data = response.json()

                        # Check if response has the expected job status
                        # field
                        if "jobStatus" in job_data:
                            current_status = job_data.get("jobStatus")
                            self.log.info("Current status of job %s: %s", self.job_id, current_status)

                            # Check if the job has reached the target state
                            if current_status in self.target_states:
                                return True

                            # Check if the job has failed or has been
                            # stopped
                            if current_status in ["FAILED", "CANCELED"]:
                                raise AirflowException(
                                    f"SeaTunnel job {self.job_id} is in {current_status} state"
                                )

                            return False
                        self.log.warning("Response does not contain 'jobStatus' field: %s", job_data)
                    # Continue to next URL if this one doesn't have
                    # the expected format
                    except json.JSONDecodeError as e:
                        self.log.error("Failed to parse JSON response: %s", e)
                        self.log.error("Response content was: %s", response.text)
                        # Continue to next URL if this one doesn't return
                        # valid JSON
                else:
                    self.log.warning("Failed with status %s: %s", response.status_code, api_url)
                    if response.status_code == 404:
                        self.log.debug("Error response: %s", response.text)
            except requests.RequestException as e:
                self.log.warning("Request failed for %s: %s", api_url, str(e))

        # If we've tried all URLs and none worked, log an error and
        # return False
        self.log.error("Failed to find working API endpoint for job %s", self.job_id)
        return False
