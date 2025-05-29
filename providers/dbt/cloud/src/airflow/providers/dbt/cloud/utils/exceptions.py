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

from airflow.exceptions import AirflowException

from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook


class DbtCloudJobRunDetailsException(AirflowException):
    """
    An exception that indicates a job run failed to complete.

    Can provide additional job run details.
    """

    def __init__(
        self,
        account_id: int,
        run_id: int,
        message: str = "The job run has failed.",
        # For extracting only a subset of the higher level job details
        # id is the run_id
        run_data_keys: list = [
            "id",
            "job_id",
            "is_error",
            "dbt_version",
            "finished_at",
            "finished_at_humanized",
            "run_duration",
            "run_duration_humanized",
        ],
        # For extracting only a subset of the run step details
        # id is the step id
        run_steps_keys: list = [
            "run_id",
            "id",
            "index",
            "name",
            "status",
            "status_humanized",
            "duration",
            "duration_humanized",
            "logs",
            "debug_logs",
        ],
    ):
        self.run_id = run_id
        self.account_id = account_id
        self.message = message
        self.run_data_keys = run_data_keys
        self.run_steps_keys = run_steps_keys

    def __str__(self):
        run_details = DbtCloudHook.get_job_run(
            run_id=self.run_id, account_id=self.account_id, include_related=["run_steps"]
        ).json()["data"]
        job_run_msg = f" Job run details: {json.dumps({k: run_details[k] for k in self.run_data_keys if k in run_details})}"
        # f"Job run {self.run_id} has failed or has been cancelled."
        msg = f"Job run {self.run_id} has failed or has been cancelled." + job_run_msg
        msg = self.message + job_run_msg

        # Just in case dbt decides to change the API again
        try:
            run_steps_msg = " Run step details: "
            # run_steps is a list of dictionaries, with each step's details
            for s in run_details["run_steps"]:
                run_steps_msg += json.dumps({k: s[k] for k in self.run_steps_keys if k in s})

            msg += run_steps_msg

        except Exception as e:
            print(str(e))

        return msg
