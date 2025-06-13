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

    Provides all run step details of a job run.

    :param account_id: The ID of a dbt Cloud account.
    :param run_id: The ID of a dbt Cloud job.
    :param message: Custom message to output in the exception.
    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    :param run_data_keys: Keys of higher level job run details.
    :param run_steps_keys: Keys of lower level job run details, i.e. run steps.
    """

    def __init__(
        self,
        account_id: int | None = None,
        run_id: int | None = None,
        message: str = "The job run did not complete successfully.",
        dbt_cloud_conn_id="dbt_cloud_default",
        # For extracting only a subset of the higher level job details
        # id is the run_id
        run_data_keys: list = (
            "id",
            "job_id",
            "is_error",
            "dbt_version",
            "finished_at",
            "finished_at_humanized",
            "run_duration",
            "run_duration_humanized",
        ),
        # For extracting only a subset of the run step details
        # id is the step id
        run_steps_keys: list = (
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
        ),
    ):
        self.run_id = run_id
        self.account_id = account_id
        self.message = message
        self.run_data_keys = run_data_keys
        self.run_steps_keys = run_steps_keys
        self.dbt_cloud_conn_id = dbt_cloud_conn_id

        # If there is no account_id and run_id provided, you cannot gather any details about the job run
        if self.account_id and self.run_id:
            # Gather run step details from the job run
            self.run_details = (
                DbtCloudHook(self.dbt_cloud_conn_id)
                .get_job_run(run_id=self.run_id, account_id=self.account_id, include_related=["run_steps"])
                .json()["data"]
            )

            # Begin to build the exception message
            self.job_run_msg = f" ###JOB RUN DETAILS: {json.dumps({k: self.run_details[k] for k in self.run_data_keys if k in self.run_details})}"
            self.message = self.message + self.job_run_msg

            # Just in case dbt decides to change the API again
            try:
                run_steps_msg = " ###RUN STEP DETAILS: "
                # run_steps is a list of dictionaries, with each step's details
                for s in self.run_details["run_steps"]:
                    run_steps_msg += json.dumps({k: s[k] for k in self.run_steps_keys if k in s})

                self.message += run_steps_msg

            except Exception as e:
                print(str(e))

    def __str__(self):
        return self.message
