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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class GlueDataBrewHook(AwsBaseHook):
    """
    Interact with AWS DataBrew.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        kwargs["client_type"] = "databrew"
        super().__init__(*args, **kwargs)

    def job_completion(self, job_name: str, run_id: str, delay: int = 10, max_attempts: int = 60) -> str:
        """
        Wait until Glue DataBrew job reaches terminal status.

        :param job_name: The name of the job being processed during this run.
        :param run_id: The unique identifier of the job run.
        :param delay: Time in seconds to delay between polls
        :param maxAttempts: Maximum number of attempts to poll for completion
        :return: job status
        """
        self.get_waiter("job_complete").wait(
            Name=job_name,
            RunId=run_id,
            WaiterConfig={"Delay": delay, "maxAttempts": max_attempts},
        )

        status = self.get_job_state(job_name, run_id)
        return status

    def get_job_state(self, job_name: str, run_id: str) -> str:
        """
        Get the status of a job run.

        :param job_name: The name of the job being processed during this run.
        :param run_id: The unique identifier of the job run.
        :return: State of the job run.
            'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED'|'TIMEOUT'
        """
        response = self.conn.describe_job_run(Name=job_name, RunId=run_id)
        return response["State"]
