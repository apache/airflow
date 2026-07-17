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

from airflow.providers.amazon.aws.hooks.glue_databrew import GlueDataBrewHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger


class GlueDataBrewJobCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Watches for a Glue DataBrew job, triggers when it finishes.

    :param job_name: Glue DataBrew job name
    :param run_id: the ID of the specific run to watch for that job
    :param waiter_delay: Number of seconds to wait between two checks. Default is 30 seconds.
    :param waiter_max_attempts: Maximum number of attempts to wait for the job to complete. Default is 60 attempts.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_name: str,
        run_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(
            serialized_fields={"job_name": job_name, "run_id": run_id},
            waiter_name="job_complete",
            waiter_args={"Name": job_name, "RunId": run_id},
            failure_message=f"Error while waiting for job {job_name} with run id {run_id} to complete",
            status_message=f"Run id: {run_id}",
            status_queries=["State"],
            return_value=run_id,
            return_key="run_id",
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> GlueDataBrewHook:
        return GlueDataBrewHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
