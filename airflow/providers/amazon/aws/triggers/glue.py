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

from typing import Any, AsyncIterator

from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class GlueJobCompleteTrigger(BaseTrigger):
    """
    Watches for a glue job, triggers when it finishes.

    :param job_name: glue job name
    :param run_id: the ID of the specific run to watch for that job
    :param verbose: whether to print the job's logs in airflow logs or not
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_name: str,
        run_id: str,
        verbose: bool,
        aws_conn_id: str,
    ):
        self.job_name = job_name
        self.run_id = run_id
        self.verbose = verbose
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            # dynamically generate the fully qualified name of the class
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "job_name": self.job_name,
                "run_id": self.run_id,
                "verbose": str(self.verbose),
                "aws_conn_id": self.aws_conn_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = GlueJobHook(aws_conn_id=self.aws_conn_id)
        await hook.async_job_completion(self.job_name, self.run_id, self.verbose)
        yield TriggerEvent({"status": "success", "message": "Job done", "value": self.run_id})
