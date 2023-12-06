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

"""This module contains Yeedu Operator."""
from typing import Optional, Tuple, Union
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from yeedu.hooks.yeedu import YeeduHook
from airflow.exceptions import AirflowException
from airflow.models import Variable  # Import Variable from airflow.models

class YeeduJobRunOperator(BaseOperator):
    """
    YeeduJobRunOperator submits a job to Yeedu and waits for its completion.

    :param job_conf_id: The job configuration ID (mandatory).
    :type job_conf_id: str
    :param hostname: Yeedu API hostname (mandatory).
    :type hostname: str
    :param workspace_id: The ID of the Yeedu workspace to execute the job within (mandatory).
    :type workspace_id: int
    :param token: Yeedu API token. If not provided, it will be retrieved from Airflow Variables.
    :type token: str

    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    """

    template_fields: Tuple[str] = ("job_id",)

    @apply_defaults
    def __init__(
        self,
        job_conf_id: str,
        hostname: str,
        workspace_id: int,
        token: str,
        *args,
        **kwargs,
    ) -> None:
        """
        Initialize the YeeduJobRunOperator.

        :param job_conf_id: The ID of the job configuration in Yeedu (mandatory).
        :param token: Yeedu API token. If not provided, retrieved from Airflow Variables.
        :param hostname: Yeedu API hostname (mandatory).
        :param workspace_id: The ID of the Yeedu workspace to execute the job within (mandatory).
        """
        super().__init__(*args, **kwargs)
        self.job_conf_id: str = job_conf_id
        self.token: str = token or  Variable.get("yeedu_token")
        self.hostname: str = hostname
        self.workspace_id: int = workspace_id
        self.hook: YeeduHook = YeeduHook(token=self.token, hostname=self.hostname, workspace_id=self.workspace_id)
        self.job_id: Optional[Union[int, None]] = None

    def execute(self, context: dict) -> None:
        """
        Execute the YeeduJobRunOperator.

        - Submits a job to Yeedu based on the provided configuration ID.
        - Waits for the job to complete and retrieves job logs.

        :param context: The execution context.
        :type context: dict
        """
        try:
            self.log.info("Job Config Id: %s",self.job_conf_id)
            job_id = self.hook.submit_job(self.job_conf_id)

            self.log.info("Job Submited (Job Id: %s)", job_id)
            job_status: str = self.hook.wait_for_completion(job_id)

            self.log.info("Final Job Status: %s", job_status)

            if job_status in ['DONE']:
                log_type: str = 'stdout'
            elif job_status in ['ERROR', 'TERMINATED', 'KILLED']:
                log_type: str = 'stdout'
            else:
                self.log.error("Job completion status is unknown.")
                return

            job_log: str = self.hook.get_job_logs(job_id, log_type)
            self.log.info("Logs for Job ID %s (Log Type: %s): %s", job_id, log_type, job_log)

            if job_status in ['ERROR', 'TERMINATED', 'KILLED']:
                raise AirflowException(job_log)
                       
        except Exception as e:
            raise AirflowException(e)