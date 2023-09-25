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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueJobSensor(BaseSensorOperator):
    """
    Waits for an AWS Glue Job to reach any of the status below.

    'FAILED', 'STOPPED', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueJobSensor`

    :param job_name: The AWS Glue Job unique name
    :param run_id: The AWS Glue current running job identifier
    :param verbose: If True, more Glue Job Run logs show in the Airflow Task Logs.  (default: False)
    """

    template_fields: Sequence[str] = ("job_name", "run_id")

    def __init__(
        self,
        *,
        job_name: str,
        run_id: str,
        verbose: bool = False,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.run_id = run_id
        self.verbose = verbose
        self.aws_conn_id = aws_conn_id
        self.success_states: list[str] = ["SUCCEEDED"]
        self.errored_states: list[str] = ["FAILED", "STOPPED", "TIMEOUT"]
        self.next_log_tokens = GlueJobHook.LogContinuationTokens()

    @cached_property
    def hook(self):
        return GlueJobHook(aws_conn_id=self.aws_conn_id)

    def poke(self, context: Context):
        self.log.info("Poking for job run status :for Glue Job %s and ID %s", self.job_name, self.run_id)
        job_state = self.hook.get_job_state(job_name=self.job_name, run_id=self.run_id)

        try:
            if job_state in self.success_states:
                self.log.info("Exiting Job %s Run State: %s", self.run_id, job_state)
                return True
            elif job_state in self.errored_states:
                job_error_message = "Exiting Job %s Run State: %s", self.run_id, job_state
                self.log.info(job_error_message)
                # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
                if self.soft_fail:
                    raise AirflowSkipException(job_error_message)
                raise AirflowException(job_error_message)
            else:
                return False
        finally:
            if self.verbose:
                self.hook.print_job_logs(
                    job_name=self.job_name,
                    run_id=self.run_id,
                    continuation_tokens=self.next_log_tokens,
                )
