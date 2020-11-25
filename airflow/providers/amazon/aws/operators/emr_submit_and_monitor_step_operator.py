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

import ast
import time
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowException
from typing import Dict, List, Optional, Set, Any, Callable, Generator, Union


class EmrSubmitAndMonitorStepOperator(BaseOperator):
    """
    An operator that adds steps to an existing EMR job_flow and optionally monitors it.
    :param job_flow_id: id of the JobFlow to add steps to. (templated)
    :type job_flow_id: Optional[str]
    :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
        job_flow_id. will search for id of JobFlow with matching name in one of the states in
        param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
    :type job_flow_name: Optional[str]
    :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
        (templated)
    :type cluster_states: list
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param steps: boto3 style steps or reference to a steps file (must be '.json') to
        be added to the jobflow. (templated)
    :type steps: list|str
    :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
    :type do_xcom_push: bool
    """

    non_terminal_states = {"PENDING", "RUNNING"}
    failed_states = {"FAILED", "CANCELLED", "INTERRUPTED", "CANCEL_PENDING"}
    template_fields = ["job_flow_id", "job_flow_name", "cluster_states", "steps", "wait_for_completion"]
    template_ext = (".json",)
    ui_color = "#f9c915"

    @apply_defaults
    def __init__(
        self,
        *,
        job_flow_id: Optional[str] = None,
        job_flow_name: Optional[str] = None,
        cluster_states: Optional[List[str]] = None,
        aws_conn_id: str = "aws_default",
        check_interval: int = 30,
        steps: Optional[Union[List[dict], str]] = None,
        wait_for_completion: bool = True,
        **kwargs,
    ):
        if kwargs.get("xcom_push") is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        if not (job_flow_id is None) ^ (job_flow_name is None):
            raise AirflowException("Exactly one of job_flow_id or job_flow_name must be specified.")
        super().__init__(**kwargs)
        cluster_states = cluster_states or []
        steps = steps or []
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        self.job_flow_name = job_flow_name
        self.cluster_states = cluster_states
        self.steps = steps
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval

    def execute(self, context: Dict[str, Any]) -> List[str]:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        emr = emr_hook.get_conn()

        job_flow_id = self.job_flow_id or emr_hook.get_cluster_id_by_name(
            str(self.job_flow_name), self.cluster_states
        )

        if not job_flow_id:
            raise AirflowException(f"No cluster found for name: {self.job_flow_name}")

        if self.do_xcom_push:
            context["ti"].xcom_push(key="job_flow_id", value=job_flow_id)

        self.log.info("Adding steps to %s", job_flow_id)

        # steps may arrive as a string representing a list
        # e.g. if we used XCom or a file then: steps="[{ step1 }, { step2 }]"
        steps = self.steps
        if isinstance(steps, str):
            steps = ast.literal_eval(steps)

        response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=steps)

        if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            raise AirflowException("Adding steps failed: %s" % response)
        else:
            # Assumption : Only a single step is submitted each time.
            step_ids = response["StepIds"]
            step_id = step_ids[0]
            if self.wait_for_completion:
                self.check_status(
                    job_flow_id,
                    step_id,
                    self.describe_step,
                    self.check_interval,
                )
            self.log.info("Steps %s added to JobFlow", response["StepIds"])
            return response["StepIds"]

    def check_status(
        self,
        job_flow_id: str,
        step_id: str,
        describe_function: Callable,
        check_interval: int,
        max_ingestion_time: Optional[int] = None,
        non_terminal_states: Optional[Set] = None,
    ):
        """
        Check status of a EMR Step
        :param job_flow_id: name of the Cluster to check status
        :type job_flow_id: str
        :param step_id: the Step Id
            that points to the Job
        :type step_id: str
        :param describe_function: the function used to retrieve the status
        :type describe_function: python callable
        :param args: the arguments for the function
        :param check_interval: the time interval in seconds which the operator
            will check the status of any EMR job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            EMR jobs that run longer than this will fail. Setting this to
            None implies no timeout for any EMR job.
        :type max_ingestion_time: int
        :param non_terminal_states: the set of non terminal states
        :type non_terminal_states: set
        :return: response of describe call after job is done
        """
        if not non_terminal_states:
            non_terminal_states = self.non_terminal_states

        sec = 0
        running = True

        while running:
            time.sleep(check_interval)
            sec += check_interval

            try:
                response = describe_function(job_flow_id, step_id)
                status = response["Step"]["Status"]["State"]
                self.log.info("Job still running for %s seconds... " "current status is %s", sec, status)
            except KeyError:
                raise AirflowException("Could not get status of the EMR job")
            except ClientError:
                raise AirflowException("AWS request failed, check logs for more info")

            if status in non_terminal_states:
                running = True
            elif status in self.failed_states:
                raise AirflowException("EMR Step failed because %s" % response["FailureReason"])
            else:
                running = False

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException(f"EMR job took more than {max_ingestion_time} seconds")

        self.log.info("EMR Job completed")
        response = describe_function(job_flow_id, step_id)
        return response

    def describe_step(self, clusterid: str, stepid: str) -> dict:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr = emr_hook.get_conn()
        return emr.describe_step(ClusterId=clusterid, StepId=stepid)
