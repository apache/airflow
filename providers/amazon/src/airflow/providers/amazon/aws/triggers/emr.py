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

import sys
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook, EmrServerlessHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class EmrAddStepsTrigger(AwsBaseWaiterTrigger):
    """
    Poll for the status of EMR steps until they reach terminal state.

    :param job_flow_id: job_flow_id which contains the steps to check the state of
    :param step_ids: steps to check the state of
    :param waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made
    :param aws_conn_id: Reference to AWS connection id

    """

    def __init__(
        self,
        job_flow_id: str,
        step_ids: list[str],
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None = "aws_default",
    ):
        super().__init__(
            serialized_fields={"job_flow_id": job_flow_id, "step_ids": step_ids},
            waiter_name="steps_wait_for_terminal",
            waiter_args={"ClusterId": job_flow_id, "StepIds": step_ids},
            failure_message=f"Error while waiting for steps {step_ids} to complete",
            status_message=f"Step ids: {step_ids}, Steps are still in non-terminal state",
            status_queries=[
                "Steps[].Status.State",
                "Steps[].Status.FailureDetails",
            ],
            return_value=step_ids,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrHook(aws_conn_id=self.aws_conn_id)


class EmrCreateJobFlowTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously poll the boto3 API and wait for the JobFlow to finish executing.

    :param job_flow_id: The id of the job flow to wait for.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_flow_id: str,
        aws_conn_id: str | None = None,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
    ):
        super().__init__(
            serialized_fields={"job_flow_id": job_flow_id},
            waiter_name="job_flow_waiting",
            waiter_args={"ClusterId": job_flow_id},
            failure_message="JobFlow creation failed",
            status_message="JobFlow creation in progress",
            status_queries=[
                "Cluster.Status.State",
                "Cluster.Status.StateChangeReason",
                "Cluster.Status.ErrorDetails",
            ],
            return_key="job_flow_id",
            return_value=job_flow_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrHook(aws_conn_id=self.aws_conn_id)


class EmrTerminateJobFlowTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously poll the boto3 API and wait for the JobFlow to finish terminating.

    :param job_flow_id: ID of the EMR Job Flow to terminate
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_flow_id: str,
        aws_conn_id: str | None = None,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
    ):
        super().__init__(
            serialized_fields={"job_flow_id": job_flow_id},
            waiter_name="job_flow_terminated",
            waiter_args={"ClusterId": job_flow_id},
            failure_message="JobFlow termination failed",
            status_message="JobFlow termination in progress",
            status_queries=[
                "Cluster.Status.State",
                "Cluster.Status.StateChangeReason",
                "Cluster.Status.ErrorDetails",
            ],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrHook(aws_conn_id=self.aws_conn_id)


class EmrContainerTrigger(AwsBaseWaiterTrigger):
    """
    Poll for the status of EMR container until reaches terminal state.

    :param virtual_cluster_id: Reference Emr cluster id
    :param job_id:  job_id to check the state
    :param aws_conn_id: Reference to AWS connection id
    :param waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made. Defaults to an infinite wait.
    """

    def __init__(
        self,
        virtual_cluster_id: str,
        job_id: str,
        aws_conn_id: str | None = "aws_default",
        waiter_delay: int = 30,
        waiter_max_attempts: int = sys.maxsize,
    ):
        super().__init__(
            serialized_fields={"virtual_cluster_id": virtual_cluster_id, "job_id": job_id},
            waiter_name="container_job_complete",
            waiter_args={"id": job_id, "virtualClusterId": virtual_cluster_id},
            failure_message="Job failed",
            status_message="Job in progress",
            status_queries=["jobRun.state", "jobRun.failureReason"],
            return_key="job_id",
            return_value=job_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrContainerHook(aws_conn_id=self.aws_conn_id)


class EmrStepSensorTrigger(AwsBaseWaiterTrigger):
    """
    Poll for the status of EMR container until reaches terminal state.

    :param job_flow_id: job_flow_id which contains the step check the state of
    :param step_id:  step to check the state of
    :param waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made
    :param aws_conn_id: Reference to AWS connection id
    """

    def __init__(
        self,
        job_flow_id: str,
        step_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ):
        super().__init__(
            serialized_fields={"job_flow_id": job_flow_id, "step_id": step_id},
            waiter_name="step_wait_for_terminal",
            waiter_args={"ClusterId": job_flow_id, "StepId": step_id},
            failure_message=f"Error while waiting for step {step_id} to complete",
            status_message=f"Step id: {step_id}, Step is still in non-terminal state",
            status_queries=[
                "Step.Status.State",
                "Step.Status.FailureDetails",
                "Step.Status.StateChangeReason",
            ],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrHook(aws_conn_id=self.aws_conn_id)


class EmrServerlessCreateApplicationTrigger(AwsBaseWaiterTrigger):
    """
    Poll an Emr Serverless application and wait for it to be created.

    :param application_id: The ID of the application being polled.
    :waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made
    :param aws_conn_id: Reference to AWS connection id
    """

    def __init__(
        self,
        application_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"application_id": application_id},
            waiter_name="serverless_app_created",
            waiter_args={"applicationId": application_id},
            failure_message="Application creation failed",
            status_message="Application status is",
            status_queries=["application.state", "application.stateDetails"],
            return_key="application_id",
            return_value=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrServerlessHook(self.aws_conn_id)


class EmrServerlessStartApplicationTrigger(AwsBaseWaiterTrigger):
    """
    Poll an Emr Serverless application and wait for it to be started.

    :param application_id: The ID of the application being polled.
    :waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made
    :param aws_conn_id: Reference to AWS connection id
    """

    def __init__(
        self,
        application_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"application_id": application_id},
            waiter_name="serverless_app_started",
            waiter_args={"applicationId": application_id},
            failure_message="Application failed to start",
            status_message="Application status is",
            status_queries=["application.state", "application.stateDetails"],
            return_key="application_id",
            return_value=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrServerlessHook(self.aws_conn_id)


class EmrServerlessStopApplicationTrigger(AwsBaseWaiterTrigger):
    """
    Poll an Emr Serverless application and wait for it to be stopped.

    :param application_id: The ID of the application being polled.
    :waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made
    :param aws_conn_id: Reference to AWS connection id.
    """

    def __init__(
        self,
        application_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"application_id": application_id},
            waiter_name="serverless_app_stopped",
            waiter_args={"applicationId": application_id},
            failure_message="Application failed to start",
            status_message="Application status is",
            status_queries=["application.state", "application.stateDetails"],
            return_key="application_id",
            return_value=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrServerlessHook(self.aws_conn_id)


class EmrServerlessStartJobTrigger(AwsBaseWaiterTrigger):
    """
    Poll an Emr Serverless job run and wait for it to be completed.

    :param application_id: The ID of the application the job in being run on.
    :param job_id: The ID of the job run.
    :waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made
    :param aws_conn_id: Reference to AWS connection id
    """

    def __init__(
        self,
        application_id: str,
        job_id: str | None,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"application_id": application_id, "job_id": job_id},
            waiter_name="serverless_job_completed",
            waiter_args={"applicationId": application_id, "jobRunId": job_id},
            failure_message="Serverless Job failed",
            status_message="Serverless Job status is",
            status_queries=["jobRun.state", "jobRun.stateDetails"],
            return_key="job_id",
            return_value=job_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrServerlessHook(self.aws_conn_id)


class EmrServerlessDeleteApplicationTrigger(AwsBaseWaiterTrigger):
    """
    Poll an Emr Serverless application and wait for it to be deleted.

    :param application_id: The ID of the application being polled.
    :waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made
    :param aws_conn_id: Reference to AWS connection id
    """

    def __init__(
        self,
        application_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"application_id": application_id},
            waiter_name="serverless_app_terminated",
            waiter_args={"applicationId": application_id},
            failure_message="Application failed to start",
            status_message="Application status is",
            status_queries=["application.state", "application.stateDetails"],
            return_key="application_id",
            return_value=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrServerlessHook(self.aws_conn_id)


class EmrServerlessCancelJobsTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for canceling a list of jobs in an EMR Serverless application.

    :param application_id: EMR Serverless application ID
    :param aws_conn_id: Reference to AWS connection id
    :param waiter_delay: Delay in seconds between each attempt to check the status
    :param waiter_max_attempts: Maximum number of attempts to check the status
    """

    def __init__(
        self,
        application_id: str,
        aws_conn_id: str | None,
        waiter_delay: int,
        waiter_max_attempts: int,
    ) -> None:
        states = list(EmrServerlessHook.JOB_INTERMEDIATE_STATES.union({"CANCELLING"}))
        super().__init__(
            serialized_fields={"application_id": application_id},
            waiter_name="no_job_running",
            waiter_args={"applicationId": application_id, "states": states},
            failure_message="Error while waiting for jobs to cancel",
            status_message="Currently running jobs",
            status_queries=["jobRuns[*].applicationId", "jobRuns[*].state"],
            return_key="application_id",
            return_value=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrServerlessHook(self.aws_conn_id)

    @property
    def hook_instance(self) -> AwsGenericHook:
        """This property is added for backward compatibility."""
        return self.hook()
