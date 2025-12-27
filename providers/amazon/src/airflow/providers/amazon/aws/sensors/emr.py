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

from collections.abc import Iterable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook, EmrServerlessHook
from airflow.providers.amazon.aws.links.emr import EmrClusterLink, EmrLogsLink, get_log_uri
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.emr import (
    EmrContainerTrigger,
    EmrStepSensorTrigger,
    EmrTerminateJobFlowTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Context


class EmrBaseSensor(AwsBaseSensor[EmrHook]):
    """
    Contains general sensor behavior for EMR.

    Subclasses should implement following methods:
        - ``get_emr_response()``
        - ``state_from_response()``
        - ``failure_message_from_response()``

    Subclasses should set ``target_states`` and ``failed_states`` fields.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.h
    """

    aws_hook_class = EmrHook
    ui_color = "#66c3ff"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.target_states: Iterable[str] = []  # will be set in subclasses
        self.failed_states: Iterable[str] = []  # will be set in subclasses

    def poke(self, context: Context):
        response = self.get_emr_response(context=context)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            self.log.info("Bad HTTP response: %s", response)
            return False

        state = self.state_from_response(response)
        self.log.info("Job flow currently %s", state)

        if state in self.target_states:
            return True

        if state in self.failed_states:
            raise AirflowException(f"EMR job failed: {self.failure_message_from_response(response)}")

        return False

    def get_emr_response(self, context: Context) -> dict[str, Any]:
        """
        Make an API call with boto3 and get response.

        :return: response
        """
        raise NotImplementedError("Please implement get_emr_response() in subclass")

    @staticmethod
    def state_from_response(response: dict[str, Any]) -> str:
        """
        Get state from boto3 response.

        :param response: response from AWS API
        :return: state
        """
        raise NotImplementedError("Please implement state_from_response() in subclass")

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get state from boto3 response.

        :param response: response from AWS API
        :return: failure message
        """
        raise NotImplementedError("Please implement failure_message_from_response() in subclass")


class EmrServerlessJobSensor(AwsBaseSensor[EmrServerlessHook]):
    """
    Poll the state of the job run until it reaches a terminal state; fails if the job run fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrServerlessJobSensor`

    :param application_id: application_id to check the state of
    :param job_run_id: job_run_id to check the state of
    :param target_states: a set of states to wait for, defaults to 'SUCCESS'
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.h
    """

    aws_hook_class = EmrServerlessHook
    template_fields: Sequence[str] = aws_template_fields(
        "application_id",
        "job_run_id",
    )

    def __init__(
        self,
        *,
        application_id: str,
        job_run_id: str,
        target_states: set | frozenset = frozenset(EmrServerlessHook.JOB_SUCCESS_STATES),
        **kwargs: Any,
    ) -> None:
        self.target_states = target_states
        self.application_id = application_id
        self.job_run_id = job_run_id
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool:
        response = self.hook.conn.get_job_run(applicationId=self.application_id, jobRunId=self.job_run_id)

        state = response["jobRun"]["state"]

        if state in EmrServerlessHook.JOB_FAILURE_STATES:
            raise AirflowException(
                f"EMR Serverless job failed: {self.failure_message_from_response(response)}"
            )

        return state in self.target_states

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        return response["jobRun"]["stateDetails"]


class EmrServerlessApplicationSensor(AwsBaseSensor[EmrServerlessHook]):
    """
    Poll the state of the application until it reaches a terminal state; fails if the application fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrServerlessApplicationSensor`

    :param application_id: application_id to check the state of
    :param target_states: a set of states to wait for, defaults to {'CREATED', 'STARTED'}
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.h
    """

    aws_hook_class = EmrServerlessHook
    template_fields: Sequence[str] = aws_template_fields(
        "application_id",
    )

    def __init__(
        self,
        *,
        application_id: str,
        target_states: set | frozenset = frozenset(EmrServerlessHook.APPLICATION_SUCCESS_STATES),
        **kwargs: Any,
    ) -> None:
        self.target_states = target_states
        self.application_id = application_id
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool:
        response = self.hook.conn.get_application(applicationId=self.application_id)

        state = response["application"]["state"]

        if state in EmrServerlessHook.APPLICATION_FAILURE_STATES:
            raise AirflowException(
                f"EMR Serverless application failed: {self.failure_message_from_response(response)}"
            )

        return state in self.target_states

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        return response["application"]["stateDetails"]


class EmrContainerSensor(AwsBaseSensor[EmrContainerHook]):
    """
    Poll the state of the job run until it reaches a terminal state; fail if the job run fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrContainerSensor`

    :param job_id: job_id to check the state of
    :param max_retries: Number of times to poll for query state before
        returning the current state, defaults to None
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.h
    :param poll_interval: Time in seconds to wait between two consecutive call to
        check query status on athena, defaults to 10
    :param deferrable: Run sensor in the deferrable mode.
    """

    INTERMEDIATE_STATES = (
        "PENDING",
        "SUBMITTED",
        "RUNNING",
    )
    FAILURE_STATES = (
        "FAILED",
        "CANCELLED",
        "CANCEL_PENDING",
    )
    SUCCESS_STATES = ("COMPLETED",)

    aws_hook_class = EmrContainerHook
    template_fields: Sequence[str] = aws_template_fields("virtual_cluster_id", "job_id")
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        *,
        virtual_cluster_id: str,
        job_id: str,
        max_retries: int | None = None,
        poll_interval: int = 10,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.virtual_cluster_id = virtual_cluster_id
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.deferrable = deferrable

    @property
    def _hook_parameters(self):
        return {**super()._hook_parameters, "virtual_cluster_id": self.virtual_cluster_id}

    def poke(self, context: Context) -> bool:
        state = self.hook.poll_query_status(
            self.job_id,
            max_polling_attempts=self.max_retries,
            poll_interval=self.poll_interval,
        )

        if state in self.FAILURE_STATES:
            raise AirflowException(f"EMR Containers sensor failed due to state: {state}")

        if state in self.INTERMEDIATE_STATES:
            return False
        return True

    def execute(self, context: Context):
        if not self.deferrable:
            super().execute(context=context)
        else:
            timeout = (
                timedelta(seconds=self.max_retries * self.poll_interval + 60)
                if self.max_retries
                else self.execution_timeout
            )
            self.defer(
                timeout=timeout,
                trigger=EmrContainerTrigger(
                    virtual_cluster_id=self.virtual_cluster_id,
                    job_id=self.job_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.poll_interval,
                    waiter_max_attempts=self.max_retries,
                )
                if self.max_retries
                else EmrContainerTrigger(
                    virtual_cluster_id=self.virtual_cluster_id,
                    job_id=self.job_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.poll_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error while running job: {validated_event}")

        self.log.info("Job completed.")


class EmrNotebookExecutionSensor(EmrBaseSensor):
    """
    Poll the EMR notebook until it reaches any of the target states; raise AirflowException on failure.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrNotebookExecutionSensor`

    :param notebook_execution_id: Unique id of the notebook execution to be poked.
    :target_states: the states the sensor will wait for the execution to reach.
        Default target_states is ``FINISHED``.
    :failed_states: if the execution reaches any of the failed_states, the sensor will fail.
        Default failed_states is ``FAILED``.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "notebook_execution_id",
    )

    FAILURE_STATES = {"FAILED"}
    COMPLETED_STATES = {"FINISHED"}

    def __init__(
        self,
        notebook_execution_id: str,
        target_states: Iterable[str] | None = None,
        failed_states: Iterable[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.notebook_execution_id = notebook_execution_id
        self.target_states = target_states or self.COMPLETED_STATES
        self.failed_states = failed_states or self.FAILURE_STATES

    def get_emr_response(self, context: Context) -> dict[str, Any]:
        self.log.info("Poking notebook %s", self.notebook_execution_id)

        return self.hook.conn.describe_notebook_execution(NotebookExecutionId=self.notebook_execution_id)

    @staticmethod
    def state_from_response(response: dict[str, Any]) -> str:
        """
        Make an API call with boto3 and get cluster-level details.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_cluster

        :return: response
        """
        return response["NotebookExecution"]["Status"]

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        cluster_status = response["NotebookExecution"]
        return cluster_status.get("LastStateChangeReason", None)


class EmrJobFlowSensor(EmrBaseSensor):
    """
    Poll the EMR JobFlow Cluster until it reaches any of the target states; raise AirflowException on failure.

    With the default target states, sensor waits cluster to be terminated.
    When target_states is set to ['RUNNING', 'WAITING'] sensor waits
    until job flow to be ready (after 'STARTING' and 'BOOTSTRAPPING' states)

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrJobFlowSensor`

    :param job_flow_id: job_flow_id to check the state of
    :param target_states: the target states, sensor waits until
        job flow reaches any of these states. In deferrable mode it would
        run until reach the terminal state.
    :param failed_states: the failure states, sensor fails when
        job flow reaches any of these states
    :param max_attempts: Maximum number of tries before failing
    :param deferrable: Run sensor in the deferrable mode.
    """

    template_fields: Sequence[str] = aws_template_fields("job_flow_id", "target_states", "failed_states")
    template_ext: Sequence[str] = ()
    operator_extra_links = (
        EmrClusterLink(),
        EmrLogsLink(),
    )

    def __init__(
        self,
        *,
        job_flow_id: str,
        target_states: Iterable[str] | None = None,
        failed_states: Iterable[str] | None = None,
        max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.target_states = target_states or ["TERMINATED"]
        self.failed_states = failed_states or ["TERMINATED_WITH_ERRORS"]
        self.max_attempts = max_attempts
        self.deferrable = deferrable

    def get_emr_response(self, context: Context) -> dict[str, Any]:
        """
        Make an API call with boto3 and get cluster-level details.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_cluster

        :return: response
        """
        self.log.info("Poking cluster %s", self.job_flow_id)
        response = self.hook.conn.describe_cluster(ClusterId=self.job_flow_id)

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            job_flow_id=self.job_flow_id,
        )
        EmrLogsLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            job_flow_id=self.job_flow_id,
            log_uri=get_log_uri(cluster=response),
        )
        return response

    @staticmethod
    def state_from_response(response: dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :return: current state of the cluster
        """
        return response["Cluster"]["Status"]["State"]

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        cluster_status = response["Cluster"]["Status"]
        state_change_reason = cluster_status.get("StateChangeReason")
        if state_change_reason:
            return (
                f"for code: {state_change_reason.get('Code', 'No code')} "
                f"with message {state_change_reason.get('Message', 'Unknown')}"
            )
        return None

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        elif not self.poke(context):
            self.defer(
                timeout=timedelta(seconds=self.poke_interval * self.max_attempts),
                trigger=EmrTerminateJobFlowTrigger(
                    job_flow_id=self.job_flow_id,
                    waiter_max_attempts=self.max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=int(self.poke_interval),
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error while running job: {validated_event}")
        self.log.info("Job completed.")


class EmrStepSensor(EmrBaseSensor):
    """
    Poll the state of the step until it reaches any of the target states; raise AirflowException on failure.

    With the default target states, sensor waits step to be completed.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrStepSensor`

    :param job_flow_id: job_flow_id which contains the step check the state of
    :param step_id: step to check the state of
    :param target_states: the target states, sensor waits until
        step reaches any of these states. In case of deferrable sensor it will
        for reach to terminal state
    :param failed_states: the failure states, sensor fails when
        step reaches any of these states
    :param max_attempts: Maximum number of tries before failing
    :param deferrable: Run sensor in the deferrable mode.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "job_flow_id", "step_id", "target_states", "failed_states"
    )
    template_ext: Sequence[str] = ()
    operator_extra_links = (
        EmrClusterLink(),
        EmrLogsLink(),
    )

    def __init__(
        self,
        *,
        job_flow_id: str,
        step_id: str,
        target_states: Iterable[str] | None = None,
        failed_states: Iterable[str] | None = None,
        max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.target_states = target_states or ["COMPLETED"]
        self.failed_states = failed_states or ["CANCELLED", "FAILED", "INTERRUPTED"]
        self.max_attempts = max_attempts
        self.deferrable = deferrable

    def get_emr_response(self, context: Context) -> dict[str, Any]:
        """
        Make an API call with boto3 and get details about the cluster step.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

        :return: response
        """
        self.log.info("Poking step %s on cluster %s", self.step_id, self.job_flow_id)
        response = self.hook.conn.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            job_flow_id=self.job_flow_id,
        )
        EmrLogsLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            job_flow_id=self.job_flow_id,
            log_uri=get_log_uri(emr_client=self.hook.conn, job_flow_id=self.job_flow_id),
        )

        return response

    @staticmethod
    def state_from_response(response: dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :return: execution state of the cluster step
        """
        return response["Step"]["Status"]["State"]

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        fail_details = response["Step"]["Status"].get("FailureDetails")
        if fail_details:
            return (
                f"for reason {fail_details.get('Reason')} "
                f"with message {fail_details.get('Message')} and log file {fail_details.get('LogFile')}"
            )
        return None

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        elif not self.poke(context):
            self.defer(
                timeout=timedelta(seconds=self.max_attempts * self.poke_interval),
                trigger=EmrStepSensorTrigger(
                    job_flow_id=self.job_flow_id,
                    step_id=self.step_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error while running job: {validated_event}")

        self.log.info("Job %s completed.", self.job_flow_id)
