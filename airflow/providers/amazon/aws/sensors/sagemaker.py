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

import time
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import LogState, SageMakerHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SageMakerBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for SageMaker.

    Subclasses should implement get_sagemaker_response() and state_from_response() methods.
    Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE methods.
    """

    ui_color = "#ededed"

    def __init__(self, *, aws_conn_id: str = "aws_default", **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.hook: SageMakerHook | None = None

    def get_hook(self) -> SageMakerHook:
        """Get SageMakerHook."""
        if self.hook:
            return self.hook
        self.hook = SageMakerHook(aws_conn_id=self.aws_conn_id)
        return self.hook

    def poke(self, context: Context):
        response = self.get_sagemaker_response()
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            self.log.info("Bad HTTP response: %s", response)
            return False
        state = self.state_from_response(response)
        self.log.info("Job currently %s", state)
        if state in self.non_terminal_states():
            return False
        if state in self.failed_states():
            failed_reason = self.get_failed_reason_from_response(response)
            raise AirflowException(f"Sagemaker job failed for the following reason: {failed_reason}")
        return True

    def non_terminal_states(self) -> set[str]:
        """Placeholder for returning states with should not terminate."""
        raise NotImplementedError("Please implement non_terminal_states() in subclass")

    def failed_states(self) -> set[str]:
        """Placeholder for returning states with are considered failed."""
        raise NotImplementedError("Please implement failed_states() in subclass")

    def get_sagemaker_response(self) -> dict:
        """Placeholder for checking status of a SageMaker task."""
        raise NotImplementedError("Please implement get_sagemaker_response() in subclass")

    def get_failed_reason_from_response(self, response: dict) -> str:
        """Placeholder for extracting the reason for failure from an AWS response."""
        return "Unknown"

    def state_from_response(self, response: dict) -> str:
        """Placeholder for extracting the state from an AWS response."""
        raise NotImplementedError("Please implement state_from_response() in subclass")


class SageMakerEndpointSensor(SageMakerBaseSensor):
    """
    Polls the endpoint state until it reaches a terminal state.  Raises an
    AirflowException with the failure reason if a failed state is reached.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerEndpointSensor`

    :param endpoint_name: Name of the endpoint instance to watch.
    """

    template_fields: Sequence[str] = ("endpoint_name",)
    template_ext: Sequence[str] = ()

    def __init__(self, *, endpoint_name, **kwargs):
        super().__init__(**kwargs)
        self.endpoint_name = endpoint_name

    def non_terminal_states(self):
        return SageMakerHook.endpoint_non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        self.log.info("Poking Sagemaker Endpoint %s", self.endpoint_name)
        return self.get_hook().describe_endpoint(self.endpoint_name)

    def get_failed_reason_from_response(self, response):
        return response["FailureReason"]

    def state_from_response(self, response):
        return response["EndpointStatus"]


class SageMakerTransformSensor(SageMakerBaseSensor):
    """
    Polls the transform job until it reaches a terminal state.  Raises an
    AirflowException with the failure reason if a failed state is reached.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerTransformSensor`

    :param job_name: Name of the transform job to watch.
    """

    template_fields: Sequence[str] = ("job_name",)
    template_ext: Sequence[str] = ()

    def __init__(self, *, job_name: str, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name

    def non_terminal_states(self):
        return SageMakerHook.non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        self.log.info("Poking Sagemaker Transform Job %s", self.job_name)
        return self.get_hook().describe_transform_job(self.job_name)

    def get_failed_reason_from_response(self, response):
        return response["FailureReason"]

    def state_from_response(self, response):
        return response["TransformJobStatus"]


class SageMakerTuningSensor(SageMakerBaseSensor):
    """
    Asks for the state of the tuning state until it reaches a terminal state.
    Raises an AirflowException with the failure reason if a failed state is reached.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerTuningSensor`

    :param job_name: Name of the tuning instance to watch.
    """

    template_fields: Sequence[str] = ("job_name",)
    template_ext: Sequence[str] = ()

    def __init__(self, *, job_name: str, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name

    def non_terminal_states(self):
        return SageMakerHook.non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        self.log.info("Poking Sagemaker Tuning Job %s", self.job_name)
        return self.get_hook().describe_tuning_job(self.job_name)

    def get_failed_reason_from_response(self, response):
        return response["FailureReason"]

    def state_from_response(self, response):
        return response["HyperParameterTuningJobStatus"]


class SageMakerTrainingSensor(SageMakerBaseSensor):
    """
    Polls the training job until it reaches a terminal state.  Raises an
    AirflowException with the failure reason if a failed state is reached.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerTrainingSensor`

    :param job_name: Name of the training job to watch.
    :param print_log: Prints the cloudwatch log if True; Defaults to True.
    """

    template_fields: Sequence[str] = ("job_name",)
    template_ext: Sequence[str] = ()

    def __init__(self, *, job_name, print_log=True, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.print_log = print_log
        self.positions = {}
        self.stream_names = []
        self.instance_count: int | None = None
        self.state: int | None = None
        self.last_description = None
        self.last_describe_job_call = None
        self.log_resource_inited = False

    def init_log_resource(self, hook: SageMakerHook) -> None:
        """Set tailing LogState for associated training job."""
        description = hook.describe_training_job(self.job_name)
        self.instance_count = description["ResourceConfig"]["InstanceCount"]
        status = description["TrainingJobStatus"]
        job_already_completed = status not in self.non_terminal_states()
        self.state = LogState.COMPLETE if job_already_completed else LogState.TAILING
        self.last_description = description
        self.last_describe_job_call = time.monotonic()
        self.log_resource_inited = True

    def non_terminal_states(self):
        return SageMakerHook.non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        if self.print_log:
            if not self.log_resource_inited:
                self.init_log_resource(self.get_hook())
            (
                self.state,
                self.last_description,
                self.last_describe_job_call,
            ) = self.get_hook().describe_training_job_with_log(
                self.job_name,
                self.positions,
                self.stream_names,
                self.instance_count,
                self.state,
                self.last_description,
                self.last_describe_job_call,
            )
        else:
            self.last_description = self.get_hook().describe_training_job(self.job_name)
        status = self.state_from_response(self.last_description)
        if (status not in self.non_terminal_states()) and (status not in self.failed_states()):
            billable_time = (
                self.last_description["TrainingEndTime"] - self.last_description["TrainingStartTime"]
            ) * self.last_description["ResourceConfig"]["InstanceCount"]
            self.log.info("Billable seconds: %s", (int(billable_time.total_seconds()) + 1))
        return self.last_description

    def get_failed_reason_from_response(self, response):
        return response["FailureReason"]

    def state_from_response(self, response):
        return response["TrainingJobStatus"]
