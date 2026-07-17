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
from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.sagemaker import LogState, SageMakerHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Context


class SageMakerBaseSensor(AwsBaseSensor[SageMakerHook]):
    """
    Contains general sensor behavior for SageMaker.

    Subclasses should implement get_sagemaker_response() and state_from_response() methods.
    Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE methods.
    """

    aws_hook_class = SageMakerHook
    ui_color = "#ededed"

    def __init__(self, *, resource_type: str = "job", **kwargs):
        super().__init__(**kwargs)
        self.resource_type = resource_type  # only used for logs, to say what kind of resource we are sensing

    def poke(self, context: Context):
        response = self.get_sagemaker_response()
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            self.log.info("Bad HTTP response: %s", response)
            return False
        state = self.state_from_response(response)
        self.log.info("%s currently %s", self.resource_type, state)
        if state in self.non_terminal_states():
            return False
        if state in self.failed_states():
            failed_reason = self.get_failed_reason_from_response(response)
            raise AirflowException(
                f"Sagemaker {self.resource_type} failed for the following reason: {failed_reason}"
            )
        return True

    def non_terminal_states(self) -> set[str]:
        """Return states with should not terminate."""
        raise NotImplementedError("Please implement non_terminal_states() in subclass")

    def failed_states(self) -> set[str]:
        """Return states with are considered failed."""
        raise NotImplementedError("Please implement failed_states() in subclass")

    def get_sagemaker_response(self) -> dict:
        """Check status of a SageMaker task."""
        raise NotImplementedError("Please implement get_sagemaker_response() in subclass")

    def get_failed_reason_from_response(self, response: dict) -> str:
        """Extract the reason for failure from an AWS response."""
        return "Unknown"

    def state_from_response(self, response: dict) -> str:
        """Extract the state from an AWS response."""
        raise NotImplementedError("Please implement state_from_response() in subclass")


class SageMakerEndpointSensor(SageMakerBaseSensor):
    """
    Poll the endpoint state until it reaches a terminal state; raise AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerEndpointSensor`

    :param endpoint_name: Name of the endpoint instance to watch.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "endpoint_name",
    )
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
        return self.hook.describe_endpoint(self.endpoint_name)

    def get_failed_reason_from_response(self, response):
        return response["FailureReason"]

    def state_from_response(self, response):
        return response["EndpointStatus"]


class SageMakerTransformSensor(SageMakerBaseSensor):
    """
    Poll the transform job until it reaches a terminal state; raise AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerTransformSensor`

    :param job_name: Name of the transform job to watch.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "job_name",
    )
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
        return self.hook.describe_transform_job(self.job_name)

    def get_failed_reason_from_response(self, response):
        return response["FailureReason"]

    def state_from_response(self, response):
        return response["TransformJobStatus"]


class SageMakerTuningSensor(SageMakerBaseSensor):
    """
    Poll the tuning state until it reaches a terminal state; raise AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerTuningSensor`

    :param job_name: Name of the tuning instance to watch.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "job_name",
    )
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
        return self.hook.describe_tuning_job(self.job_name)

    def get_failed_reason_from_response(self, response):
        return response["FailureReason"]

    def state_from_response(self, response):
        return response["HyperParameterTuningJobStatus"]


class SageMakerTrainingSensor(SageMakerBaseSensor):
    """
    Poll the training job until it reaches a terminal state; raise AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerTrainingSensor`

    :param job_name: Name of the training job to watch.
    :param print_log: Prints the cloudwatch log if True; Defaults to True.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "job_name",
    )
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
        return SageMakerHook.training_failed_states

    def get_sagemaker_response(self):
        if self.print_log:
            if not self.log_resource_inited:
                self.init_log_resource(self.hook)
            (
                self.state,
                self.last_description,
                self.last_describe_job_call,
            ) = self.hook.describe_training_job_with_log(
                self.job_name,
                self.positions,
                self.stream_names,
                self.instance_count,
                self.state,
                self.last_description,
                self.last_describe_job_call,
            )
        else:
            self.last_description = self.hook.describe_training_job(self.job_name)
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


class SageMakerPipelineSensor(SageMakerBaseSensor):
    """
    Poll the pipeline until it reaches a terminal state; raise AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerPipelineSensor`

    :param pipeline_exec_arn: ARN of the pipeline to watch.
    :param verbose: Whether to print steps details while waiting for completion.
            Defaults to true, consider turning off for pipelines that have thousands of steps.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "pipeline_exec_arn",
    )

    def __init__(self, *, pipeline_exec_arn: str, verbose: bool = True, **kwargs):
        super().__init__(resource_type="pipeline", **kwargs)
        self.pipeline_exec_arn = pipeline_exec_arn
        self.verbose = verbose

    def non_terminal_states(self) -> set[str]:
        return SageMakerHook.pipeline_non_terminal_states

    def failed_states(self) -> set[str]:
        return SageMakerHook.failed_states

    def get_sagemaker_response(self) -> dict:
        self.log.info("Poking Sagemaker Pipeline Execution %s", self.pipeline_exec_arn)
        return self.hook.describe_pipeline_exec(self.pipeline_exec_arn, self.verbose)

    def state_from_response(self, response: dict) -> str:
        return response["PipelineExecutionStatus"]


class SageMakerAutoMLSensor(SageMakerBaseSensor):
    """
    Poll the auto ML job until it reaches a terminal state; raise AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerAutoMLSensor`

    :param job_name: unique name of the AutoML job to watch.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "job_name",
    )

    def __init__(self, *, job_name: str, **kwargs):
        super().__init__(resource_type="autoML job", **kwargs)
        self.job_name = job_name

    def non_terminal_states(self) -> set[str]:
        return SageMakerHook.non_terminal_states

    def failed_states(self) -> set[str]:
        return SageMakerHook.failed_states

    def get_sagemaker_response(self) -> dict:
        self.log.info("Poking Sagemaker AutoML Execution %s", self.job_name)
        return self.hook._describe_auto_ml_job(self.job_name)

    def state_from_response(self, response: dict) -> str:
        return response["AutoMLJobStatus"]


class SageMakerProcessingSensor(SageMakerBaseSensor):
    """
    Poll the processing job until it reaches a terminal state; raise AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SageMakerProcessingSensor`

    :param job_name: Name of the processing job to watch.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "job_name",
    )
    template_ext: Sequence[str] = ()

    def __init__(self, *, job_name: str, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name

    def non_terminal_states(self) -> set[str]:
        return SageMakerHook.processing_job_non_terminal_states

    def failed_states(self) -> set[str]:
        return SageMakerHook.processing_job_failed_states

    def get_sagemaker_response(self) -> dict:
        self.log.info("Poking Sagemaker ProcessingJob %s", self.job_name)
        return self.hook.describe_processing_job(self.job_name)

    def state_from_response(self, response: dict) -> str:
        return response["ProcessingJobStatus"]
