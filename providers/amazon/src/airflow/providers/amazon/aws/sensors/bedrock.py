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

import abc
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentHook, BedrockHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.bedrock import (
    BedrockBatchInferenceCompletedTrigger,
    BedrockBatchInferenceScheduledTrigger,
    BedrockCustomizeModelCompletedTrigger,
    BedrockIngestionJobTrigger,
    BedrockKnowledgeBaseActiveTrigger,
    BedrockProvisionModelThroughputCompletedTrigger,
)
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, conf

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.triggers.bedrock import BedrockBaseBatchInferenceTrigger
    from airflow.sdk import Context


_GenericBedrockHook = TypeVar("_GenericBedrockHook", BedrockAgentHook, BedrockHook)


class BedrockBaseSensor(AwsBaseSensor[_GenericBedrockHook]):
    """
    General sensor behavior for Amazon Bedrock.

    Subclasses must implement following methods:
        - ``get_state()``

    Subclasses must set the following fields:
        - ``INTERMEDIATE_STATES``
        - ``FAILURE_STATES``
        - ``SUCCESS_STATES``
        - ``FAILURE_MESSAGE``

    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    """

    INTERMEDIATE_STATES: tuple[str, ...] = ()
    FAILURE_STATES: tuple[str, ...] = ()
    SUCCESS_STATES: tuple[str, ...] = ()
    FAILURE_MESSAGE = ""

    aws_hook_class: type[_GenericBedrockHook]
    ui_color = "#66c3ff"

    def __init__(
        self,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.deferrable = deferrable

    def poke(self, context: Context, **kwargs) -> bool:
        state = self.get_state()
        if state in self.FAILURE_STATES:
            raise AirflowException(self.FAILURE_MESSAGE)

        return state not in self.INTERMEDIATE_STATES

    @abc.abstractmethod
    def get_state(self) -> str:
        """Implement in subclasses."""


class BedrockCustomizeModelCompletedSensor(BedrockBaseSensor[BedrockHook]):
    """
    Poll the state of the model customization job until it reaches a terminal state; fails if the job fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BedrockCustomizeModelCompletedSensor`

    :param job_name: The name of the Bedrock model customization job.

    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 120)
    :param max_retries: Number of times before returning the current state. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    INTERMEDIATE_STATES: tuple[str, ...] = ("InProgress",)
    FAILURE_STATES: tuple[str, ...] = ("Failed", "Stopping", "Stopped")
    SUCCESS_STATES: tuple[str, ...] = ("Completed",)
    FAILURE_MESSAGE = "Bedrock model customization job sensor failed."

    aws_hook_class = BedrockHook

    template_fields: Sequence[str] = aws_template_fields("job_name")

    def __init__(
        self,
        *,
        job_name: str,
        max_retries: int = 75,
        poke_interval: int = 120,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.job_name = job_name

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=BedrockCustomizeModelCompletedTrigger(
                    job_name=self.job_name,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)

    def get_state(self) -> str:
        return self.hook.conn.get_model_customization_job(jobIdentifier=self.job_name)["status"]


class BedrockProvisionModelThroughputCompletedSensor(BedrockBaseSensor[BedrockHook]):
    """
    Poll the provisioned model throughput job until it reaches a terminal state; fails if the job fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BedrockProvisionModelThroughputCompletedSensor`

    :param model_id: The ARN or name of the provisioned throughput.

    :param deferrable: If True, the sensor will operate in deferrable more. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 60)
    :param max_retries: Number of times before returning the current state (default: 20)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    INTERMEDIATE_STATES: tuple[str, ...] = ("Creating", "Updating")
    FAILURE_STATES: tuple[str, ...] = ("Failed",)
    SUCCESS_STATES: tuple[str, ...] = ("InService",)
    FAILURE_MESSAGE = "Bedrock provision model throughput sensor failed."

    aws_hook_class = BedrockHook

    template_fields: Sequence[str] = aws_template_fields("model_id")

    def __init__(
        self,
        *,
        model_id: str,
        poke_interval: int = 60,
        max_retries: int = 20,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.model_id = model_id

    def get_state(self) -> str:
        return self.hook.conn.get_provisioned_model_throughput(provisionedModelId=self.model_id)["status"]

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=BedrockProvisionModelThroughputCompletedTrigger(
                    provisioned_model_id=self.model_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)


class BedrockKnowledgeBaseActiveSensor(BedrockBaseSensor[BedrockAgentHook]):
    """
    Poll the Knowledge Base status until it reaches a terminal state; fails if creation fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BedrockKnowledgeBaseActiveSensor`

    :param knowledge_base_id: The unique identifier of the knowledge base for which to get information. (templated)

    :param deferrable: If True, the sensor will operate in deferrable more. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 5)
    :param max_retries: Number of times before returning the current state (default: 24)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    INTERMEDIATE_STATES: tuple[str, ...] = ("CREATING", "UPDATING")
    FAILURE_STATES: tuple[str, ...] = ("DELETING", "FAILED")
    SUCCESS_STATES: tuple[str, ...] = ("ACTIVE",)
    FAILURE_MESSAGE = "Bedrock Knowledge Base Active sensor failed."

    aws_hook_class = BedrockAgentHook

    template_fields: Sequence[str] = aws_template_fields("knowledge_base_id")

    def __init__(
        self,
        *,
        knowledge_base_id: str,
        poke_interval: int = 5,
        max_retries: int = 24,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.knowledge_base_id = knowledge_base_id

    def get_state(self) -> str:
        return self.hook.conn.get_knowledge_base(knowledgeBaseId=self.knowledge_base_id)["knowledgeBase"][
            "status"
        ]

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=BedrockKnowledgeBaseActiveTrigger(
                    knowledge_base_id=self.knowledge_base_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)


class BedrockIngestionJobSensor(BedrockBaseSensor[BedrockAgentHook]):
    """
    Poll the ingestion job status until it reaches a terminal state; fails if creation fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BedrockIngestionJobSensor`

    :param knowledge_base_id: The unique identifier of the knowledge base for which to get information. (templated)
    :param data_source_id: The unique identifier of the data source in the ingestion job. (templated)
    :param ingestion_job_id: The unique identifier of the ingestion job. (templated)

    :param deferrable: If True, the sensor will operate in deferrable more. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 60)
    :param max_retries: Number of times before returning the current state (default: 10)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    INTERMEDIATE_STATES: tuple[str, ...] = ("STARTING", "IN_PROGRESS")
    FAILURE_STATES: tuple[str, ...] = ("FAILED",)
    SUCCESS_STATES: tuple[str, ...] = ("COMPLETE",)
    FAILURE_MESSAGE = "Bedrock ingestion job sensor failed."

    aws_hook_class = BedrockAgentHook

    template_fields: Sequence[str] = aws_template_fields(
        "knowledge_base_id", "data_source_id", "ingestion_job_id"
    )

    def __init__(
        self,
        *,
        knowledge_base_id: str,
        data_source_id: str,
        ingestion_job_id: str,
        poke_interval: int = 60,
        max_retries: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.knowledge_base_id = knowledge_base_id
        self.data_source_id = data_source_id
        self.ingestion_job_id = ingestion_job_id

    def get_state(self) -> str:
        return self.hook.conn.get_ingestion_job(
            knowledgeBaseId=self.knowledge_base_id,
            ingestionJobId=self.ingestion_job_id,
            dataSourceId=self.data_source_id,
        )["ingestionJob"]["status"]

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=BedrockIngestionJobTrigger(
                    knowledge_base_id=self.knowledge_base_id,
                    ingestion_job_id=self.ingestion_job_id,
                    data_source_id=self.data_source_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)


class BedrockBatchInferenceSensor(BedrockBaseSensor[BedrockHook]):
    """
    Poll the batch inference job status until it reaches a terminal state; fails if creation fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BedrockBatchInferenceSensor`

    :param job_arn: The Amazon Resource Name (ARN) of the batch inference job. (templated)
    :param success_state: A BedrockBatchInferenceSensor.TargetState; defaults to 'SCHEDULED' (templated)

    :param deferrable: If True, the sensor will operate in deferrable more. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 5)
    :param max_retries: Number of times before returning the current state (default: 24)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    class SuccessState:
        """
        Target state for the BedrockBatchInferenceSensor.

        Bedrock adds batch inference jobs to a queue, and they may take some time to complete.
        If you want to wait for the job to complete, use TargetState.COMPLETED, but if you only want
        to wait until the service confirms that the job is in the queue, use TargetState.SCHEDULED.

        The normal successful progression of states is:
            Submitted > Validating > Scheduled > InProgress > PartiallyCompleted > Completed
        """

        SCHEDULED = "scheduled"
        COMPLETED = "completed"

    INTERMEDIATE_STATES: tuple[str, ...]  # Defined in __init__ based on target state
    FAILURE_STATES: tuple[str, ...] = ("Failed", "Stopped", "PartiallyCompleted", "Expired")
    SUCCESS_STATES: tuple[str, ...]  # Defined in __init__ based on target state
    FAILURE_MESSAGE = "Bedrock batch inference job sensor failed."
    INVALID_SUCCESS_STATE_MESSAGE = "success_state must be an instance of TargetState."

    aws_hook_class = BedrockHook

    template_fields: Sequence[str] = aws_template_fields("job_arn", "success_state")

    def __init__(
        self,
        *,
        job_arn: str,
        success_state: SuccessState | str = SuccessState.SCHEDULED,
        poke_interval: int = 120,
        max_retries: int = 75,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.job_arn = job_arn
        self.success_state = success_state

        base_success_states: tuple[str, ...] = ("Completed",)
        base_intermediate_states: tuple[str, ...] = ("Submitted", "InProgress", "Stopping", "Validating")
        scheduled_state = ("Scheduled",)
        self.trigger_class: type[BedrockBaseBatchInferenceTrigger]

        if self.success_state == BedrockBatchInferenceSensor.SuccessState.COMPLETED:
            intermediate_states = base_intermediate_states + scheduled_state
            success_states = base_success_states
            self.trigger_class = BedrockBatchInferenceCompletedTrigger
        elif self.success_state == BedrockBatchInferenceSensor.SuccessState.SCHEDULED:
            intermediate_states = base_intermediate_states
            success_states = base_success_states + scheduled_state
            self.trigger_class = BedrockBatchInferenceScheduledTrigger
        else:
            raise ValueError(
                "Success states for BedrockBatchInferenceSensor must be set using a BedrockBatchInferenceSensor.SuccessState"
            )

        BedrockBatchInferenceSensor.INTERMEDIATE_STATES = intermediate_states or base_intermediate_states
        BedrockBatchInferenceSensor.SUCCESS_STATES = success_states or base_success_states

    def get_state(self) -> str:
        return self.hook.conn.get_model_invocation_job(jobIdentifier=self.job_arn)["status"]

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=self.trigger_class(
                    job_arn=self.job_arn,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)
