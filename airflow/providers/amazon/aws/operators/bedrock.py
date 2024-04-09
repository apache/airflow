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

import json
from typing import TYPE_CHECKING, Any, Sequence

from botocore.exceptions import ClientError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook, BedrockRuntimeHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.bedrock import BedrockCustomizeModelCompletedTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.helpers import prune_dict
from airflow.utils.timezone import utcnow

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BedrockInvokeModelOperator(AwsBaseOperator[BedrockRuntimeHook]):
    """
    Invoke the specified Bedrock model to run inference using the input provided.

    Use InvokeModel to run inference for text models, image models, and embedding models.
    To see the format and content of the input_data field for different models, refer to
    `Inference parameters docs <https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters.html>`_.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockInvokeModelOperator`

    :param model_id: The ID of the Bedrock model. (templated)
    :param input_data: Input data in the format specified in the content-type request header. (templated)
    :param content_type: The MIME type of the input data in the request. (templated) Default: application/json
    :param accept: The desired MIME type of the inference body in the response.
        (templated) Default: application/json

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

    aws_hook_class = BedrockRuntimeHook
    template_fields: Sequence[str] = aws_template_fields(
        "model_id", "input_data", "content_type", "accept_type"
    )

    def __init__(
        self,
        model_id: str,
        input_data: dict[str, Any],
        content_type: str | None = None,
        accept_type: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.model_id = model_id
        self.input_data = input_data
        self.content_type = content_type
        self.accept_type = accept_type

    def execute(self, context: Context) -> dict[str, str | int]:
        # These are optional values which the API defaults to "application/json" if not provided here.
        invoke_kwargs = prune_dict({"contentType": self.content_type, "accept": self.accept_type})

        response = self.hook.conn.invoke_model(
            body=json.dumps(self.input_data),
            modelId=self.model_id,
            **invoke_kwargs,
        )

        response_body = json.loads(response["body"].read())
        self.log.info("Bedrock %s prompt: %s", self.model_id, self.input_data)
        self.log.info("Bedrock model response: %s", response_body)
        return response_body


class BedrockCustomizeModelOperator(AwsBaseOperator[BedrockHook]):
    """
    Create a fine-tuning job to customize a base model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockCustomizeModelOperator`

    :param job_name: A unique name for the fine-tuning job.
    :param custom_model_name: A name for the custom model being created.
    :param role_arn: The Amazon Resource Name (ARN) of an IAM role that Amazon Bedrock can assume
        to perform tasks on your behalf.
    :param base_model_id: Name of the base model.
    :param training_data_uri: The S3 URI where the training data is stored.
    :param output_data_uri: The S3 URI where the output data is stored.
    :param hyperparameters: Parameters related to tuning the model.
    :param ensure_unique_job_name: If set to true, operator will check whether a model customization
        job already exists for the name in the config and append the current timestamp if there is a
        name conflict. (Default: True)
    :param customization_job_kwargs: Any optional parameters to pass to the API.

    :param wait_for_completion: Whether to wait for cluster to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 120)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 75)
    :param deferrable: If True, the operator will wait asynchronously for the cluster to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
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

    aws_hook_class = BedrockHook
    template_fields: Sequence[str] = aws_template_fields(
        "job_name",
        "custom_model_name",
        "role_arn",
        "base_model_id",
        "hyperparameters",
        "ensure_unique_job_name",
        "customization_job_kwargs",
    )

    def __init__(
        self,
        job_name: str,
        custom_model_name: str,
        role_arn: str,
        base_model_id: str,
        training_data_uri: str,
        output_data_uri: str,
        hyperparameters: dict[str, str],
        ensure_unique_job_name: bool = True,
        customization_job_kwargs: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

        self.job_name = job_name
        self.custom_model_name = custom_model_name
        self.role_arn = role_arn
        self.base_model_id = base_model_id
        self.training_data_config = {"s3Uri": training_data_uri}
        self.output_data_config = {"s3Uri": output_data_uri}
        self.hyperparameters = hyperparameters
        self.ensure_unique_job_name = ensure_unique_job_name
        self.customization_job_kwargs = customization_job_kwargs or {}

        self.valid_action_if_job_exists: set[str] = {"timestamp", "fail"}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")

        self.log.info("Bedrock model customization job `%s` complete.", self.job_name)
        return self.hook.conn.get_model_customization_job(jobIdentifier=event["job_name"])["jobArn"]

    def execute(self, context: Context) -> dict:
        response = {}
        retry = True
        while retry:
            # If there is a name conflict and ensure_unique_job_name is True, append the current timestamp
            # to the name and retry until there is no name conflict.
            # - Break the loop when the API call returns success.
            # - If the API returns an exception other than a name conflict, raise that exception.
            # - If the API returns a name conflict and ensure_unique_job_name is false, raise that exception.
            try:
                # Ensure the loop is executed at least once, and not repeat unless explicitly set to do so.
                retry = False
                self.log.info("Creating Bedrock model customization job '%s'.", self.job_name)

                response = self.hook.conn.create_model_customization_job(
                    jobName=self.job_name,
                    customModelName=self.custom_model_name,
                    roleArn=self.role_arn,
                    baseModelIdentifier=self.base_model_id,
                    trainingDataConfig=self.training_data_config,
                    outputDataConfig=self.output_data_config,
                    hyperParameters=self.hyperparameters,
                    **self.customization_job_kwargs,
                )
            except ClientError as error:
                if error.response["Error"]["Message"] != "The provided job name is currently in use.":
                    raise error
                if not self.ensure_unique_job_name:
                    raise error
                retry = True
                self.job_name = f"{self.job_name}-{int(utcnow().timestamp())}"
                self.log.info("Changed job name to '%s' to avoid collision.", self.job_name)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 201:
            raise AirflowException(f"Bedrock model customization job creation failed: {response}")

        task_description = f"Bedrock model customization job {self.job_name} to complete."
        if self.deferrable:
            self.log.info("Deferring for %s", task_description)
            self.defer(
                trigger=BedrockCustomizeModelCompletedTrigger(
                    job_name=self.job_name,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            self.log.info("Waiting for %s", task_description)
            self.hook.get_waiter("model_customization_job_complete").wait(
                jobIdentifier=self.job_name,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return response["jobArn"]
