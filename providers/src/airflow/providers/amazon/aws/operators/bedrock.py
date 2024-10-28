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
from time import sleep
from typing import TYPE_CHECKING, Any, Sequence

from botocore.exceptions import ClientError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.bedrock import (
    BedrockAgentHook,
    BedrockAgentRuntimeHook,
    BedrockHook,
    BedrockRuntimeHook,
)
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.bedrock import (
    BedrockCustomizeModelCompletedTrigger,
    BedrockIngestionJobTrigger,
    BedrockKnowledgeBaseActiveTrigger,
    BedrockProvisionModelThroughputCompletedTrigger,
)
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
        invoke_kwargs = prune_dict(
            {"contentType": self.content_type, "accept": self.accept_type}
        )

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
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
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

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")

        self.log.info("Bedrock model customization job `%s` complete.", self.job_name)
        return self.hook.conn.get_model_customization_job(
            jobIdentifier=event["job_name"]
        )["jobArn"]

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
                self.log.info(
                    "Creating Bedrock model customization job '%s'.", self.job_name
                )

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
                if (
                    error.response["Error"]["Message"]
                    != "The provided job name is currently in use."
                ):
                    raise error
                if not self.ensure_unique_job_name:
                    raise error
                retry = True
                self.job_name = f"{self.job_name}-{int(utcnow().timestamp())}"
                self.log.info(
                    "Changed job name to '%s' to avoid collision.", self.job_name
                )

        if response["ResponseMetadata"]["HTTPStatusCode"] != 201:
            raise AirflowException(
                f"Bedrock model customization job creation failed: {response}"
            )

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
                WaiterConfig={
                    "Delay": self.waiter_delay,
                    "MaxAttempts": self.waiter_max_attempts,
                },
            )

        return response["jobArn"]


class BedrockCreateProvisionedModelThroughputOperator(AwsBaseOperator[BedrockHook]):
    """
    Create a fine-tuning job to customize a base model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockCreateProvisionedModelThroughputOperator`

    :param model_units: Number of model units to allocate. (templated)
    :param provisioned_model_name: Unique name for this provisioned throughput. (templated)
    :param model_id: Name or ARN of the model to associate with this provisioned throughput. (templated)
    :param create_throughput_kwargs: Any optional parameters to pass to the API.

    :param wait_for_completion: Whether to wait for cluster to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 60)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 20)
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
        "model_units",
        "provisioned_model_name",
        "model_id",
    )

    def __init__(
        self,
        model_units: int,
        provisioned_model_name: str,
        model_id: str,
        create_throughput_kwargs: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.model_units = model_units
        self.provisioned_model_name = provisioned_model_name
        self.model_id = model_id
        self.create_throughput_kwargs = create_throughput_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> str:
        provisioned_model_id = self.hook.conn.create_provisioned_model_throughput(
            modelUnits=self.model_units,
            provisionedModelName=self.provisioned_model_name,
            modelId=self.model_id,
            **self.create_throughput_kwargs,
        )["provisionedModelArn"]

        if self.deferrable:
            self.log.info("Deferring for provisioned throughput.")
            self.defer(
                trigger=BedrockProvisionModelThroughputCompletedTrigger(
                    provisioned_model_id=provisioned_model_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            self.log.info("Waiting for provisioned throughput.")
            self.hook.get_waiter("provisioned_model_throughput_complete").wait(
                provisionedModelId=provisioned_model_id,
                WaiterConfig={
                    "Delay": self.waiter_delay,
                    "MaxAttempts": self.waiter_max_attempts,
                },
            )

        return provisioned_model_id

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")

        self.log.info(
            "Bedrock provisioned throughput job `%s` complete.",
            event["provisioned_model_id"],
        )
        return event["provisioned_model_id"]


class BedrockCreateKnowledgeBaseOperator(AwsBaseOperator[BedrockAgentHook]):
    """
    Create a knowledge base that contains data sources used by Amazon Bedrock LLMs and Agents.

    To create a knowledge base, you must first set up your data sources and configure a supported vector store.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockCreateKnowledgeBaseOperator`

    :param name: The name of the knowledge base. (templated)
    :param embedding_model_arn: ARN of the model used to create vector embeddings for the knowledge base. (templated)
    :param role_arn: The ARN of the IAM role with permissions to create the knowledge base. (templated)
    :param storage_config: Configuration details of the vector database used for the knowledge base. (templated)
    :param wait_for_indexing: Vector indexing can take some time and there is no apparent way to check the state
        before trying to create the Knowledge Base.  If this is True, and creation fails due to the index not
        being available, the operator will wait and retry.  (default: True) (templated)
    :param indexing_error_retry_delay: Seconds between retries if an index error is encountered. (default 5) (templated)
    :param indexing_error_max_attempts: Maximum number of times to retry when encountering an index error. (default 20) (templated)
    :param create_knowledge_base_kwargs: Any additional optional parameters to pass to the API call. (templated)

    :param wait_for_completion: Whether to wait for cluster to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 60)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 20)
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

    aws_hook_class = BedrockAgentHook
    template_fields: Sequence[str] = aws_template_fields(
        "name",
        "embedding_model_arn",
        "role_arn",
        "storage_config",
        "wait_for_indexing",
        "indexing_error_retry_delay",
        "indexing_error_max_attempts",
        "create_knowledge_base_kwargs",
    )

    def __init__(
        self,
        name: str,
        embedding_model_arn: str,
        role_arn: str,
        storage_config: dict[str, Any],
        create_knowledge_base_kwargs: dict[str, Any] | None = None,
        wait_for_indexing: bool = True,
        indexing_error_retry_delay: int = 5,  # seconds
        indexing_error_max_attempts: int = 20,
        wait_for_completion: bool = True,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name
        self.role_arn = role_arn
        self.storage_config = storage_config
        self.create_knowledge_base_kwargs = create_knowledge_base_kwargs or {}
        self.embedding_model_arn = embedding_model_arn
        self.knowledge_base_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": self.embedding_model_arn
            },
        }
        self.wait_for_indexing = wait_for_indexing
        self.indexing_error_retry_delay = indexing_error_retry_delay
        self.indexing_error_max_attempts = indexing_error_max_attempts

        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")

        self.log.info("Bedrock knowledge base creation job `%s` complete.", self.name)
        return event["knowledge_base_id"]

    def execute(self, context: Context) -> str:
        def _create_kb():
            # This API call will return the following if the index has not completed, but there is no apparent
            # way to check the state of the index beforehand, so retry on index failure if set to do so.
            #       botocore.errorfactory.ValidationException: An error occurred (ValidationException)
            #       when calling the CreateKnowledgeBase operation: The knowledge base storage configuration
            #       provided is invalid... no such index [bedrock-sample-rag-index-abc108]
            try:
                return self.hook.conn.create_knowledge_base(
                    name=self.name,
                    roleArn=self.role_arn,
                    knowledgeBaseConfiguration=self.knowledge_base_config,
                    storageConfiguration=self.storage_config,
                    **self.create_knowledge_base_kwargs,
                )["knowledgeBase"]["knowledgeBaseId"]
            except ClientError as error:
                if all(
                    [
                        error.response["Error"]["Code"] == "ValidationException",
                        "no such index" in error.response["Error"]["Message"],
                        self.wait_for_indexing,
                        self.indexing_error_max_attempts > 0,
                    ]
                ):
                    self.indexing_error_max_attempts -= 1
                    self.log.warning(
                        "Vector index not ready, retrying in %s seconds.",
                        self.indexing_error_retry_delay,
                    )
                    self.log.debug(
                        "%s retries remaining.", self.indexing_error_max_attempts
                    )
                    sleep(self.indexing_error_retry_delay)
                    return _create_kb()
                raise

        self.log.info("Creating Amazon Bedrock Knowledge Base %s", self.name)
        knowledge_base_id = _create_kb()

        if self.deferrable:
            self.log.info("Deferring for Knowledge base creation.")
            self.defer(
                trigger=BedrockKnowledgeBaseActiveTrigger(
                    knowledge_base_id=knowledge_base_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            self.log.info("Waiting for Knowledge Base creation.")
            self.hook.get_waiter("knowledge_base_active").wait(
                knowledgeBaseId=knowledge_base_id,
                WaiterConfig={
                    "Delay": self.waiter_delay,
                    "MaxAttempts": self.waiter_max_attempts,
                },
            )

        return knowledge_base_id


class BedrockCreateDataSourceOperator(AwsBaseOperator[BedrockAgentHook]):
    """
    Set up an Amazon Bedrock Data Source to be added to an Amazon Bedrock Knowledge Base.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockCreateDataSourceOperator`

    :param name: name for the Amazon Bedrock Data Source being created. (templated).
    :param bucket_name: The name of the Amazon S3 bucket to use for data source storage. (templated)
    :param knowledge_base_id: The unique identifier of the knowledge base to which to add the data source. (templated)
    :param create_data_source_kwargs: Any additional optional parameters to pass to the API call. (templated)

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

    aws_hook_class = BedrockAgentHook
    template_fields: Sequence[str] = aws_template_fields(
        "name",
        "bucket_name",
        "knowledge_base_id",
        "create_data_source_kwargs",
    )

    def __init__(
        self,
        name: str,
        knowledge_base_id: str,
        bucket_name: str | None = None,
        create_data_source_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name
        self.knowledge_base_id = knowledge_base_id
        self.bucket_name = bucket_name
        self.create_data_source_kwargs = create_data_source_kwargs or {}

    def execute(self, context: Context) -> str:
        create_ds_response = self.hook.conn.create_data_source(
            name=self.name,
            knowledgeBaseId=self.knowledge_base_id,
            dataSourceConfiguration={
                "type": "S3",
                "s3Configuration": {"bucketArn": f"arn:aws:s3:::{self.bucket_name}"},
            },
            **self.create_data_source_kwargs,
        )

        return create_ds_response["dataSource"]["dataSourceId"]


class BedrockIngestDataOperator(AwsBaseOperator[BedrockAgentHook]):
    """
    Begin an ingestion job, in which an Amazon Bedrock data source is added to an Amazon Bedrock knowledge base.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockIngestDataOperator`

    :param knowledge_base_id: The unique identifier of the knowledge base to which to add the data source. (templated)
    :param data_source_id: The unique identifier of the data source to ingest. (templated)
    :param ingest_data_kwargs: Any additional optional parameters to pass to the API call. (templated)

    :param wait_for_completion: Whether to wait for cluster to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 60)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 10)
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

    aws_hook_class = BedrockAgentHook
    template_fields: Sequence[str] = aws_template_fields(
        "knowledge_base_id",
        "data_source_id",
        "ingest_data_kwargs",
    )

    def __init__(
        self,
        knowledge_base_id: str,
        data_source_id: str,
        ingest_data_kwargs: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 10,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.knowledge_base_id = knowledge_base_id
        self.data_source_id = data_source_id
        self.ingest_data_kwargs = ingest_data_kwargs or {}

        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running ingestion job: {event}")

        self.log.info("Bedrock ingestion job `%s` complete.", event["ingestion_job_id"])

        return event["ingestion_job_id"]

    def execute(self, context: Context) -> str:
        ingestion_job_id = self.hook.conn.start_ingestion_job(
            knowledgeBaseId=self.knowledge_base_id, dataSourceId=self.data_source_id
        )["ingestionJob"]["ingestionJobId"]

        if self.deferrable:
            self.log.info("Deferring for ingestion job.")
            self.defer(
                trigger=BedrockIngestionJobTrigger(
                    knowledge_base_id=self.knowledge_base_id,
                    data_source_id=self.data_source_id,
                    ingestion_job_id=ingestion_job_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            self.log.info("Waiting for ingestion job %s", ingestion_job_id)
            self.hook.get_waiter(waiter_name="ingestion_job_complete").wait(
                knowledgeBaseId=self.knowledge_base_id,
                dataSourceId=self.data_source_id,
                ingestionJobId=ingestion_job_id,
            )

        return ingestion_job_id


class BedrockRaGOperator(AwsBaseOperator[BedrockAgentRuntimeHook]):
    """
    Query a knowledge base and generate responses based on the retrieved results with sources citations.

    NOTE:  Support for EXTERNAL SOURCES was added in botocore 1.34.90

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockRaGOperator`

    :param input: The query to be made to the knowledge base. (templated)
    :param source_type: The type of resource that is queried by the request. (templated)
        Must be one of 'KNOWLEDGE_BASE' or 'EXTERNAL_SOURCES', and the appropriate config values must also be provided.
        If set to 'KNOWLEDGE_BASE' then `knowledge_base_id` must be provided, and `vector_search_config` may be.
        If set to `EXTERNAL_SOURCES` then `sources` must also be provided.
        NOTE:  Support for EXTERNAL SOURCES was added in botocore 1.34.90
    :param model_arn: The ARN of the foundation model used to generate a response. (templated)
    :param prompt_template: The template for the prompt that's sent to the model for response generation.
        You can include prompt placeholders, which are replaced before the prompt is sent to the model
        to provide instructions and context to the model. In addition, you can include XML tags to delineate
        meaningful sections of the prompt template. (templated)
    :param knowledge_base_id: The unique identifier of the knowledge base that is queried. (templated)
            Can only be specified if source_type='KNOWLEDGE_BASE'.
    :param vector_search_config: How the results from the vector search should be returned. (templated)
        Can only be specified if source_type='KNOWLEDGE_BASE'.
        For more information, see https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html.
    :param sources: The documents used as reference for the response. (templated)
        Can only be specified if source_type='EXTERNAL_SOURCES'
        NOTE:  Support for EXTERNAL SOURCES was added in botocore 1.34.90
    :param rag_kwargs: Additional keyword arguments to pass to the  API call. (templated)
    """

    aws_hook_class = BedrockAgentRuntimeHook
    template_fields: Sequence[str] = aws_template_fields(
        "input",
        "source_type",
        "model_arn",
        "prompt_template",
        "knowledge_base_id",
        "vector_search_config",
        "sources",
        "rag_kwargs",
    )

    def __init__(
        self,
        input: str,
        source_type: str,
        model_arn: str,
        prompt_template: str | None = None,
        knowledge_base_id: str | None = None,
        vector_search_config: dict[str, Any] | None = None,
        sources: list[dict[str, Any]] | None = None,
        rag_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.input = input
        self.prompt_template = prompt_template
        self.source_type = source_type.upper()
        self.knowledge_base_id = knowledge_base_id
        self.model_arn = model_arn
        self.vector_search_config = vector_search_config
        self.sources = sources
        self.rag_kwargs = rag_kwargs or {}

    def validate_inputs(self):
        if self.source_type == "KNOWLEDGE_BASE":
            if self.knowledge_base_id is None:
                raise AttributeError(
                    "If `source_type` is set to 'KNOWLEDGE_BASE' then `knowledge_base_id` must be provided."
                )
            if self.sources is not None:
                raise AttributeError(
                    "`sources` can not be used when `source_type` is set to 'KNOWLEDGE_BASE'."
                )
        elif self.source_type == "EXTERNAL_SOURCES":
            if not self.sources is not None:
                raise AttributeError(
                    "If `source_type` is set to `EXTERNAL_SOURCES` then `sources` must also be provided."
                )
            if self.vector_search_config or self.knowledge_base_id:
                raise AttributeError(
                    "`vector_search_config` and `knowledge_base_id` can not be used "
                    "when `source_type` is set to `EXTERNAL_SOURCES`"
                )
        else:
            raise AttributeError(
                "`source_type` must be one of 'KNOWLEDGE_BASE' or 'EXTERNAL_SOURCES', "
                "and the appropriate config values must also be provided."
            )

    def build_rag_config(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        base_config: dict[str, Any] = {
            "modelArn": self.model_arn,
        }

        if self.prompt_template:
            base_config["generationConfiguration"] = {
                "promptTemplate": {"textPromptTemplate": self.prompt_template}
            }

        if self.source_type == "KNOWLEDGE_BASE":
            if self.vector_search_config:
                base_config["retrievalConfiguration"] = {
                    "vectorSearchConfiguration": self.vector_search_config
                }

            result = {
                "type": self.source_type,
                "knowledgeBaseConfiguration": {
                    **base_config,
                    "knowledgeBaseId": self.knowledge_base_id,
                },
            }

        if self.source_type == "EXTERNAL_SOURCES":
            result = {
                "type": self.source_type,
                "externalSourcesConfiguration": {**base_config, "sources": self.sources},
            }
        return result

    def execute(self, context: Context) -> Any:
        self.validate_inputs()

        result = self.hook.conn.retrieve_and_generate(
            input={"text": self.input},
            retrieveAndGenerateConfiguration=self.build_rag_config(),
            **self.rag_kwargs,
        )

        self.log.info(
            "\nPrompt: %s\nResponse: %s\nCitations: %s",
            self.input,
            result["output"]["text"],
            result["citations"],
        )
        return result


class BedrockRetrieveOperator(AwsBaseOperator[BedrockAgentRuntimeHook]):
    """
    Query a knowledge base and retrieve results with source citations.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockRetrieveOperator`

    :param retrieval_query: The query to be made to the knowledge base. (templated)
    :param knowledge_base_id: The unique identifier of the knowledge base that is queried. (templated)
    :param vector_search_config: How the results from the vector search should be returned. (templated)
        For more information, see https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html.
    :param retrieve_kwargs: Additional keyword arguments to pass to the  API call. (templated)
    """

    aws_hook_class = BedrockAgentRuntimeHook
    template_fields: Sequence[str] = aws_template_fields(
        "retrieval_query",
        "knowledge_base_id",
        "vector_search_config",
        "retrieve_kwargs",
    )

    def __init__(
        self,
        retrieval_query: str,
        knowledge_base_id: str,
        vector_search_config: dict[str, Any] | None = None,
        retrieve_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.retrieval_query = retrieval_query
        self.knowledge_base_id = knowledge_base_id
        self.vector_search_config = vector_search_config
        self.retrieve_kwargs = retrieve_kwargs or {}

    def execute(self, context: Context) -> Any:
        retrieval_configuration = (
            {
                "retrievalConfiguration": {
                    "vectorSearchConfiguration": self.vector_search_config
                }
            }
            if self.vector_search_config
            else {}
        )

        result = self.hook.conn.retrieve(
            retrievalQuery={"text": self.retrieval_query},
            knowledgeBaseId=self.knowledge_base_id,
            **retrieval_configuration,
            **self.retrieve_kwargs,
        )

        self.log.info(
            "\nQuery: %s\nRetrieved: %s", self.retrieval_query, result["retrievalResults"]
        )
        return result
