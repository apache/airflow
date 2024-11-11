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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentHook, BedrockHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class BedrockCustomizeModelCompletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a Bedrock model customization job is complete.

    :param job_name: The name of the Bedrock model customization job.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 120)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        job_name: str,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={"job_name": job_name},
            waiter_name="model_customization_job_complete",
            waiter_args={"jobIdentifier": job_name},
            failure_message="Bedrock model customization failed.",
            status_message="Status of Bedrock model customization job is",
            status_queries=["status"],
            return_key="job_name",
            return_value=job_name,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return BedrockHook(aws_conn_id=self.aws_conn_id)


class BedrockKnowledgeBaseActiveTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a Bedrock Knowledge Base reaches the ACTIVE state.

    :param knowledge_base_id: The unique identifier of the knowledge base for which to get information.

    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 5)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 24)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        knowledge_base_id: str,
        waiter_delay: int = 5,
        waiter_max_attempts: int = 24,
        aws_conn_id: str | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={"knowledge_base_id": knowledge_base_id},
            waiter_name="knowledge_base_active",
            waiter_args={"knowledgeBaseId": knowledge_base_id},
            failure_message="Bedrock Knowledge Base creation failed.",
            status_message="Status of Bedrock Knowledge Base job is",
            status_queries=["status"],
            return_key="knowledge_base_id",
            return_value=knowledge_base_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return BedrockAgentHook(aws_conn_id=self.aws_conn_id)


class BedrockProvisionModelThroughputCompletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a provisioned throughput job is complete.

    :param provisioned_model_id: The ARN or name of the provisioned throughput.

    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 120)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        provisioned_model_id: str,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={"provisioned_model_id": provisioned_model_id},
            waiter_name="provisioned_model_throughput_complete",
            waiter_args={"provisionedModelId": provisioned_model_id},
            failure_message="Bedrock provisioned throughput job failed.",
            status_message="Status of Bedrock provisioned throughput job is",
            status_queries=["status"],
            return_key="provisioned_model_id",
            return_value=provisioned_model_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return BedrockHook(aws_conn_id=self.aws_conn_id)


class BedrockIngestionJobTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a Bedrock ingestion job reaches the COMPLETE state.

    :param knowledge_base_id: The unique identifier of the knowledge base for which to get information.
    :param data_source_id: The unique identifier of the data source in the ingestion job.
    :param ingestion_job_id: The unique identifier of the ingestion job.

    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 60)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 10)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        knowledge_base_id: str,
        data_source_id: str,
        ingestion_job_id: str,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 10,
        aws_conn_id: str | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={
                "knowledge_base_id": knowledge_base_id,
                "data_source_id": data_source_id,
                "ingestion_job_id": ingestion_job_id,
            },
            waiter_name="ingestion_job_complete",
            waiter_args={
                "knowledgeBaseId": knowledge_base_id,
                "dataSourceId": data_source_id,
                "ingestionJobId": ingestion_job_id,
            },
            failure_message="Bedrock ingestion job creation failed.",
            status_message="Status of Bedrock ingestion job is",
            status_queries=["status"],
            return_key="ingestion_job_id",
            return_value=ingestion_job_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return BedrockAgentHook(aws_conn_id=self.aws_conn_id)
