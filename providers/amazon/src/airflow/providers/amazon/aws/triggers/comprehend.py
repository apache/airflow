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

from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger


class ComprehendPiiEntitiesDetectionJobCompletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a Comprehend pii entities detection job is complete.

    :param job_id: The id of the Comprehend pii entities detection job.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 120)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: The AWS region where the resources to watch are.
    :param verify: Whether or not to verify SSL certificates.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = ComprehendHook

    def __init__(
        self,
        *,
        job_id: str,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={"job_id": job_id},
            waiter_name="pii_entities_detection_job_complete",
            waiter_args={"JobId": job_id},
            failure_message="Comprehend start pii entities detection job failed.",
            status_message="Status of Comprehend start pii entities detection job is",
            status_queries=["PiiEntitiesDetectionJobProperties.JobStatus"],
            return_key="job_id",
            return_value=job_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )


class ComprehendCreateDocumentClassifierCompletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a Comprehend document classifier is complete.

    :param document_classifier_arn: The arn of the Comprehend document classifier.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 120)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: The AWS region where the resources to watch are.
    :param verify: Whether or not to verify SSL certificates.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = ComprehendHook

    def __init__(
        self,
        *,
        document_classifier_arn: str,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={"document_classifier_arn": document_classifier_arn},
            waiter_name="create_document_classifier_complete",
            waiter_args={"DocumentClassifierArn": document_classifier_arn},
            failure_message="Comprehend create document classifier failed.",
            status_message="Status of Comprehend create document classifier is",
            status_queries=["DocumentClassifierProperties.Status"],
            return_key="document_classifier_arn",
            return_value=document_classifier_arn,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )
