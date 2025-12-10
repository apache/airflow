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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.common.compat.sdk import AirflowException


class ComprehendHook(AwsBaseHook):
    """
    Interact with AWS Comprehend.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("comprehend") <Comprehend.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "comprehend"
        super().__init__(*args, **kwargs)

    def validate_document_classifier_training_status(
        self, document_classifier_arn: str, fail_on_warnings: bool = False
    ) -> None:
        """
        Log the Information about the document classifier.

        NumberOfLabels
        NumberOfTrainedDocuments
        NumberOfTestDocuments
        EvaluationMetrics

        """
        response = self.conn.describe_document_classifier(DocumentClassifierArn=document_classifier_arn)

        status = response["DocumentClassifierProperties"]["Status"]

        if status == "TRAINED_WITH_WARNING":
            self.log.info(
                "AWS Comprehend document classifier training completed with %s, Message: %s please review the skipped files folder in the output location %s",
                status,
                response["DocumentClassifierProperties"]["Message"],
                response["DocumentClassifierProperties"]["OutputDataConfig"]["S3Uri"],
            )

            if fail_on_warnings:
                raise AirflowException("Warnings in AWS Comprehend document classifier training.")

        self.log.info(
            "AWS Comprehend document classifier metadata: %s",
            response["DocumentClassifierProperties"]["ClassifierMetadata"],
        )
