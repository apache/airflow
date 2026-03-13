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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.links.comprehend import (
    ComprehendDocumentClassifierLink,
    ComprehendPiiEntitiesDetectionLink,
)
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.comprehend import (
    ComprehendCreateDocumentClassifierCompletedTrigger,
    ComprehendPiiEntitiesDetectionJobCompletedTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, conf, timezone

if TYPE_CHECKING:
    import boto3

    from airflow.sdk import Context


class ComprehendBaseOperator(AwsBaseOperator[ComprehendHook]):
    """
    This is the base operator for Comprehend Service operators (not supposed to be used directly in DAGs).

    :param input_data_config: The input properties for a PII entities detection job. (templated)
    :param output_data_config: Provides `conﬁguration` parameters for the output of PII entity detection
        jobs. (templated)
    :param data_access_role_arn: The Amazon Resource Name (ARN) of the IAM role that grants Amazon Comprehend
        read access to your input data. (templated)
    :param language_code: The language of the input documents. (templated)
    """

    aws_hook_class = ComprehendHook

    template_fields: Sequence[str] = aws_template_fields(
        "input_data_config", "output_data_config", "data_access_role_arn", "language_code"
    )

    template_fields_renderers: ClassVar[dict] = {"input_data_config": "json", "output_data_config": "json"}

    def __init__(
        self,
        input_data_config: dict,
        output_data_config: dict,
        data_access_role_arn: str,
        language_code: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.input_data_config = input_data_config
        self.output_data_config = output_data_config
        self.data_access_role_arn = data_access_role_arn
        self.language_code = language_code

    @cached_property
    def client(self) -> boto3.client:
        """Create and return the Comprehend client."""
        return self.hook.conn

    def execute(self, context: Context):
        """Must overwrite in child classes."""
        raise NotImplementedError("Please implement execute() in subclass")


class ComprehendStartPiiEntitiesDetectionJobOperator(ComprehendBaseOperator):
    """
    Create a comprehend pii entities detection job for a collection of documents.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComprehendStartPiiEntitiesDetectionJobOperator`

    :param input_data_config: The input properties for a PII entities detection job. (templated)
    :param output_data_config: Provides `conﬁguration` parameters for the output of PII entity detection
        jobs. (templated)
    :param mode: Specifies whether the output provides the locations (offsets) of PII  entities or a file in
        which PII entities are redacted. If you set the mode parameter to ONLY_REDACTION. In that case you
        must provide a RedactionConfig in start_pii_entities_kwargs.
    :param data_access_role_arn: The Amazon Resource Name (ARN) of the IAM role that grants Amazon Comprehend
        read access to your input data. (templated)
    :param language_code: The language of the input documents. (templated)
    :param start_pii_entities_kwargs: Any optional parameters to pass to the job. If JobName is not provided
        in start_pii_entities_kwargs, operator will create.

    :param wait_for_completion: Whether to wait for job to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 60)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 20)
    :param deferrable: If True, the operator will wait asynchronously for the job to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    operator_extra_links = (ComprehendPiiEntitiesDetectionLink(),)

    def __init__(
        self,
        input_data_config: dict,
        output_data_config: dict,
        mode: str,
        data_access_role_arn: str,
        language_code: str,
        start_pii_entities_kwargs: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(
            input_data_config=input_data_config,
            output_data_config=output_data_config,
            data_access_role_arn=data_access_role_arn,
            language_code=language_code,
            **kwargs,
        )
        self.mode = mode
        self.start_pii_entities_kwargs = start_pii_entities_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> str:
        if self.start_pii_entities_kwargs.get("JobName", None) is None:
            self.start_pii_entities_kwargs["JobName"] = (
                f"start_pii_entities_detection_job-{int(timezone.utcnow().timestamp())}"
            )

        self.log.info(
            "Submitting start pii entities detection job '%s'.", self.start_pii_entities_kwargs["JobName"]
        )
        job_id = self.client.start_pii_entities_detection_job(
            InputDataConfig=self.input_data_config,
            OutputDataConfig=self.output_data_config,
            Mode=self.mode,
            DataAccessRoleArn=self.data_access_role_arn,
            LanguageCode=self.language_code,
            **self.start_pii_entities_kwargs,
        )["JobId"]

        job_url = ComprehendPiiEntitiesDetectionLink.format_str.format(
            aws_domain=ComprehendPiiEntitiesDetectionLink.get_aws_domain(self.hook.conn_partition),
            region_name=self.hook.conn_region_name,
            job_id=job_id,
        )
        ComprehendPiiEntitiesDetectionLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            job_id=job_id,
        )
        self.log.info("You can view the PII entities detection job at %s", job_url)
        message_description = f"start pii entities detection job {job_id} to complete."
        if self.deferrable:
            self.log.info("Deferring %s", message_description)
            self.defer(
                trigger=ComprehendPiiEntitiesDetectionJobCompletedTrigger(
                    job_id=job_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            self.log.info("Waiting for %s", message_description)
            self.hook.get_waiter("pii_entities_detection_job_complete").wait(
                JobId=job_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return job_id

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        validated_event = validate_execute_complete_event(event)
        if validated_event["status"] != "success":
            raise AirflowException("Error while running job: %s", validated_event)

        self.log.info("Comprehend pii entities detection job `%s` complete.", validated_event["job_id"])
        return validated_event["job_id"]


class ComprehendCreateDocumentClassifierOperator(AwsBaseOperator[ComprehendHook]):
    """
    Create a comprehend document classifier that can categorize documents.

    Provide a set of training documents that are labeled with the categories.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComprehendCreateDocumentClassifierOperator`

    :param document_classifier_name: The name of the document classifier. (templated)
    :param input_data_config: Specifies the format and location of the input data for the job. (templated)
    :param mode: Indicates the mode in which the classifier will be trained. (templated)
    :param data_access_role_arn: The Amazon Resource Name (ARN) of the IAM role that grants Amazon Comprehend
        read access to your input data. (templated)
    :param language_code: The language of the input documents. You can specify any of the languages supported by
        Amazon Comprehend. All documents must be in the same language. (templated)
    :param fail_on_warnings: If set to True, the document classifier training job will throw an error when the
        status is TRAINED_WITH_WARNING. (default False)
    :param output_data_config: Specifies the location for the output files from a custom classifier job.
        This parameter is required for a request that creates a native document model. (templated)
    :param document_classifier_kwargs: Any optional parameters to pass to the document classifier. (templated)

    :param wait_for_completion: Whether to wait for job to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 60)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 20)
    :param deferrable: If True, the operator will wait asynchronously for the job to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = ComprehendHook
    operator_extra_links = (ComprehendDocumentClassifierLink(),)

    template_fields: Sequence[str] = aws_template_fields(
        "document_classifier_name",
        "input_data_config",
        "mode",
        "data_access_role_arn",
        "language_code",
        "output_data_config",
        "document_classifier_kwargs",
    )

    template_fields_renderers: ClassVar[dict] = {
        "input_data_config": "json",
        "output_data_config": "json",
        "document_classifier_kwargs": "json",
    }

    def __init__(
        self,
        document_classifier_name: str,
        input_data_config: dict[str, Any],
        mode: str,
        data_access_role_arn: str,
        language_code: str,
        fail_on_warnings: bool = False,
        output_data_config: dict[str, Any] | None = None,
        document_classifier_kwargs: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.document_classifier_name = document_classifier_name
        self.input_data_config = input_data_config
        self.mode = mode
        self.data_access_role_arn = data_access_role_arn
        self.language_code = language_code
        self.fail_on_warnings = fail_on_warnings
        self.output_data_config = output_data_config
        self.document_classifier_kwargs = document_classifier_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> str:
        if self.output_data_config:
            self.document_classifier_kwargs["OutputDataConfig"] = self.output_data_config

        document_classifier_arn = self.hook.conn.create_document_classifier(
            DocumentClassifierName=self.document_classifier_name,
            InputDataConfig=self.input_data_config,
            Mode=self.mode,
            DataAccessRoleArn=self.data_access_role_arn,
            LanguageCode=self.language_code,
            **self.document_classifier_kwargs,
        )["DocumentClassifierArn"]

        # create the link to console
        job_url = ComprehendDocumentClassifierLink.format_str.format(
            aws_domain=ComprehendDocumentClassifierLink.get_aws_domain(self.hook.conn_partition),
            region_name=self.hook.conn_region_name,
            arn=document_classifier_arn,
        )

        ComprehendDocumentClassifierLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            arn=document_classifier_arn,
        )
        self.log.info("You can monitor the classifier at %s", job_url)

        message_description = f"document classifier {document_classifier_arn} to complete."
        if self.deferrable:
            self.log.info("Deferring %s", message_description)
            self.defer(
                trigger=ComprehendCreateDocumentClassifierCompletedTrigger(
                    document_classifier_arn=document_classifier_arn,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            self.log.info("Waiting for %s", message_description)

            self.hook.get_waiter("create_document_classifier_complete").wait(
                DocumentClassifierArn=document_classifier_arn,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

            self.hook.validate_document_classifier_training_status(
                document_classifier_arn=document_classifier_arn, fail_on_warnings=self.fail_on_warnings
            )

        return document_classifier_arn

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        validated_event = validate_execute_complete_event(event)
        if validated_event["status"] != "success":
            raise AirflowException(
                "Error while running comprehend create document classifier: %s", validated_event
            )

        self.hook.validate_document_classifier_training_status(
            document_classifier_arn=validated_event["document_classifier_arn"],
            fail_on_warnings=self.fail_on_warnings,
        )

        self.log.info(
            "Comprehend document classifier `%s` complete.", validated_event["document_classifier_arn"]
        )

        return validated_event["document_classifier_arn"]
