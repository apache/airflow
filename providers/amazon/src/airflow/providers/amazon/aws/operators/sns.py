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
"""Publish message to SNS queue."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.sns import SnsHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.sdk import Context


class SnsPublishOperator(AwsBaseOperator[SnsHook]):
    """
    Publish a message to Amazon SNS.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SnsPublishOperator`

    :param target_arn: either a TopicArn or an EndpointArn
    :param message: the default message you want to send (templated)
    :param subject: the message subject you want to send (templated)
    :param message_attributes: the message attributes you want to send as a flat dict (data type will be
        determined automatically)
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
    :param message_deduplication_id: Every message must have a unique message_deduplication_id.
        This parameter applies only to FIFO (first-in-first-out) topics.
    :param message_group_id: Tag that specifies that a message belongs to a specific message group.
        This parameter applies only to FIFO (first-in-first-out) topics.
    """

    aws_hook_class = SnsHook
    template_fields: Sequence[str] = aws_template_fields(
        "target_arn",
        "message",
        "subject",
        "message_attributes",
        "message_deduplication_id",
        "message_group_id",
    )
    template_fields_renderers = {"message_attributes": "json"}

    def __init__(
        self,
        *,
        target_arn: str,
        message: str,
        subject: str | None = None,
        message_attributes: dict | None = None,
        message_deduplication_id: str | None = None,
        message_group_id: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_arn = target_arn
        self.message = message
        self.subject = subject
        self.message_attributes = message_attributes
        self.message_deduplication_id = message_deduplication_id
        self.message_group_id = message_group_id

    def execute(self, context: Context):
        self.log.info(
            "Sending SNS notification to %s using %s:\nsubject=%s\nattributes=%s\nmessage=%s\nmessage_deduplication_id=%s\nmessage_group_id=%s",
            self.target_arn,
            self.aws_conn_id,
            self.subject,
            self.message_attributes,
            self.message,
            self.message_deduplication_id,
            self.message_group_id,
        )

        return self.hook.publish_to_target(
            target_arn=self.target_arn,
            message=self.message,
            subject=self.subject,
            message_attributes=self.message_attributes,
            message_deduplication_id=self.message_deduplication_id,
            message_group_id=self.message_group_id,
        )
