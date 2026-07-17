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
"""Publish message to SQS queue."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.sdk import Context


class SqsPublishOperator(AwsBaseOperator[SqsHook]):
    """
    Publish a message to an Amazon SQS queue.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SqsPublishOperator`

    :param sqs_queue: The SQS queue url (templated)
    :param message_content: The message content (templated)
    :param message_attributes: additional attributes for the message (default: None)
        For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
    :param delay_seconds: message delay (templated) (default: 1 second)
    :param message_group_id: This parameter applies only to FIFO (first-in-first-out) queues. (default: None)
        For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
    :param message_deduplication_id: This applies only to FIFO (first-in-first-out) queues.
        For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
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

    aws_hook_class = SqsHook
    template_fields: Sequence[str] = aws_template_fields(
        "sqs_queue",
        "message_content",
        "delay_seconds",
        "message_attributes",
        "message_group_id",
        "message_deduplication_id",
    )
    template_fields_renderers = {"message_attributes": "json"}
    ui_color = "#6ad3fa"

    def __init__(
        self,
        *,
        sqs_queue: str,
        message_content: str,
        message_attributes: dict | None = None,
        delay_seconds: int = 0,
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sqs_queue = sqs_queue
        self.message_content = message_content
        self.delay_seconds = delay_seconds
        self.message_attributes = message_attributes or {}
        self.message_group_id = message_group_id
        self.message_deduplication_id = message_deduplication_id

    def execute(self, context: Context) -> dict:
        """
        Publish the message to the Amazon SQS queue.

        :param context: the context object
        :return: dict with information about the message sent
            For details of the returned dict see :py:meth:`botocore.client.SQS.send_message`
        """
        result = self.hook.send_message(
            queue_url=self.sqs_queue,
            message_body=self.message_content,
            delay_seconds=self.delay_seconds,
            message_attributes=self.message_attributes,
            message_group_id=self.message_group_id,
            message_deduplication_id=self.message_deduplication_id,
        )

        self.log.info("send_message result: %s", result)

        return result
