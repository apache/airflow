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
"""Publish message to SQS queue"""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.sqs import SqsHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SqsPublishOperator(BaseOperator):
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
    :param aws_conn_id: AWS connection id (default: aws_default)
    """

    template_fields: Sequence[str] = (
        "sqs_queue",
        "message_content",
        "delay_seconds",
        "message_attributes",
        "message_group_id",
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
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sqs_queue = sqs_queue
        self.aws_conn_id = aws_conn_id
        self.message_content = message_content
        self.delay_seconds = delay_seconds
        self.message_attributes = message_attributes or {}
        self.message_group_id = message_group_id

    def execute(self, context: Context) -> dict:
        """
        Publish the message to the Amazon SQS queue

        :param context: the context object
        :return: dict with information about the message sent
            For details of the returned dict see :py:meth:`botocore.client.SQS.send_message`
        """
        hook = SqsHook(aws_conn_id=self.aws_conn_id)

        result = hook.send_message(
            queue_url=self.sqs_queue,
            message_body=self.message_content,
            delay_seconds=self.delay_seconds,
            message_attributes=self.message_attributes,
            message_group_id=self.message_group_id,
        )

        self.log.info("send_message result: %s", result)

        return result
