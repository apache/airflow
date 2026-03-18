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

from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.common.compat.notifier import BaseNotifier


class SqsNotifier(BaseNotifier):
    """
    Amazon SQS (Simple Queue Service) Notifier.

    .. seealso::
        For more information on how to use this notifier, take a look at the guide:
        :ref:`howto/notifier:SqsNotifier`

    :param aws_conn_id: The :ref:`Amazon Web Services Connection id <howto/connection:aws>`
        used for AWS credentials. If this is None or empty then the default boto3 behaviour is used.
    :param queue_url: The URL of the Amazon SQS queue to which a message is sent.
    :param message_body: The message to send.
    :param message_attributes: additional attributes for the message.
        For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`.
    :param message_group_id: This parameter applies only to FIFO (first-in-first-out) queues.
        For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`.
    :param delay_seconds: The length of time, in seconds, for which to delay a message.
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    """

    template_fields: Sequence[str] = (
        "queue_url",
        "message_body",
        "message_attributes",
        "message_group_id",
        "delay_seconds",
        "aws_conn_id",
        "region_name",
    )

    def __init__(
        self,
        *,
        aws_conn_id: str | None = SqsHook.default_conn_name,
        queue_url: str,
        message_body: str,
        message_attributes: dict | None = None,
        message_group_id: str | None = None,
        delay_seconds: int = 0,
        region_name: str | None = None,
        **kwargs,
    ):
        if AIRFLOW_V_3_1_PLUS:
            #  Support for passing context was added in 3.1.0
            super().__init__(**kwargs)
        else:
            super().__init__()
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.queue_url = queue_url
        self.message_body = message_body
        self.message_attributes = message_attributes or {}
        self.message_group_id = message_group_id
        self.delay_seconds = delay_seconds

    @cached_property
    def hook(self) -> SqsHook:
        """Amazon SQS Hook (cached)."""
        return SqsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def notify(self, context):
        """Publish the notification message to Amazon SQS queue."""
        self.hook.send_message(
            queue_url=self.queue_url,
            message_body=self.message_body,
            delay_seconds=self.delay_seconds,
            message_attributes=self.message_attributes,
            message_group_id=self.message_group_id,
        )

    async def async_notify(self, context):
        """Publish the notification message to Amazon SQS queue (async)."""
        await self.hook.asend_message(
            queue_url=self.queue_url,
            message_body=self.message_body,
            delay_seconds=self.delay_seconds,
            message_attributes=self.message_attributes,
            message_group_id=self.message_group_id,
        )


send_sqs_notification = SqsNotifier
