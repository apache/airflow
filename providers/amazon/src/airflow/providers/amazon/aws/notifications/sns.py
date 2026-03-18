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

from airflow.providers.amazon.aws.hooks.sns import SnsHook
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.common.compat.notifier import BaseNotifier


class SnsNotifier(BaseNotifier):
    """
    Amazon SNS (Simple Notification Service) Notifier.

    .. seealso::
        For more information on how to use this notifier, take a look at the guide:
        :ref:`howto/notifier:SnsNotifier`

    :param aws_conn_id: The :ref:`Amazon Web Services Connection id <howto/connection:aws>`
        used for AWS credentials. If this is None or empty then the default boto3 behaviour is used.
    :param target_arn: Either a TopicArn or an EndpointArn.
    :param message: The message you want to send.
    :param subject: The message subject you want to send.
    :param message_attributes: The message attributes you want to send as a flat dict (data type will be
        determined automatically).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    """

    template_fields: Sequence[str] = (
        "target_arn",
        "message",
        "subject",
        "message_attributes",
        "aws_conn_id",
        "region_name",
    )

    def __init__(
        self,
        *,
        aws_conn_id: str | None = SnsHook.default_conn_name,
        target_arn: str,
        message: str,
        subject: str | None = None,
        message_attributes: dict | None = None,
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
        self.target_arn = target_arn
        self.message = message
        self.subject = subject
        self.message_attributes = message_attributes

    @cached_property
    def hook(self) -> SnsHook:
        """Amazon SNS Hook (cached)."""
        return SnsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def notify(self, context):
        """Publish the notification message to Amazon SNS."""
        self.hook.publish_to_target(
            target_arn=self.target_arn,
            message=self.message,
            subject=self.subject,
            message_attributes=self.message_attributes,
        )

    async def async_notify(self, context):
        """Publish the notification message to Amazon SNS (async)."""
        await self.hook.apublish_to_target(
            target_arn=self.target_arn,
            message=self.message,
            subject=self.subject,
            message_attributes=self.message_attributes,
        )


send_sns_notification = SnsNotifier
