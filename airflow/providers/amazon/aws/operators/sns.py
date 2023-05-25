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
"""Publish message to SNS queue"""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.sns import SnsHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SnsPublishOperator(BaseOperator):
    """
    Publish a message to Amazon SNS.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SnsPublishOperator`

    :param aws_conn_id: aws connection to use
    :param target_arn: either a TopicArn or an EndpointArn
    :param message: the default message you want to send (templated)
    :param subject: the message subject you want to send (templated)
    :param message_attributes: the message attributes you want to send as a flat dict (data type will be
        determined automatically)
    """

    template_fields: Sequence[str] = ("target_arn", "message", "subject", "message_attributes", "aws_conn_id")
    template_ext: Sequence[str] = ()
    template_fields_renderers = {"message_attributes": "json"}

    def __init__(
        self,
        *,
        target_arn: str,
        message: str,
        subject: str | None = None,
        message_attributes: dict | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_arn = target_arn
        self.message = message
        self.subject = subject
        self.message_attributes = message_attributes
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        sns = SnsHook(aws_conn_id=self.aws_conn_id)

        self.log.info(
            "Sending SNS notification to %s using %s:\nsubject=%s\nattributes=%s\nmessage=%s",
            self.target_arn,
            self.aws_conn_id,
            self.subject,
            self.message_attributes,
            self.message,
        )

        return sns.publish_to_target(
            target_arn=self.target_arn,
            message=self.message,
            subject=self.subject,
            message_attributes=self.message_attributes,
        )
