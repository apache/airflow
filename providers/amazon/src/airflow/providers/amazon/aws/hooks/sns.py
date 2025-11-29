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
"""This module contains AWS SNS hook."""

from __future__ import annotations

import json

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.helpers import prune_dict


def _get_message_attribute(o):
    if isinstance(o, bytes):
        return {"DataType": "Binary", "BinaryValue": o}
    if isinstance(o, str):
        return {"DataType": "String", "StringValue": o}
    if isinstance(o, (int, float)):
        return {"DataType": "Number", "StringValue": str(o)}
    if hasattr(o, "__iter__"):
        return {"DataType": "String.Array", "StringValue": json.dumps(o)}
    raise TypeError(
        f"Values in MessageAttributes must be one of bytes, str, int, float, or iterable; got {type(o)}"
    )


def _build_publish_kwargs(
    target_arn: str,
    message: str,
    subject: str | None = None,
    message_attributes: dict | None = None,
    message_deduplication_id: str | None = None,
    message_group_id: str | None = None,
) -> dict[str, str | dict]:
    publish_kwargs: dict[str, str | dict] = prune_dict(
        {
            "TargetArn": target_arn,
            "MessageStructure": "json",
            "Message": json.dumps({"default": message}),
            "Subject": subject,
            "MessageDeduplicationId": message_deduplication_id,
            "MessageGroupId": message_group_id,
        }
    )

    if message_attributes:
        publish_kwargs["MessageAttributes"] = {
            key: _get_message_attribute(val) for key, val in message_attributes.items()
        }

    return publish_kwargs


class SnsHook(AwsBaseHook):
    """
    Interact with Amazon Simple Notification Service.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("sns") <SNS.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type="sns", *args, **kwargs)

    def publish_to_target(
        self,
        target_arn: str,
        message: str,
        subject: str | None = None,
        message_attributes: dict | None = None,
        message_deduplication_id: str | None = None,
        message_group_id: str | None = None,
    ):
        """
        Publish a message to a SNS topic or an endpoint.

        .. seealso::
            - :external+boto3:py:meth:`SNS.Client.publish`

        :param target_arn: either a TopicArn or an EndpointArn
        :param message: the default message you want to send
        :param subject: subject of message
        :param message_attributes: additional attributes to publish for message filtering. This should be
            a flat dict; the DataType to be sent depends on the type of the value:

            - bytes = Binary
            - str = String
            - int, float = Number
            - iterable = String.Array
        :param message_deduplication_id: Every message must have a unique message_deduplication_id.
            This parameter applies only to FIFO (first-in-first-out) topics.
        :param message_group_id: Tag that specifies that a message belongs to a specific message group.
            This parameter applies only to FIFO (first-in-first-out) topics.
        """
        return self.get_conn().publish(
            **_build_publish_kwargs(
                target_arn, message, subject, message_attributes, message_deduplication_id, message_group_id
            )
        )

    async def apublish_to_target(
        self,
        target_arn: str,
        message: str,
        subject: str | None = None,
        message_attributes: dict | None = None,
        message_deduplication_id: str | None = None,
        message_group_id: str | None = None,
    ):
        """
        Publish a message to a SNS topic or an endpoint.

        .. seealso::
            - :external+boto3:py:meth:`SNS.Client.publish`

        :param target_arn: either a TopicArn or an EndpointArn
        :param message: the default message you want to send
        :param subject: subject of message
        :param message_attributes: additional attributes to publish for message filtering. This should be
            a flat dict; the DataType to be sent depends on the type of the value:

            - bytes = Binary
            - str = String
            - int, float = Number
            - iterable = String.Array
        :param message_deduplication_id: Every message must have a unique message_deduplication_id.
            This parameter applies only to FIFO (first-in-first-out) topics.
        :param message_group_id: Tag that specifies that a message belongs to a specific message group.
            This parameter applies only to FIFO (first-in-first-out) topics.
        """
        async with await self.get_async_conn() as async_client:
            return await async_client.publish(
                **_build_publish_kwargs(
                    target_arn,
                    message,
                    subject,
                    message_attributes,
                    message_deduplication_id,
                    message_group_id,
                )
            )
