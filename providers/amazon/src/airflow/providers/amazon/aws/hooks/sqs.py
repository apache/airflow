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
"""This module contains AWS SQS hook."""

from __future__ import annotations

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.helpers import prune_dict


class SqsHook(AwsBaseHook):
    """
    Interact with Amazon Simple Queue Service.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("sqs") <SQS.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "sqs"
        super().__init__(*args, **kwargs)

    def create_queue(self, queue_name: str, attributes: dict | None = None) -> dict:
        """
        Create queue using connection object.

        .. seealso::
            - :external+boto3:py:meth:`SQS.Client.create_queue`

        :param queue_name: name of the queue.
        :param attributes: additional attributes for the queue (default: None)
        :return: dict with the information about the queue.
        """
        return self.get_conn().create_queue(QueueName=queue_name, Attributes=attributes or {})

    @staticmethod
    def _build_msg_params(
        queue_url: str,
        message_body: str,
        delay_seconds: int = 0,
        message_attributes: dict | None = None,
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
    ) -> dict:
        return prune_dict(
            {
                "QueueUrl": queue_url,
                "MessageBody": message_body,
                "DelaySeconds": delay_seconds,
                "MessageAttributes": message_attributes or {},
                "MessageGroupId": message_group_id,
                "MessageDeduplicationId": message_deduplication_id,
            }
        )

    def send_message(
        self,
        queue_url: str,
        message_body: str,
        delay_seconds: int = 0,
        message_attributes: dict | None = None,
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
    ) -> dict:
        """
        Send message to the queue.

        .. seealso::
            - :external+boto3:py:meth:`SQS.Client.send_message`

        :param queue_url: queue url
        :param message_body: the contents of the message
        :param delay_seconds: seconds to delay the message
        :param message_attributes: additional attributes for the message (default: None)
        :param message_group_id: This applies only to FIFO (first-in-first-out) queues. (default: None)
        :param message_deduplication_id: This applies only to FIFO (first-in-first-out) queues.
        :return: dict with the information about the message sent
        """
        params = self._build_msg_params(
            queue_url=queue_url,
            message_body=message_body,
            delay_seconds=delay_seconds,
            message_attributes=message_attributes,
            message_group_id=message_group_id,
            message_deduplication_id=message_deduplication_id,
        )
        return self.get_conn().send_message(**params)

    async def asend_message(
        self,
        queue_url: str,
        message_body: str,
        delay_seconds: int = 0,
        message_attributes: dict | None = None,
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
    ) -> dict:
        """
        Send message to the queue (async).

        .. seealso::
            - :external+boto3:py:meth:`SQS.Client.send_message`

        :param queue_url: queue url
        :param message_body: the contents of the message
        :param delay_seconds: seconds to delay the message
        :param message_attributes: additional attributes for the message (default: None)
        :param message_group_id: This applies only to FIFO (first-in-first-out) queues. (default: None)
        :param message_deduplication_id: This applies only to FIFO (first-in-first-out) queues.
        :return: dict with the information about the message sent
        """
        params = self._build_msg_params(
            queue_url=queue_url,
            message_body=message_body,
            delay_seconds=delay_seconds,
            message_attributes=message_attributes,
            message_group_id=message_group_id,
            message_deduplication_id=message_deduplication_id,
        )

        async with await self.get_async_conn() as async_conn:
            return await async_conn.send_message(**params)
